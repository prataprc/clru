use std::hash::{BuildHasher, Hash};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use crate::{list, Error, Result, Value};

const MAX_SLEEP: f64 = 10.0; // in millisecons

/// Evictor will remove an access node if,
/// * Node is marked as deleted.
/// * Node is older than configured elapsed time, optional.
/// * Number of nodes in the access list exceed the count-limit, `max_entries`.
/// * Memory footprint of cache exceeds size-limit, `max_memory`.
pub(crate) struct Evictor<K, V, H> {
    pub(crate) max_entries: usize,
    pub(crate) max_memory: Option<usize>,
    pub(crate) max_old: Option<Duration>,

    pub(crate) cur_entries: Arc<AtomicUsize>,
    pub(crate) cur_memory: Arc<AtomicUsize>,
    pub(crate) n_evicted: usize,
    pub(crate) n_deleted: usize,
    pub(crate) n_older: usize,
    pub(crate) n_removed: usize,

    pub(crate) map: cmap::Map<K, Value<K, V>, H>,
    pub(crate) list: Arc<list::List<K>>,
    pub(crate) closed: Arc<AtomicBool>,
}

impl<K, V, H> Evictor<K, V, H>
where
    K: Clone + PartialEq + Hash,
    V: Clone,
    H: BuildHasher,
{
    pub fn run(mut self) -> Result<Self> {
        let max_old = self.max_old.as_ref();

        loop {
            if self.closed.load(SeqCst) {
                break;
            }

            match self.sleep_for() {
                Some(dur) => std::thread::sleep(dur),
                None => std::thread::yield_now(),
            }

            let prev_node: &mut list::Node<K> = match self.list.as_mut_head() {
                Some(node) => node,
                None => continue,
            };
            let mut node: &mut list::Node<K> = match prev_node {
                list::Node::T { next, .. } => next.as_mut().unwrap(),
                _ => unreachable!(),
            };

            let now = err_at!(Fatal, UNIX_EPOCH.elapsed())?;

            let mut to_evict = {
                let a = self.cur_entries.load(SeqCst);
                if self.max_entries < a {
                    a - self.max_entries
                } else {
                    0
                }
            };
            loop {
                let node_next: Box<list::Node<K>> = match (node, max_old) {
                    (list::Node::Z, _) => break,
                    (list::Node::T { deleted, next, .. }, _) if deleted.load(SeqCst) => {
                        self.n_deleted += 1;
                        next.take().unwrap()
                    }
                    (
                        list::Node::T {
                            key, born, next, ..
                        },
                        Some(max_old),
                    ) if &(now - *born) > max_old => {
                        self.n_older += 1;
                        match self.map.remove(key) {
                            Some(value) => {
                                self.n_removed += 1;
                                self.cur_entries.fetch_sub(1, SeqCst);
                                unsafe {
                                    let ptr = value.access.load(SeqCst);
                                    ptr.as_ref().unwrap().delete()
                                };
                            }
                            None => (),
                        }
                        next.take().unwrap()
                    }
                    (list::Node::T { key, next, .. }, _) if to_evict > 0 => {
                        match self.map.remove(key) {
                            Some(value) => {
                                self.n_removed += 1;
                                self.cur_entries.fetch_sub(1, SeqCst);
                                unsafe {
                                    let ptr = value.access.load(SeqCst);
                                    ptr.as_ref().unwrap().delete()
                                };
                            }
                            None => (),
                        }
                        next.take().unwrap()
                    }
                    (list::Node::T { next, .. }, _) => {
                        node = next.as_mut().unwrap();
                        continue;
                    }
                };

                self.n_evicted += 1;
                to_evict -= 1;

                let _drop_node = match prev_node {
                    list::Node::T { next, .. } => next.replace(node_next),
                    _ => unreachable!(),
                };

                node = match prev_node {
                    list::Node::T { next, .. } => next.as_mut().unwrap(),
                    _ => unreachable!(),
                }
            }
        }

        Ok(self)
    }

    fn sleep_for(&self) -> Option<Duration> {
        use std::cmp::Ordering;

        let entries = self.cur_entries.load(SeqCst);
        let memory = self.cur_memory.load(SeqCst);

        let ratio1 = (entries as f64) / (self.max_entries as f64);
        let ratio2 = match self.max_memory.clone() {
            Some(max_memory) => (memory as f64) / (max_memory as f64),
            None => 0.0,
        };

        let ratio = match ratio1.total_cmp(&ratio2) {
            Ordering::Less => ratio2,
            _ => ratio1,
        };

        match ratio {
            ratio if ratio >= 1.0 => None,
            ratio => Some(Duration::from_millis((MAX_SLEEP * (1.0 - ratio)) as u64)),
        }
    }
}
