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
pub(crate) struct Evictor<K> {
    pub(crate) max_entries: usize,
    pub(crate) max_memory: Option<usize>,
    pub(crate) max_old: Option<Duration>,

    pub(crate) cur_entries: Arc<AtomicUsize>,
    pub(crate) cur_memory: Arc<AtomicUsize>,
    pub(crate) n_evicted: usize,
    pub(crate) n_deleted: usize,
    pub(crate) n_older: usize,

    pub(crate) list: Arc<list::List<K>>,
    pub(crate) closed: Arc<AtomicBool>,
}

impl<K> Evictor<K>
where
    K: Clone + PartialEq + Hash,
{
    pub fn run<V, H>(mut self, mut map: cmap::Map<K, Value<K, V>, H>) -> Result<Self>
    where
        V: Clone,
        H: BuildHasher,
    {
        let mut remove = |key: &K| match map.remove(key) {
            Some(value) => {
                self.cur_entries.fetch_sub(1, SeqCst);
                unsafe {
                    let ptr = value.access.load(SeqCst);
                    ptr.as_ref().unwrap().delete()
                };
            }
            None => (),
        };

        let mut n_evicted: usize = 0;
        let mut n_deleted: usize = 0;
        let mut n_older: usize = 0;

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

            let mut num_evicts = self.num_evicts();
            let mut counts = 0;
            loop {
                let (key, born, deleted, next) = match node {
                    list::Node::Z => break,
                    list::Node::T {
                        key,
                        born,
                        deleted,
                        next,
                    } => (key, born, deleted, next),
                };

                let node_next: Box<list::Node<K>> = match self.max_old {
                    _ if deleted.load(SeqCst) => {
                        n_deleted += 1;
                        next.take().unwrap()
                    }
                    _ if counts > self.max_entries && num_evicts > 0 => {
                        remove(key);
                        n_older += 1;
                        num_evicts -= 1;
                        next.take().unwrap()
                    }
                    Some(max_old) if (now - *born) > max_old => {
                        remove(key);
                        n_older += 1;
                        next.take().unwrap()
                    }
                    _ => {
                        node = next.as_mut().unwrap();
                        counts += 1;
                        continue;
                    }
                };

                n_evicted += 1;

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

        self.n_evicted = n_evicted;
        self.n_deleted = n_deleted;
        self.n_older = n_older;

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

    fn num_evicts(&self) -> usize {
        let a = self.cur_entries.load(SeqCst);
        if self.max_entries < a {
            a - self.max_entries
        } else {
            0
        }
    }
}
