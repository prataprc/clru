use cmap::Map;

use std::{
    hash::{BuildHasher, Hash},
    sync::{atomic::Ordering::SeqCst, Arc, Mutex},
    time::{self, Duration},
};

use crate::access::Access;

pub fn evictor<K, V, H>(
    max_count: usize,
    max_old: Duration,
    mut map: Map<K, V, H>,
    close: Arc<Mutex<bool>>,
    head: Arc<Access<K>>,
) where
    K: Clone + PartialEq + Hash,
    V: Clone,
    H: BuildHasher,
{
    loop {
        if *close.lock().unwrap() {
            break;
        }

        // initialize vars for this iteration.
        let mut count = 0;
        let mut evict = false;
        let epoch = time::UNIX_EPOCH.elapsed().unwrap() - max_old;
        // skip the sentinel.
        let mut node: &mut Access<K> = match head.as_ref() {
            Access::S { next } => unsafe { next.load(SeqCst).as_mut().unwrap() },
            _ => unreachable!(),
        };
        // iterate on the access-list.
        loop {
            evict = evict || count > max_count;
            node = match *node.take_next() {
                Access::T { next, deleted, .. } if deleted.load(SeqCst) => {
                    node.set_next(next.unwrap());
                    node.get_next_mut()
                }
                Access::T {
                    key, born, next, ..
                } if evict || born < epoch => {
                    map.remove(&key);
                    node.set_next(next.unwrap());
                    node.get_next_mut()
                }
                Access::T { .. } => {
                    count += 1;
                    node.get_next_mut()
                }
                Access::N => break,
                _ => unreachable!(),
            }
        }
    }

    let _node: Box<Access<K>> = match head.as_ref() {
        Access::S { next } => unsafe { Box::from_raw(next.load(SeqCst)) },
        _ => unreachable!(),
    };
    // _node drop the entire chain of access list.
}