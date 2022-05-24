use log::debug;

use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering::SeqCst};
use std::time::{self, Duration};

use crate::{Error, Result};

// Use this as Arc<List>
pub struct List<K> {
    head: AtomicPtr<Node<K>>,
}

impl<K> Default for List<K> {
    fn default() -> List<K> {
        List {
            head: AtomicPtr::new(Box::leak(Box::new(Node::Z))),
        }
    }
}

impl<K> Drop for List<K> {
    fn drop(&mut self) {
        let node = self.head.load(SeqCst);
        // node and its entire chain shall be dropped.
        let now = time::Instant::now();
        std::mem::drop(node);
        debug!("took {:?} to drop all the nodes", now.elapsed());
    }
}

impl<K> List<K> {
    pub fn prepend(&self, mut key: K) -> Result<*mut Node<K>> {
        loop {
            let old_ptr = self.head.load(SeqCst);
            let next = unsafe { Box::from_raw(old_ptr) };

            let node = Node::new_node(key, next)?;
            let new_ptr = Box::leak(node);

            match self.head.compare_exchange(old_ptr, new_ptr, SeqCst, SeqCst) {
                Ok(_) => break Ok(new_ptr),
                Err(_) => {
                    let (k, next) = unsafe { Box::from_raw(new_ptr).unwrap() };
                    key = k;
                    Box::leak(next);
                }
            }
        }
    }

    pub fn as_mut_head(&self) -> Option<&mut Node<K>> {
        let mut skip = 5;
        let mut node: &mut Node<K> = unsafe { self.head.load(SeqCst).as_mut().unwrap() };

        loop {
            node = match node {
                Node::Z => break None,
                Node::T { .. } if skip == 0 => break Some(node),
                Node::T { next, .. } => {
                    skip -= 1;
                    next.as_mut().unwrap()
                }
            }
        }
    }
}

// T - Accessed key time-stamp
// Z - Last node.
pub enum Node<K> {
    T {
        key: K,
        born: Duration, // elapsed time in uS since UNIX_EPOCH.
        deleted: AtomicBool,
        next: Option<Box<Node<K>>>,
    },
    Z,
}

impl<K> Node<K> {
    fn new_node(key: K, next: Box<Node<K>>) -> Result<Box<Node<K>>> {
        let node = Node::T {
            key,
            deleted: AtomicBool::new(false),
            born: err_at!(Fatal, time::UNIX_EPOCH.elapsed())?,
            next: Some(next),
        };

        Ok(Box::new(node))
    }

    fn unwrap(self) -> (K, Box<Node<K>>) {
        match self {
            Node::T { key, next, .. } => (key, next.unwrap()),
            _ => unreachable!(),
        }
    }
}

impl<K> Node<K> {
    pub fn delete(&self) {
        match self {
            Node::T { deleted, .. } => deleted.store(true, SeqCst),
            _ => unreachable!(),
        }
    }
}
