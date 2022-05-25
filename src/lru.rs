use log::{debug, error};

use std::hash::{BuildHasher, Hash};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering::SeqCst};
use std::{borrow::Borrow, sync::Arc, thread, time::Duration};

use crate::{evictor::Evictor, list, Result, Value};

#[derive(Clone, Copy)]
pub struct LruBuilder {
    /// maximum number of entries allowed to be cached, default is MAX_ENTRIES
    pub max_entries: usize,
    /// footprint of cache not to exceed configured `max_memory`, default is MAX_MEMORY
    pub max_memory: Option<usize>,
    /// evict all entries older than `max_old`
    pub max_old: Option<Duration>, // in seconds.
    /// maximum number of concurrent instances allowed on Lru, defaults to number of
    /// physical cores.
    pub max_threads: usize,
}

impl Default for LruBuilder {
    fn default() -> LruBuilder {
        LruBuilder {
            max_entries: crate::MAX_ENTRIES,
            max_memory: None,
            max_old: None,
            max_threads: num_cpus::get_physical(),
        }
    }
}

impl LruBuilder {
    pub fn build<K, V, H>(self, hash_builder: H) -> Lru<K, V, H>
    where
        K: 'static + Send + Clone + PartialEq + Hash,
        V: 'static + Send + Clone,
        H: 'static + Send + Clone + BuildHasher,
    {
        let map = cmap::Map::new(self.max_threads + 1, hash_builder);
        let access_list = Arc::new(list::List::default());
        let cur_entries = Arc::new(AtomicUsize::new(0));
        let cur_memory = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicBool::new(false));

        let evictor = Evictor {
            max_entries: self.max_entries,
            max_memory: self.max_memory,
            max_old: self.max_old,

            list: Arc::clone(&access_list),
            cur_entries: Arc::clone(&cur_entries),
            cur_memory: Arc::clone(&cur_memory),
            closed: Arc::clone(&closed),

            n_evicted: 0,
            n_deleted: 0,
            n_older: 0,
        };
        let handle = {
            let map = map.cloned();
            thread::spawn(move || evictor.run(map))
        };

        let inner = Inner {
            evictor: Some(handle),
            n_gets: AtomicUsize::new(0),
            n_sets: AtomicUsize::new(0),
            closed,
        };

        Lru {
            max_entries: self.max_entries,
            max_memory: self.max_memory,
            max_old: self.max_old,

            map,
            inner: Arc::new(inner),
            list: access_list,
            cur_entries,
            cur_memory,
        }
    }
}

pub struct Lru<K, V, H = cmap::DefaultHasher> {
    max_entries: usize,
    max_memory: Option<usize>,
    max_old: Option<Duration>,

    map: cmap::Map<K, Value<K, V>, H>,
    inner: Arc<Inner<K>>,
    list: Arc<list::List<K>>,
    cur_entries: Arc<AtomicUsize>,
    cur_memory: Arc<AtomicUsize>,
}

struct Inner<K> {
    evictor: Option<thread::JoinHandle<Result<Evictor<K>>>>,
    n_gets: AtomicUsize,
    n_sets: AtomicUsize,
    closed: Arc<AtomicBool>,
}

impl<K> Drop for Inner<K> {
    fn drop(&mut self) {
        self.closed.store(true, SeqCst);

        match self.evictor.take().unwrap().join() {
            Ok(Ok(evictor)) => {
                let stats = Stats {
                    n_gets: self.n_gets.load(SeqCst),
                    n_sets: self.n_sets.load(SeqCst),
                    n_evicted: evictor.n_evicted,
                    n_deleted: evictor.n_deleted,
                    n_older: evictor.n_older,
                };
                debug!("{:?}", stats);
            }
            Ok(Err(err)) => error!("evictor fail: {}", err),
            Err(err) => error!("evictor thread fail {:?}", err),
        }
    }
}

impl<K, V, H> Clone for Lru<K, V, H> {
    fn clone(&self) -> Self {
        Lru {
            max_entries: self.max_entries,
            max_memory: self.max_memory,
            max_old: self.max_old,

            map: self.map.cloned(),
            inner: Arc::clone(&self.inner),
            list: Arc::clone(&self.list),
            cur_entries: Arc::clone(&self.cur_entries),
            cur_memory: Arc::clone(&self.cur_memory),
        }
    }
}

impl<K, V, H> Lru<K, V, H> {
    pub fn get<Q>(&self, key: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: ToOwned<Owned = K> + PartialEq + Hash,
        H: BuildHasher,
        V: Clone,
    {
        let val = self.map.get_with(key, |value: &Value<K, V>| loop {
            let optr = value.access.load(SeqCst);
            let nptr = self.list.prepend(key.to_owned())?;
            match value.access.compare_exchange(optr, nptr, SeqCst, SeqCst) {
                Ok(_) => {
                    unsafe { optr.as_ref().unwrap() }.delete();
                    break Ok(value.value.clone());
                }
                Err(_) => {
                    unsafe { nptr.as_ref().unwrap() }.delete();
                }
            }

            self.inner.n_gets.fetch_add(1, SeqCst);
        });

        val.transpose()
    }

    pub fn set(&mut self, key: K, value: V) -> Result<Option<V>>
    where
        K: Clone + PartialEq + Hash,
        V: Clone,
        H: BuildHasher,
    {
        self.inner.n_sets.fetch_add(1, SeqCst);

        let value = Value {
            value,
            access: AtomicPtr::new(self.list.prepend(key.clone())?),
        };

        match self.map.set(key, value) {
            Some(Value { value, access }) => {
                unsafe { access.load(SeqCst).as_ref().unwrap() }.delete();
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }
}

#[derive(Debug)]
pub struct Stats {
    pub n_gets: usize,
    pub n_sets: usize,
    pub n_evicted: usize,
    pub n_deleted: usize,
    pub n_older: usize,
}

#[cfg(test)]
#[path = "lru_test.rs"]
mod lru_test;
