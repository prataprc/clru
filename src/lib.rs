use cmap::{DefaultHasher, Map};

use std::{
    borrow::Borrow,
    hash::{BuildHasher, Hash, Hasher},
    sync::{
        atomic::{AtomicPtr, Ordering::SeqCst},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

use crate::{access::Access, evictor::evictor};

mod access;
mod evictor;

pub struct Cache<K, V> {
    value: V,
    access: AtomicPtr<Access<K>>,
}

impl<K, V> Clone for Cache<K, V>
where
    V: Clone,
{
    fn clone(&self) -> Self {
        Cache {
            value: self.value.clone(),
            access: AtomicPtr::new(self.access.load(SeqCst)),
        }
    }
}

pub struct Lru<K, V, H = DefaultHasher> {
    maps: Vec<Map<K, Cache<K, V>, H>>,
    heads: Vec<Arc<Access<K>>>,
    hash_builder: H,
    max_count: usize,
    max_old: Duration,
    close: Arc<Mutex<bool>>,
}

impl<K, V, H> Drop for Lru<K, V, H> {
    fn drop(&mut self) {
        *self.close.lock().unwrap() = true;
    }
}

impl<K, V, H> Clone for Lru<K, V, H>
where
    H: Clone,
{
    fn clone(&self) -> Self {
        Lru {
            maps: self.maps.iter().map(|m| m.clone()).collect(),
            heads: self.heads.iter().map(|a| Arc::clone(a)).collect(),
            max_count: self.max_count,
            max_old: self.max_old,
            close: Arc::clone(&self.close),
            hash_builder: self.hash_builder.clone(),
        }
    }
}

impl<K, V, H> Lru<K, V, H> {
    pub fn new(
        shards: usize,
        max_count: usize,
        max_old: Duration,
        concurrency: usize,
        hash_builder: H,
    ) -> Lru<K, V, H>
    where
        K: 'static + Send + Sync + Clone + PartialEq + Hash,
        V: 'static + Send + Clone,
        H: 'static + Send + Clone + BuildHasher,
    {
        let maps: Vec<Map<K, Cache<K, V>, H>> = {
            let concurrency = concurrency + 1;
            let iter = (0..shards).map(|_| Map::new(concurrency, hash_builder.clone()));
            iter.collect()
        };
        let close = Arc::new(Mutex::new(false));
        let val = Lru {
            maps,
            heads: (0..shards).map(|_| Access::new_list()).collect(),
            hash_builder,
            max_count,
            max_old,
            close,
        };

        for (i, map) in val.maps.iter().enumerate() {
            let map = map.clone();
            let close = Arc::clone(&val.close);
            let head = Arc::clone(&val.heads[i]);
            thread::spawn(move || evictor(max_count, max_old, map, close, head));
        }

        val
    }

    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q> + Clone,
        Q: ToOwned<Owned = K> + PartialEq + ?Sized + Hash,
        H: BuildHasher,
        V: Clone,
    {
        let shard = {
            let hasher = self.hash_builder.build_hasher();
            (key_to_hash32(key, hasher) % (self.maps.len() as u32)) as usize
        };

        let (map, head) = (&self.maps[shard], &self.heads[shard]);
        let access_ptr = Box::leak(Access::new(key.to_owned())) as *const Access<K>;

        map.get_with(key, |cache: &Cache<K, V>| {
            let old = cache.access.load(SeqCst);
            let new = access_ptr as *mut Access<K>;
            match cache.access.compare_exchange(old, new, SeqCst, SeqCst) {
                Ok(_) => {
                    unsafe { old.as_ref().unwrap() }.delete();
                    head.prepend(unsafe { Box::from_raw(new) });
                }
                Err(_) => {
                    let _access = unsafe { Box::from_raw(new) }; // drop this access
                }
            }
            cache.value.clone()
        })
    }

    pub fn set(&mut self, key: K, value: V)
    where
        K: Clone + PartialEq + Hash,
        V: Clone,
        H: BuildHasher,
    {
        let shard = {
            let hasher = self.hash_builder.build_hasher();
            (key_to_hash32(&key, hasher) % (self.maps.len() as u32)) as usize
        };

        let (map, head) = (&mut self.maps[shard], &self.heads[shard]);
        let access_ptr = Box::leak(Access::new(key.to_owned()));
        let value = Cache {
            value,
            access: AtomicPtr::new(access_ptr),
        };

        head.prepend(unsafe { Box::from_raw(access_ptr) });
        match map.set(key, value) {
            Some(Cache { access, .. }) => {
                let access = unsafe { access.load(SeqCst).as_ref().unwrap() };
                access.delete()
            }
            None => (),
        }
    }

    pub fn remove<Q>(&mut self, key: &Q)
    where
        K: Clone + Borrow<Q>,
        V: Clone,
        Q: PartialEq + Hash + ?Sized,
        H: BuildHasher,
    {
        let shard = {
            let hasher = self.hash_builder.build_hasher();
            (key_to_hash32(&key, hasher) % (self.maps.len() as u32)) as usize
        };

        let map = &mut self.maps[shard];

        match map.remove(key) {
            Some(Cache { access, .. }) => {
                let access = unsafe { access.load(SeqCst).as_ref().unwrap() };
                access.delete()
            }
            None => (),
        }
    }
}

fn key_to_hash32<K, H>(key: &K, mut hasher: H) -> u32
where
    K: Hash + ?Sized,
    H: Hasher,
{
    key.hash(&mut hasher);
    let code: u64 = hasher.finish();
    (((code >> 32) ^ code) & 0xFFFFFFFF) as u32
}
