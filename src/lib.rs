use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, RandomState};
use std::num::NonZeroUsize;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, RwLock};

pub mod sync;

pub struct Cache<Key, Value, H = RandomState> {
    head: RwLock<Option<Entry<Key, Value>>>,
    tail: RwLock<Option<Entry<Key, Value>>>,
    map: HashMap<Key, Entry<Key, Value>, H>,
    capacity: usize,
}

impl<Key, Value> Cache<Key, Value, RandomState>
where
    Key: Eq + Hash + Clone,
{
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self::with_hasher(RandomState::new(), capacity)
    }
}

impl<Key, Value, H> Cache<Key, Value, H>
where
    Key: Eq + Hash + Clone,
    H: BuildHasher,
{
    pub fn with_hasher(hash_builder: H, capacity: NonZeroUsize) -> Self {
        Self {
            head: RwLock::new(None),
            tail: RwLock::new(None),
            map: HashMap::with_hasher(hash_builder),
            capacity: capacity.get(),
        }
    }

    pub fn with<T>(&self, key: &Key, operation: impl FnOnce(&Value) -> T) -> Option<T> {
        let entry = self.promote(key)?;
        let result = operation(&entry.read().value);

        Some(result)
    }

    pub fn insert(&mut self, key: Key, value: Value) -> Option<Value> {
        let mut ret = None;

        if self.head.read().expect("read").is_none() {
            let entry = Entry::new(key.clone(), value);
            *self.head.write().expect("write") = Some(entry.clone());
            *self.tail.write().expect("write") = Some(entry.clone());
            self.map.insert(key, entry);
        } else if self.map.contains_key(&key) {
            let entry = self.promote(&key).expect("promote");
            ret = Some(std::mem::replace(
                &mut entry.0.write().expect("write").value,
                value,
            ));
        } else {
            let entry = Entry::new(key.clone(), value);
            entry.write().prev = self.head.read().expect("read").as_ref().cloned();
            self.map.insert(key.clone(), entry);
            self.promote(&key);
        }

        if self.map.len() > self.capacity {
            self.evict();
        }

        ret
    }

    fn promote(&self, key: &Key) -> Option<Entry<Key, Value>> {
        let (head, tail) = self
            .head
            .read()
            .expect("read")
            .as_ref()
            .cloned()
            .zip(self.tail.read().expect("read").as_ref().cloned())?;
        let entry = self.map.get(key).cloned()?;

        if entry.read().key == head.read().key {
            return Some(entry);
        }

        let prev = entry.write().prev.take();
        let next = entry.write().next.take();
        if let Some(prev) = prev.clone() {
            prev.write().next = next.as_ref().cloned();
        }
        if let Some(next) = next.clone() {
            next.write().prev = prev;
        }
        if entry.read().key == tail.read().key {
            *self.tail.write().expect("write") = next;
        }

        head.write().next = Some(entry.clone());
        entry.write().prev = Some(head);
        *self.head.write().expect("write") = Some(entry.clone());

        Some(entry)
    }

    fn evict(&mut self) {
        let tail = self.tail.read().expect("read").as_ref().cloned();
        if let Some(tail) = tail {
            *self.tail.write().expect("write") = tail.read().next.as_ref().cloned();
            self.map.remove(&tail.read().key);
        }
    }
}

struct Entry<Key, Value>(Arc<RwLock<Item<Key, Value>>>);

impl<Key, Value> Clone for Entry<Key, Value> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<Key, Value> Entry<Key, Value> {
    fn new(key: Key, value: Value) -> Self {
        Self(Arc::new(RwLock::new(Item {
            key,
            value,
            prev: None,
            next: None,
        })))
    }

    fn read(&self) -> impl Deref<Target = Item<Key, Value>> + '_ {
        self.0.read().expect("read")
    }

    fn write(&self) -> impl DerefMut<Target = Item<Key, Value>> + '_ {
        self.0.write().expect("write")
    }
}

#[derive(Default)]
struct Item<Key, Value> {
    key: Key,
    value: Value,
    prev: Option<Entry<Key, Value>>,
    next: Option<Entry<Key, Value>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lc() {
        /// A wrapper of `Cache` to solve LeetCode problem #146
        struct LRUCache {
            inner: Cache<i32, i32>,
        }

        impl LRUCache {
            fn new(capacity: i32) -> Self {
                Self {
                    inner: Cache::new(
                        NonZeroUsize::new(capacity as usize).expect("capacity must be non-zero!"),
                    ),
                }
            }

            fn get(&self, key: i32) -> i32 {
                self.inner.with(&key, |n| *n).unwrap_or(-1)
            }

            fn put(&mut self, key: i32, value: i32) {
                let _ = self.inner.insert(key, value);
            }
        }

        let mut cache = LRUCache::new(2);
        cache.put(1, 1);
        cache.put(2, 2);
        assert_eq!(cache.get(1), 1);
        cache.put(3, 3);
        assert_eq!(cache.get(2), -1);
        cache.put(4, 4);
        assert_eq!(cache.get(1), -1);
        assert_eq!(cache.get(3), 3);
        assert_eq!(cache.get(4), 4);
    }
}
