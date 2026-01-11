//! LRU (Least Recently Used) 缓存实现
//!
//! 基于双向链表和 HashMap 的 LRU 缓存

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::time::Instant;

/// LRU 缓存条目
struct LruEntry<K, V> {
    key: K,
    value: V,
    access_time: Instant,
    size_bytes: usize,
}

/// LRU 缓存
pub struct LruCache<K, V>
where
    K: Hash + Eq + Clone,
{
    entries: HashMap<K, LruEntry<K, V>>,
    access_order: VecDeque<K>,
    max_size: usize,
    max_bytes: usize,
    current_bytes: usize,
    hits: u64,
    misses: u64,
}

impl<K, V> LruCache<K, V>
where
    K: Hash + Eq + Clone,
{
    /// 创建新的 LRU 缓存
    pub fn new(max_size: usize) -> Self {
        Self {
            entries: HashMap::new(),
            access_order: VecDeque::new(),
            max_size,
            max_bytes: usize::MAX,
            current_bytes: 0,
            hits: 0,
            misses: 0,
        }
    }

    /// 设置最大字节数限制
    pub fn with_max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    /// 获取缓存值
    pub fn get(&mut self, key: &K) -> Option<&V> {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.access_time = Instant::now();
            self.hits += 1;

            // 更新访问顺序（将访问的键移到末尾）
            self.access_order.retain(|k| k != key);
            self.access_order.push_back(key.clone());

            Some(&entry.value)
        } else {
            self.misses += 1;
            None
        }
    }

    /// 插入缓存值
    pub fn put(&mut self, key: K, value: V, size_bytes: usize)
    where
        V: Clone,
    {
        // 如果键已存在，更新值
        if self.entries.contains_key(&key) {
            self.remove(&key);
        }

        // 检查是否需要淘汰
        while self.should_evict(size_bytes) {
            self.evict_one();
        }

        let entry = LruEntry {
            key: key.clone(),
            value,
            access_time: Instant::now(),
            size_bytes,
        };

        self.entries.insert(key.clone(), entry);
        self.access_order.push_back(key);
        self.current_bytes += size_bytes;
    }

    /// 移除缓存值
    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(entry) = self.entries.remove(key) {
            self.access_order.retain(|k| k != key);
            self.current_bytes -= entry.size_bytes;
            Some(entry.value)
        } else {
            None
        }
    }

    /// 检查是否需要淘汰
    fn should_evict(&self, new_size: usize) -> bool {
        self.entries.len() >= self.max_size
            || self.current_bytes + new_size > self.max_bytes
    }

    /// 淘汰一个最久未使用的条目
    fn evict_one(&mut self) -> Option<(K, V)> {
        if let Some(key) = self.access_order.pop_front() {
            if let Some(entry) = self.entries.remove(&key) {
                self.current_bytes -= entry.size_bytes;
                return Some((entry.key, entry.value));
            }
        }
        None
    }

    /// 清空缓存
    pub fn clear(&mut self) {
        self.entries.clear();
        self.access_order.clear();
        self.current_bytes = 0;
    }

    /// 获取缓存大小
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// 检查缓存是否为空
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// 获取当前内存使用（字节）
    pub fn current_bytes(&self) -> usize {
        self.current_bytes
    }

    /// 获取命中率
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }

    /// 获取命中次数
    pub fn hits(&self) -> u64 {
        self.hits
    }

    /// 获取未命中次数
    pub fn misses(&self) -> u64 {
        self.misses
    }

    /// 获取所有键
    pub fn keys(&self) -> Vec<K>
    where
        K: Clone,
    {
        self.entries.keys().cloned().collect()
    }

    /// 迭代器
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.entries.iter().map(|(k, v)| (k, &v.value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut cache = LruCache::new(3);

        cache.put(1, "a", 10);
        cache.put(2, "b", 10);
        cache.put(3, "c", 10);

        assert_eq!(cache.get(&1), Some(&"a"));
        assert_eq!(cache.get(&2), Some(&"b"));
        assert_eq!(cache.get(&3), Some(&"c"));
        assert_eq!(cache.get(&4), None);
    }

    #[test]
    fn test_lru_eviction() {
        let mut cache = LruCache::new(2);

        cache.put(1, "a", 10);
        cache.put(2, "b", 10);
        cache.put(3, "c", 10); // 应该淘汰 1

        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&2), Some(&"b"));
        assert_eq!(cache.get(&3), Some(&"c"));
    }

    #[test]
    fn test_update_existing() {
        let mut cache = LruCache::new(2);

        cache.put(1, "a", 10);
        cache.put(2, "b", 10);
        cache.put(1, "a2", 10); // 更新现有键

        assert_eq!(cache.get(&1), Some(&"a2"));
        assert_eq!(cache.get(&2), Some(&"b"));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_hit_rate() {
        let mut cache = LruCache::new(10);

        cache.put(1, "a", 10);
        cache.put(2, "b", 10);

        // 5次命中，1次未命中
        let _ = cache.get(&1);
        let _ = cache.get(&1);
        let _ = cache.get(&2);
        let _ = cache.get(&2);
        let _ = cache.get(&1);
        let _ = cache.get(&3);

        assert!((cache.hit_rate() - 0.8333).abs() < 0.01);
        assert_eq!(cache.hits(), 5);
        assert_eq!(cache.misses(), 1);
    }

    #[test]
    fn test_clear() {
        let mut cache = LruCache::new(10);

        cache.put(1, "a", 10);
        cache.put(2, "b", 10);

        cache.clear();

        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_size_limit() {
        let mut cache = LruCache::new(10).with_max_bytes(25);

        cache.put(1, "a", 10);
        cache.put(2, "b", 10);
        cache.put(3, "c", 10); // 超过限制，应该淘汰一个

        assert!(cache.current_bytes() <= 25);
    }
}
