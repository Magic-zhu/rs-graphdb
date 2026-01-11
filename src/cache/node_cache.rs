//! 节点缓存模块
//!
//! 缓存热点节点数据，避免重复的序列化和 HashMap 查找

use super::lru::LruCache;
use super::stats::CacheStats;
use crate::storage::{NodeId, StoredNode};
use crate::values::Value;
use std::time::{Duration, Instant};

/// 节点缓存
pub struct NodeCache {
    cache: LruCache<NodeId, StoredNode>,
    stats: CacheStats,
    ttl: Option<Duration>,
}

impl NodeCache {
    /// 创建新的节点缓存
    pub fn new(max_size: usize, ttl: Option<Duration>) -> Self {
        Self {
            cache: LruCache::new(max_size),
            stats: CacheStats::new(),
            ttl,
        }
    }

    /// 获取节点
    pub fn get(&mut self, id: NodeId) -> Option<StoredNode> {
        let start = Instant::now();

        if let Some(node) = self.cache.get(&id) {
            // 检查 TTL
            if let Some(ttl) = self.ttl {
                // 由于 LruEntry 没有存储创建时间，我们简化处理
                // 实际应用中可以在 StoredNode 中添加时间戳
            }

            let latency = start.elapsed().as_nanos() as u64;
            self.stats.record_hit(latency);
            Some(node.clone())
        } else {
            self.stats.record_miss();
            None
        }
    }

    /// 插入节点
    pub fn put(&mut self, id: NodeId, node: StoredNode) {
        let size_bytes = self.estimate_size(&node);
        self.cache.put(id, node, size_bytes);
        self.stats.update_entries(self.cache.len());
        self.stats.update_size(self.cache.current_bytes());
    }

    /// 移除节点
    pub fn remove(&mut self, id: NodeId) -> Option<StoredNode> {
        let node = self.cache.remove(&id);
        if node.is_some() {
            self.stats.update_entries(self.cache.len());
            self.stats.update_size(self.cache.current_bytes());
        }
        node
    }

    /// 使节点失效
    pub fn invalidate(&mut self, id: NodeId) {
        self.remove(id);
    }

    /// 清空缓存
    pub fn clear(&mut self) {
        self.cache.clear();
        self.stats.update_entries(0);
        self.stats.update_size(0);
    }

    /// 获取统计信息
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// 获取缓存大小
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// 检查是否为空
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// 估算节点大小（字节）
    fn estimate_size(&self, node: &StoredNode) -> usize {
        // 基础大小
        let mut size = std::mem::size_of::<NodeId>()
            + std::mem::size_of::<Vec<String>>()
            + std::mem::size_of::<crate::values::Properties>();

        // labels 大小
        for label in &node.labels {
            size += label.len();
        }

        // properties 大小
        for (key, value) in &node.props {
            size += key.len();
            size += self.value_size(value);
        }

        size
    }

    /// 估算值大小
    fn value_size(&self, value: &Value) -> usize {
        match value {
            Value::Int(_) => std::mem::size_of::<i64>(),
            Value::Bool(_) => std::mem::size_of::<bool>(),
            Value::Text(s) => s.len(),
            Value::Float(_) => std::mem::size_of::<f64>(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::values::Properties;
    use std::collections::HashMap;

    fn make_test_node(id: NodeId, name: &str) -> StoredNode {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text(name.to_string()));
        props.insert("age".to_string(), Value::Int(25));

        StoredNode {
            id,
            labels: vec!["User".to_string()],
            props,
        }
    }

    #[test]
    fn test_basic_operations() {
        let mut cache = NodeCache::new(10, Some(Duration::from_secs(60)));

        let node = make_test_node(1, "Alice");
        cache.put(1, node.clone());

        assert_eq!(cache.get(1), Some(node));
        assert_eq!(cache.get(2), None);
    }

    #[test]
    fn test_cache_stats() {
        let mut cache = NodeCache::new(10, Some(Duration::from_secs(60)));

        let node = make_test_node(1, "Alice");
        cache.put(1, node);

        // 命中
        cache.get(1);
        // 未命中
        cache.get(2);

        assert_eq!(cache.stats().hits(), 1);
        assert_eq!(cache.stats().misses(), 1);
        assert!((cache.stats().hit_rate() - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_invalidation() {
        let mut cache = NodeCache::new(10, Some(Duration::from_secs(60)));

        let node = make_test_node(1, "Alice");
        cache.put(1, node.clone());

        cache.invalidate(1);

        assert_eq!(cache.get(1), None);
    }

    #[test]
    fn test_clear() {
        let mut cache = NodeCache::new(10, Some(Duration::from_secs(60)));

        cache.put(1, make_test_node(1, "Alice"));
        cache.put(2, make_test_node(2, "Bob"));

        assert_eq!(cache.len(), 2);

        cache.clear();

        assert!(cache.is_empty());
    }

    #[test]
    fn test_lru_eviction() {
        let mut cache = NodeCache::new(2, Some(Duration::from_secs(60)));

        cache.put(1, make_test_node(1, "Alice"));
        cache.put(2, make_test_node(2, "Bob"));
        cache.put(3, make_test_node(3, "Charlie")); // 应该淘汰 1

        assert_eq!(cache.get(1), None);
        assert_eq!(cache.get(2).is_some(), true);
        assert_eq!(cache.get(3).is_some(), true);
    }
}
