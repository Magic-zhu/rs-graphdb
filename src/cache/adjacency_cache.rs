//! 邻接表缓存模块
//!
//! 三层缓存设计：Bloom filter → 关系ID列表 → 完整关系详情

use super::lru::LruCache;
use super::stats::CacheStats;
use crate::storage::{NodeId, RelId, StoredRel};
use std::collections::HashMap;
use std::time::Duration;

/// 邻接表缓存
pub struct AdjacencyCache {
    /// 出边关系ID缓存
    outgoing_ids: LruCache<NodeId, Vec<RelId>>,
    /// 入边关系ID缓存
    incoming_ids: LruCache<NodeId, Vec<RelId>>,
    /// 关系详情缓存
    rel_details: LruCache<RelId, StoredRel>,
    /// 统计信息
    stats: CacheStats,
    /// TTL
    ttl: Option<Duration>,
}

impl AdjacencyCache {
    /// 创建新的邻接表缓存
    pub fn new(max_size: usize, ttl: Option<Duration>) -> Self {
        Self {
            outgoing_ids: LruCache::new(max_size),
            incoming_ids: LruCache::new(max_size),
            rel_details: LruCache::new(max_size * 2), // 关系详情可以缓存更多
            stats: CacheStats::new(),
            ttl,
        }
    }

    /// 获取出边关系ID列表
    pub fn get_outgoing_ids(&mut self, node: NodeId) -> Option<Vec<RelId>> {
        let start = std::time::Instant::now();

        if let Some(ids) = self.outgoing_ids.get(&node) {
            let latency = start.elapsed().as_nanos() as u64;
            self.stats.record_hit(latency);
            Some(ids.clone())
        } else {
            self.stats.record_miss();
            None
        }
    }

    /// 插入出边关系ID列表
    pub fn put_outgoing_ids(&mut self, node: NodeId, ids: Vec<RelId>) {
        let size_bytes = ids.len() * std::mem::size_of::<RelId>();
        self.outgoing_ids.put(node, ids, size_bytes);
        self.update_stats();
    }

    /// 获取入边关系ID列表
    pub fn get_incoming_ids(&mut self, node: NodeId) -> Option<Vec<RelId>> {
        let start = std::time::Instant::now();

        if let Some(ids) = self.incoming_ids.get(&node) {
            let latency = start.elapsed().as_nanos() as u64;
            self.stats.record_hit(latency);
            Some(ids.clone())
        } else {
            self.stats.record_miss();
            None
        }
    }

    /// 插入入边关系ID列表
    pub fn put_incoming_ids(&mut self, node: NodeId, ids: Vec<RelId>) {
        let size_bytes = ids.len() * std::mem::size_of::<RelId>();
        self.incoming_ids.put(node, ids, size_bytes);
        self.update_stats();
    }

    /// 获取关系详情
    pub fn get_rel(&mut self, id: RelId) -> Option<StoredRel> {
        let start = std::time::Instant::now();

        if let Some(rel) = self.rel_details.get(&id) {
            let latency = start.elapsed().as_nanos() as u64;
            self.stats.record_hit(latency);
            Some(rel.clone())
        } else {
            self.stats.record_miss();
            None
        }
    }

    /// 插入关系详情
    pub fn put_rel(&mut self, rel: StoredRel) {
        let size_bytes = self.estimate_rel_size(&rel);
        self.rel_details.put(rel.id, rel, size_bytes);
        self.update_stats();
    }

    /// 使节点相关的邻接表失效
    pub fn invalidate_node(&mut self, node: NodeId) {
        self.outgoing_ids.remove(&node);
        self.incoming_ids.remove(&node);
        self.update_stats();
    }

    /// 使关系失效
    pub fn invalidate_rel(&mut self, id: RelId) {
        self.rel_details.remove(&id);
        self.update_stats();
    }

    /// 使关系变更失效（需要更新源节点和目标节点的邻接表）
    pub fn invalidate_rel_nodes(&mut self, start: NodeId, end: NodeId) {
        self.invalidate_node(start);
        self.invalidate_node(end);
    }

    /// 清空所有缓存
    pub fn clear(&mut self) {
        self.outgoing_ids.clear();
        self.incoming_ids.clear();
        self.rel_details.clear();
        self.update_stats();
    }

    /// 获取统计信息
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// 更新统计信息
    fn update_stats(&self) {
        let total_entries = self.outgoing_ids.len()
            + self.incoming_ids.len()
            + self.rel_details.len();
        self.stats.update_entries(total_entries);

        let total_bytes = self.outgoing_ids.current_bytes()
            + self.incoming_ids.current_bytes()
            + self.rel_details.current_bytes();
        self.stats.update_size(total_bytes);
    }

    /// 估算关系大小
    fn estimate_rel_size(&self, rel: &StoredRel) -> usize {
        let mut size = std::mem::size_of::<RelId>()
            + std::mem::size_of::<NodeId>()
            + std::mem::size_of::<NodeId>()
            + std::mem::size_of::<String>()
            + std::mem::size_of::<crate::values::Properties>();

        // 关系类型字符串
        size += rel.typ.len();

        // 属性
        for (key, value) in &rel.props {
            size += key.len();
            size += self.value_size(value);
        }

        size
    }

    /// 估算值大小
    fn value_size(&self, value: &crate::values::Value) -> usize {
        match value {
            crate::values::Value::Int(_) => std::mem::size_of::<i64>(),
            crate::values::Value::Bool(_) => std::mem::size_of::<bool>(),
            crate::values::Value::Text(s) => s.len(),
            crate::values::Value::Float(_) => std::mem::size_of::<f64>(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::values::Properties;

    fn make_test_rel(id: RelId, start: NodeId, end: NodeId, typ: &str) -> StoredRel {
        StoredRel {
            id,
            start,
            end,
            typ: typ.to_string(),
            props: Properties::new(),
        }
    }

    #[test]
    fn test_basic_operations() {
        let mut cache = AdjacencyCache::new(10, Some(Duration::from_secs(120)));

        // 测试出边
        let outgoing_ids = vec![1, 2, 3];
        cache.put_outgoing_ids(10, outgoing_ids.clone());
        assert_eq!(cache.get_outgoing_ids(10), Some(outgoing_ids));

        // 测试入边
        let incoming_ids = vec![4, 5];
        cache.put_incoming_ids(10, incoming_ids.clone());
        assert_eq!(cache.get_incoming_ids(10), Some(incoming_ids));

        // 测试关系详情
        let rel = make_test_rel(1, 10, 20, "FRIEND");
        cache.put_rel(rel.clone());
        assert_eq!(cache.get_rel(1), Some(rel));
    }

    #[test]
    fn test_invalidation() {
        let mut cache = AdjacencyCache::new(10, Some(Duration::from_secs(120)));

        let outgoing_ids = vec![1, 2, 3];
        cache.put_outgoing_ids(10, outgoing_ids);

        cache.invalidate_node(10);

        assert_eq!(cache.get_outgoing_ids(10), None);
    }

    #[test]
    fn test_rel_invalidation() {
        let mut cache = AdjacencyCache::new(10, Some(Duration::from_secs(120)));

        let rel = make_test_rel(1, 10, 20, "FRIEND");
        cache.put_rel(rel.clone());

        cache.invalidate_rel(1);

        assert_eq!(cache.get_rel(1), None);
    }

    #[test]
    fn test_rel_nodes_invalidation() {
        let mut cache = AdjacencyCache::new(10, Some(Duration::from_secs(120)));

        cache.put_outgoing_ids(10, vec![1, 2]);
        cache.put_incoming_ids(20, vec![1]);

        cache.invalidate_rel_nodes(10, 20);

        assert_eq!(cache.get_outgoing_ids(10), None);
        assert_eq!(cache.get_incoming_ids(20), None);
    }

    #[test]
    fn test_clear() {
        let mut cache = AdjacencyCache::new(10, Some(Duration::from_secs(120)));

        cache.put_outgoing_ids(10, vec![1, 2]);
        cache.put_rel(make_test_rel(1, 10, 20, "FRIEND"));

        cache.clear();

        assert_eq!(cache.get_outgoing_ids(10), None);
        assert_eq!(cache.get_rel(1), None);
    }

    #[test]
    fn test_stats() {
        let mut cache = AdjacencyCache::new(10, Some(Duration::from_secs(120)));

        cache.put_outgoing_ids(10, vec![1, 2]);

        // 命中
        cache.get_outgoing_ids(10);
        // 未命中
        cache.get_outgoing_ids(20);

        assert_eq!(cache.stats().hits(), 1);
        assert_eq!(cache.stats().misses(), 1);
    }
}
