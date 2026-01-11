//! 缓存管理器模块
//!
//! 统一管理所有类型的缓存，协调失效策略

use super::{config::CacheConfig, node_cache::NodeCache, adjacency_cache::AdjacencyCache};
use super::{query_cache::QueryCache, index_cache::IndexCache, stats::OverallCacheReport};
use crate::storage::{NodeId, RelId, StoredNode, StoredRel};
use crate::values::Properties;
use std::sync::{Arc, RwLock};

/// 缓存管理器
pub struct CacheManager {
    /// 节点缓存
    node_cache: Arc<RwLock<NodeCache>>,
    /// 邻接表缓存
    adjacency_cache: Arc<RwLock<AdjacencyCache>>,
    /// 查询缓存
    query_cache: Arc<RwLock<QueryCache>>,
    /// 索引缓存
    index_cache: Arc<RwLock<IndexCache>>,
    /// 配置
    config: CacheConfig,
}

impl CacheManager {
    /// 创建新的缓存管理器
    pub fn new(config: CacheConfig) -> Self {
        if !config.enabled {
            // 返回一个禁用的缓存管理器
            return Self::disabled();
        }

        let sizes = config.allocate(CacheConfig::get_available_memory());

        Self {
            node_cache: Arc::new(RwLock::new(NodeCache::new(
                sizes.node / 500, // 假设平均节点大小 500 字节
                Some(config.node_ttl),
            ))),
            adjacency_cache: Arc::new(RwLock::new(AdjacencyCache::new(
                sizes.adjacency / 100, // 假设平均关系大小 100 字节
                Some(config.adjacency_ttl),
            ))),
            query_cache: Arc::new(RwLock::new(QueryCache::new(
                1000, // 最多 1000 个查询结果
                config.query_ttl,
                10_000, // 单个结果最多 10KB
            ))),
            index_cache: Arc::new(RwLock::new(IndexCache::new(
                sizes.index / 100,
                Some(config.index_ttl),
            ))),
            config,
        }
    }

    /// 创建禁用的缓存管理器
    pub fn disabled() -> Self {
        Self {
            node_cache: Arc::new(RwLock::new(NodeCache::new(0, None))),
            adjacency_cache: Arc::new(RwLock::new(AdjacencyCache::new(0, None))),
            query_cache: Arc::new(RwLock::new(QueryCache::new(0, Default::default(), 0))),
            index_cache: Arc::new(RwLock::new(IndexCache::new(0, None))),
            config: CacheConfig { enabled: false, ..Default::default() },
        }
    }

    /// 检查缓存是否启用
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    // ========== 节点缓存操作 ==========

    /// 获取节点
    pub fn get_node(&self, id: NodeId) -> Option<StoredNode> {
        if !self.is_enabled() {
            return None;
        }
        let mut cache = self.node_cache.write().unwrap();
        cache.get(id)
    }

    /// 插入节点
    pub fn put_node(&self, id: NodeId, node: StoredNode) {
        if !self.is_enabled() {
            return;
        }
        let mut cache = self.node_cache.write().unwrap();
        cache.put(id, node);
    }

    // ========== 邻接表缓存操作 ==========

    /// 获取出边关系ID列表
    pub fn get_outgoing_ids(&self, node: NodeId) -> Option<Vec<RelId>> {
        if !self.is_enabled() {
            return None;
        }
        let mut cache = self.adjacency_cache.write().unwrap();
        cache.get_outgoing_ids(node)
    }

    /// 插入出边关系ID列表
    pub fn put_outgoing_ids(&self, node: NodeId, ids: Vec<RelId>) {
        if !self.is_enabled() {
            return;
        }
        let mut cache = self.adjacency_cache.write().unwrap();
        cache.put_outgoing_ids(node, ids);
    }

    /// 获取入边关系ID列表
    pub fn get_incoming_ids(&self, node: NodeId) -> Option<Vec<RelId>> {
        if !self.is_enabled() {
            return None;
        }
        let mut cache = self.adjacency_cache.write().unwrap();
        cache.get_incoming_ids(node)
    }

    /// 插入入边关系ID列表
    pub fn put_incoming_ids(&self, node: NodeId, ids: Vec<RelId>) {
        if !self.is_enabled() {
            return;
        }
        let mut cache = self.adjacency_cache.write().unwrap();
        cache.put_incoming_ids(node, ids);
    }

    /// 获取关系详情
    pub fn get_rel(&self, id: RelId) -> Option<StoredRel> {
        if !self.is_enabled() {
            return None;
        }
        let mut cache = self.adjacency_cache.write().unwrap();
        cache.get_rel(id)
    }

    /// 插入关系详情
    pub fn put_rel(&self, rel: StoredRel) {
        if !self.is_enabled() {
            return;
        }
        let mut cache = self.adjacency_cache.write().unwrap();
        cache.put_rel(rel);
    }

    // ========== 查询缓存操作 ==========

    /// 获取查询结果
    pub fn get_query(&self, fingerprint: &crate::cache::query_cache::QueryFingerprint) -> Option<Vec<NodeId>> {
        if !self.is_enabled() {
            return None;
        }
        let mut cache = self.query_cache.write().unwrap();
        cache.get(fingerprint)
    }

    /// 插入查询结果
    pub fn put_query(&self, fingerprint: crate::cache::query_cache::QueryFingerprint, node_ids: Vec<NodeId>) {
        if !self.is_enabled() {
            return;
        }
        let mut cache = self.query_cache.write().unwrap();
        cache.put(fingerprint, node_ids);
    }

    // ========== 索引缓存操作 ==========

    /// 获取索引结果
    pub fn get_index(&self, label: &str, prop_name: &str, value: &crate::values::Value) -> Option<Vec<NodeId>> {
        if !self.is_enabled() {
            return None;
        }
        let mut cache = self.index_cache.write().unwrap();
        cache.get(label, prop_name, value)
    }

    /// 插入索引结果
    pub fn put_index(&self, label: &str, prop_name: &str, value: &crate::values::Value, node_ids: Vec<NodeId>) {
        if !self.is_enabled() {
            return;
        }
        let mut cache = self.index_cache.write().unwrap();
        cache.put(label, prop_name, value, node_ids);
    }

    // ========== 缓存失效操作 ==========

    /// 节点创建时调用
    pub fn on_node_created(&self, _id: NodeId) {
        // 节点创建不影响现有缓存
    }

    /// 节点更新时调用
    pub fn on_node_updated(&self, id: NodeId, label: &str, props: &Properties) {
        if !self.is_enabled() {
            return;
        }

        // 失效节点缓存
        {
            let mut cache = self.node_cache.write().unwrap();
            cache.invalidate(id);
        }

        // 失效相关索引缓存
        {
            let mut cache = self.index_cache.write().unwrap();
            cache.invalidate_node(label, props);
        }

        // 失效所有查询缓存（因为查询结果可能包含此节点）
        {
            let mut cache = self.query_cache.write().unwrap();
            cache.invalidate_all();
        }
    }

    /// 节点删除时调用
    pub fn on_node_deleted(&self, id: NodeId, label: &str, props: &Properties) {
        if !self.is_enabled() {
            return;
        }

        // 失效节点缓存
        {
            let mut cache = self.node_cache.write().unwrap();
            cache.invalidate(id);
        }

        // 失效邻接表缓存
        {
            let mut cache = self.adjacency_cache.write().unwrap();
            cache.invalidate_node(id);
        }

        // 失效索引缓存
        {
            let mut cache = self.index_cache.write().unwrap();
            cache.invalidate_node(label, props);
        }

        // 失效所有查询缓存
        {
            let mut cache = self.query_cache.write().unwrap();
            cache.invalidate_all();
        }
    }

    /// 关系创建时调用
    pub fn on_rel_created(&self, _id: RelId, start: NodeId, end: NodeId) {
        if !self.is_enabled() {
            return;
        }

        // 失效邻接表缓存
        {
            let mut cache = self.adjacency_cache.write().unwrap();
            cache.invalidate_rel_nodes(start, end);
        }

        // 失效所有查询缓存
        {
            let mut cache = self.query_cache.write().unwrap();
            cache.invalidate_all();
        }
    }

    /// 关系删除时调用
    pub fn on_rel_deleted(&self, id: RelId, start: NodeId, end: NodeId) {
        if !self.is_enabled() {
            return;
        }

        // 失效关系详情
        {
            let mut cache = self.adjacency_cache.write().unwrap();
            cache.invalidate_rel(id);
            cache.invalidate_rel_nodes(start, end);
        }

        // 失效所有查询缓存
        {
            let mut cache = self.query_cache.write().unwrap();
            cache.invalidate_all();
        }
    }

    // ========== 统计和监控 ==========

    /// 获取整体报告
    pub fn overall_report(&self) -> OverallCacheReport {
        let node_cache = self.node_cache.read().unwrap();
        let adjacency_cache = self.adjacency_cache.read().unwrap();
        let query_cache = self.query_cache.read().unwrap();
        let index_cache = self.index_cache.read().unwrap();

        OverallCacheReport::from_stats(
            &node_cache.stats(),
            &adjacency_cache.stats(),
            &query_cache.stats(),
            &index_cache.stats(),
        )
    }

    /// 清空所有缓存
    pub fn clear_all(&self) {
        if !self.is_enabled() {
            return;
        }

        {
            let mut cache = self.node_cache.write().unwrap();
            cache.clear();
        }
        {
            let mut cache = self.adjacency_cache.write().unwrap();
            cache.clear();
        }
        {
            let mut cache = self.query_cache.write().unwrap();
            cache.clear();
        }
        {
            let mut cache = self.index_cache.write().unwrap();
            cache.clear();
        }
    }

    /// 清理过期条目
    pub fn cleanup_expired(&self) {
        if !self.is_enabled() {
            return;
        }

        {
            let mut cache = self.query_cache.write().unwrap();
            cache.cleanup_expired();
        }
    }

    /// 克隆缓存管理器
    pub fn clone(&self) -> Self {
        Self {
            node_cache: Arc::clone(&self.node_cache),
            adjacency_cache: Arc::clone(&self.adjacency_cache),
            query_cache: Arc::clone(&self.query_cache),
            index_cache: Arc::clone(&self.index_cache),
            config: self.config.clone(),
        }
    }
}

impl Clone for CacheManager {
    fn clone(&self) -> Self {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::values::Value;

    #[test]
    fn test_cache_manager_disabled() {
        let manager = CacheManager::disabled();

        assert!(!manager.is_enabled());
        assert_eq!(manager.get_node(1), None);
    }

    #[test]
    fn test_cache_manager_enabled() {
        let config = CacheConfig::default();
        let manager = CacheManager::new(config);

        assert!(manager.is_enabled());
    }

    #[test]
    fn test_node_operations() {
        let manager = CacheManager::new(CacheConfig::default());

        let node = StoredNode {
            id: 1,
            labels: vec!["User".to_string()],
            props: Properties::new(),
        };

        manager.put_node(1, node.clone());
        assert_eq!(manager.get_node(1), Some(node));
    }

    #[test]
    fn test_invalidation() {
        let manager = CacheManager::new(CacheConfig::default());

        let node = StoredNode {
            id: 1,
            labels: vec!["User".to_string()],
            props: {
                let mut props = Properties::new();
                props.insert("name".to_string(), Value::Text("Alice".to_string()));
                props
            },
        };

        manager.put_node(1, node);

        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Alice".to_string()));

        manager.on_node_updated(1, "User", &props);

        assert_eq!(manager.get_node(1), None);
    }

    #[test]
    fn test_clear_all() {
        let manager = CacheManager::new(CacheConfig::default());

        let node = StoredNode {
            id: 1,
            labels: vec!["User".to_string()],
            props: Properties::new(),
        };

        manager.put_node(1, node);
        manager.put_outgoing_ids(1, vec![2, 3]);
        manager.put_query(
            crate::cache::query_cache::QueryFingerprint::label_query("User"),
            vec![1, 2, 3],
        );

        manager.clear_all();

        assert_eq!(manager.get_node(1), None);
        assert_eq!(manager.get_outgoing_ids(1), None);
    }
}
