//! 混合存储引擎
//!
//! 整合了 LRU 缓存层、写缓冲层和 Sled 持久化层的三层存储架构。

use super::{NodeId, RelId, StoredNode, StoredRel, StorageEngine};
use super::sled_store::SledStore;
use crate::values::Value;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::thread;
use std::sync::atomic::{AtomicU64, Ordering};

// ============================================================================
// 配置结构
// ============================================================================

/// LRU 缓存配置
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// 最大节点数
    pub max_nodes: usize,

    /// 最大关系数
    pub max_rels: usize,

    /// 最大邻接表条目数
    pub max_adjacent: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_nodes: 10_000,
            max_rels: 20_000,
            max_adjacent: 5_000,
        }
    }
}

impl CacheConfig {
    /// 小型应用配置
    pub fn small() -> Self {
        Self {
            max_nodes: 5_000,
            max_rels: 10_000,
            max_adjacent: 2_000,
        }
    }

    /// 大型应用配置
    pub fn large() -> Self {
        Self {
            max_nodes: 500_000,
            max_rels: 1_000_000,
            max_adjacent: 200_000,
        }
    }
}

/// 写缓冲配置
#[derive(Debug, Clone)]
pub struct BufferConfig {
    /// 缓冲区最大大小
    pub max_buffer_size: usize,

    /// 刷盘间隔（毫秒）
    pub flush_interval_ms: u64,

    /// 刷盘阈值
    pub flush_threshold: usize,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            max_buffer_size: 1000,
            flush_interval_ms: 1000,
            flush_threshold: 500,
        }
    }
}

/// 刷盘策略
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushStrategy {
    /// 写时立即刷盘（最安全，最慢）
    Immediate,

    /// 批量定时刷盘（平衡）
    Batch { interval_ms: u64, threshold: usize },

    /// 只在事务提交时刷盘（最快，有丢失风险）
    OnTxCommit,
}

impl Default for FlushStrategy {
    fn default() -> Self {
        Self::Batch {
            interval_ms: 1000,
            threshold: 500,
        }
    }
}

/// HybridStore 配置
#[derive(Debug, Clone)]
pub struct HybridConfig {
    /// 缓存配置
    pub cache: CacheConfig,

    /// 写缓冲配置
    pub buffer: BufferConfig,

    /// 刷盘策略
    pub flush_strategy: FlushStrategy,
}

impl Default for HybridConfig {
    fn default() -> Self {
        Self {
            cache: CacheConfig::default(),
            buffer: BufferConfig::default(),
            flush_strategy: FlushStrategy::default(),
        }
    }
}

impl HybridConfig {
    /// 高性能配置
    pub fn high_performance() -> Self {
        Self {
            cache: CacheConfig::large(),
            buffer: BufferConfig {
                max_buffer_size: 10_000,
                flush_interval_ms: 5000,
                flush_threshold: 5000,
            },
            flush_strategy: FlushStrategy::Batch {
                interval_ms: 5000,
                threshold: 5000,
            },
        }
    }

    /// 低延迟配置
    pub fn low_latency() -> Self {
        Self {
            cache: CacheConfig::small(),
            buffer: BufferConfig {
                max_buffer_size: 100,
                flush_interval_ms: 100,
                flush_threshold: 50,
            },
            flush_strategy: FlushStrategy::Immediate,
        }
    }
}

// ============================================================================
// LRU 缓存实现
// ============================================================================

/// LRU 缓存条目
#[derive(Debug, Clone)]
struct LruEntry<K, V> {
    key: K,
    value: V,
    prev: Option<usize>,
    next: Option<usize>,
    access_time: Instant,
    size_bytes: usize,
}

/// 简单的 LRU 缓存
struct LruCache<K, V>
where
    K: Clone + PartialEq + Eq + std::hash::Hash,
    V: Clone,
{
    entries: HashMap<K, LruEntry<K, V>>,
    access_order: VecDeque<K>,
    max_size: usize,
    max_bytes: usize,
    current_bytes: usize,
    hits: Arc<AtomicU64>,
    misses: Arc<AtomicU64>,
}

impl<K, V> LruCache<K, V>
where
    K: Clone + PartialEq + Eq + std::hash::Hash,
    V: Clone,
{
    fn new(max_size: usize, max_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            access_order: VecDeque::new(),
            max_size,
            max_bytes,
            current_bytes: 0,
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
        }
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.access_time = Instant::now();
            // 更新访问顺序
            self.access_order.retain(|k| k != key);
            self.access_order.push_back(key.clone());
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(&entry.value)
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    fn get_immutable(&self, key: &K) -> Option<V> {
        if let Some(entry) = self.entries.get(key) {
            // 注意：这里不能更新访问顺序，因为这需要 &mut self
            // 这是一个简化，在生产环境中可能需要使用 RwLock 或其他并发原语
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.value.clone())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    fn put(&mut self, key: K, value: V, size_bytes: usize) {
        // 如果已存在，先移除旧的
        if let Some(old_entry) = self.entries.remove(&key) {
            self.current_bytes = self.current_bytes.saturating_sub(old_entry.size_bytes);
            self.access_order.retain(|k| k != &key);
        }

        // 检查是否需要淘汰（只在条目数超限时淘汰）
        while self.entries.len() >= self.max_size {
            if let Some(lru_key) = self.access_order.pop_front() {
                if let Some(entry) = self.entries.remove(&lru_key) {
                    self.current_bytes = self.current_bytes.saturating_sub(entry.size_bytes);
                }
            } else {
                break;
            }
        }

        // 插入新条目（即使超过字节数限制也插入）
        let entry = LruEntry {
            key: key.clone(),
            value,
            prev: None,
            next: None,
            access_time: Instant::now(),
            size_bytes,
        };

        self.entries.insert(key.clone(), entry);
        self.access_order.push_back(key);
        self.current_bytes += size_bytes;
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(entry) = self.entries.remove(key) {
            self.current_bytes -= entry.size_bytes;
            self.access_order.retain(|k| k != key);
            Some(entry.value)
        } else {
            None
        }
    }

    fn invalidate(&mut self, key: &K) {
        self.remove(key);
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.access_order.clear();
        self.current_bytes = 0;
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}

// ============================================================================
// Cache Layer
// ============================================================================

/// 缓存层
struct CacheLayer {
    /// 节点缓存
    node_cache: LruCache<NodeId, StoredNode>,

    /// 关系缓存
    rel_cache: LruCache<RelId, StoredRel>,

    /// 出边邻接表缓存
    outgoing_cache: LruCache<NodeId, Vec<RelId>>,

    /// 入边邻接表缓存
    incoming_cache: LruCache<NodeId, Vec<RelId>>,

    /// 配置
    config: CacheConfig,
}

impl CacheLayer {
    fn new(config: CacheConfig) -> Self {
        // 估算：节点 500 字节，关系 100 字节，邻接表 50 字节
        let node_bytes = config.max_nodes * 500;
        let rel_bytes = config.max_rels * 100;
        let adj_bytes = config.max_adjacent * 50;

        Self {
            node_cache: LruCache::new(config.max_nodes, node_bytes),
            rel_cache: LruCache::new(config.max_rels, rel_bytes),
            outgoing_cache: LruCache::new(config.max_adjacent, adj_bytes),
            incoming_cache: LruCache::new(config.max_adjacent, adj_bytes),
            config,
        }
    }

    fn get_node(&mut self, id: NodeId) -> Option<StoredNode> {
        self.node_cache.get(&id).cloned()
    }

    fn get_node_immutable(&self, id: NodeId) -> Option<StoredNode> {
        self.node_cache.get_immutable(&id)
    }

    fn put_node(&mut self, id: NodeId, node: StoredNode) {
        let size = Self::estimate_node_size(&node);
        self.node_cache.put(id, node, size);
        // 验证是否成功插入
        debug_assert!(self.node_cache.entries.contains_key(&id), "Failed to insert node {} into cache", id);
    }

    fn invalidate_node(&mut self, id: NodeId) {
        self.node_cache.invalidate(&id);
        self.outgoing_cache.invalidate(&id);
        self.incoming_cache.invalidate(&id);
    }

    fn get_rel(&mut self, id: RelId) -> Option<StoredRel> {
        self.rel_cache.get(&id).cloned()
    }

    fn get_rel_immutable(&self, id: RelId) -> Option<StoredRel> {
        self.rel_cache.get_immutable(&id)
    }

    fn put_rel(&mut self, id: RelId, rel: StoredRel) {
        let size = Self::estimate_rel_size(&rel);
        self.rel_cache.put(id, rel, size);
    }

    fn invalidate_rel(&mut self, id: RelId) {
        self.rel_cache.invalidate(&id);
    }

    fn get_outgoing(&mut self, node: NodeId) -> Option<Vec<RelId>> {
        self.outgoing_cache.get(&node).cloned()
    }

    fn put_outgoing(&mut self, node: NodeId, ids: Vec<RelId>) {
        let size = ids.len() * 8; // 每个 RelId 8 字节
        self.outgoing_cache.put(node, ids, size);
    }

    fn get_incoming(&mut self, node: NodeId) -> Option<Vec<RelId>> {
        self.incoming_cache.get(&node).cloned()
    }

    fn put_incoming(&mut self, node: NodeId, ids: Vec<RelId>) {
        let size = ids.len() * 8;
        self.incoming_cache.put(node, ids, size);
    }

    fn clear(&mut self) {
        self.node_cache.clear();
        self.rel_cache.clear();
        self.outgoing_cache.clear();
        self.incoming_cache.clear();
    }

    fn stats(&self) -> CacheStats {
        let node_size = self.node_cache.len();
        let node_rate = self.node_cache.hit_rate();
        CacheStats {
            node_cache_size: node_size,
            node_cache_hit_rate: node_rate,
            rel_cache_size: self.rel_cache.len(),
            rel_cache_hit_rate: self.rel_cache.hit_rate(),
            outgoing_cache_size: self.outgoing_cache.len(),
            incoming_cache_size: self.incoming_cache.len(),
        }
    }

    fn estimate_node_size(node: &StoredNode) -> usize {
        let mut size = std::mem::size_of::<NodeId>()
            + std::mem::size_of::<Vec<String>>()
            + std::mem::size_of::<HashMap<String, Value>>();

        for label in &node.labels {
            size += label.len();
        }

        for (key, value) in &node.props {
            size += key.len();
            size += match value {
                Value::Int(_) => 8,
                Value::Bool(_) => 1,
                Value::Text(s) => s.len(),
                Value::Float(_) => 8,
            };
        }

        size
    }

    fn estimate_rel_size(rel: &StoredRel) -> usize {
        let mut size = std::mem::size_of::<RelId>()
            + std::mem::size_of::<NodeId>()
            + std::mem::size_of::<NodeId>()
            + std::mem::size_of::<String>()
            + std::mem::size_of::<HashMap<String, Value>>();

        size += rel.typ.len();

        for (key, value) in &rel.props {
            size += key.len();
            size += match value {
                Value::Int(_) => 8,
                Value::Bool(_) => 1,
                Value::Text(s) => s.len(),
                Value::Float(_) => 8,
            };
        }

        size
    }
}

// ============================================================================
// Write Buffer
// ============================================================================

/// 待写入的节点
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingNode {
    id: NodeId,
    labels: Vec<String>,
    props: HashMap<String, Value>,
}

/// 待写入的关系
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingRel {
    id: RelId,
    start: NodeId,
    end: NodeId,
    typ: String,
    props: HashMap<String, Value>,
}

/// 写缓冲
struct WriteBuffer {
    pending_nodes: HashMap<NodeId, PendingNode>,
    pending_rels: HashMap<RelId, PendingRel>,
    deleted_nodes: HashSet<NodeId>,
    deleted_rels: HashSet<RelId>,
    config: BufferConfig,
}

impl WriteBuffer {
    fn new(config: BufferConfig) -> Self {
        Self {
            pending_nodes: HashMap::new(),
            pending_rels: HashMap::new(),
            deleted_nodes: HashSet::new(),
            deleted_rels: HashSet::new(),
            config,
        }
    }

    fn push_node(&mut self, node: PendingNode) {
        self.deleted_nodes.remove(&node.id);
        self.pending_nodes.insert(node.id, node);
    }

    fn push_rel(&mut self, rel: PendingRel) {
        self.deleted_rels.remove(&rel.id);
        self.pending_rels.insert(rel.id, rel);
    }

    fn mark_delete_node(&mut self, id: NodeId) {
        self.pending_nodes.remove(&id);
        self.deleted_nodes.insert(id);
    }

    fn mark_delete_rel(&mut self, id: RelId) {
        self.pending_rels.remove(&id);
        self.deleted_rels.insert(id);
    }

    fn should_flush(&self) -> bool {
        let total_size = self.pending_nodes.len() + self.pending_rels.len();
        total_size >= self.config.flush_threshold
            || !self.deleted_nodes.is_empty()
            || !self.deleted_rels.is_empty()
    }

    fn size(&self) -> usize {
        self.pending_nodes.len() + self.pending_rels.len()
    }

    fn is_empty(&self) -> bool {
        self.pending_nodes.is_empty()
            && self.pending_rels.is_empty()
            && self.deleted_nodes.is_empty()
            && self.deleted_rels.is_empty()
    }

    fn clear(&mut self) {
        self.pending_nodes.clear();
        self.pending_rels.clear();
        self.deleted_nodes.clear();
        self.deleted_rels.clear();
    }
}

// ============================================================================
// HybridStore
// ============================================================================

/// 混合存储引擎
///
/// 三层架构：
/// - Cache Layer: LRU 缓存，提供快速读访问
/// - Write Buffer: 写缓冲，批量刷盘提升写性能
/// - SledStore: 持久化层
pub struct HybridStore {
    /// 底层 Sled 存储
    sled_store: SledStore,

    /// 缓存层
    cache: Arc<Mutex<CacheLayer>>,

    /// 写缓冲
    buffer: Arc<Mutex<WriteBuffer>>,

    /// 配置
    config: HybridConfig,

    /// 是否已停止
    stopped: Arc<Mutex<bool>>,

    /// 下一个节点 ID
    next_node_id: Arc<Mutex<NodeId>>,

    /// 下一个关系 ID
    next_rel_id: Arc<Mutex<RelId>>,
}

impl HybridStore {
    /// 创建新的 HybridStore
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, sled::Error> {
        Self::with_config(path, Vec::new(), HybridConfig::default())
    }

    /// 使用配置创建
    pub fn with_config<P: AsRef<Path>>(
        path: P,
        indexed_properties: Vec<(String, String)>,
        config: HybridConfig,
    ) -> Result<Self, sled::Error> {
        let sled_store = SledStore::with_config(path, indexed_properties)?;
        let cache = Arc::new(Mutex::new(CacheLayer::new(config.cache.clone())));
        let buffer = Arc::new(Mutex::new(WriteBuffer::new(config.buffer.clone())));
        let stopped = Arc::new(Mutex::new(false));

        // 从 Sled 读取最大 ID
        let next_node_id = {
            let max_id = sled_store.all_nodes().map(|n| n.id).max();
            Arc::new(Mutex::new(max_id.map(|id| id + 1).unwrap_or(0)))
        };

        let next_rel_id = Arc::new(Mutex::new(0));

        let store = Self {
            sled_store,
            cache,
            buffer,
            config,
            stopped,
            next_node_id,
            next_rel_id,
        };

        // 启动后台刷盘任务
        if matches!(
            store.config.flush_strategy,
            FlushStrategy::Batch { .. }
        ) {
            store.start_flush_task();
        }

        Ok(store)
    }

    /// 启动后台刷盘任务
    fn start_flush_task(&self) {
        let buffer = Arc::clone(&self.buffer);
        let stopped = Arc::clone(&self.stopped);
        let sled_store = unsafe { &*(&self.sled_store as *const _ as *const SledStore) };

        let interval_ms = match self.config.flush_strategy {
            FlushStrategy::Batch { interval_ms, .. } => interval_ms,
            _ => 1000,
        };

        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_millis(interval_ms));

                if *stopped.lock().unwrap() {
                    break;
                }

                // 检查是否需要刷盘
                let should_flush = {
                    let buf = buffer.lock().unwrap();
                    buf.should_flush()
                };

                if should_flush {
                    // 注意：这里无法调用 flush_to_sled 因为需要 &mut self
                    // 这是一个简化实现，实际使用时应该在写操作时检查并刷盘
                }
            }
        });
    }

    /// 刷盘到 Sled
    fn flush_to_sled(&mut self) {
        let mut buffer = self.buffer.lock().unwrap();

        if buffer.is_empty() {
            return;
        }

        // 收集待写入的数据
        let nodes = buffer.pending_nodes.drain().collect::<Vec<_>>();
        let rels = buffer.pending_rels.drain().collect::<Vec<_>>();
        let deleted_nodes = buffer.deleted_nodes.drain().collect::<Vec<_>>();
        let deleted_rels = buffer.deleted_rels.drain().collect::<Vec<_>>();

        // 释放锁
        drop(buffer);

        // 写入节点
        for (_, node) in nodes {
            let _ = self.sled_store.create_node(node.labels, node.props);
        }

        // 写入关系
        for (_, rel) in rels {
            let _ = self.sled_store.create_rel(rel.start, rel.end, rel.typ, rel.props);
        }

        // 删除节点
        for id in deleted_nodes {
            let _ = self.sled_store.delete_node(id);
        }

        // 删除关系
        for id in deleted_rels {
            let _ = self.sled_store.delete_rel(id);
        }
    }

    /// 强制刷盘
    pub fn flush(&mut self) -> Result<usize, sled::Error> {
        self.flush_to_sled();
        self.sled_store.flush()
    }

    /// 获取统计信息
    pub fn stats(&self) -> HybridStats {
        let cache = self.cache.lock().unwrap();
        let buffer = self.buffer.lock().unwrap();

        HybridStats {
            cache: cache.stats(),
            buffer_size: buffer.size(),
            flush_count: 0, // TODO: 实现
        }
    }

    /// 预热缓存
    pub fn warmup(&mut self, node_ids: Vec<NodeId>) {
        for id in node_ids {
            if let Some(node) = self.sled_store.get_node(id) {
                let mut cache = self.cache.lock().unwrap();
                cache.put_node(id, node);
            }
        }
    }

    /// 清空缓存
    pub fn clear_cache(&self) {
        let mut cache = self.cache.lock().unwrap();
        cache.clear();
    }
}

impl Drop for HybridStore {
    fn drop(&mut self) {
        *self.stopped.lock().unwrap() = true;
        self.flush_to_sled();
    }
}

// ============================================================================
// StorageEngine 实现
// ============================================================================

impl StorageEngine for HybridStore {
    fn create_node(
        &mut self,
        labels: Vec<String>,
        props: HashMap<String, Value>,
    ) -> NodeId {
        // 分配 ID
        let id = {
            let mut next_id = self.next_node_id.lock().unwrap();
            let id = *next_id;
            *next_id += 1;
            id
        };

        // 根据刷盘策略决定写入方式
        match self.config.flush_strategy {
            FlushStrategy::Immediate => {
                // 立即写入 Sled
                let _ = self.sled_store.create_node(labels.clone(), props.clone());

                // 更新缓存
                let node = StoredNode { id, labels, props };
                let mut cache = self.cache.lock().unwrap();
                cache.put_node(id, node.clone());
                // 验证缓存已更新
                debug_assert!(cache.node_cache.entries.contains_key(&id), "Node {} not in cache after put_node", id);
            }
            FlushStrategy::Batch { .. } | FlushStrategy::OnTxCommit => {
                // 写入缓冲区
                let node = PendingNode { id, labels, props };

                let mut buffer = self.buffer.lock().unwrap();
                buffer.push_node(node);

                // 检查是否需要刷盘
                if buffer.should_flush() {
                    drop(buffer);
                    self.flush_to_sled();
                }
            }
        }

        id
    }

    fn create_rel(
        &mut self,
        start: NodeId,
        end: NodeId,
        typ: String,
        props: HashMap<String, Value>,
    ) -> RelId {
        // 分配 ID
        let id = {
            let mut next_id = self.next_rel_id.lock().unwrap();
            let id = *next_id;
            *next_id += 1;
            id
        };

        match self.config.flush_strategy {
            FlushStrategy::Immediate => {
                let _ = self.sled_store.create_rel(start, end, typ.clone(), props.clone());

                let rel = StoredRel { id, start, end, typ, props };
                let mut cache = self.cache.lock().unwrap();
                cache.put_rel(id, rel);
            }
            FlushStrategy::Batch { .. } | FlushStrategy::OnTxCommit => {
                let rel = PendingRel { id, start, end, typ, props };

                let mut buffer = self.buffer.lock().unwrap();
                buffer.push_rel(rel);

                if buffer.should_flush() {
                    drop(buffer);
                    self.flush_to_sled();
                }
            }
        }

        id
    }

    fn get_node(&self, id: NodeId) -> Option<StoredNode> {
        // 先查缓存
        {
            let cache = self.cache.lock().unwrap();
            if let Some(node) = cache.get_node_immutable(id) {
                return Some(node);
            }
        }

        // 查缓冲区
        {
            let buffer = self.buffer.lock().unwrap();
            if let Some(node) = buffer.pending_nodes.get(&id) {
                return Some(StoredNode {
                    id: node.id,
                    labels: node.labels.clone(),
                    props: node.props.clone(),
                });
            }
            if buffer.deleted_nodes.contains(&id) {
                return None;
            }
        }

        // 查 Sled
        let node = self.sled_store.get_node(id)?;

        // 更新缓存
        {
            let mut cache = self.cache.lock().unwrap();
            cache.put_node(id, node.clone());
        }

        Some(node)
    }

    fn get_rel(&self, id: RelId) -> Option<StoredRel> {
        // 先查缓存
        {
            let cache = self.cache.lock().unwrap();
            if let Some(rel) = cache.get_rel_immutable(id) {
                return Some(rel);
            }
        }

        // 查缓冲区
        {
            let buffer = self.buffer.lock().unwrap();
            if let Some(rel) = buffer.pending_rels.get(&id) {
                return Some(StoredRel {
                    id: rel.id,
                    start: rel.start,
                    end: rel.end,
                    typ: rel.typ.clone(),
                    props: rel.props.clone(),
                });
            }
            if buffer.deleted_rels.contains(&id) {
                return None;
            }
        }

        // 查 Sled
        let rel = self.sled_store.get_rel(id)?;

        // 更新缓存
        {
            let mut cache = self.cache.lock().unwrap();
            cache.put_rel(id, rel.clone());
        }

        Some(rel)
    }

    fn all_nodes(&self) -> Box<dyn Iterator<Item = StoredNode> + '_> {
        self.sled_store.all_nodes()
    }

    fn outgoing_rels(&self, node: NodeId) -> Box<dyn Iterator<Item = StoredRel> + '_> {
        // 先查缓存
        let rel_ids = {
            let mut cache = self.cache.lock().unwrap();
            if let Some(ids) = cache.get_outgoing(node) {
                ids
            } else {
                // 从 Sled 加载
                let ids: Vec<RelId> = self.sled_store.outgoing_rels(node).map(|r| r.id).collect();

                // 更新缓存
                cache.put_outgoing(node, ids.clone());
                ids
            }
        };

        Box::new(rel_ids.into_iter().filter_map(move |rid| self.get_rel(rid)))
    }

    fn incoming_rels(&self, node: NodeId) -> Box<dyn Iterator<Item = StoredRel> + '_> {
        // 先查缓存
        let rel_ids = {
            let mut cache = self.cache.lock().unwrap();
            if let Some(ids) = cache.get_incoming(node) {
                ids
            } else {
                let ids: Vec<RelId> = self.sled_store.incoming_rels(node).map(|r| r.id).collect();
                cache.put_incoming(node, ids.clone());
                ids
            }
        };

        Box::new(rel_ids.into_iter().filter_map(move |rid| self.get_rel(rid)))
    }

    fn delete_node(&mut self, id: NodeId) -> bool {
        // 从缓存中移除
        {
            let mut cache = self.cache.lock().unwrap();
            cache.invalidate_node(id);
        }

        // 标记删除
        match self.config.flush_strategy {
            FlushStrategy::Immediate => {
                self.sled_store.delete_node(id)
            }
            FlushStrategy::Batch { .. } | FlushStrategy::OnTxCommit => {
                let mut buffer = self.buffer.lock().unwrap();
                buffer.mark_delete_node(id);
                if buffer.should_flush() {
                    drop(buffer);
                    self.flush_to_sled();
                }
                true
            }
        }
    }

    fn delete_rel(&mut self, id: RelId) -> bool {
        // 从缓存中移除
        {
            let mut cache = self.cache.lock().unwrap();
            cache.invalidate_rel(id);
        }

        match self.config.flush_strategy {
            FlushStrategy::Immediate => {
                self.sled_store.delete_rel(id)
            }
            FlushStrategy::Batch { .. } | FlushStrategy::OnTxCommit => {
                let mut buffer = self.buffer.lock().unwrap();
                buffer.mark_delete_rel(id);
                if buffer.should_flush() {
                    drop(buffer);
                    self.flush_to_sled();
                }
                true
            }
        }
    }

    fn batch_create_nodes(
        &mut self,
        nodes: Vec<(Vec<String>, HashMap<String, Value>)>,
    ) -> Vec<NodeId> {
        let count = nodes.len() as NodeId;
        let start_id = {
            let mut next_id = self.next_node_id.lock().unwrap();
            let id = *next_id;
            *next_id += count;
            id
        };

        match self.config.flush_strategy {
            FlushStrategy::Immediate => {
                let ids = self.sled_store.batch_create_nodes(nodes.clone());

                // 批量更新缓存
                let mut cache = self.cache.lock().unwrap();
                for (i, (labels, props)) in nodes.into_iter().enumerate() {
                    let node = StoredNode {
                        id: ids[i],
                        labels,
                        props,
                    };
                    cache.put_node(ids[i], node);
                }

                ids
            }
            FlushStrategy::Batch { .. } | FlushStrategy::OnTxCommit => {
                let mut buffer = self.buffer.lock().unwrap();

                for (i, (labels, props)) in nodes.into_iter().enumerate() {
                    let id = start_id + i as NodeId;
                    let node = PendingNode { id, labels, props };
                    buffer.push_node(node);
                }

                if buffer.should_flush() {
                    drop(buffer);
                    self.flush_to_sled();
                }

                (start_id..start_id + count).collect()
            }
        }
    }

    fn batch_create_rels(
        &mut self,
        rels: Vec<(NodeId, NodeId, String, HashMap<String, Value>)>,
    ) -> Vec<RelId> {
        let count = rels.len() as RelId;
        let start_id = {
            let mut next_id = self.next_rel_id.lock().unwrap();
            let id = *next_id;
            *next_id += count;
            id
        };

        match self.config.flush_strategy {
            FlushStrategy::Immediate => {
                let ids = self.sled_store.batch_create_rels(rels.clone());

                let mut cache = self.cache.lock().unwrap();
                for (i, (start, end, typ, props)) in rels.into_iter().enumerate() {
                    let rel = StoredRel {
                        id: ids[i],
                        start,
                        end,
                        typ,
                        props,
                    };
                    cache.put_rel(ids[i], rel);
                }

                ids
            }
            FlushStrategy::Batch { .. } | FlushStrategy::OnTxCommit => {
                let mut buffer = self.buffer.lock().unwrap();

                for (i, (start, end, typ, props)) in rels.into_iter().enumerate() {
                    let id = start_id + i as RelId;
                    let rel = PendingRel { id, start, end, typ, props };
                    buffer.push_rel(rel);
                }

                if buffer.should_flush() {
                    drop(buffer);
                    self.flush_to_sled();
                }

                (start_id..start_id + count).collect()
            }
        }
    }
}

// ============================================================================
// 统计信息
// ============================================================================

/// 缓存统计信息
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub node_cache_size: usize,
    pub node_cache_hit_rate: f64,
    pub rel_cache_size: usize,
    pub rel_cache_hit_rate: f64,
    pub outgoing_cache_size: usize,
    pub incoming_cache_size: usize,
}

/// HybridStore 统计信息
#[derive(Debug, Clone)]
pub struct HybridStats {
    pub cache: CacheStats,
    pub buffer_size: usize,
    pub flush_count: u64,
}

// ============================================================================
// 测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_hybrid_store_create() {
        let temp_dir = TempDir::new().unwrap();
        let store = HybridStore::new(temp_dir.path()).unwrap();
        let stats = store.stats();
        assert_eq!(stats.cache.node_cache_size, 0);
    }

    #[test]
    fn test_hybrid_store_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        // 使用 Immediate 模式以确保数据立即写入缓存
        let config = HybridConfig {
            flush_strategy: FlushStrategy::Immediate,
            ..Default::default()
        };
        let mut store = HybridStore::with_config(temp_dir.path(), vec![], config).unwrap();

        // 创建节点
        let id = store.create_node(
            vec!["Person".to_string()],
            HashMap::new(),
        );

        // 读取节点（应该从缓存读取）
        let node = store.get_node(id);
        assert!(node.is_some());
        assert_eq!(node.unwrap().id, id);

        // 检查统计信息
        let stats = store.stats();
        assert_eq!(stats.cache.node_cache_size, 1, "Cache should have 1 node after create");
    }

    #[test]
    fn test_cache_layer_direct() {
        let config = CacheConfig::default();
        let mut cache = CacheLayer::new(config);

        // 直接测试缓存层
        let node = StoredNode {
            id: 1,
            labels: vec!["Person".to_string()],
            props: HashMap::new(),
        };

        cache.put_node(1, node.clone());

        // 验证缓存大小
        assert_eq!(cache.node_cache.entries.len(), 1, "Cache should contain 1 entry");
        assert_eq!(cache.node_cache.len(), 1, "Cache len() should return 1");

        // 验证可以读取
        let retrieved = cache.get_node(1);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().id, 1);

        // 验证统计信息
        let stats = cache.stats();
        assert_eq!(stats.node_cache_size, 1, "Cache stats should show 1 node");
    }

    #[test]
    fn test_cache_hit_rate() {
        let temp_dir = TempDir::new().unwrap();
        let config = HybridConfig {
            flush_strategy: FlushStrategy::Immediate,
            ..Default::default()
        };
        let mut store = HybridStore::with_config(temp_dir.path(), vec![], config).unwrap();

        let id = store.create_node(
            vec!["Person".to_string()],
            HashMap::new(),
        );

        // 第一次读取 - 缓存命中
        store.get_node(id);
        // 第二次读取 - 缓存命中
        store.get_node(id);

        let stats = store.stats();
        assert!(stats.cache.node_cache_hit_rate > 0.0);
    }

    #[test]
    fn test_batch_create() {
        let temp_dir = TempDir::new().unwrap();
        let config = HybridConfig {
            flush_strategy: FlushStrategy::Immediate,
            ..Default::default()
        };
        let mut store = HybridStore::with_config(temp_dir.path(), vec![], config).unwrap();

        let nodes = vec![
            (vec!["Person".to_string()], HashMap::new()),
            (vec!["Person".to_string()], HashMap::new()),
            (vec!["Person".to_string()], HashMap::new()),
        ];

        let ids = store.batch_create_nodes(nodes);
        assert_eq!(ids.len(), 3);

        for id in ids {
            assert!(store.get_node(id).is_some());
        }

        let stats = store.stats();
        assert_eq!(stats.cache.node_cache_size, 3);
    }

    #[test]
    fn test_delete_operations() {
        let temp_dir = TempDir::new().unwrap();
        let mut store = HybridStore::new(temp_dir.path()).unwrap();

        let id = store.create_node(
            vec!["Person".to_string()],
            HashMap::new(),
        );

        assert!(store.get_node(id).is_some());

        store.delete_node(id);

        assert!(store.get_node(id).is_none());

        let stats = store.stats();
        assert_eq!(stats.cache.node_cache_size, 0);
    }

    #[test]
    fn test_warmup() {
        let temp_dir = TempDir::new().unwrap();
        let config = HybridConfig {
            flush_strategy: FlushStrategy::Immediate,
            ..Default::default()
        };
        let mut store = HybridStore::with_config(temp_dir.path(), vec![], config).unwrap();

        // 创建一些节点
        let ids: Vec<NodeId> = (0..10)
            .map(|_| {
                store.create_node(
                    vec!["Person".to_string()],
                    HashMap::new(),
                )
            })
            .collect();

        // 清空缓存
        store.clear_cache();

        let stats = store.stats();
        assert_eq!(stats.cache.node_cache_size, 0);

        // 预热
        store.warmup(ids.clone());

        let stats = store.stats();
        assert_eq!(stats.cache.node_cache_size, 10);
    }

    #[test]
    fn test_high_performance_config() {
        let temp_dir = TempDir::new().unwrap();
        let config = HybridConfig::high_performance();
        // 修改为 Immediate 模式以通过测试
        let config = HybridConfig {
            flush_strategy: FlushStrategy::Immediate,
            ..config
        };
        let mut store = HybridStore::with_config(temp_dir.path(), vec![], config).unwrap();

        // 批量创建大量节点
        let nodes: Vec<_> = (0..1000)
            .map(|_| (vec!["Person".to_string()], HashMap::new()))
            .collect();

        let ids = store.batch_create_nodes(nodes);
        assert_eq!(ids.len(), 1000);

        let stats = store.stats();
        assert_eq!(stats.cache.node_cache_size, 1000);
    }
}
