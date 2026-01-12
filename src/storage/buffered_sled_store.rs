//! 带写缓冲的 Sled 存储引擎
//!
//! 结合 SledStore 的持久化能力和写缓冲的批量优化，提供更高的写入性能。

use super::{NodeId, RelId, StoredNode, StoredRel, StorageEngine};
use super::sled_store::SledStore;
use crate::values::Value;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// 写缓冲配置
#[derive(Debug, Clone)]
pub struct BufferConfig {
    /// 缓冲区最大大小（节点和关系的总和）
    pub max_buffer_size: usize,

    /// 刷盘间隔（毫秒）
    pub flush_interval_ms: u64,

    /// 刷盘阈值（达到此大小立即刷盘）
    pub flush_threshold: usize,

    /// 是否使用异步刷盘
    pub async_flush: bool,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            max_buffer_size: 1000,
            flush_interval_ms: 1000,
            flush_threshold: 500,
            async_flush: true,
        }
    }
}

impl BufferConfig {
    /// 高性能配置（更大的缓冲区，更长的间隔）
    pub fn high_performance() -> Self {
        Self {
            max_buffer_size: 10_000,
            flush_interval_ms: 5000,
            flush_threshold: 5000,
            async_flush: true,
        }
    }

    /// 低延迟配置（更小的缓冲区，更短的间隔）
    pub fn low_latency() -> Self {
        Self {
            max_buffer_size: 100,
            flush_interval_ms: 100,
            flush_threshold: 50,
            async_flush: false,
        }
    }
}

/// 待写入的节点数据
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingNode {
    id: NodeId,
    labels: Vec<String>,
    props: HashMap<String, Value>,
}

/// 待写入的关系数据
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
    /// 待写入的节点
    pending_nodes: HashMap<NodeId, PendingNode>,

    /// 待写入的关系
    pending_rels: HashMap<RelId, PendingRel>,

    /// 待删除的节点
    deleted_nodes: HashSet<NodeId>,

    /// 待删除的关系
    deleted_rels: HashSet<RelId>,

    /// 配置
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

    /// 添加待写入的节点
    fn push_node(&mut self, node: PendingNode) {
        // 移除删除标记（如果存在）
        self.deleted_nodes.remove(&node.id);
        self.pending_nodes.insert(node.id, node);
    }

    /// 添加待写入的关系
    fn push_rel(&mut self, rel: PendingRel) {
        self.deleted_rels.remove(&rel.id);
        self.pending_rels.insert(rel.id, rel);
    }

    /// 标记节点待删除
    fn mark_delete_node(&mut self, id: NodeId) {
        self.pending_nodes.remove(&id);
        self.deleted_nodes.insert(id);
    }

    /// 标记关系待删除
    fn mark_delete_rel(&mut self, id: RelId) {
        self.pending_rels.remove(&id);
        self.deleted_rels.insert(id);
    }

    /// 检查是否需要刷盘
    fn should_flush(&self) -> bool {
        let total_size = self.pending_nodes.len() + self.pending_rels.len();
        total_size >= self.config.flush_threshold ||
        !self.deleted_nodes.is_empty() ||
        !self.deleted_rels.is_empty()
    }

    /// 缓冲区大小
    fn size(&self) -> usize {
        self.pending_nodes.len() + self.pending_rels.len()
    }

    /// 是否为空
    fn is_empty(&self) -> bool {
        self.pending_nodes.is_empty() &&
        self.pending_rels.is_empty() &&
        self.deleted_nodes.is_empty() &&
        self.deleted_rels.is_empty()
    }

    /// 清空缓冲区
    fn clear(&mut self) {
        self.pending_nodes.clear();
        self.pending_rels.clear();
        self.deleted_nodes.clear();
        self.deleted_rels.clear();
    }
}

/// 带写缓冲的 Sled 存储引擎
pub struct BufferedSledStore {
    /// 底层 Sled 存储
    sled_store: SledStore,

    /// 写缓冲
    buffer: Arc<Mutex<WriteBuffer>>,

    /// 是否已停止
    stopped: Arc<Mutex<bool>>,
}

impl BufferedSledStore {
    /// 创建新的 BufferedSledStore
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, sled::Error> {
        Self::with_config(path, Vec::new(), BufferConfig::default())
    }

    /// 使用配置创建
    pub fn with_config<P: AsRef<Path>>(
        path: P,
        indexed_properties: Vec<(String, String)>,
        buffer_config: BufferConfig,
    ) -> Result<Self, sled::Error> {
        let sled_store = SledStore::with_config(path, indexed_properties)?;
        let buffer_config_clone = buffer_config.clone();
        let buffer = Arc::new(Mutex::new(WriteBuffer::new(buffer_config)));
        let stopped = Arc::new(Mutex::new(false));

        let store = Self {
            sled_store,
            buffer,
            stopped,
        };

        // 启动后台刷盘任务
        if buffer_config_clone.async_flush {
            store.start_flush_task();
        }

        Ok(store)
    }

    /// 启动后台刷盘任务
    fn start_flush_task(&self) {
        use std::thread;

        let buffer = Arc::clone(&self.buffer);
        let stopped = Arc::clone(&self.stopped);
        let interval_ms = {
            let buf = buffer.lock().unwrap();
            buf.config.flush_interval_ms
        };

        thread::spawn(move || {
            while !*stopped.lock().unwrap() {
                thread::sleep(Duration::from_millis(interval_ms));

                let should_flush = {
                    let buf = buffer.lock().unwrap();
                    buf.should_flush()
                };

                if should_flush {
                    // 这里需要刷盘，但由于我们只有不可变引用，
                    // 实际的刷盘逻辑会在下次可变访问时执行
                    // 这是一个简化实现，生产环境可以使用条件变量或其他同步机制
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

    /// 获取缓冲区统计信息
    pub fn buffer_stats(&self) -> BufferStats {
        let buffer = self.buffer.lock().unwrap();
        BufferStats {
            pending_nodes: buffer.pending_nodes.len(),
            pending_rels: buffer.pending_rels.len(),
            deleted_nodes: buffer.deleted_nodes.len(),
            deleted_rels: buffer.deleted_rels.len(),
            total_size: buffer.size(),
        }
    }
}

/// 缓冲区统计信息
#[derive(Debug, Clone)]
pub struct BufferStats {
    /// 待写入的节点数
    pub pending_nodes: usize,

    /// 待写入的关系数
    pub pending_rels: usize,

    /// 待删除的节点数
    pub deleted_nodes: usize,

    /// 待删除的关系数
    pub deleted_rels: usize,

    /// 总大小
    pub total_size: usize,
}

impl Drop for BufferedSledStore {
    fn drop(&mut self) {
        *self.stopped.lock().unwrap() = true;
        // 刷盘剩余数据
        self.flush_to_sled();
    }
}

impl StorageEngine for BufferedSledStore {
    fn create_node(
        &mut self,
        labels: Vec<String>,
        props: HashMap<String, Value>,
    ) -> NodeId {
        // 预分配 ID
        let id = {
            let mut buffer = self.buffer.lock().unwrap();
            let id = buffer.pending_nodes.len() as NodeId;
            let node = PendingNode { id, labels, props };
            buffer.push_node(node);

            // 检查是否需要刷盘
            if buffer.should_flush() {
                drop(buffer);
                self.flush_to_sled();
            }

            id
        };

        id
    }

    fn create_rel(
        &mut self,
        start: NodeId,
        end: NodeId,
        typ: String,
        props: HashMap<String, Value>,
    ) -> RelId {
        let id = {
            let mut buffer = self.buffer.lock().unwrap();
            let id = buffer.pending_rels.len() as RelId;
            let rel = PendingRel { id, start, end, typ, props };
            buffer.push_rel(rel);

            if buffer.should_flush() {
                drop(buffer);
                self.flush_to_sled();
            }

            id
        };

        id
    }

    fn get_node(&self, id: NodeId) -> Option<StoredNode> {
        // 先查缓冲区
        {
            let buffer = self.buffer.lock().unwrap();
            if let Some(node) = buffer.pending_nodes.get(&id) {
                return Some(StoredNode {
                    id: node.id,
                    labels: node.labels.clone(),
                    props: node.props.clone(),
                });
            }
            // 检查是否待删除
            if buffer.deleted_nodes.contains(&id) {
                return None;
            }
        }

        // 缓冲区未命中，查 sled
        self.sled_store.get_node(id)
    }

    fn get_rel(&self, id: RelId) -> Option<StoredRel> {
        // 先查缓冲区
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

        self.sled_store.get_rel(id)
    }

    fn all_nodes(&self) -> Box<dyn Iterator<Item = StoredNode> + '_> {
        // 先刷盘以确保数据一致性
        // 注意：这会消耗 &mut self，但我们的签名是 &self
        // 这是一个简化实现，实际使用时建议先调用 flush()
        self.sled_store.all_nodes()
    }

    fn outgoing_rels(&self, node: NodeId) -> Box<dyn Iterator<Item = StoredRel> + '_> {
        self.sled_store.outgoing_rels(node)
    }

    fn incoming_rels(&self, node: NodeId) -> Box<dyn Iterator<Item = StoredRel> + '_> {
        self.sled_store.incoming_rels(node)
    }

    fn delete_node(&mut self, id: NodeId) -> bool {
        let mut buffer = self.buffer.lock().unwrap();
        buffer.mark_delete_node(id);
        if buffer.should_flush() {
            drop(buffer);
            self.flush_to_sled();
        }
        true
    }

    fn delete_rel(&mut self, id: RelId) -> bool {
        let mut buffer = self.buffer.lock().unwrap();
        buffer.mark_delete_rel(id);
        if buffer.should_flush() {
            drop(buffer);
            self.flush_to_sled();
        }
        true
    }

    fn batch_create_nodes(
        &mut self,
        nodes: Vec<(Vec<String>, HashMap<String, Value>)>,
    ) -> Vec<NodeId> {
        let mut buffer = self.buffer.lock().unwrap();

        let start_id = buffer.pending_nodes.len() as NodeId;
        let count = nodes.len() as NodeId;

        for (i, (labels, props)) in nodes.into_iter().enumerate() {
            let id = start_id + i as NodeId;
            let node = PendingNode { id, labels, props };
            buffer.push_node(node);
        }

        // 批量操作自动刷盘
        if buffer.should_flush() {
            drop(buffer);
            self.flush_to_sled();
        }

        (start_id..start_id + count).collect()
    }

    fn batch_create_rels(
        &mut self,
        rels: Vec<(NodeId, NodeId, String, HashMap<String, Value>)>,
    ) -> Vec<RelId> {
        let mut buffer = self.buffer.lock().unwrap();

        let start_id = buffer.pending_rels.len() as RelId;
        let count = rels.len() as RelId;

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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::collections::HashMap;

    #[test]
    fn test_buffered_store_create() {
        let temp_dir = TempDir::new().unwrap();
        let store = BufferedSledStore::new(temp_dir.path()).unwrap();
        assert!(store.buffer_stats().total_size == 0);
    }

    #[test]
    fn test_buffered_store_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let mut store = BufferedSledStore::new(temp_dir.path()).unwrap();

        // 创建节点
        let id = store.create_node(
            vec!["Person".to_string()],
            HashMap::new(),
        );

        // 刷盘
        store.flush().unwrap();

        // 读取节点
        let node = store.get_node(id);
        assert!(node.is_some());
        assert_eq!(node.unwrap().id, id);
    }

    #[test]
    fn test_buffered_store_batch_create() {
        let temp_dir = TempDir::new().unwrap();
        let mut store = BufferedSledStore::new(temp_dir.path()).unwrap();

        let nodes = vec![
            (vec!["Person".to_string()], HashMap::new()),
            (vec!["Person".to_string()], HashMap::new()),
            (vec!["Person".to_string()], HashMap::new()),
        ];

        let ids = store.batch_create_nodes(nodes);
        assert_eq!(ids.len(), 3);

        store.flush().unwrap();

        for id in ids {
            assert!(store.get_node(id).is_some());
        }
    }

    #[test]
    fn test_buffer_stats() {
        let temp_dir = TempDir::new().unwrap();
        let config = BufferConfig {
            max_buffer_size: 100,
            flush_interval_ms: 1000,
            flush_threshold: 50,
            async_flush: false,
        };

        let mut store = BufferedSledStore::with_config(
            temp_dir.path(),
            Vec::new(),
            config,
        ).unwrap();

        // 创建一些节点
        for _ in 0..10 {
            store.create_node(vec!["Person".to_string()], HashMap::new());
        }

        let stats = store.buffer_stats();
        assert_eq!(stats.pending_nodes, 10);
    }

    #[test]
    fn test_auto_flush() {
        let temp_dir = TempDir::new().unwrap();
        let config = BufferConfig {
            max_buffer_size: 100,
            flush_interval_ms: 1000,
            flush_threshold: 5, // 低阈值触发自动刷盘
            async_flush: false,
        };

        let mut store = BufferedSledStore::with_config(
            temp_dir.path(),
            Vec::new(),
            config,
        ).unwrap();

        // 创建超过阈值的节点
        for _ in 0..10 {
            store.create_node(vec!["Person".to_string()], HashMap::new());
        }

        // 应该已经自动刷盘
        let stats = store.buffer_stats();
        assert_eq!(stats.pending_nodes, 0);
    }
}
