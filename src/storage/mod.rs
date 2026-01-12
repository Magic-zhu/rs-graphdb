pub mod mem_store;
pub mod sled_store;
pub mod buffered_sled_store;
pub mod hybrid_store;
pub mod async_store;

pub use async_store::AsyncStorage;
pub use buffered_sled_store::{BufferedSledStore, BufferConfig, BufferStats};
pub use hybrid_store::{HybridStore, HybridConfig, CacheConfig, FlushStrategy, HybridStats, CacheStats};

use crate::values::Value;
use std::collections::HashMap;

pub type NodeId = u64;
pub type RelId = u64;

#[derive(Debug, Clone, PartialEq)]
pub struct StoredNode {
    pub id: NodeId,
    pub labels: Vec<String>,
    pub props: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoredRel {
    pub id: RelId,
    pub start: NodeId,
    pub end: NodeId,
    pub typ: String,
    pub props: HashMap<String, Value>,
}

#[derive(Debug)]
pub enum StorageError {
    TxNotSupported,
    Other(String),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct TxHandle(u64);

pub trait StorageEngine: Send + Sync {
    fn create_node(
        &mut self,
        labels: Vec<String>,
        props: HashMap<String, Value>,
    ) -> NodeId;

    fn create_rel(
        &mut self,
        start: NodeId,
        end: NodeId,
        typ: String,
        props: HashMap<String, Value>,
    ) -> RelId;

    fn get_node(&self, id: NodeId) -> Option<StoredNode>;
    fn get_rel(&self, id: RelId) -> Option<StoredRel>;

    fn all_nodes(&self) -> Box<dyn Iterator<Item = StoredNode> + '_>;
    fn outgoing_rels(&self, node: NodeId) -> Box<dyn Iterator<Item = StoredRel> + '_>;
    fn incoming_rels(&self, node: NodeId) -> Box<dyn Iterator<Item = StoredRel> + '_>;

    /// 删除节点（会同时删除所有关联的关系）
    fn delete_node(&mut self, id: NodeId) -> bool;

    /// 删除关系
    fn delete_rel(&mut self, id: RelId) -> bool;

    fn begin_tx(&mut self) -> Result<TxHandle, StorageError> {
        Err(StorageError::TxNotSupported)
    }

    fn commit_tx(&mut self, _tx: TxHandle) -> Result<(), StorageError> {
        Err(StorageError::TxNotSupported)
    }

    fn rollback_tx(&mut self, _tx: TxHandle) -> Result<(), StorageError> {
        Err(StorageError::TxNotSupported)
    }

    /// 批量创建节点，返回创建的节点ID列表
    fn batch_create_nodes(&mut self, nodes: Vec<(Vec<String>, HashMap<String, Value>)>) -> Vec<NodeId> {
        // 默认实现：逐个创建
        nodes.into_iter()
            .map(|(labels, props)| self.create_node(labels, props))
            .collect()
    }

    /// 批量创建关系，返回创建的关系ID列表
    fn batch_create_rels(&mut self, rels: Vec<(NodeId, NodeId, String, HashMap<String, Value>)>) -> Vec<RelId> {
        // 默认实现：逐个创建
        rels.into_iter()
            .map(|(start, end, typ, props)| self.create_rel(start, end, typ, props))
            .collect()
    }
}
