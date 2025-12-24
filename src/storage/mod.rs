pub mod mem_store;
pub mod sled_store;

use crate::values::Value;
use std::collections::HashMap;

pub type NodeId = u64;
pub type RelId = u64;

#[derive(Debug, Clone)]
pub struct StoredNode {
    pub id: NodeId,
    pub labels: Vec<String>,
    pub props: HashMap<String, Value>,
}

#[derive(Debug, Clone)]
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
}
