use super::{NodeId, RelId, StoredNode, StoredRel, StorageEngine, StorageError, TxHandle};
use crate::values::{Value, Properties};
use std::collections::HashMap;

/// 事务操作记录（公开用于测试）
#[derive(Debug, Clone)]
pub enum TxOp {
    CreateNode(NodeId, Vec<String>, Properties),
    CreateRel(RelId, NodeId, NodeId, String, Properties),
    DeleteNode(NodeId, StoredNode),
    DeleteRel(RelId, StoredRel),
    UpdateNode(NodeId, Properties),
    UpdateRel(RelId, Properties),
}

/// 事务状态
#[derive(Debug)]
struct Transaction {
    id: u64,
    ops: Vec<TxOp>,
    committed: bool,
}

pub struct MemStore {
    next_node_id: NodeId,
    next_rel_id: RelId,
    next_tx_id: u64,
    nodes: HashMap<NodeId, StoredNode>,
    rels: HashMap<RelId, StoredRel>,
    outgoing: HashMap<NodeId, Vec<RelId>>,
    incoming: HashMap<NodeId, Vec<RelId>>,
    transactions: HashMap<u64, Transaction>,
}

impl MemStore {
    pub fn new() -> Self {
        Self {
            next_node_id: 0,
            next_rel_id: 0,
            next_tx_id: 0,
            nodes: HashMap::new(),
            rels: HashMap::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            transactions: HashMap::new(),
        }
    }
}

impl StorageEngine for MemStore {
    fn create_node(
        &mut self,
        labels: Vec<String>,
        props: HashMap<String, Value>,
    ) -> NodeId {
        let id = self.next_node_id;
        self.next_node_id += 1;

        let node = StoredNode { id, labels, props };
        self.nodes.insert(id, node);
        id
    }

    fn create_rel(
        &mut self,
        start: NodeId,
        end: NodeId,
        typ: String,
        props: HashMap<String, Value>,
    ) -> RelId {
        let id = self.next_rel_id;
        self.next_rel_id += 1;

        let rel = StoredRel {
            id,
            start,
            end,
            typ,
            props,
        };
        self.rels.insert(id, rel);

        self.outgoing.entry(start).or_default().push(id);
        self.incoming.entry(end).or_default().push(id);

        id
    }

    fn get_node(&self, id: NodeId) -> Option<StoredNode> {
        self.nodes.get(&id).cloned()
    }

    fn get_rel(&self, id: RelId) -> Option<StoredRel> {
        self.rels.get(&id).cloned()
    }

    fn all_nodes(&self) -> Box<dyn Iterator<Item = StoredNode> + '_> {
        Box::new(self.nodes.values().cloned())
    }

    fn outgoing_rels(&self, node: NodeId) -> Box<dyn Iterator<Item = StoredRel> + '_> {
        if let Some(rel_ids) = self.outgoing.get(&node) {
            let it = rel_ids
                .iter()
                .filter_map(move |rid| self.rels.get(rid).cloned());
            Box::new(it)
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn incoming_rels(&self, node: NodeId) -> Box<dyn Iterator<Item = StoredRel> + '_> {
        if let Some(rel_ids) = self.incoming.get(&node) {
            let it = rel_ids
                .iter()
                .filter_map(move |rid| self.rels.get(rid).cloned());
            Box::new(it)
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn delete_node(&mut self, id: NodeId) -> bool {
        // 删除节点前先删除所有相关的关系
        let mut rels_to_delete = Vec::new();

        // 收集所有出边和入边
        if let Some(out_rels) = self.outgoing.get(&id) {
            rels_to_delete.extend(out_rels.iter().copied());
        }
        if let Some(in_rels) = self.incoming.get(&id) {
            rels_to_delete.extend(in_rels.iter().copied());
        }

        // 删除所有相关关系
        for rel_id in rels_to_delete {
            self.delete_rel(rel_id);
        }

        // 清理邻接表
        self.outgoing.remove(&id);
        self.incoming.remove(&id);

        // 删除节点本身
        self.nodes.remove(&id).is_some()
    }

    fn delete_rel(&mut self, id: RelId) -> bool {
        if let Some(rel) = self.rels.remove(&id) {
            // 从邻接表中移除
            if let Some(out_list) = self.outgoing.get_mut(&rel.start) {
                out_list.retain(|&r| r != id);
            }
            if let Some(in_list) = self.incoming.get_mut(&rel.end) {
                in_list.retain(|&r| r != id);
            }
            true
        } else {
            false
        }
    }

    fn batch_create_nodes(
        &mut self,
        nodes: Vec<(Vec<String>, HashMap<String, Value>)>,
    ) -> Vec<NodeId> {
        // 预分配 ID
        let start_id = self.next_node_id;
        let count = nodes.len() as NodeId;
        self.next_node_id += count;

        // 批量创建节点
        for (i, (labels, props)) in nodes.into_iter().enumerate() {
            let id = start_id + i as NodeId;
            let node = StoredNode { id, labels, props };
            self.nodes.insert(id, node);
        }

        (start_id..start_id + count).collect()
    }

    fn batch_create_rels(
        &mut self,
        rels: Vec<(NodeId, NodeId, String, HashMap<String, Value>)>,
    ) -> Vec<RelId> {
        // 预分配 ID
        let start_id = self.next_rel_id;
        let count = rels.len() as RelId;
        self.next_rel_id += count;

        // 批量创建关系
        for (i, (start, end, typ, props)) in rels.into_iter().enumerate() {
            let id = start_id + i as RelId;
            let rel = StoredRel {
                id,
                start,
                end,
                typ,
                props,
            };
            self.rels.insert(id, rel);
            self.outgoing.entry(start).or_default().push(id);
            self.incoming.entry(end).or_default().push(id);
        }

        (start_id..start_id + count).collect()
    }

    // ========== 事务支持 ==========

    fn begin_tx(&mut self) -> Result<TxHandle, StorageError> {
        let tx_id = self.next_tx_id;
        self.next_tx_id += 1;

        self.transactions.insert(tx_id, Transaction {
            id: tx_id,
            ops: Vec::new(),
            committed: false,
        });

        Ok(TxHandle(tx_id))
    }

    fn commit_tx(&mut self, tx_handle: TxHandle) -> Result<(), StorageError> {
        let TxHandle(tx_id) = tx_handle;

        let mut tx = self.transactions.remove(&tx_id)
            .ok_or_else(|| StorageError::Other(format!("Transaction {} not found", tx_id)))?;

        if tx.committed {
            return Err(StorageError::Other(format!("Transaction {} already committed", tx_id)));
        }

        // 应用所有操作
        for op in &tx.ops {
            match op {
                TxOp::CreateNode(id, labels, props) => {
                    if !self.nodes.contains_key(id) {
                        let node = StoredNode {
                            id: *id,
                            labels: labels.clone(),
                            props: props.clone(),
                        };
                        self.nodes.insert(*id, node);
                    }
                }
                TxOp::CreateRel(id, start, end, typ, props) => {
                    if !self.rels.contains_key(id) {
                        let rel = StoredRel {
                            id: *id,
                            start: *start,
                            end: *end,
                            typ: typ.clone(),
                            props: props.clone(),
                        };
                        self.rels.insert(*id, rel);
                        self.outgoing.entry(*start).or_default().push(*id);
                        self.incoming.entry(*end).or_default().push(*id);
                    }
                }
                TxOp::DeleteNode(id, _) => {
                    self.delete_node(*id);
                }
                TxOp::DeleteRel(id, _) => {
                    self.delete_rel(*id);
                }
                TxOp::UpdateNode(id, props) => {
                    if let Some(node) = self.nodes.get_mut(id) {
                        // 合并属性
                        for (k, v) in props {
                            node.props.insert(k.clone(), v.clone());
                        }
                    }
                }
                TxOp::UpdateRel(id, props) => {
                    if let Some(rel) = self.rels.get_mut(id) {
                        // 合并属性
                        for (k, v) in props {
                            rel.props.insert(k.clone(), v.clone());
                        }
                    }
                }
            }
        }

        tx.committed = true;
        Ok(())
    }

    fn rollback_tx(&mut self, tx_handle: TxHandle) -> Result<(), StorageError> {
        let TxHandle(tx_id) = tx_handle;

        self.transactions.remove(&tx_id)
            .ok_or_else(|| StorageError::Other(format!("Transaction {} not found", tx_id)))?;

        // 简单地移除事务，不应用任何操作
        Ok(())
    }

    fn update_node_props(&mut self, id: NodeId, props: HashMap<String, Value>) -> bool {
        self.do_update_node_props(id, props)
    }

    fn update_rel_props(&mut self, id: RelId, props: HashMap<String, Value>) -> bool {
        self.do_update_rel_props(id, props)
    }
}

impl MemStore {
    // ========== 存储层更新 API（私有辅助方法） ==========

    /// 更新节点属性的内部实现
    fn do_update_node_props(&mut self, id: NodeId, props: Properties) -> bool {
        if let Some(node) = self.nodes.get_mut(&id) {
            // 合并属性
            for (k, v) in props {
                node.props.insert(k, v);
            }
            true
        } else {
            false
        }
    }

    /// 更新关系属性的内部实现
    fn do_update_rel_props(&mut self, id: RelId, props: Properties) -> bool {
        if let Some(rel) = self.rels.get_mut(&id) {
            // 合并属性
            for (k, v) in props {
                rel.props.insert(k, v);
            }
            true
        } else {
            false
        }
    }

    // ========== 事务操作辅助方法 ==========

    /// 记录事务操作（用于测试）
    pub fn record_tx_op(&mut self, tx_handle: TxHandle, op: TxOp) -> Result<(), StorageError> {
        let TxHandle(tx_id) = tx_handle;

        let tx = self.transactions.get_mut(&tx_id)
            .ok_or_else(|| StorageError::Other(format!("Transaction {} not found", tx_id)))?;

        tx.ops.push(op);
        Ok(())
    }

    /// 获取事务操作数量（用于测试）
    pub fn tx_op_count(&self, tx_handle: TxHandle) -> Result<usize, StorageError> {
        let TxHandle(tx_id) = tx_handle;

        self.transactions.get(&tx_id)
            .map(|tx| tx.ops.len())
            .ok_or_else(|| StorageError::Other(format!("Transaction {} not found", tx_id)))
    }
}
