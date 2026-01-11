use super::{NodeId, RelId, StoredNode, StoredRel, StorageEngine};
use crate::values::Value;
use std::collections::HashMap;

pub struct MemStore {
    next_node_id: NodeId,
    next_rel_id: RelId,
    nodes: HashMap<NodeId, StoredNode>,
    rels: HashMap<RelId, StoredRel>,
    outgoing: HashMap<NodeId, Vec<RelId>>,
    incoming: HashMap<NodeId, Vec<RelId>>,
}

impl MemStore {
    pub fn new() -> Self {
        Self {
            next_node_id: 0,
            next_rel_id: 0,
            nodes: HashMap::new(),
            rels: HashMap::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
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
}
