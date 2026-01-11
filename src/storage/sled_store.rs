use super::{NodeId, RelId, StoredNode, StoredRel, StorageEngine};
use crate::values::Value;
use crate::index_persistent::PersistentPropertyIndex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize)]
struct SerializedNode {
    id: NodeId,
    labels: Vec<String>,
    props: HashMap<String, Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SerializedRel {
    id: RelId,
    start: NodeId,
    end: NodeId,
    typ: String,
    props: HashMap<String, Value>,
}

pub struct SledStore {
    db: sled::Db,
    nodes: sled::Tree,
    rels: sled::Tree,
    outgoing: sled::Tree,
    incoming: sled::Tree,
    index: sled::Tree,
    property_index: PersistentPropertyIndex,
    indexed_properties: Vec<(String, String)>, // (label, property) pairs to index
    next_node_id: NodeId,
    next_rel_id: RelId,
}

impl SledStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, sled::Error> {
        Self::with_config(path, Default::default())
    }

    pub fn with_config<P: AsRef<Path>>(path: P, indexed_properties: Vec<(String, String)>) -> Result<Self, sled::Error> {
        let db = sled::open(path)?;
        let nodes = db.open_tree("nodes")?;
        let rels = db.open_tree("rels")?;
        let outgoing = db.open_tree("outgoing")?;
        let incoming = db.open_tree("incoming")?;
        let index = db.open_tree("index")?;

        // 读取最大 ID
        let next_node_id = nodes
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .filter_map(|k| bincode::deserialize::<NodeId>(&k).ok())
            .max()
            .map(|id| id + 1)
            .unwrap_or(0);

        let next_rel_id = rels
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .filter_map(|k| bincode::deserialize::<RelId>(&k).ok())
            .max()
            .map(|id| id + 1)
            .unwrap_or(0);

        let property_index = PersistentPropertyIndex::new(index.clone());

        let mut store = Self {
            db,
            nodes,
            rels,
            outgoing,
            incoming,
            index,
            property_index,
            indexed_properties,
            next_node_id,
            next_rel_id,
        };

        // 重建索引（从现有节点）
        store.rebuild_index()?;

        Ok(store)
    }

    pub fn flush(&self) -> Result<usize, sled::Error> {
        self.db.flush()
    }

    fn rebuild_index(&mut self) -> Result<(), sled::Error> {
        let nodes: Vec<StoredNode> = self.all_nodes().collect();
        self.property_index
            .rebuild(&nodes, &self.indexed_properties)
            .map_err(|e| sled::Error::Io(std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e))))?;
        Ok(())
    }

    fn node_key(&self, id: NodeId) -> Vec<u8> {
        bincode::serialize(&id).unwrap()
    }

    fn rel_key(&self, id: RelId) -> Vec<u8> {
        bincode::serialize(&id).unwrap()
    }

    fn adj_key(&self, node_id: NodeId) -> Vec<u8> {
        bincode::serialize(&node_id).unwrap()
    }

    /// 查询持久化索引
    pub fn query_index(
        &self,
        label: &str,
        property: &str,
        value: &Value,
    ) -> Result<Vec<NodeId>, Box<dyn std::error::Error>> {
        self.property_index.find(label, property, value)
    }

    /// 获取索引条目数量
    pub fn index_count(&self) -> usize {
        self.property_index.count()
    }
}

impl StorageEngine for SledStore {
    fn create_node(
        &mut self,
        labels: Vec<String>,
        props: HashMap<String, Value>,
    ) -> NodeId {
        let id = self.next_node_id;
        self.next_node_id += 1;

        let node = SerializedNode { id, labels: labels.clone(), props: props.clone() };
        let key = self.node_key(id);
        let value = bincode::serialize(&node).unwrap();

        self.nodes.insert(key, value).unwrap();

        // 更新持久化索引
        for label in &labels {
            for (indexed_label, indexed_prop) in &self.indexed_properties {
                if label == indexed_label {
                    if let Some(value) = props.get(indexed_prop) {
                        let _ = self.property_index.add(label, indexed_prop, value, id);
                    }
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
        let id = self.next_rel_id;
        self.next_rel_id += 1;

        let rel = SerializedRel {
            id,
            start,
            end,
            typ,
            props,
        };

        let key = self.rel_key(id);
        let value = bincode::serialize(&rel).unwrap();
        self.rels.insert(key, value).unwrap();

        // 更新邻接表
        let out_key = self.adj_key(start);
        let mut out_list: Vec<RelId> = self
            .outgoing
            .get(&out_key)
            .unwrap()
            .and_then(|v| bincode::deserialize(&v).ok())
            .unwrap_or_default();
        out_list.push(id);
        self.outgoing
            .insert(out_key, bincode::serialize(&out_list).unwrap())
            .unwrap();

        let in_key = self.adj_key(end);
        let mut in_list: Vec<RelId> = self
            .incoming
            .get(&in_key)
            .unwrap()
            .and_then(|v| bincode::deserialize(&v).ok())
            .unwrap_or_default();
        in_list.push(id);
        self.incoming
            .insert(in_key, bincode::serialize(&in_list).unwrap())
            .unwrap();

        id
    }

    fn get_node(&self, id: NodeId) -> Option<StoredNode> {
        let key = self.node_key(id);
        self.nodes
            .get(key)
            .ok()?
            .and_then(|v| bincode::deserialize::<SerializedNode>(&v).ok())
            .map(|n| StoredNode {
                id: n.id,
                labels: n.labels,
                props: n.props,
            })
    }

    fn get_rel(&self, id: RelId) -> Option<StoredRel> {
        let key = self.rel_key(id);
        self.rels
            .get(key)
            .ok()?
            .and_then(|v| bincode::deserialize::<SerializedRel>(&v).ok())
            .map(|r| StoredRel {
                id: r.id,
                start: r.start,
                end: r.end,
                typ: r.typ,
                props: r.props,
            })
    }

    fn all_nodes(&self) -> Box<dyn Iterator<Item = StoredNode> + '_> {
        Box::new(
            self.nodes
                .iter()
                .filter_map(|r| r.ok())
                .filter_map(|(_, v)| bincode::deserialize::<SerializedNode>(&v).ok())
                .map(|n| StoredNode {
                    id: n.id,
                    labels: n.labels,
                    props: n.props,
                }),
        )
    }

    fn outgoing_rels(&self, node: NodeId) -> Box<dyn Iterator<Item = StoredRel> + '_> {
        let key = self.adj_key(node);
        let rel_ids: Vec<RelId> = self
            .outgoing
            .get(key)
            .ok()
            .flatten()
            .and_then(|v| bincode::deserialize(&v).ok())
            .unwrap_or_default();

        Box::new(rel_ids.into_iter().filter_map(move |rid| self.get_rel(rid)))
    }

    fn incoming_rels(&self, node: NodeId) -> Box<dyn Iterator<Item = StoredRel> + '_> {
        let key = self.adj_key(node);
        let rel_ids: Vec<RelId> = self
            .incoming
            .get(key)
            .ok()
            .flatten()
            .and_then(|v| bincode::deserialize(&v).ok())
            .unwrap_or_default();

        Box::new(rel_ids.into_iter().filter_map(move |rid| self.get_rel(rid)))
    }

    fn delete_node(&mut self, id: NodeId) -> bool {
        // 先获取节点信息以便清理索引
        let node = self.get_node(id);

        // 收集所有相关关系
        let out_key = self.adj_key(id);
        let in_key = self.adj_key(id);

        let out_rels: Vec<RelId> = self
            .outgoing
            .get(&out_key)
            .unwrap()
            .and_then(|v| bincode::deserialize(&v).ok())
            .unwrap_or_default();

        let in_rels: Vec<RelId> = self
            .incoming
            .get(&in_key)
            .unwrap()
            .and_then(|v| bincode::deserialize(&v).ok())
            .unwrap_or_default();

        // 删除所有相关关系
        for rel_id in out_rels.iter().chain(in_rels.iter()) {
            self.delete_rel(*rel_id);
        }

        // 清理邻接表
        self.outgoing.remove(&out_key).unwrap();
        self.incoming.remove(&in_key).unwrap();

        // 删除节点本身
        let key = self.node_key(id);
        let deleted = self.nodes.remove(key).unwrap().is_some();

        // 从持久化索引中移除
        if let Some(node) = node {
            for label in &node.labels {
                for (indexed_label, indexed_prop) in &self.indexed_properties {
                    if label == indexed_label {
                        if let Some(value) = node.props.get(indexed_prop) {
                            let _ = self.property_index.remove(label, indexed_prop, value, id);
                        }
                    }
                }
            }
        }

        deleted
    }

    fn delete_rel(&mut self, id: RelId) -> bool {
        let key = self.rel_key(id);
        if let Some(rel) = self.get_rel(id) {
            // 从邻接表中移除
            let out_key = self.adj_key(rel.start);
            if let Ok(Some(data)) = self.outgoing.get(&out_key) {
                let mut out_list: Vec<RelId> = bincode::deserialize(&data).unwrap_or_default();
                out_list.retain(|&r| r != id);
                self.outgoing
                    .insert(out_key, bincode::serialize(&out_list).unwrap())
                    .unwrap();
            }

            let in_key = self.adj_key(rel.end);
            if let Ok(Some(data)) = self.incoming.get(&in_key) {
                let mut in_list: Vec<RelId> = bincode::deserialize(&data).unwrap_or_default();
                in_list.retain(|&r| r != id);
                self.incoming
                    .insert(in_key, bincode::serialize(&in_list).unwrap())
                    .unwrap();
            }

            // 删除关系本身
            self.rels.remove(key).unwrap();
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

        // 使用 sled 的 batch 事务批量写入
        let mut batch = sled::Batch::default();

        let mut nodes_with_ids = Vec::new();

        for (i, (labels, props)) in nodes.into_iter().enumerate() {
            let id = start_id + i as NodeId;
            nodes_with_ids.push((id, labels.clone(), props.clone()));
            let node = SerializedNode { id, labels, props };
            let key = self.node_key(id);
            let value = bincode::serialize(&node).unwrap();
            batch.insert(key, value);
        }

        // 一次性写入所有节点
        self.nodes.apply_batch(batch).unwrap();

        // 更新持久化索引
        for (id, labels, props) in nodes_with_ids {
            for label in &labels {
                for (indexed_label, indexed_prop) in &self.indexed_properties {
                    if label == indexed_label {
                        if let Some(value) = props.get(indexed_prop) {
                            let _ = self.property_index.add(label, indexed_prop, value, id);
                        }
                    }
                }
            }
        }

        // 返回分配的 ID 列表
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

        // 准备所有写入操作
        let mut node_batch = sled::Batch::default();
        let mut outgoing_batch = sled::Batch::default();
        let mut incoming_batch = sled::Batch::default();

        // 读取现有的邻接表数据
        let mut outgoing_adj: HashMap<NodeId, Vec<RelId>> = HashMap::new();
        let mut incoming_adj: HashMap<NodeId, Vec<RelId>> = HashMap::new();

        // 第一遍：收集所有需要更新的节点和读取现有邻接表
        for (i, (start, end, typ, props)) in rels.iter().enumerate() {
            let id = start_id + i as RelId;

            // 读取现有邻接表
            if !outgoing_adj.contains_key(start) {
                let out_key = self.adj_key(*start);
                let out_list: Vec<RelId> = self
                    .outgoing
                    .get(&out_key)
                    .unwrap()
                    .and_then(|v| bincode::deserialize(&v).ok())
                    .unwrap_or_default();
                outgoing_adj.insert(*start, out_list);
            }

            if !incoming_adj.contains_key(end) {
                let in_key = self.adj_key(*end);
                let in_list: Vec<RelId> = self
                    .incoming
                    .get(&in_key)
                    .unwrap()
                    .and_then(|v| bincode::deserialize(&v).ok())
                    .unwrap_or_default();
                incoming_adj.insert(*end, in_list);
            }
        }

        // 第二遍：构建批量写入
        for (i, (start, end, typ, props)) in rels.into_iter().enumerate() {
            let id = start_id + i as RelId;

            // 序列化关系
            let rel = SerializedRel {
                id,
                start,
                end,
                typ,
                props,
            };
            let rel_key = self.rel_key(id);
            let rel_value = bincode::serialize(&rel).unwrap();
            node_batch.insert(rel_key, rel_value);

            // 更新出边邻接表
            outgoing_adj.entry(start).or_default().push(id);

            // 更新入边邻接表
            incoming_adj.entry(end).or_default().push(id);
        }

        // 准备邻接表的批量写入
        for (node_id, rel_ids) in &outgoing_adj {
            let key = self.adj_key(*node_id);
            let value = bincode::serialize(rel_ids).unwrap();
            outgoing_batch.insert(key, value);
        }

        for (node_id, rel_ids) in &incoming_adj {
            let key = self.adj_key(*node_id);
            let value = bincode::serialize(rel_ids).unwrap();
            incoming_batch.insert(key, value);
        }

        // 一次性写入所有数据
        self.rels.apply_batch(node_batch).unwrap();
        self.outgoing.apply_batch(outgoing_batch).unwrap();
        self.incoming.apply_batch(incoming_batch).unwrap();

        // 返回分配的 ID 列表
        (start_id..start_id + count).collect()
    }
}
