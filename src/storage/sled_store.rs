use super::{NodeId, RelId, StoredNode, StoredRel, StorageEngine};
use crate::values::Value;
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
    next_node_id: NodeId,
    next_rel_id: RelId,
}

impl SledStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, sled::Error> {
        let db = sled::open(path)?;
        let nodes = db.open_tree("nodes")?;
        let rels = db.open_tree("rels")?;
        let outgoing = db.open_tree("outgoing")?;
        let incoming = db.open_tree("incoming")?;

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

        Ok(Self {
            db,
            nodes,
            rels,
            outgoing,
            incoming,
            next_node_id,
            next_rel_id,
        })
    }

    pub fn flush(&self) -> Result<usize, sled::Error> {
        self.db.flush()
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
}

impl StorageEngine for SledStore {
    fn create_node(
        &mut self,
        labels: Vec<String>,
        props: HashMap<String, Value>,
    ) -> NodeId {
        let id = self.next_node_id;
        self.next_node_id += 1;

        let node = SerializedNode { id, labels, props };
        let key = self.node_key(id);
        let value = bincode::serialize(&node).unwrap();

        self.nodes.insert(key, value).unwrap();

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
        self.nodes.remove(key).unwrap().is_some()
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
}
