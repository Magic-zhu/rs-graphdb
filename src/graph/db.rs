use crate::graph::model::{Node, Relationship};
use crate::storage::{mem_store::MemStore, NodeId, RelId, StorageEngine};
use crate::values::Properties;

#[derive(Debug)]
pub enum GraphError {
    Storage(String),
    NotFound,
}

use crate::index::PropertyIndex;
use crate::index_schema::IndexSchema;

pub struct GraphDatabase<E: StorageEngine> {
    pub(crate) engine: E,
    pub(crate) index: PropertyIndex,
    pub(crate) schema: IndexSchema,
}

impl GraphDatabase<MemStore> {
    pub fn new_in_memory() -> Self {
        Self {
            engine: MemStore::new(),
            index: PropertyIndex::new(),
            schema: IndexSchema::default(),
        }
    }

    pub fn new_in_memory_with_schema(schema: IndexSchema) -> Self {
        Self {
            engine: MemStore::new(),
            index: PropertyIndex::new(),
            schema,
        }
    }
}

impl<E: StorageEngine> GraphDatabase<E> {
    pub fn from_engine(engine: E) -> Self {
        Self {
            engine,
            index: PropertyIndex::new(),
            schema: IndexSchema::default(),
        }
    }

    pub fn from_engine_with_schema(engine: E, schema: IndexSchema) -> Self {
        Self {
            engine,
            index: PropertyIndex::new(),
            schema,
        }
    }

    /// 根据 schema 自动为节点的属性建索引
    fn index_node(&mut self, id: NodeId, labels: &[String], props: &Properties) {
        for label in labels {
            for (prop_name, value) in props {
                if self.schema.should_index(label, prop_name) {
                    self.index.add(label, prop_name, value, id);
                }
            }
        }
    }

    pub fn create_node(
        &mut self,
        labels: Vec<&str>,
        props: Properties,
    ) -> NodeId {
        let labels_owned: Vec<String> = labels.into_iter().map(|s| s.to_string()).collect();
        let id = self.engine.create_node(labels_owned.clone(), props.clone());
        self.index_node(id, &labels_owned, &props);
        id
    }


    pub fn create_rel(
        &mut self,
        start: NodeId,
        end: NodeId,
        typ: &str,
        props: Properties,
    ) -> RelId {
        self.engine
            .create_rel(start, end, typ.to_string(), props)
    }

    pub fn delete_node(&mut self, id: NodeId) -> bool {
        // TODO: 删除节点时同时从索引中移除
        self.engine.delete_node(id)
    }

    pub fn delete_rel(&mut self, id: RelId) -> bool {
        self.engine.delete_rel(id)
    }

    pub fn flush(&mut self) -> Result<(), String> {
        // For storage engines that support flush (like sled)
        // We'd need to add a flush method to StorageEngine trait
        // For now, this is a no-op for in-memory
        Ok(())
    }

    pub fn get_node(&self, id: NodeId) -> Option<Node> {
        self.engine.get_node(id).map(|sn| Node {
            id: sn.id,
            labels: sn.labels,
            props: sn.props,
        })
    }

    pub fn get_rel(&self, id: RelId) -> Option<Relationship> {
        self.engine.get_rel(id).map(|sr| Relationship {
            id: sr.id,
            start: sr.start,
            end: sr.end,
            typ: sr.typ,
            props: sr.props,
        })
    }

    pub fn neighbors_out(
        &self,
        node: NodeId,
    ) -> impl Iterator<Item = Relationship> + '_ {
        self.engine
            .outgoing_rels(node)
            .map(|sr| Relationship {
                id: sr.id,
                start: sr.start,
                end: sr.end,
                typ: sr.typ,
                props: sr.props,
            })
    }

    pub fn neighbors_in(
        &self,
        node: NodeId,
    ) -> impl Iterator<Item = Relationship> + '_ {
        self.engine
            .incoming_rels(node)
            .map(|sr| Relationship {
                id: sr.id,
                start: sr.start,
                end: sr.end,
                typ: sr.typ,
                props: sr.props,
            })
    }

    pub(crate) fn all_stored_nodes(&self) -> impl Iterator<Item = crate::storage::StoredNode> + '_ {
        self.engine.all_nodes()
    }
}

