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

#[cfg(feature = "caching")]
use crate::cache::CacheManager;

pub struct GraphDatabase<E: StorageEngine> {
    pub(crate) engine: E,
    pub(crate) index: PropertyIndex,
    pub(crate) schema: IndexSchema,
    #[cfg(feature = "caching")]
    cache: Option<CacheManager>,
}

impl GraphDatabase<MemStore> {
    pub fn new_in_memory() -> Self {
        Self {
            engine: MemStore::new(),
            index: PropertyIndex::new(),
            schema: IndexSchema::default(),
            #[cfg(feature = "caching")]
            cache: None,
        }
    }

    pub fn new_in_memory_with_schema(schema: IndexSchema) -> Self {
        Self {
            engine: MemStore::new(),
            index: PropertyIndex::new(),
            schema,
            #[cfg(feature = "caching")]
            cache: None,
        }
    }
}

impl<E: StorageEngine> GraphDatabase<E> {
    pub fn from_engine(engine: E) -> Self {
        Self {
            engine,
            index: PropertyIndex::new(),
            schema: IndexSchema::default(),
            #[cfg(feature = "caching")]
            cache: None,
        }
    }

    pub fn from_engine_with_schema(engine: E, schema: IndexSchema) -> Self {
        Self {
            engine,
            index: PropertyIndex::new(),
            schema,
            #[cfg(feature = "caching")]
            cache: None,
        }
    }

    #[cfg(feature = "caching")]
    pub fn with_cache(mut self, cache: CacheManager) -> Self {
        self.cache = Some(cache);
        self
    }

    #[cfg(feature = "caching")]
    pub fn set_cache(&mut self, cache: CacheManager) {
        self.cache = Some(cache);
    }

    #[cfg(feature = "caching")]
    pub fn cache(&self) -> Option<&CacheManager> {
        self.cache.as_ref()
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

        #[cfg(feature = "caching")]
        if let Some(cache) = &self.cache {
            cache.on_node_created(id);
        }

        id
    }


    pub fn create_rel(
        &mut self,
        start: NodeId,
        end: NodeId,
        typ: &str,
        props: Properties,
    ) -> RelId {
        let id = self.engine
            .create_rel(start, end, typ.to_string(), props);

        #[cfg(feature = "caching")]
        if let Some(cache) = &self.cache {
            cache.on_rel_created(id, start, end);
        }

        id
    }

    /// 批量创建节点，返回创建的节点ID列表
    pub fn batch_create_nodes(
        &mut self,
        nodes: Vec<(Vec<String>, Properties)>,
    ) -> Vec<NodeId> {
        // 转换数据格式
        let storage_nodes: Vec<(Vec<String>, Properties)> = nodes;

        // 批量创建节点
        let ids = self.engine.batch_create_nodes(
            storage_nodes.iter().map(|(labels, props)| (labels.clone(), props.clone())).collect()
        );

        // 为每个节点建立索引
        for (i, id) in ids.iter().enumerate() {
            if let Some((labels, props)) = storage_nodes.get(i) {
                self.index_node(*id, labels, props);
            }
        }

        ids
    }

    /// 批量创建关系，返回创建的关系ID列表
    pub fn batch_create_rels(
        &mut self,
        rels: Vec<(NodeId, NodeId, String, Properties)>,
    ) -> Vec<RelId> {
        let storage_rels: Vec<(NodeId, NodeId, String, Properties)> = rels;
        self.engine.batch_create_rels(
            storage_rels.into_iter()
                .map(|(start, end, typ, props)| (start, end, typ, props))
                .collect()
        )
    }

    pub fn delete_node(&mut self, id: NodeId) -> bool {
        // 先获取节点信息用于缓存失效
        #[cfg(feature = "caching")]
        let node_info = self.engine.get_node(id.clone());

        let result = self.engine.delete_node(id);

        #[cfg(feature = "caching")]
        if let Some(cache) = &self.cache {
            if let Some(stored_node) = node_info {
                let label = stored_node.labels.first().map(|s| s.as_str()).unwrap_or("");
                cache.on_node_deleted(id, label, &stored_node.props);
            }
        }

        result
    }

    pub fn delete_rel(&mut self, id: RelId) -> bool {
        // 先获取关系信息用于缓存失效
        #[cfg(feature = "caching")]
        let rel_info = self.engine.get_rel(id.clone());

        let result = self.engine.delete_rel(id);

        #[cfg(feature = "caching")]
        if let Some(cache) = &self.cache {
            if let Some(stored_rel) = rel_info {
                cache.on_rel_deleted(id, stored_rel.start, stored_rel.end);
            }
        }

        result
    }

    pub fn flush(&mut self) -> Result<(), String> {
        // For storage engines that support flush (like sled)
        // We'd need to add a flush method to StorageEngine trait
        // For now, this is a no-op for in-memory
        Ok(())
    }

    pub fn get_node(&self, id: NodeId) -> Option<Node> {
        #[cfg(feature = "caching")]
        if let Some(cache) = &self.cache {
            if let Some(stored_node) = cache.get_node(id) {
                return Some(Node {
                    id: stored_node.id,
                    labels: stored_node.labels,
                    props: stored_node.props,
                });
            }
        }

        self.engine.get_node(id).map(|sn| {
            #[cfg(feature = "caching")]
            if let Some(cache) = &self.cache {
                cache.put_node(id, sn.clone());
            }

            Node {
                id: sn.id,
                labels: sn.labels,
                props: sn.props,
            }
        })
    }

    pub fn get_rel(&self, id: RelId) -> Option<Relationship> {
        #[cfg(feature = "caching")]
        if let Some(cache) = &self.cache {
            if let Some(stored_rel) = cache.get_rel(id) {
                return Some(Relationship {
                    id: stored_rel.id,
                    start: stored_rel.start,
                    end: stored_rel.end,
                    typ: stored_rel.typ,
                    props: stored_rel.props,
                });
            }
        }

        self.engine.get_rel(id).map(|sr| {
            #[cfg(feature = "caching")]
            if let Some(cache) = &self.cache {
                cache.put_rel(sr.clone());
            }

            Relationship {
                id: sr.id,
                start: sr.start,
                end: sr.end,
                typ: sr.typ,
                props: sr.props,
            }
        })
    }

    pub fn neighbors_out(
        &self,
        node: NodeId,
    ) -> impl Iterator<Item = Relationship> + '_ {
        #[cfg(feature = "caching")]
        if let Some(cache) = &self.cache {
            if let Some(rel_ids) = cache.get_outgoing_ids(node) {
                // 使用缓存的关系ID列表
                let rels: Vec<Relationship> = rel_ids
                    .into_iter()
                    .filter_map(|id| self.get_rel(id))
                    .collect();

                // 同时填充ID缓存到存储层的结果
                return Box::new(rels.into_iter()) as Box<dyn Iterator<Item = Relationship> + '_>;
            }
        }

        // 缓存未命中，从存储层获取
        let rels: Vec<Relationship> = self.engine
            .outgoing_rels(node)
            .map(|sr| {
                #[cfg(feature = "caching")]
                if let Some(cache) = &self.cache {
                    cache.put_rel(sr.clone());
                }
                Relationship {
                    id: sr.id,
                    start: sr.start,
                    end: sr.end,
                    typ: sr.typ,
                    props: sr.props,
                }
            })
            .collect();

        // 缓存ID列表
        #[cfg(feature = "caching")]
        if let Some(cache) = &self.cache {
            let ids: Vec<RelId> = rels.iter().map(|r| r.id).collect();
            cache.put_outgoing_ids(node, ids);
        }

        Box::new(rels.into_iter()) as Box<dyn Iterator<Item = Relationship> + '_>
    }

    pub fn neighbors_in(
        &self,
        node: NodeId,
    ) -> impl Iterator<Item = Relationship> + '_ {
        #[cfg(feature = "caching")]
        if let Some(cache) = &self.cache {
            if let Some(rel_ids) = cache.get_incoming_ids(node) {
                // 使用缓存的关系ID列表
                let rels: Vec<Relationship> = rel_ids
                    .into_iter()
                    .filter_map(|id| self.get_rel(id))
                    .collect();

                return Box::new(rels.into_iter()) as Box<dyn Iterator<Item = Relationship> + '_>;
            }
        }

        // 缓存未命中，从存储层获取
        let rels: Vec<Relationship> = self.engine
            .incoming_rels(node)
            .map(|sr| {
                #[cfg(feature = "caching")]
                if let Some(cache) = &self.cache {
                    cache.put_rel(sr.clone());
                }
                Relationship {
                    id: sr.id,
                    start: sr.start,
                    end: sr.end,
                    typ: sr.typ,
                    props: sr.props,
                }
            })
            .collect();

        // 缓存ID列表
        #[cfg(feature = "caching")]
        if let Some(cache) = &self.cache {
            let ids: Vec<RelId> = rels.iter().map(|r| r.id).collect();
            cache.put_incoming_ids(node, ids);
        }

        Box::new(rels.into_iter()) as Box<dyn Iterator<Item = Relationship> + '_>
    }

    pub fn all_stored_nodes(&self) -> impl Iterator<Item = crate::storage::StoredNode> + '_ {
        self.engine.all_nodes()
    }
}

