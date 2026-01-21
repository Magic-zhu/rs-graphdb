use crate::graph::model::{Node, Relationship};
use crate::storage::{mem_store::MemStore, NodeId, RelId, StorageEngine, StorageError, TxHandle};
use crate::values::{Properties, Value};
use crate::transactions::{TransactionManager, TransactionConfig};

#[derive(Debug)]
pub enum GraphError {
    Storage(String),
    NotFound,
}

use crate::index::PropertyIndex;
use crate::index_schema::IndexSchema;
use crate::constraints::ConstraintManager;
use std::sync::Arc;

#[cfg(feature = "caching")]
use crate::cache::CacheManager;

pub struct GraphDatabase<E: StorageEngine> {
    pub(crate) engine: E,
    pub(crate) index: PropertyIndex,
    pub(crate) schema: IndexSchema,
    pub constraints: Arc<ConstraintManager>,
    #[cfg(feature = "caching")]
    cache: Option<CacheManager>,
    /// 事务管理器
    pub transactions: TransactionManager,
}

impl GraphDatabase<MemStore> {
    pub fn new_in_memory() -> Self {
        Self {
            engine: MemStore::new(),
            index: PropertyIndex::new(),
            schema: IndexSchema::default(),
            constraints: Arc::new(ConstraintManager::new()),
            #[cfg(feature = "caching")]
            cache: None,
            transactions: TransactionManager::new(),
        }
    }

    pub fn new_in_memory_with_schema(schema: IndexSchema) -> Self {
        Self {
            engine: MemStore::new(),
            index: PropertyIndex::new(),
            schema,
            constraints: Arc::new(ConstraintManager::new()),
            #[cfg(feature = "caching")]
            cache: None,
            transactions: TransactionManager::new(),
        }
    }
}

impl<E: StorageEngine> GraphDatabase<E> {
    pub fn from_engine(engine: E) -> Self {
        Self {
            engine,
            index: PropertyIndex::new(),
            schema: IndexSchema::default(),
            constraints: Arc::new(ConstraintManager::new()),
            #[cfg(feature = "caching")]
            cache: None,
            transactions: TransactionManager::new(),
        }
    }

    pub fn from_engine_with_schema(engine: E, schema: IndexSchema) -> Self {
        Self {
            engine,
            index: PropertyIndex::new(),
            schema,
            constraints: Arc::new(ConstraintManager::new()),
            #[cfg(feature = "caching")]
            cache: None,
            transactions: TransactionManager::new(),
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
            // 单属性索引
            for (prop_name, value) in props {
                if self.schema.should_index(label, prop_name) {
                    self.index.add(label, prop_name, value, id);
                }
            }

            // 复合索引
            for (_name, (index_label, properties)) in self.schema.get_all_composite_indexes() {
                if index_label == label {
                    // 检查所有属性是否都存在
                    let mut values = Vec::new();
                    let mut all_exist = true;
                    for prop in properties {
                        if let Some(value) = props.get(prop) {
                            values.push(value.clone());
                        } else {
                            all_exist = false;
                            break;
                        }
                    }

                    // 如果所有属性都存在，则添加复合索引
                    if all_exist {
                        let props_refs: Vec<&str> = properties.iter().map(|s| s.as_str()).collect();
                        self.index.add_composite(label, &props_refs, &values, id);
                    }
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

    // ========== 复合索引管理 ==========

    /// 创建复合索引
    ///
    /// # 参数
    /// - `name`: 索引名称（用于标识和删除索引）
    /// - `label`: 节点标签
    /// - `properties`: 属性名列表（按索引顺序）
    ///
    /// # 示例
    /// ```
    /// use rs_graphdb::GraphDatabase;
    ///
    /// let mut db = GraphDatabase::new_in_memory();
    /// db.create_composite_index("user_name_email", "User", &["name", "email"]);
    /// ```
    pub fn create_composite_index(&mut self, name: &str, label: &str, properties: &[&str]) {
        self.schema.add_composite_index(name, label, properties);

        // 为现有节点重建复合索引
        // 先收集所有需要索引的节点，避免借用冲突
        let nodes_to_index: Vec<(NodeId, Vec<Value>)> = self
            .all_stored_nodes()
            .filter(|stored_node| stored_node.labels.contains(&label.to_string()))
            .filter_map(|stored_node| {
                // 检查所有属性是否都存在
                let mut values = Vec::new();
                for prop in properties {
                    if let Some(value) = stored_node.props.get(*prop) {
                        values.push(value.clone());
                    } else {
                        return None;
                    }
                }
                Some((stored_node.id, values))
            })
            .collect();

        // 然后添加索引
        for (node_id, values) in nodes_to_index {
            self.index.add_composite(label, properties, &values, node_id);
        }
    }

    /// 删除复合索引
    ///
    /// # 参数
    /// - `name`: 索引名称
    ///
    /// # 返回
    /// 如果成功删除返回 true，如果索引不存在返回 false
    pub fn drop_composite_index(&mut self, name: &str) -> bool {
        self.schema.remove_composite_index(name)
    }

    /// 使用复合索引查询节点
    ///
    /// # 参数
    /// - `label`: 节点标签
    /// - `properties`: 属性名列表
    /// - `values`: 属性值列表
    ///
    /// # 返回
    /// 匹配的节点ID列表
    ///
    /// # 示例
    /// ```
    /// use rs_graphdb::GraphDatabase;
    /// use rs_graphdb::values::{Properties, Value};
    ///
    /// let mut db = GraphDatabase::new_in_memory();
    /// db.create_composite_index("user_name_age", "User", &["name", "age"]);
    ///
    /// // 创建节点...
    ///
    /// // 使用复合索引查询
    /// let ids = db.find_by_composite_index("User", &["name", "age"], &[
    ///     Value::Text("Alice".to_string()),
    ///     Value::Int(30),
    /// ]);
    /// ```
    pub fn find_by_composite_index(
        &self,
        label: &str,
        properties: &[&str],
        values: &[Value],
    ) -> Vec<NodeId> {
        self.index.find_composite(label, properties, values)
    }

    /// 获取索引统计信息
    ///
    /// # 返回
    /// (单属性索引数量, 复合索引数量)
    pub fn index_stats(&self) -> (usize, usize) {
        (
            self.index.single_index_count(),
            self.index.composite_index_count(),
        )
    }

    // ========== 高级索引 API ==========

    /// 添加全文索引
    ///
    /// # 参数
    /// - `label`: 节点标签
    /// - `property_name`: 属性名
    /// - `node_id`: 节点ID
    ///
    /// # 示例
    /// ```no_run
    /// # use rs_graphdb::GraphDatabase;
    /// # use rs_graphdb::values::{Properties, Value};
    /// # let mut db = GraphDatabase::new_in_memory();
    /// # let mut props = Properties::new();
    /// # props.insert("bio".to_string(), Value::Text("software engineer".to_string()));
    /// # let id = db.create_node(vec!["User"], props);
    /// db.add_fulltext_index("User", "bio", id);
    /// ```
    pub fn add_fulltext_index(
        &mut self,
        label: &str,
        property_name: &str,
        node_id: NodeId,
    ) {
        if let Some(node) = self.engine.get_node(node_id) {
            // 获取属性值
            if let Some(Value::Text(text)) = node.props.get(property_name) {
                self.index.add_fulltext(label, property_name, text, node_id);
            }
        }
    }

    /// 全文搜索（OR 查询）
    ///
    /// 返回包含任意搜索词的节点
    ///
    /// # 参数
    /// - `label`: 节点标签
    /// - `property_name`: 属性名
    /// - `query`: 搜索词
    pub fn search_fulltext(
        &self,
        label: &str,
        property_name: &str,
        query: &str,
    ) -> Vec<NodeId> {
        self.index.search_fulltext(label, property_name, query)
    }

    /// 全文搜索（AND 查询）
    ///
    /// 返回同时包含所有搜索词的节点
    pub fn search_fulltext_and(
        &self,
        label: &str,
        property_name: &str,
        query: &str,
    ) -> Vec<NodeId> {
        self.index.search_fulltext_and(label, property_name, query)
    }

    /// 添加范围索引（自动处理数值类型）
    ///
    /// 如果属性值是数值类型，自动添加到范围索引
    pub fn add_range_index(
        &mut self,
        label: &str,
        property_name: &str,
        node_id: NodeId,
    ) {
        if let Some(node) = self.engine.get_node(node_id) {
            if let Some(value) = node.props.get(property_name) {
                self.index.add_range(label, property_name, value, node_id);
            }
        }
    }

    /// 范围查询：大于
    pub fn range_greater_than(
        &self,
        label: &str,
        property_name: &str,
        value: Value,
    ) -> Vec<NodeId> {
        self.index.range_greater_than(label, property_name, &value)
    }

    /// 范围查询：小于
    pub fn range_less_than(
        &self,
        label: &str,
        property_name: &str,
        value: Value,
    ) -> Vec<NodeId> {
        self.index.range_less_than(label, property_name, &value)
    }

    /// 范围查询：范围之间
    pub fn range_between(
        &self,
        label: &str,
        property_name: &str,
        min_value: Value,
        max_value: Value,
    ) -> Vec<NodeId> {
        self.index.range_between(label, property_name, &min_value, &max_value)
    }

    // ========== 事务支持 ==========

    /// 开始一个新事务（使用默认配置）
    pub fn begin_tx(&mut self) -> Result<TxHandle, StorageError> {
        self.engine.begin_tx()
    }

    /// 开始一个新事务（使用自定义配置）
    pub fn begin_tx_with_config(&mut self, config: TransactionConfig) -> u64 {
        self.transactions.begin_transaction().id
    }

    /// 提交事务
    pub fn commit_tx(&mut self, tx: TxHandle) -> Result<(), StorageError> {
        self.engine.commit_tx(tx)
    }

    /// 提交事务（使用事务管理器）
    pub fn commit_transaction(&mut self, tx_id: u64) -> Result<(), crate::transactions::TransactionError> {
        self.transactions.commit(tx_id)
    }

    /// 回滚事务
    pub fn rollback_tx(&mut self, tx: TxHandle) -> Result<(), StorageError> {
        self.engine.rollback_tx(tx)
    }

    /// 回滚事务（使用事务管理器）
    pub fn rollback_transaction(&mut self, tx_id: u64) -> Result<(), crate::transactions::TransactionError> {
        self.transactions.rollback(tx_id)
    }

    /// 获取活动事务数量
    pub fn active_transaction_count(&self) -> usize {
        self.transactions.active_count()
    }

    /// 获取已完成事务数量
    pub fn completed_transaction_count(&self) -> usize {
        self.transactions.completed_count()
    }

    /// 清理已完成的旧事务
    pub fn cleanup_transactions(&mut self, keep_last: usize) {
        self.transactions.cleanup_completed(keep_last)
    }

    /// 记录操作到事务
    pub fn record_operation(
        &mut self,
        tx_id: u64,
        op: crate::transactions::TransactionOp,
    ) -> Result<(), crate::transactions::TransactionError> {
        self.transactions.record_op(tx_id, op)
    }

    // ========== 更新 API ==========

    /// 更新节点属性（合并模式：新属性会覆盖旧属性）
    pub fn update_node_props(&mut self, id: NodeId, props: Properties) -> bool {
        self.engine.update_node_props(id, props)
    }

    /// 更新关系属性（合并模式：新属性会覆盖旧属性）
    pub fn update_rel_props(&mut self, id: RelId, props: Properties) -> bool {
        self.engine.update_rel_props(id, props)
    }

    // ========== 可视化 API ==========

    /// 创建整个图的GraphView用于可视化
    pub fn to_graph_view(&self) -> crate::visualization::GraphView {
        let mut graph_view = crate::visualization::GraphView::new();

        // 添加所有节点
        for stored_node in self.all_stored_nodes() {
            let vis_node = crate::visualization::VisNode::new(
                stored_node.id,
                stored_node.labels,
                stored_node.props,
            );
            graph_view.add_node(vis_node);
        }

        // 添加所有边（通过遍历节点的出边）
        let mut seen_rels = std::collections::HashSet::new();
        for stored_node in self.all_stored_nodes() {
            for rel in self.neighbors_out(stored_node.id) {
                if seen_rels.insert(rel.id) {
                    let vis_edge = crate::visualization::VisEdge::new(
                        rel.start,
                        rel.end,
                        rel.typ,
                        rel.props,
                    ).with_id(format!("{}", rel.id));
                    graph_view.add_edge(vis_edge);
                }
            }
        }

        graph_view
    }

    /// 创建子图的GraphView（只包含指定的节点ID）
    pub fn to_subgraph_view(&self, node_ids: &[NodeId]) -> crate::visualization::GraphView {
        let mut graph_view = crate::visualization::GraphView::new();
        let node_set: std::collections::HashSet<NodeId> = node_ids.iter().cloned().collect();

        // 添加指定节点
        for &node_id in node_ids {
            if let Some(stored_node) = self.engine.get_node(node_id) {
                let vis_node = crate::visualization::VisNode::new(
                    stored_node.id,
                    stored_node.labels,
                    stored_node.props,
                );
                graph_view.add_node(vis_node);
            }
        }

        // 添加这些节点之间的边
        let mut seen_rels = std::collections::HashSet::new();
        for &node_id in node_ids {
            for rel in self.neighbors_out(node_id) {
                if node_set.contains(&rel.end) && seen_rels.insert(rel.id) {
                    let vis_edge = crate::visualization::VisEdge::new(
                        rel.start,
                        rel.end,
                        rel.typ,
                        rel.props,
                    ).with_id(format!("{}", rel.id));
                    graph_view.add_edge(vis_edge);
                }
            }
        }

        graph_view
    }

    /// 导出图为指定格式（JSON或DOT）
    pub fn export_graph(&self, format: crate::visualization::GraphFormat) -> Result<String, String> {
        let graph_view = self.to_graph_view();
        graph_view.export(format)
    }

    /// 导出子图为指定格式
    pub fn export_subgraph(&self, node_ids: &[NodeId], format: crate::visualization::GraphFormat) -> Result<String, String> {
        let graph_view = self.to_subgraph_view(node_ids);
        graph_view.export(format)
    }
}

