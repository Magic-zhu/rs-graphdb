//! 异步图数据库
//!
//! 提供异步 API 的图数据库包装器

use crate::graph::model::{Node, Relationship};
use crate::storage::{AsyncStorage, NodeId, RelId, StorageEngine};
use crate::values::Properties;

/// 异步图数据库
///
/// 使用异步存储引擎的图数据库包装器，提供真正的异步 API
pub struct AsyncGraphDB<E: StorageEngine + Send + 'static> {
    storage: AsyncStorage<E>,
    _phantom: std::marker::PhantomData<E>,
}

impl<E: StorageEngine + Send + 'static> AsyncGraphDB<E> {
    /// 从存储引擎创建异步图数据库
    pub fn from_engine(engine: E) -> Self {
        Self {
            storage: AsyncStorage::new(engine),
            _phantom: std::marker::PhantomData,
        }
    }

    /// 获取存储的克隆引用
    pub fn clone_storage(&self) -> AsyncStorage<E> {
        self.storage.clone()
    }

    // ========== 异步写入操作 ==========

    /// 异步创建节点
    pub async fn create_node_async(
        &self,
        labels: Vec<&str>,
        props: Properties,
    ) -> Result<NodeId, AsyncError> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let labels_owned: Vec<String> = labels.into_iter().map(|s| s.to_string()).collect();

        // 在后台任务中执行创建
        let mut storage = self.storage.clone();
        tokio::spawn(async move {
            let id = storage.create_node(labels_owned, props);
            let _ = tx.send(id);
        });

        rx.await
            .map_err(|_| AsyncError::ChannelClosed("Node creation response channel closed".into()))
    }

    /// 异步批量创建节点
    pub async fn batch_create_nodes_async(
        &self,
        nodes: Vec<(Vec<String>, Properties)>,
    ) -> Result<Vec<NodeId>, AsyncError> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let mut storage = self.storage.clone();
        tokio::spawn(async move {
            let ids = storage.batch_create_nodes(nodes);
            let _ = tx.send(ids);
        });

        rx.await
            .map_err(|_| AsyncError::ChannelClosed("Batch node creation response channel closed".into()))
    }

    /// 异步创建关系
    pub async fn create_rel_async(
        &self,
        start: NodeId,
        end: NodeId,
        typ: &str,
        props: Properties,
    ) -> Result<RelId, AsyncError> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let typ_owned = typ.to_string();

        let mut storage = self.storage.clone();
        tokio::spawn(async move {
            let id = storage.create_rel(start, end, typ_owned, props);
            let _ = tx.send(id);
        });

        rx.await
            .map_err(|_| AsyncError::ChannelClosed("Rel creation response channel closed".into()))
    }

    /// 异步批量创建关系
    pub async fn batch_create_rels_async(
        &self,
        rels: Vec<(NodeId, NodeId, String, Properties)>,
    ) -> Result<Vec<RelId>, AsyncError> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let mut storage = self.storage.clone();
        tokio::spawn(async move {
            let ids = storage.batch_create_rels(rels);
            let _ = tx.send(ids);
        });

        rx.await
            .map_err(|_| AsyncError::ChannelClosed("Batch rel creation response channel closed".into()))
    }

    // ========== 异步读操作 ==========

    /// 异步获取节点
    pub async fn get_node_async(&self, id: NodeId) -> Result<Option<Node>, AsyncError> {
        let storage = self.storage.clone();

        tokio::task::spawn_blocking(move || {
            Ok(storage.get_node(id).map(|sn| Node {
                id: sn.id,
                labels: sn.labels,
                props: sn.props,
            }))
        })
        .await
        .map_err(|e| AsyncError::TaskJoin(e.to_string()))?
    }

    /// 异步获取关系
    pub async fn get_rel_async(&self, id: RelId) -> Result<Option<Relationship>, AsyncError> {
        let storage = self.storage.clone();

        tokio::task::spawn_blocking(move || {
            Ok(storage.get_rel(id).map(|sr| Relationship {
                id: sr.id,
                start: sr.start,
                end: sr.end,
                typ: sr.typ,
                props: sr.props,
            }))
        })
        .await
        .map_err(|e| AsyncError::TaskJoin(e.to_string()))?
    }

    // ========== 流式并发写入 ==========

    /// 流式创建大量节点，自动分批处理
    pub async fn stream_create_nodes<I>(
        &self,
        nodes: I,
        batch_size: usize,
    ) -> Result<Vec<NodeId>, AsyncError>
    where
        I: IntoIterator<Item = (Vec<String>, Properties)> + Send + 'static,
        I::IntoIter: Send,
    {
        let mut all_ids = Vec::new();
        let mut batch = Vec::with_capacity(batch_size);

        for node_data in nodes {
            batch.push(node_data);

            if batch.len() >= batch_size {
                let ids = self.batch_create_nodes_async(batch.drain(..).collect()).await?;
                all_ids.extend(ids);
            }
        }

        // 处理剩余的节点
        if !batch.is_empty() {
            let ids = self.batch_create_nodes_async(batch).await?;
            all_ids.extend(ids);
        }

        Ok(all_ids)
    }

    /// 并发创建多个独立的节点
    pub async fn parallel_create_nodes(
        &self,
        nodes: Vec<(Vec<String>, Properties)>,
    ) -> Result<Vec<NodeId>, AsyncError> {
        let mut tasks = Vec::new();

        for node_data in nodes {
            let db = self.clone_storage();
            let task = tokio::spawn(async move {
                let mut storage = db;
                Ok(storage.create_node(node_data.0, node_data.1))
            });
            tasks.push(task);
        }

        let mut ids = Vec::new();
        for task in tasks {
            let id = task.await
                .map_err(|e| AsyncError::TaskJoin(e.to_string()))??;
            ids.push(id);
        }

        Ok(ids)
    }
}

impl<E: StorageEngine + Send + 'static> Clone for AsyncGraphDB<E> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

/// 异步错误类型
#[derive(Debug)]
pub enum AsyncError {
    ChannelClosed(String),
    TaskJoin(String),
    Storage(String),
}

impl std::fmt::Display for AsyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AsyncError::ChannelClosed(msg) => write!(f, "Channel closed: {}", msg),
            AsyncError::TaskJoin(msg) => write!(f, "Task join error: {}", msg),
            AsyncError::Storage(msg) => write!(f, "Storage error: {}", msg),
        }
    }
}

impl std::error::Error for AsyncError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mem_store::MemStore;
    use crate::values::Value;

    fn make_props(name: &str) -> Properties {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text(name.to_string()));
        props
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_async_create_node() {
        let engine = MemStore::new();
        let db = AsyncGraphDB::from_engine(engine);

        let id = db
            .create_node_async(vec!["User"], make_props("Alice"))
            .await
            .unwrap();

        let node = db.get_node_async(id).await.unwrap().unwrap();
        assert!(node.has_label("User"));
        assert_eq!(node.get("name"), Some(&Value::Text("Alice".to_string())));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_async_batch_create_nodes() {
        let engine = MemStore::new();
        let db = AsyncGraphDB::from_engine(engine);

        let nodes = vec![
            (vec!["User".to_string()], make_props("Alice")),
            (vec!["User".to_string()], make_props("Bob")),
            (vec!["Admin".to_string()], make_props("Admin")),
        ];

        let ids = db
            .batch_create_nodes_async(nodes)
            .await
            .unwrap();

        assert_eq!(ids.len(), 3);

        // 验证数据
        let alice = db.get_node_async(ids[0]).await.unwrap().unwrap();
        assert_eq!(alice.get("name"), Some(&Value::Text("Alice".to_string())));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_async_stream_create_nodes() {
        let engine = MemStore::new();
        let db = AsyncGraphDB::from_engine(engine);

        let nodes: Vec<_> = (0..100)
            .map(|i| {
                let mut props = Properties::new();
                props.insert("id".to_string(), Value::Int(i));
                (vec!["Node".to_string()], props)
            })
            .collect();

        let ids = db
            .stream_create_nodes(nodes, 10)
            .await
            .unwrap();

        assert_eq!(ids.len(), 100);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_async_parallel_create_nodes() {
        let engine = MemStore::new();
        let db = AsyncGraphDB::from_engine(engine);

        let nodes: Vec<_> = (0..10)
            .map(|i| {
                let mut props = Properties::new();
                props.insert("id".to_string(), Value::Int(i));
                (vec!["Node".to_string()], props)
            })
            .collect();

        let ids = db
            .parallel_create_nodes(nodes)
            .await
            .unwrap();

        assert_eq!(ids.len(), 10);
    }
}
