//! 异步存储引擎
//!
//! 使用 channel 将写入操作发送到后台任务，实现异步写入

use super::{NodeId, RelId, StoredNode, StoredRel, StorageEngine};
use crate::values::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::marker::PhantomData;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::task::JoinHandle;

/// 异步写入命令
enum AsyncCommand {
    CreateNode {
        labels: Vec<String>,
        props: HashMap<String, Value>,
        response: oneshot::Sender<NodeId>,
    },
    CreateRel {
        start: NodeId,
        end: NodeId,
        typ: String,
        props: HashMap<String, Value>,
        response: oneshot::Sender<RelId>,
    },
    BatchCreateNodes {
        nodes: Vec<(Vec<String>, HashMap<String, Value>)>,
        response: oneshot::Sender<Vec<NodeId>>,
    },
    BatchCreateRels {
        rels: Vec<(NodeId, NodeId, String, HashMap<String, Value>)>,
        response: oneshot::Sender<Vec<RelId>>,
    },
    GetNode {
        id: NodeId,
        response: oneshot::Sender<Option<StoredNode>>,
    },
    GetRel {
        id: RelId,
        response: oneshot::Sender<Option<StoredRel>>,
    },
    Shutdown {
        response: oneshot::Sender<()>,
    },
}

/// 异步存储引擎包装器
///
/// 将同步存储引擎包装成异步版本，写入操作通过 channel 发送到后台任务
pub struct AsyncStorage<E: StorageEngine + Send + 'static> {
    cmd_tx: mpsc::Sender<AsyncCommand>,
    _handle: Arc<RwLock<Option<JoinHandle<()>>>>,
    _phantom: PhantomData<E>,
}

impl<E: StorageEngine + Send + 'static> AsyncStorage<E> {
    /// 创建新的异步存储引擎
    pub fn new(mut engine: E) -> Self {
        let (cmd_tx, mut cmd_rx) = mpsc::channel(1000);

        // 启动后台任务处理写入
        let handle = tokio::spawn(async move {
            loop {
                match cmd_rx.recv().await {
                    Some(AsyncCommand::CreateNode { labels, props, response }) => {
                        let id = engine.create_node(labels, props);
                        let _ = response.send(id);
                    }
                    Some(AsyncCommand::CreateRel { start, end, typ, props, response }) => {
                        let id = engine.create_rel(start, end, typ, props);
                        let _ = response.send(id);
                    }
                    Some(AsyncCommand::BatchCreateNodes { nodes, response }) => {
                        let ids = engine.batch_create_nodes(nodes);
                        let _ = response.send(ids);
                    }
                    Some(AsyncCommand::BatchCreateRels { rels, response }) => {
                        let ids = engine.batch_create_rels(rels);
                        let _ = response.send(ids);
                    }
                    Some(AsyncCommand::GetNode { id, response }) => {
                        let node = engine.get_node(id);
                        let _ = response.send(node);
                    }
                    Some(AsyncCommand::GetRel { id, response }) => {
                        let rel = engine.get_rel(id);
                        let _ = response.send(rel);
                    }
                    Some(AsyncCommand::Shutdown { response }) => {
                        let _ = response.send(());
                        break;
                    }
                    None => {
                        // channel 关闭，退出
                        break;
                    }
                }
            }
        });

        Self {
            cmd_tx,
            _handle: Arc::new(RwLock::new(Some(handle))),
            _phantom: PhantomData,
        }
    }
}

impl<E: StorageEngine + Send + 'static> Clone for AsyncStorage<E> {
    fn clone(&self) -> Self {
        Self {
            cmd_tx: self.cmd_tx.clone(),
            _handle: Arc::clone(&self._handle),
            _phantom: PhantomData,
        }
    }
}

/// 为异步存储实现存储引擎 trait
impl<E: StorageEngine + Send + 'static> StorageEngine for AsyncStorage<E> {
    fn create_node(
        &mut self,
        labels: Vec<String>,
        props: HashMap<String, Value>,
    ) -> NodeId {
        let (tx, rx) = oneshot::channel();

        let cmd = AsyncCommand::CreateNode {
            labels,
            props,
            response: tx,
        };

        tokio::task::block_in_place(|| {
            let handle = self.cmd_tx.clone();
            tokio::runtime::Handle::current().block_on(async move {
                let _ = handle.send(cmd).await;
                rx.await
            })
        })
        .unwrap()
    }

    fn create_rel(
        &mut self,
        start: NodeId,
        end: NodeId,
        typ: String,
        props: HashMap<String, Value>,
    ) -> RelId {
        let (tx, rx) = oneshot::channel();

        let cmd = AsyncCommand::CreateRel {
            start,
            end,
            typ,
            props,
            response: tx,
        };

        tokio::task::block_in_place(|| {
            let handle = self.cmd_tx.clone();
            tokio::runtime::Handle::current().block_on(async move {
                let _ = handle.send(cmd).await;
                rx.await
            })
        })
        .unwrap()
    }

    fn batch_create_nodes(
        &mut self,
        nodes: Vec<(Vec<String>, HashMap<String, Value>)>,
    ) -> Vec<NodeId> {
        let (tx, rx) = oneshot::channel();

        let cmd = AsyncCommand::BatchCreateNodes {
            nodes,
            response: tx,
        };

        tokio::task::block_in_place(|| {
            let handle = self.cmd_tx.clone();
            tokio::runtime::Handle::current().block_on(async move {
                let _ = handle.send(cmd).await;
                rx.await
            })
        })
        .unwrap()
    }

    fn batch_create_rels(
        &mut self,
        rels: Vec<(NodeId, NodeId, String, HashMap<String, Value>)>,
    ) -> Vec<RelId> {
        let (tx, rx) = oneshot::channel();

        let cmd = AsyncCommand::BatchCreateRels {
            rels,
            response: tx,
        };

        tokio::task::block_in_place(|| {
            let handle = self.cmd_tx.clone();
            tokio::runtime::Handle::current().block_on(async move {
                let _ = handle.send(cmd).await;
                rx.await
            })
        })
        .unwrap()
    }

    fn get_node(&self, id: NodeId) -> Option<StoredNode> {
        let (tx, rx) = oneshot::channel();

        let cmd = AsyncCommand::GetNode {
            id,
            response: tx,
        };

        tokio::task::block_in_place(|| {
            let handle = self.cmd_tx.clone();
            tokio::runtime::Handle::current().block_on(async move {
                let _ = handle.send(cmd).await;
                rx.await.ok()
            })
        })
        .flatten()
    }

    fn get_rel(&self, id: RelId) -> Option<StoredRel> {
        let (tx, rx) = oneshot::channel();

        let cmd = AsyncCommand::GetRel {
            id,
            response: tx,
        };

        tokio::task::block_in_place(|| {
            let handle = self.cmd_tx.clone();
            tokio::runtime::Handle::current().block_on(async move {
                let _ = handle.send(cmd).await;
                rx.await.ok()
            })
        })
        .flatten()
    }

    fn all_nodes(&self) -> Box<dyn Iterator<Item = StoredNode> + '_> {
        Box::new(std::iter::empty())
    }

    fn outgoing_rels(&self, _node: NodeId) -> Box<dyn Iterator<Item = StoredRel> + '_> {
        Box::new(std::iter::empty())
    }

    fn incoming_rels(&self, _node: NodeId) -> Box<dyn Iterator<Item = StoredRel> + '_> {
        Box::new(std::iter::empty())
    }

    fn delete_node(&mut self, _id: NodeId) -> bool {
        false
    }

    fn delete_rel(&mut self, _id: RelId) -> bool {
        false
    }
}
