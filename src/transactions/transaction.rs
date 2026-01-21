// 事务操作定义
//
// 定义所有可以在事务中执行的操作类型

use crate::storage::{NodeId, RelId};
use crate::values::Properties;
use serde::{Deserialize, Serialize};
use std::fmt;

/// 事务操作类型
///
/// 记录所有修改数据的操作，用于重放和回滚
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransactionOp {
    /// 创建节点
    CreateNode {
        id: NodeId,
        labels: Vec<String>,
        properties: Properties,
    },

    /// 创建关系
    CreateRel {
        id: RelId,
        start: NodeId,
        end: NodeId,
        typ: String,
        properties: Properties,
    },

    /// 删除节点（包含被删除节点的完整数据，用于回滚）
    DeleteNode {
        id: NodeId,
        node: NodeData,
    },

    /// 删除关系（包含被删除关系的完整数据，用于回滚）
    DeleteRel {
        id: RelId,
        rel: RelData,
    },

    /// 更新节点属性
    UpdateNode {
        id: NodeId,
        old_properties: Properties, // 用于回滚
        new_properties: Properties,  // 用于重放
    },

    /// 更新关系属性
    UpdateRel {
        id: RelId,
        old_properties: Properties, // 用于回滚
        new_properties: Properties,  // 用于重放
    },
}

/// 节点数据（用于快照和回滚）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeData {
    pub id: NodeId,
    pub labels: Vec<String>,
    pub properties: Properties,
}

/// 关系数据（用于快照和回滚）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelData {
    pub id: RelId,
    pub start: NodeId,
    pub end: NodeId,
    pub typ: String,
    pub properties: Properties,
}

/// 事务状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    /// 活动中
    Active,
    /// 已提交
    Committed,
    /// 已回滚
    RolledBack,
}

/// 事务错误类型
#[derive(Debug)]
pub enum TransactionError {
    /// 事务未找到
    TransactionNotFound(u64),
    /// 事务已完成
    TransactionAlreadyCompleted(u64, TransactionStatus),
    /// 存储错误
    StorageError(String),
    /// 快照错误
    SnapshotError(String),
    /// 其他错误
    Other(String),
    /// 保存点未找到
    SavepointNotFound(String),
    /// 保存点已存在
    SavepointAlreadyExists(String),
    /// 版本冲突（乐观锁）
    VersionConflict {
        expected: u64,
        actual: u64,
    },
}

impl fmt::Display for TransactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionError::TransactionNotFound(id) => write!(f, "Transaction {} not found", id),
            TransactionError::TransactionAlreadyCompleted(id, status) => {
                write!(f, "Transaction {} already completed with status {:?}", id, status)
            }
            TransactionError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            TransactionError::SnapshotError(msg) => write!(f, "Snapshot error: {}", msg),
            TransactionError::Other(msg) => write!(f, "Other error: {}", msg),
            TransactionError::SavepointNotFound(name) => write!(f, "Savepoint '{}' not found", name),
            TransactionError::SavepointAlreadyExists(name) => write!(f, "Savepoint '{}' already exists", name),
            TransactionError::VersionConflict { expected, actual } => {
                write!(f, "Version conflict: expected {}, found {}", expected, actual)
            }
        }
    }
}

impl std::error::Error for TransactionError {}

/// 事务结果类型
pub type TransactionResult<T> = Result<T, TransactionError>;

/// 保存点
///
/// 用于事务中的部分回滚
#[derive(Debug, Clone)]
pub struct Savepoint {
    /// 保存点名称
    pub name: String,
    /// 操作日志的快照（在创建保存点时的操作数量）
    pub ops_count: usize,
}

impl Savepoint {
    pub fn new(name: String, ops_count: usize) -> Self {
        Self { name, ops_count }
    }
}

/// 事务
///
/// 表示一个正在进行中的事务
#[derive(Debug, Clone)]
pub struct Transaction {
    /// 事务ID
    pub id: u64,
    /// 操作日志
    pub ops: Vec<TransactionOp>,
    /// 事务状态
    pub status: TransactionStatus,
    /// 开始时间戳
    pub start_time: u64,
    /// 关联的快照ID（如果有）
    pub snapshot_id: Option<u64>,
    /// 保存点列表
    pub savepoints: Vec<Savepoint>,
}

impl Transaction {
    /// 创建新事务
    pub fn new(id: u64) -> Self {
        Self {
            id,
            ops: Vec::new(),
            status: TransactionStatus::Active,
            start_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            snapshot_id: None,
            savepoints: Vec::new(),
        }
    }

    /// 添加操作到事务日志
    pub fn add_op(&mut self, op: TransactionOp) {
        self.ops.push(op);
    }

    /// 获取操作数量
    pub fn op_count(&self) -> usize {
        self.ops.len()
    }

    /// 标记为已提交
    pub fn mark_committed(&mut self) {
        self.status = TransactionStatus::Committed;
    }

    /// 标记为已回滚
    pub fn mark_rolled_back(&mut self) {
        self.status = TransactionStatus::RolledBack;
    }

    /// 是否已完成
    pub fn is_completed(&self) -> bool {
        matches!(self.status, TransactionStatus::Committed | TransactionStatus::RolledBack)
    }

    /// 创建保存点
    pub fn create_savepoint(&mut self, name: String) -> TransactionResult<()> {
        // 检查保存点名称是否已存在
        if self.savepoints.iter().any(|s| s.name == name) {
            return Err(TransactionError::SavepointAlreadyExists(name));
        }

        let savepoint = Savepoint::new(name, self.ops.len());
        self.savepoints.push(savepoint);
        Ok(())
    }

    /// 回滚到保存点
    pub fn rollback_to_savepoint(&mut self, name: String) -> TransactionResult<()> {
        // 查找保存点
        let savepoint_index = self.savepoints
            .iter()
            .position(|s| s.name == name)
            .ok_or_else(|| TransactionError::SavepointNotFound(name.clone()))?;

        let savepoint = &self.savepoints[savepoint_index];

        // 移除保存点之后的所有操作
        self.ops.truncate(savepoint.ops_count);

        // 移除该保存点之后创建的所有保存点
        self.savepoints.truncate(savepoint_index + 1);

        Ok(())
    }

    /// 释放保存点
    pub fn release_savepoint(&mut self, name: String) -> TransactionResult<()> {
        let index = self.savepoints
            .iter()
            .position(|s| s.name == name)
            .ok_or_else(|| TransactionError::SavepointNotFound(name))?;

        self.savepoints.remove(index);
        Ok(())
    }

    /// 获取保存点数量
    pub fn savepoint_count(&self) -> usize {
        self.savepoints.len()
    }

    /// 检查保存点是否存在
    pub fn has_savepoint(&self, name: &str) -> bool {
        self.savepoints.iter().any(|s| s.name == name)
    }
}

/// 事务管理器
///
/// 管理所有事务的生命周期
#[derive(Debug)]
pub struct TransactionManager {
    /// 活动的事务
    active_transactions: std::collections::HashMap<u64, Transaction>,
    /// 已完成的事务（用于审计）
    completed_transactions: Vec<Transaction>,
    /// 下一个事务ID
    next_tx_id: u64,
    /// 默认超时时间（秒）
    default_timeout_secs: u64,
}

impl TransactionManager {
    /// 创建新的事务管理器
    pub fn new() -> Self {
        Self {
            active_transactions: std::collections::HashMap::new(),
            completed_transactions: Vec::new(),
            next_tx_id: 0,
            default_timeout_secs: 30, // 默认30秒超时
        }
    }

    /// 创建带默认超时的事务管理器
    pub fn with_timeout(timeout_secs: u64) -> Self {
        Self {
            active_transactions: std::collections::HashMap::new(),
            completed_transactions: Vec::new(),
            next_tx_id: 0,
            default_timeout_secs: timeout_secs,
        }
    }

    /// 开始新事务
    pub fn begin_transaction(&mut self) -> Transaction {
        self.begin_transaction_with_timeout(self.default_timeout_secs)
    }

    /// 开始带超时的新事务
    pub fn begin_transaction_with_timeout(&mut self, timeout_secs: u64) -> Transaction {
        let id = self.next_tx_id;
        self.next_tx_id += 1;

        let mut tx = Transaction::new(id);
        // 存储超时时间在 snapshot_id 字段中（临时解决方案）
        // 实际应该添加单独的 timeout 字段
        tx.snapshot_id = Some(timeout_secs);

        self.active_transactions.insert(id, tx.clone());
        tx
    }

    /// 提交事务
    pub fn commit(&mut self, tx_id: u64) -> TransactionResult<()> {
        let mut tx = self.active_transactions.remove(&tx_id)
            .ok_or_else(|| TransactionError::TransactionNotFound(tx_id))?;

        if tx.is_completed() {
            return Err(TransactionError::TransactionAlreadyCompleted(tx_id, tx.status));
        }

        tx.mark_committed();
        self.completed_transactions.push(tx);
        Ok(())
    }

    /// 回滚事务
    pub fn rollback(&mut self, tx_id: u64) -> TransactionResult<()> {
        let mut tx = self.active_transactions.remove(&tx_id)
            .ok_or_else(|| TransactionError::TransactionNotFound(tx_id))?;

        if tx.is_completed() {
            return Err(TransactionError::TransactionAlreadyCompleted(tx_id, tx.status));
        }

        tx.mark_rolled_back();
        self.completed_transactions.push(tx);
        Ok(())
    }

    /// 检查并清理超时的事务
    /// 返回被回滚的事务ID列表
    pub fn cleanup_expired_transactions(&mut self) -> Vec<u64> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let expired_ids: Vec<u64> = self.active_transactions
            .iter()
            .filter(|(_, tx)| {
                if let Some(timeout) = tx.snapshot_id {
                    current_time - tx.start_time > timeout
                } else {
                    false
                }
            })
            .map(|(id, _)| *id)
            .collect();

        for id in &expired_ids {
            if let Some(mut tx) = self.active_transactions.remove(id) {
                tx.mark_rolled_back();
                self.completed_transactions.push(tx);
            }
        }

        expired_ids
    }

    /// 获取事务的剩余超时时间（秒）
    pub fn get_timeout_remaining(&self, tx_id: u64) -> Option<u64> {
        let tx = self.active_transactions.get(&tx_id)?;
        let timeout = tx.snapshot_id?;
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        if current_time >= tx.start_time + timeout {
            Some(0)
        } else {
            Some(tx.start_time + timeout - current_time)
        }
    }

    /// 检查事务是否已超时
    pub fn is_expired(&self, tx_id: u64) -> bool {
        self.get_timeout_remaining(tx_id).map_or(false, |t| t == 0)
    }

    /// 获取事务
    pub fn get_transaction(&self, tx_id: u64) -> Option<&Transaction> {
        self.active_transactions.get(&tx_id)
    }

    /// 获取可变事务
    pub fn get_transaction_mut(&mut self, tx_id: u64) -> Option<&mut Transaction> {
        self.active_transactions.get_mut(&tx_id)
    }

    /// 记录操作到事务
    pub fn record_op(&mut self, tx_id: u64, op: TransactionOp) -> TransactionResult<()> {
        let tx = self.active_transactions.get_mut(&tx_id)
            .ok_or_else(|| TransactionError::TransactionNotFound(tx_id))?;

        if tx.is_completed() {
            return Err(TransactionError::TransactionAlreadyCompleted(tx_id, tx.status));
        }

        tx.add_op(op);
        Ok(())
    }

    /// 获取活动事务数量
    pub fn active_count(&self) -> usize {
        self.active_transactions.len()
    }

    /// 获取已完成事务数量
    pub fn completed_count(&self) -> usize {
        self.completed_transactions.len()
    }

    /// 清理已完成的旧事务
    pub fn cleanup_completed(&mut self, keep_last: usize) {
        if self.completed_transactions.len() > keep_last {
            let remove_count = self.completed_transactions.len() - keep_last;
            for _ in 0..remove_count {
                self.completed_transactions.remove(0);
            }
        }
    }

    /// 获取所有活动事务ID
    pub fn active_transaction_ids(&self) -> Vec<u64> {
        self.active_transactions.keys().cloned().collect()
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionOp {
    /// 获取操作的描述
    pub fn description(&self) -> String {
        match self {
            TransactionOp::CreateNode { id, .. } => format!("CreateNode({})", id),
            TransactionOp::CreateRel { id, .. } => format!("CreateRel({})", id),
            TransactionOp::DeleteNode { id, .. } => format!("DeleteNode({})", id),
            TransactionOp::DeleteRel { id, .. } => format!("DeleteRel({})", id),
            TransactionOp::UpdateNode { id, .. } => format!("UpdateNode({})", id),
            TransactionOp::UpdateRel { id, .. } => format!("UpdateRel({})", id),
        }
    }

    /// 判断操作是否修改数据
    pub fn is_mutating(&self) -> bool {
        !matches!(self, TransactionOp::CreateNode { .. } | TransactionOp::CreateRel { .. })
    }

    /// 获取受影响的节点ID
    pub fn affected_node(&self) -> Option<NodeId> {
        match self {
            TransactionOp::CreateNode { id, .. } => Some(*id),
            TransactionOp::DeleteNode { id, .. } => Some(*id),
            TransactionOp::UpdateNode { id, .. } => Some(*id),
            _ => None,
        }
    }

    /// 获取受影响的关系ID
    pub fn affected_rel(&self) -> Option<RelId> {
        match self {
            TransactionOp::CreateRel { id, .. } => Some(*id),
            TransactionOp::DeleteRel { id, .. } => Some(*id),
            TransactionOp::UpdateRel { id, .. } => Some(*id),
            _ => None,
        }
    }
}

impl From<crate::storage::mem_store::TxOp> for TransactionOp {
    fn from(op: crate::storage::mem_store::TxOp) -> Self {
        match op {
            crate::storage::mem_store::TxOp::CreateNode(id, labels, props) => {
                TransactionOp::CreateNode { id, labels, properties: props }
            }
            crate::storage::mem_store::TxOp::CreateRel(id, start, end, typ, props) => {
                TransactionOp::CreateRel { id, start, end, typ, properties: props }
            }
            crate::storage::mem_store::TxOp::DeleteNode(id, node) => {
                TransactionOp::DeleteNode {
                    id,
                    node: NodeData {
                        id,
                        labels: node.labels,
                        properties: node.props,
                    },
                }
            }
            crate::storage::mem_store::TxOp::DeleteRel(id, rel) => {
                TransactionOp::DeleteRel {
                    id,
                    rel: RelData {
                        id,
                        start: rel.start,
                        end: rel.end,
                        typ: rel.typ,
                        properties: rel.props,
                    },
                }
            }
            crate::storage::mem_store::TxOp::UpdateNode(id, props) => {
                TransactionOp::UpdateNode {
                    id,
                    old_properties: Properties::new(), // 需要调用者提供
                    new_properties: props,
                }
            }
            crate::storage::mem_store::TxOp::UpdateRel(id, props) => {
                TransactionOp::UpdateRel {
                    id,
                    old_properties: Properties::new(), // 需要调用者提供
                    new_properties: props,
                }
            }
        }
    }
}
