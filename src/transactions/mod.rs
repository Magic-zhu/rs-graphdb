// 事务管理模块
//
// 提供完整的事务支持，包括：
// - 事务生命周期管理
// - 操作日志记录
// - 快照机制
// - 回滚支持
// - 锁管理

pub mod snapshot;
pub mod transaction;
pub mod locks;
pub mod optimistic_lock;
pub mod isolation;
pub mod deadlock;

pub use snapshot::{Snapshot, SnapshotManager, SnapshotNode, SnapshotRel};
pub use transaction::{
    Transaction, TransactionManager, TransactionOp, TransactionResult,
    TransactionError, TransactionStatus, NodeData, RelData, Savepoint,
};
pub use locks::{LockManager, LockType, LockRequest, LockEntry};
pub use optimistic_lock::{
    OptimisticLock, OptimisticLockManager, OptimisticLockStats,
    OptimisticReadContext, Version,
};
pub use isolation::{
    IsolationExecutor, IsolationStats, ReadSet, WriteSet,
};
pub use deadlock::{
    DeadlockDetector, DeadlockInfo, DeadlockStats,
    WaitGraph, WaitGraphStats, TimeoutDetector, TimeoutStats,
    PreventiveDeadlockDetector, PreventiveStats, Resource, LockHolder,
};

use crate::storage::{NodeId, RelId};
use crate::values::Properties;

/// 事务句柄（公开类型）
pub type TxHandle = crate::storage::TxHandle;

/// 事务隔离级别
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// 读未提交
    ReadUncommitted,
    /// 读已提交
    ReadCommitted,
    /// 可重复读
    RepeatableRead,
    /// 可串行化
    Serializable,
}

impl Default for IsolationLevel {
    fn default() -> Self {
        Self::ReadCommitted
    }
}

/// 事务配置
#[derive(Debug, Clone)]
pub struct TransactionConfig {
    /// 隔离级别
    pub isolation_level: IsolationLevel,
    /// 是否启用快照
    pub enable_snapshot: bool,
    /// 超时时间（秒）
    pub timeout_secs: Option<u64>,
}

impl Default for TransactionConfig {
    fn default() -> Self {
        Self {
            isolation_level: IsolationLevel::default(),
            enable_snapshot: true,
            timeout_secs: Some(30),
        }
    }
}

impl TransactionConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_isolation_level(mut self, level: IsolationLevel) -> Self {
        self.isolation_level = level;
        self
    }

    pub fn with_snapshot(mut self, enable: bool) -> Self {
        self.enable_snapshot = enable;
        self
    }

    pub fn with_timeout(mut self, secs: u64) -> Self {
        self.timeout_secs = Some(secs);
        self
    }

    pub fn no_timeout(mut self) -> Self {
        self.timeout_secs = None;
        self
    }
}
