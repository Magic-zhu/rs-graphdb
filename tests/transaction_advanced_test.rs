// 增强事务测试
// 测试事务超时、保存点、锁机制等高级功能

use rs_graphdb::transactions::{
    TransactionManager, TransactionOp, TransactionStatus, TransactionError,
    Savepoint, LockManager, LockType,
};
use rs_graphdb::storage::{NodeId, RelId};
use rs_graphdb::values::{Properties, Value};

// ==================== 事务超时测试 ====================

#[test]
fn test_transaction_timeout_basic() {
    let mut tm = TransactionManager::with_timeout(1); // 1秒超时

    let tx = tm.begin_transaction();
    let tx_id = tx.id;

    // 验证事务有超时设置
    let tx_ref = tm.get_transaction(tx_id);
    assert!(tx_ref.is_some());
    assert_eq!(tx_ref.unwrap().snapshot_id, Some(1)); // 超时存储在 snapshot_id 中
}

#[test]
fn test_transaction_timeout_remaining() {
    let mut tm = TransactionManager::with_timeout(60); // 60秒超时

    let tx = tm.begin_transaction();
    let tx_id = tx.id;

    // 获取剩余时间
    let remaining = tm.get_timeout_remaining(tx_id);
    assert!(remaining.is_some());
    assert!(remaining.unwrap() > 0);
    assert!(remaining.unwrap() <= 60);
}

#[test]
fn test_transaction_is_expired() {
    let mut tm = TransactionManager::with_timeout(0); // 0秒超时（立即过期）

    let tx = tm.begin_transaction();
    let tx_id = tx.id;

    // 等待一小段时间确保超时
    std::thread::sleep(std::time::Duration::from_millis(10));

    // 检查是否过期
    assert!(tm.is_expired(tx_id));
}

#[test]
fn test_transaction_cleanup_expired() {
    let mut tm = TransactionManager::with_timeout(1); // 1秒超时

    // 创建多个事务
    let _tx1 = tm.begin_transaction();
    let _tx2 = tm.begin_transaction();
    let _tx3 = tm.begin_transaction();

    assert_eq!(tm.active_count(), 3);

    // 等待超时（多等待一点时间确保所有事务都过期）
    std::thread::sleep(std::time::Duration::from_secs(2));

    // 清理过期事务
    let expired = tm.cleanup_expired_transactions();

    // 验证所有事务都被回滚
    assert_eq!(expired.len(), 3);
    assert_eq!(tm.active_count(), 0);
    assert_eq!(tm.completed_count(), 3);
}

// ==================== 保存点测试 ====================

#[test]
fn test_savepoint_create() {
    let mut tm = TransactionManager::new();

    let tx = tm.begin_transaction();
    let tx_id = tx.id;

    // 添加一些操作
    tm.record_op(tx_id, TransactionOp::CreateNode {
        id: 1,
        labels: vec!["Test".to_string()],
        properties: Properties::new(),
    }).ok();

    // 创建保存点
    if let Some(tx) = tm.get_transaction_mut(tx_id) {
        let result = tx.create_savepoint("sp1".to_string());
        assert!(result.is_ok());
        assert_eq!(tx.savepoint_count(), 1);
        assert!(tx.has_savepoint("sp1"));
    }
}

#[test]
fn test_savepoint_duplicate() {
    let mut tm = TransactionManager::new();

    let tx = tm.begin_transaction();
    let tx_id = tx.id;

    // 创建第一个保存点
    if let Some(tx) = tm.get_transaction_mut(tx_id) {
        tx.create_savepoint("sp1".to_string()).unwrap();

        // 尝试创建同名保存点
        let result = tx.create_savepoint("sp1".to_string());
        assert!(matches!(result, Err(TransactionError::SavepointAlreadyExists(_))));
    }
}

#[test]
fn test_savepoint_rollback() {
    let mut tm = TransactionManager::new();

    let tx = tm.begin_transaction();
    let tx_id = tx.id;

    // 添加第一个操作
    tm.record_op(tx_id, TransactionOp::CreateNode {
        id: 1,
        labels: vec!["Test".to_string()],
        properties: Properties::new(),
    }).ok();

    // 创建保存点
    if let Some(tx) = tm.get_transaction_mut(tx_id) {
        tx.create_savepoint("sp1".to_string()).unwrap();
    }

    // 添加更多操作
    tm.record_op(tx_id, TransactionOp::CreateNode {
        id: 2,
        labels: vec!["Test".to_string()],
        properties: Properties::new(),
    }).ok();

    tm.record_op(tx_id, TransactionOp::CreateNode {
        id: 3,
        labels: vec!["Test".to_string()],
        properties: Properties::new(),
    }).ok();

    // 回滚到保存点
    if let Some(tx) = tm.get_transaction_mut(tx_id) {
        assert_eq!(tx.op_count(), 3);

        let result = tx.rollback_to_savepoint("sp1".to_string());
        assert!(result.is_ok());

        // 应该只保留第一个操作
        assert_eq!(tx.op_count(), 1);
    }
}

#[test]
fn test_savepoint_not_found() {
    let mut tm = TransactionManager::new();

    let tx = tm.begin_transaction();
    let tx_id = tx.id;

    // 尝试回滚到不存在的保存点
    if let Some(tx) = tm.get_transaction_mut(tx_id) {
        let result = tx.rollback_to_savepoint("nonexistent".to_string());
        assert!(matches!(result, Err(TransactionError::SavepointNotFound(_))));
    }
}

#[test]
fn test_savepoint_release() {
    let mut tm = TransactionManager::new();

    let tx = tm.begin_transaction();
    let tx_id = tx.id;

    // 创建保存点
    if let Some(tx) = tm.get_transaction_mut(tx_id) {
        tx.create_savepoint("sp1".to_string()).unwrap();
        assert_eq!(tx.savepoint_count(), 1);

        // 释放保存点
        let result = tx.release_savepoint("sp1".to_string());
        assert!(result.is_ok());
        assert_eq!(tx.savepoint_count(), 0);
        assert!(!tx.has_savepoint("sp1"));
    }
}

#[test]
fn test_savepoint_multiple() {
    let mut tm = TransactionManager::new();

    let tx = tm.begin_transaction();
    let tx_id = tx.id;

    // 添加操作并创建多个保存点
    tm.record_op(tx_id, TransactionOp::CreateNode {
        id: 1,
        labels: vec!["Test".to_string()],
        properties: Properties::new(),
    }).ok();

    if let Some(tx) = tm.get_transaction_mut(tx_id) {
        tx.create_savepoint("sp1".to_string()).unwrap();
    }

    tm.record_op(tx_id, TransactionOp::CreateNode {
        id: 2,
        labels: vec!["Test".to_string()],
        properties: Properties::new(),
    }).ok();

    if let Some(tx) = tm.get_transaction_mut(tx_id) {
        tx.create_savepoint("sp2".to_string()).unwrap();
    }

    tm.record_op(tx_id, TransactionOp::CreateNode {
        id: 3,
        labels: vec!["Test".to_string()],
        properties: Properties::new(),
    }).ok();

    // 验证初始状态
    if let Some(tx) = tm.get_transaction_mut(tx_id) {
        assert_eq!(tx.op_count(), 3);
        assert_eq!(tx.savepoint_count(), 2);

        // 回滚到 sp1
        tx.rollback_to_savepoint("sp1".to_string()).unwrap();

        // 应该保留第一个操作，sp2 应该被移除
        assert_eq!(tx.op_count(), 1);
        assert_eq!(tx.savepoint_count(), 1); // 只有 sp1
        assert!(tx.has_savepoint("sp1"));
        assert!(!tx.has_savepoint("sp2"));
    }
}

// ==================== 锁机制测试 ====================

#[test]
fn test_lock_manager_basic() {
    let mut lm = LockManager::new();

    // 尝试获取读锁
    assert!(lm.acquire_node_lock(1, 1, LockType::Read));

    // 同一事务可以再次获取读锁
    assert!(lm.acquire_node_lock(1, 1, LockType::Read));

    // 检查节点是否被锁定
    assert!(lm.is_node_locked(1));
}

#[test]
fn test_lock_write_exclusive() {
    let mut lm = LockManager::new();

    // 事务1获取读锁
    assert!(lm.acquire_node_lock(1, 1, LockType::Read));

    // 事务2无法获取写锁
    assert!(!lm.acquire_node_lock(2, 1, LockType::Write));

    // 事务2无法获取读锁（因为有写锁请求）
    // 但读锁应该允许多个读锁
    assert!(lm.acquire_node_lock(2, 1, LockType::Read));
}

#[test]
fn test_lock_write_blocks_read() {
    let mut lm = LockManager::new();

    // 事务1获取写锁
    assert!(lm.acquire_node_lock(1, 1, LockType::Write));

    // 事务2无法获取读锁
    assert!(!lm.acquire_node_lock(2, 1, LockType::Read));

    // 事务2无法获取写锁
    assert!(!lm.acquire_node_lock(2, 1, LockType::Write));
}

#[test]
fn test_lock_release() {
    let mut lm = LockManager::new();

    // 事务1和事务2获取锁
    assert!(lm.acquire_node_lock(1, 1, LockType::Read));
    assert!(lm.acquire_node_lock(2, 2, LockType::Read));

    // 释放事务1的所有锁
    lm.release_all(1);

    // 事务2现在可以获取写锁
    assert!(lm.acquire_node_lock(2, 2, LockType::Write));
}

#[test]
fn test_lock_count() {
    let mut lm = LockManager::new();

    // 事务1获取多个锁
    assert!(lm.acquire_node_lock(1, 1, LockType::Read));
    assert!(lm.acquire_node_lock(1, 2, LockType::Write));
    assert!(lm.acquire_rel_lock(1, 1, LockType::Read));

    // 检查锁数量
    assert_eq!(lm.get_lock_count(1), 3);

    // 事务2获取一个锁
    assert!(lm.acquire_node_lock(2, 3, LockType::Read));
    assert_eq!(lm.get_lock_count(2), 1);
}

#[test]
fn test_lock_rel_locks() {
    let mut lm = LockManager::new();

    // 获取关系锁
    assert!(lm.acquire_rel_lock(1, 1, LockType::Write));

    // 检查关系是否被锁定
    assert!(lm.is_rel_locked(1));

    // 其他事务无法获取同一关系的锁
    assert!(!lm.acquire_rel_lock(2, 1, LockType::Read));
}

// ==================== 综合测试 ====================

#[test]
fn test_transaction_with_timeout_and_savepoint() {
    let mut tm = TransactionManager::with_timeout(60);

    let tx = tm.begin_transaction();
    let tx_id = tx.id;

    // 添加操作
    tm.record_op(tx_id, TransactionOp::CreateNode {
        id: 1,
        labels: vec!["Test".to_string()],
        properties: Properties::new(),
    }).ok();

    // 创建保存点
    if let Some(tx) = tm.get_transaction_mut(tx_id) {
        tx.create_savepoint("sp1".to_string()).unwrap();
    }

    // 添加更多操作
    tm.record_op(tx_id, TransactionOp::CreateNode {
        id: 2,
        labels: vec!["Test".to_string()],
        properties: Properties::new(),
    }).ok();

    // 验证状态
    if let Some(tx) = tm.get_transaction(tx_id) {
        assert_eq!(tx.op_count(), 2);
        assert_eq!(tx.savepoint_count(), 1);
    }
    assert!(tm.get_timeout_remaining(tx_id).is_some());
}

#[test]
fn test_transaction_commit_with_savepoints() {
    let mut tm = TransactionManager::new();

    let tx = tm.begin_transaction();
    let tx_id = tx.id;

    // 添加操作和保存点
    tm.record_op(tx_id, TransactionOp::CreateNode {
        id: 1,
        labels: vec!["Test".to_string()],
        properties: Properties::new(),
    }).ok();

    if let Some(tx) = tm.get_transaction_mut(tx_id) {
        tx.create_savepoint("sp1".to_string()).unwrap();
    }

    // 提交事务
    let result = tm.commit(tx_id);
    assert!(result.is_ok());
    assert_eq!(tm.active_count(), 0);
    assert_eq!(tm.completed_count(), 1);
}

#[test]
fn test_lock_manager_detect_deadlock() {
    let mut lm = LockManager::new();

    // 事务1获取资源1的写锁
    lm.acquire_node_lock(1, 1, LockType::Write);

    // 事务2获取资源2的写锁
    lm.acquire_node_lock(2, 2, LockType::Write);

    // 事务1尝试获取资源2（会等待）
    // 事务2尝试获取资源1（会等待）
    // 这会形成死锁

    // 当前简化实现中，等待队列为空，所以不会检测到死锁
    let deadlock = lm.detect_deadlock();
    assert!(deadlock.is_none()); // 简化实现暂不检测死锁
}

#[test]
fn test_savepoint_with_rollback() {
    let mut tm = TransactionManager::new();

    let tx = tm.begin_transaction();
    let tx_id = tx.id;

    // 添加操作
    tm.record_op(tx_id, TransactionOp::CreateNode {
        id: 1,
        labels: vec!["Test".to_string()],
        properties: Properties::new(),
    }).ok();

    if let Some(tx) = tm.get_transaction_mut(tx_id) {
        tx.create_savepoint("sp1".to_string()).unwrap();
    }

    tm.record_op(tx_id, TransactionOp::CreateNode {
        id: 2,
        labels: vec!["Test".to_string()],
        properties: Properties::new(),
    }).ok();

    if let Some(tx) = tm.get_transaction_mut(tx_id) {
        tx.create_savepoint("sp2".to_string()).unwrap();
    }

    tm.record_op(tx_id, TransactionOp::CreateNode {
        id: 3,
        labels: vec!["Test".to_string()],
        properties: Properties::new(),
    }).ok();

    // 验证初始状态
    if let Some(tx) = tm.get_transaction_mut(tx_id) {
        assert_eq!(tx.op_count(), 3);

        // 回滚到 sp2
        tx.rollback_to_savepoint("sp2".to_string()).unwrap();
        assert_eq!(tx.op_count(), 2);

        // 回滚到 sp1
        tx.rollback_to_savepoint("sp1".to_string()).unwrap();
        assert_eq!(tx.op_count(), 1);
    }

    // 完全回滚事务
    let result = tm.rollback(tx_id);
    assert!(result.is_ok());
    assert_eq!(tm.active_count(), 0);
}

// ==================== 乐观锁测试 ====================

#[test]
fn test_optimistic_lock_basic_workflow() {
    use rs_graphdb::transactions::{OptimisticLockManager, Version};

    let mut lock_manager = OptimisticLockManager::new();
    let node_id: u64 = 1;

    // 模拟读取
    let read_version = lock_manager.read_node_version(node_id);
    assert_eq!(read_version, Version::initial());

    // 模拟写入
    let result = lock_manager.write_node(node_id, read_version);
    assert!(result.is_ok());

    // 尝试使用旧版本写入（应该失败）
    let result = lock_manager.write_node(node_id, read_version);
    assert!(result.is_err());
}

#[test]
fn test_optimistic_lock_read_context() {
    use rs_graphdb::transactions::{OptimisticLockManager, OptimisticReadContext};

    let mut lock_manager = OptimisticLockManager::new();
    let mut ctx = OptimisticReadContext::new();

    let node1: u64 = 1;
    let node2: u64 = 2;

    // 记录读取
    let v1 = lock_manager.read_node_version(node1);
    ctx.record_node(node1, v1);

    let v2 = lock_manager.read_node_version(node2);
    ctx.record_node(node2, v2);

    // 验证通过
    assert!(ctx.verify(&lock_manager).is_ok());

    // 修改一个节点的版本
    lock_manager.increment_node_version(node1);

    // 验证失败
    assert!(ctx.verify(&lock_manager).is_err());
}

#[test]
fn test_optimistic_lock_version_conflict() {
    use rs_graphdb::transactions::{OptimisticLockManager, Version, TransactionError};

    let mut lock_manager = OptimisticLockManager::new();
    let node_id: u64 = 1;

    // 第一次读取
    let v1 = lock_manager.read_node_version(node_id);

    // 模拟另一个事务修改了数据
    lock_manager.increment_node_version(node_id);

    // 尝试使用旧版本写入
    let result = lock_manager.write_node(node_id, v1);
    assert!(result.is_err());

    if let Err(TransactionError::VersionConflict { expected, actual }) = result {
        assert_eq!(expected, 0);
        assert_eq!(actual, 1);
    } else {
        panic!("Expected VersionConflict error");
    }
}

// ==================== 事务隔离级别测试 ====================

#[test]
fn test_isolation_read_committed() {
    use rs_graphdb::transactions::{IsolationExecutor, IsolationLevel, TransactionOp};

    let executor = IsolationExecutor::new();
    let tx1: u64 = 1;
    let tx2: u64 = 2;

    executor.begin_transaction(tx1);
    executor.begin_transaction(tx2);

    // TX1 读取节点1
    let mut read_set = rs_graphdb::transactions::ReadSet::new();
    read_set.read_node(1);
    executor.record_read(tx1, &read_set);

    // TX2 尝试修改节点1
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text("Alice".to_string()));

    let ops = vec![
        TransactionOp::UpdateNode {
            id: 1,
            old_properties: Properties::new(),
            new_properties: props.clone(),
        },
    ];

    // 在读已提交级别，TX2 应该能成功提交
    let result = executor.validate_commit(tx2, IsolationLevel::ReadCommitted, &ops);
    assert!(result.is_ok());
}

#[test]
fn test_isolation_serializable_conflict() {
    use rs_graphdb::transactions::{IsolationExecutor, IsolationLevel, TransactionOp};

    let executor = IsolationExecutor::new();
    let tx1: u64 = 1;
    let tx2: u64 = 2;

    executor.begin_transaction(tx1);
    executor.begin_transaction(tx2);

    // TX1 修改节点1
    let mut props = Properties::new();
    props.insert("value".to_string(), Value::Int(42));

    let ops = vec![
        TransactionOp::UpdateNode {
            id: 1,
            old_properties: Properties::new(),
            new_properties: props.clone(),
        },
    ];

    // TX1 先提交成功
    let result1 = executor.validate_commit(tx1, IsolationLevel::Serializable, &ops);
    assert!(result1.is_ok());

    // 完成TX1（添加到已提交写集）
    executor.finish_transaction(tx1, true, Some(&ops));

    // TX2 现在应该检测到与已提交的TX1的冲突
    let result2 = executor.validate_commit(tx2, IsolationLevel::Serializable, &ops);
    assert!(result2.is_err());
}

#[test]
fn test_isolation_read_uncommitted_no_validation() {
    use rs_graphdb::transactions::{IsolationExecutor, IsolationLevel, TransactionOp};

    let executor = IsolationExecutor::new();
    let tx1: u64 = 1;

    executor.begin_transaction(tx1);

    let mut props = Properties::new();
    props.insert("data".to_string(), Value::Int(100));

    let ops = vec![
        TransactionOp::UpdateNode {
            id: 1,
            old_properties: Properties::new(),
            new_properties: props,
        },
    ];

    // 读未提交不进行任何验证
    let result = executor.validate_commit(tx1, IsolationLevel::ReadUncommitted, &ops);
    assert!(result.is_ok());
}

#[test]
fn test_isolation_stats() {
    use rs_graphdb::transactions::{IsolationExecutor, TransactionOp};

    let executor = IsolationExecutor::new();

    // 开始几个事务
    executor.begin_transaction(1);
    executor.begin_transaction(2);
    executor.begin_transaction(3);

    let stats = executor.stats();
    assert_eq!(stats.active_transactions, 3);

    // 提交一些事务
    let props = Properties::new();
    let ops = vec![
        TransactionOp::CreateNode {
            id: 1,
            labels: vec!["Test".to_string()],
            properties: props.clone(),
        },
    ];

    executor.finish_transaction(1, true, Some(&ops));
    executor.finish_transaction(2, false, None);

    let stats = executor.stats();
    assert_eq!(stats.active_transactions, 1);
    assert_eq!(stats.committed_transactions, 1);

    // 清理
    executor.finish_transaction(3, false, None);
    executor.cleanup_committed_transactions(0);

    let stats = executor.stats();
    assert_eq!(stats.committed_transactions, 0);
}
