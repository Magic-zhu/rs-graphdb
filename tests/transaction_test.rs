// 事务和回滚测试
// 测试事务管理、快照机制、回滚功能

use rs_graphdb::transactions::{
    TransactionManager, TransactionOp, TransactionStatus, TransactionError,
    Snapshot, SnapshotManager, NodeData, RelData, Transaction,
};
use rs_graphdb::storage::{NodeId, RelId};
use rs_graphdb::values::{Properties, Value};

// 辅助函数：创建测试节点数据
fn create_node_data(id: NodeId, name: &str) -> NodeData {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    NodeData {
        id,
        labels: vec!["Test".to_string()],
        properties: props,
    }
}

// 辅助函数：创建SnapshotNode
fn create_snapshot_node(id: NodeId, name: &str) -> rs_graphdb::transactions::SnapshotNode {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    rs_graphdb::transactions::SnapshotNode {
        id,
        labels: vec!["Test".to_string()],
        properties: props,
    }
}

// 辅助函数：创建测试关系数据
fn create_rel_data(id: RelId, start: NodeId, end: NodeId) -> RelData {
    RelData {
        id,
        start,
        end,
        typ: "TEST_REL".to_string(),
        properties: Properties::new(),
    }
}

// 辅助函数：创建SnapshotRel
fn create_snapshot_rel(id: RelId, start: NodeId, end: NodeId) -> rs_graphdb::transactions::SnapshotRel {
    rs_graphdb::transactions::SnapshotRel {
        id,
        start,
        end,
        typ: "TEST_REL".to_string(),
        properties: Properties::new(),
    }
}

#[test]
fn test_transaction_manager_begin() {
    let mut tm = TransactionManager::new();

    // 开始事务
    let tx = tm.begin_transaction();

    assert_eq!(tx.id, 0);
    assert_eq!(tx.status, TransactionStatus::Active);
    assert_eq!(tx.op_count(), 0);
    assert_eq!(tm.active_count(), 1);
}

#[test]
fn test_transaction_manager_multiple_transactions() {
    let mut tm = TransactionManager::new();

    let tx1 = tm.begin_transaction();
    let tx2 = tm.begin_transaction();
    let tx3 = tm.begin_transaction();

    assert_eq!(tx1.id, 0);
    assert_eq!(tx2.id, 1);
    assert_eq!(tx3.id, 2);
    assert_eq!(tm.active_count(), 3);
}

#[test]
fn test_transaction_commit() {
    let mut tm = TransactionManager::new();

    let tx = tm.begin_transaction();
    let tx_id = tx.id;

    // 提交事务
    let result = tm.commit(tx_id);
    assert!(result.is_ok());
    assert_eq!(tm.active_count(), 0);
    assert_eq!(tm.completed_count(), 1);
}

#[test]
fn test_transaction_rollback() {
    let mut tm = TransactionManager::new();

    let tx = tm.begin_transaction();
    let tx_id = tx.id;

    // 回滚事务
    let result = tm.rollback(tx_id);
    assert!(result.is_ok());
    assert_eq!(tm.active_count(), 0);
    assert_eq!(tm.completed_count(), 1);

    // 检查事务状态
    assert!(tm.get_transaction(tx_id).is_none());
}

#[test]
fn test_transaction_commit_not_found() {
    let mut tm = TransactionManager::new();

    let result = tm.commit(999);
    assert!(matches!(result, Err(TransactionError::TransactionNotFound(999))));
}

#[test]
fn test_transaction_rollback_not_found() {
    let mut tm = TransactionManager::new();

    let result = tm.rollback(999);
    assert!(matches!(result, Err(TransactionError::TransactionNotFound(999))));
}

#[test]
fn test_transaction_double_commit() {
    let mut tm = TransactionManager::new();

    let tx = tm.begin_transaction();
    let tx_id = tx.id;

    // 第一次提交
    assert!(tm.commit(tx_id).is_ok());

    // 第二次提交应该失败
    let result = tm.commit(tx_id);
    assert!(result.is_err());
}

#[test]
fn test_transaction_record_op() {
    let mut tm = TransactionManager::new();

    let tx = tm.begin_transaction();
    let tx_id = tx.id;

    // 记录操作
    let op = TransactionOp::CreateNode {
        id: 1,
        labels: vec!["Test".to_string()],
        properties: Properties::new(),
    };

    assert!(tm.record_op(tx_id, op).is_ok());
    assert_eq!(tm.get_transaction(tx_id).unwrap().op_count(), 1);
}

#[test]
fn test_transaction_cleanup_completed() {
    let mut tm = TransactionManager::new();

    // 创建并完成多个事务
    for _ in 0..5 {
        let tx = tm.begin_transaction();
        tm.commit(tx.id).ok();
    }

    assert_eq!(tm.completed_count(), 5);

    // 清理，只保留最近3个
    tm.cleanup_completed(3);
    assert_eq!(tm.completed_count(), 3);
}

#[test]
fn test_snapshot_creation() {
    let snapshot = Snapshot::new(1);

    assert_eq!(snapshot.id, 1);
    assert_eq!(snapshot.node_count(), 0);
    assert_eq!(snapshot.rel_count(), 0);
}

#[test]
fn test_snapshot_add_node() {
    let mut snapshot = Snapshot::new(1);
    let node = create_snapshot_node(1, "Test");

    snapshot.add_node(node);

    assert_eq!(snapshot.node_count(), 1);
}

#[test]
fn test_snapshot_add_rel() {
    let mut snapshot = Snapshot::new(1);
    let rel = create_snapshot_rel(1, 2, 3);

    snapshot.add_rel(rel);

    assert_eq!(snapshot.rel_count(), 1);
    assert_eq!(snapshot.outgoing.len(), 1);
    assert_eq!(snapshot.incoming.len(), 1);
}

#[test]
fn test_snapshot_manager() {
    let mut sm = SnapshotManager::new(5);

    let s1 = sm.create_snapshot();
    let s2 = sm.create_snapshot();

    assert_eq!(s1.id, 0);
    assert_eq!(s2.id, 1);
    assert_eq!(sm.count(), 2);
}

#[test]
fn test_snapshot_manager_max_limit() {
    let mut sm = SnapshotManager::new(2);

    sm.create_snapshot();
    sm.create_snapshot();
    sm.create_snapshot();

    // 只应该保留最新的2个
    assert_eq!(sm.count(), 2);
    assert!(sm.get(0).is_none());
    assert!(sm.get(1).is_some());
    assert!(sm.get(2).is_some());
}

#[test]
fn test_transaction_op_create_node() {
    let op = TransactionOp::CreateNode {
        id: 1,
        labels: vec!["Person".to_string()],
        properties: Properties::new(),
    };

    assert_eq!(op.description(), "CreateNode(1)");
    assert_eq!(op.affected_node(), Some(1));
    assert_eq!(op.affected_rel(), None);
}

#[test]
fn test_transaction_op_delete_node() {
    let op = TransactionOp::DeleteNode {
        id: 1,
        node: create_node_data(1, "Alice"),
    };

    assert_eq!(op.description(), "DeleteNode(1)");
    assert_eq!(op.affected_node(), Some(1));
    assert!(op.is_mutating());
}

#[test]
fn test_transaction_op_update_node() {
    let mut old_props = Properties::new();
    old_props.insert("age".to_string(), Value::Int(25));

    let mut new_props = Properties::new();
    new_props.insert("age".to_string(), Value::Int(26));

    let op = TransactionOp::UpdateNode {
        id: 1,
        old_properties: old_props,
        new_properties: new_props,
    };

    assert_eq!(op.description(), "UpdateNode(1)");
    assert_eq!(op.affected_node(), Some(1));
}

#[test]
fn test_transaction_op_create_rel() {
    let op = TransactionOp::CreateRel {
        id: 1,
        start: 2,
        end: 3,
        typ: "KNOWS".to_string(),
        properties: Properties::new(),
    };

    assert_eq!(op.description(), "CreateRel(1)");
    assert_eq!(op.affected_rel(), Some(1));
    assert_eq!(op.affected_node(), None);
}

#[test]
fn test_transaction_status() {
    assert_ne!(TransactionStatus::Active, TransactionStatus::Committed);
    assert_ne!(TransactionStatus::Active, TransactionStatus::RolledBack);
    assert_ne!(TransactionStatus::Committed, TransactionStatus::RolledBack);
}

#[test]
fn test_transaction_is_completed() {
    let mut tm = TransactionManager::new();

    let mut tx = tm.begin_transaction();
    assert!(!tx.is_completed());

    tx.mark_committed();
    assert!(tx.is_completed());
}

#[test]
fn test_transaction_snapshot_id() {
    let mut tx = Transaction::new(1);

    assert_eq!(tx.snapshot_id, None);

    tx.snapshot_id = Some(100);
    assert_eq!(tx.snapshot_id, Some(100));
}

#[test]
fn test_snapshot_clear() {
    let mut snapshot = Snapshot::new(1);
    snapshot.add_node(create_snapshot_node(1, "Test"));
    snapshot.add_rel(create_snapshot_rel(1, 2, 3));

    assert_eq!(snapshot.node_count(), 1);
    assert_eq!(snapshot.rel_count(), 1);

    snapshot.clear();

    assert_eq!(snapshot.node_count(), 0);
    assert_eq!(snapshot.rel_count(), 0);
}

#[test]
fn test_snapshot_manager_remove() {
    let mut sm = SnapshotManager::new(10);

    sm.create_snapshot();
    sm.create_snapshot();

    assert!(sm.remove(0));
    assert_eq!(sm.count(), 1);
    assert!(!sm.remove(0));
    assert!(sm.remove(1));
}

#[test]
fn test_snapshot_manager_clear() {
    let mut sm = SnapshotManager::new(10);

    sm.create_snapshot();
    sm.create_snapshot();
    sm.create_snapshot();

    assert_eq!(sm.count(), 3);

    sm.clear();

    assert_eq!(sm.count(), 0);
}

#[test]
fn test_snapshot_manager_snapshot_ids() {
    let mut sm = SnapshotManager::new(10);

    sm.create_snapshot();
    sm.create_snapshot();
    sm.create_snapshot();

    let ids = sm.snapshot_ids();
    assert_eq!(ids, vec![0, 1, 2]);
}

#[test]
fn test_node_data_creation() {
    let node = create_node_data(1, "Alice");

    assert_eq!(node.id, 1);
    assert_eq!(node.labels, vec!["Test"]);
    assert_eq!(node.properties.get("name"), Some(&Value::Text("Alice".to_string())));
}

#[test]
fn test_rel_data_creation() {
    let rel = create_rel_data(1, 2, 3);

    assert_eq!(rel.id, 1);
    assert_eq!(rel.start, 2);
    assert_eq!(rel.end, 3);
    assert_eq!(rel.typ, "TEST_REL");
}

#[test]
fn test_transaction_active_ids() {
    let mut tm = TransactionManager::new();

    tm.begin_transaction();
    tm.begin_transaction();
    tm.begin_transaction();

    let ids = tm.active_transaction_ids();
    // HashMap keys aren't ordered, just check length and that all IDs exist
    assert_eq!(ids.len(), 3);
    assert!(ids.contains(&0));
    assert!(ids.contains(&1));
    assert!(ids.contains(&2));
}

#[test]
fn test_transaction_error_display() {
    let err = TransactionError::TransactionNotFound(42);
    assert_eq!(format!("{}", err), "Transaction 42 not found");

    let err2 = TransactionError::TransactionAlreadyCompleted(42, TransactionStatus::Committed);
    assert!(format!("{}", err2).contains("already completed"));
}
