// GraphDatabase 事务集成测试
// 测试事务管理器与图数据库的集成功能

use rs_graphdb::GraphDatabase;
use rs_graphdb::transactions::{
    TransactionOp, TransactionConfig,
    IsolationLevel,
};
use rs_graphdb::values::{Properties, Value};

// 辅助函数：创建测试属性
fn create_test_properties(name: &str, age: i64) -> Properties {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props.insert("age".to_string(), Value::Int(age));
    props
}

#[test]
fn test_graph_database_has_transaction_manager() {
    let db = GraphDatabase::new_in_memory();

    // 验证 GraphDatabase 包含事务管理器
    assert_eq!(db.transactions.active_count(), 0);
    assert_eq!(db.transactions.completed_count(), 0);
}

#[test]
fn test_graph_database_transaction_lifecycle() {
    let mut db = GraphDatabase::new_in_memory();

    // 开始事务
    let tx_id = db.transactions.begin_transaction().id;
    assert_eq!(db.active_transaction_count(), 1);
    assert_eq!(db.completed_transaction_count(), 0);

    // 提交事务
    assert!(db.transactions.commit(tx_id).is_ok());
    assert_eq!(db.active_transaction_count(), 0);
    assert_eq!(db.completed_transaction_count(), 1);
}

#[test]
fn test_graph_database_transaction_rollback() {
    let mut db = GraphDatabase::new_in_memory();

    // 开始事务
    let tx_id = db.transactions.begin_transaction().id;
    assert_eq!(db.active_transaction_count(), 1);

    // 回滚事务
    assert!(db.transactions.rollback(tx_id).is_ok());
    assert_eq!(db.active_transaction_count(), 0);
    assert_eq!(db.completed_transaction_count(), 1);

    // 验证事务状态
    let tx = db.transactions.get_transaction(tx_id);
    assert!(tx.is_none());
}

#[test]
fn test_graph_database_multiple_transactions() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建多个事务
    let tx1 = db.transactions.begin_transaction().id;
    let tx2 = db.transactions.begin_transaction().id;
    let tx3 = db.transactions.begin_transaction().id;

    assert_eq!(db.active_transaction_count(), 3);

    // 提交部分事务
    assert!(db.transactions.commit(tx1).is_ok());
    assert_eq!(db.active_transaction_count(), 2);
    assert_eq!(db.completed_transaction_count(), 1);

    // 回滚部分事务
    assert!(db.transactions.rollback(tx2).is_ok());
    assert_eq!(db.active_transaction_count(), 1);
    assert_eq!(db.completed_transaction_count(), 2);

    // 提交最后一个事务
    assert!(db.transactions.commit(tx3).is_ok());
    assert_eq!(db.active_transaction_count(), 0);
    assert_eq!(db.completed_transaction_count(), 3);
}

#[test]
fn test_graph_database_record_operation() {
    let mut db = GraphDatabase::new_in_memory();

    // 开始事务
    let tx = db.transactions.begin_transaction();
    let tx_id = tx.id;

    // 创建节点（实际操作）
    let node_id = db.create_node(
        vec!["User"],
        create_test_properties("Alice", 30),
    );

    // 记录操作到事务日志
    let op = TransactionOp::CreateNode {
        id: node_id,
        labels: vec!["User".to_string()],
        properties: create_test_properties("Alice", 30),
    };
    assert!(db.record_operation(tx_id, op).is_ok());

    // 验证操作已记录
    let recorded_tx = db.transactions.get_transaction(tx_id).unwrap();
    assert_eq!(recorded_tx.op_count(), 1);

    // 清理
    assert!(db.transactions.commit(tx_id).is_ok());
}

#[test]
fn test_graph_database_transaction_with_config() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建自定义事务配置
    let config = TransactionConfig::new()
        .with_isolation_level(IsolationLevel::Serializable)
        .with_snapshot(true)
        .with_timeout(60);

    // 开始事务（使用配置）
    let tx_id = db.begin_tx_with_config(config);
    assert_eq!(db.active_transaction_count(), 1);

    // 提交事务
    assert!(db.commit_transaction(tx_id).is_ok());
    assert_eq!(db.active_transaction_count(), 0);
}

#[test]
fn test_graph_database_transaction_cleanup() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建并完成多个事务
    for _ in 0..10 {
        let tx_id = db.transactions.begin_transaction().id;
        db.transactions.commit(tx_id).ok();
    }

    assert_eq!(db.completed_transaction_count(), 10);

    // 清理，只保留最近5个
    db.cleanup_transactions(5);
    assert_eq!(db.completed_transaction_count(), 5);
}

#[test]
fn test_graph_database_transaction_with_create_node() {
    let mut db = GraphDatabase::new_in_memory();

    // 开始事务
    let tx_id = db.transactions.begin_transaction().id;

    // 创建节点
    let node_id = db.create_node(
        vec!["User"],
        create_test_properties("Bob", 25),
    );

    // 记录操作
    let op = TransactionOp::CreateNode {
        id: node_id,
        labels: vec!["User".to_string()],
        properties: create_test_properties("Bob", 25),
    };
    db.record_operation(tx_id, op).ok();

    // 提交事务
    assert!(db.commit_transaction(tx_id).is_ok());

    // 验证节点已创建
    let node = db.get_node(node_id);
    assert!(node.is_some());
    assert_eq!(node.unwrap().props.get("name"), Some(&Value::Text("Bob".to_string())));
}

#[test]
fn test_graph_database_transaction_with_delete_node() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建节点
    let node_id = db.create_node(
        vec!["User"],
        create_test_properties("Charlie", 35),
    );

    // 开始事务
    let tx_id = db.transactions.begin_transaction().id;

    // 获取节点信息用于回滚
    let node = db.get_node(node_id).unwrap();

    // 删除节点
    assert!(db.delete_node(node_id));

    // 记录删除操作
    use rs_graphdb::transactions::NodeData;
    let op = TransactionOp::DeleteNode {
        id: node_id,
        node: NodeData {
            id: node_id,
            labels: node.labels,
            properties: node.props,
        },
    };
    db.record_operation(tx_id, op).ok();

    // 提交事务
    assert!(db.commit_transaction(tx_id).is_ok());

    // 验证节点已删除
    assert!(db.get_node(node_id).is_none());
}

#[test]
fn test_graph_database_transaction_with_update_node() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建节点
    let node_id = db.create_node(
        vec!["User"],
        create_test_properties("David", 40),
    );

    // 开始事务
    let tx_id = db.transactions.begin_transaction().id;

    // 获取旧属性
    let old_props = db.get_node(node_id).unwrap().props.clone();

    // 更新节点属性
    let mut new_props = Properties::new();
    new_props.insert("name".to_string(), Value::Text("David Updated".to_string()));
    new_props.insert("age".to_string(), Value::Int(41));
    assert!(db.update_node_props(node_id, new_props.clone()));

    // 记录更新操作
    let op = TransactionOp::UpdateNode {
        id: node_id,
        old_properties: old_props,
        new_properties: new_props,
    };
    db.record_operation(tx_id, op).ok();

    // 提交事务
    assert!(db.commit_transaction(tx_id).is_ok());

    // 验证节点已更新
    let node = db.get_node(node_id).unwrap();
    assert_eq!(node.props.get("name"), Some(&Value::Text("David Updated".to_string())));
    assert_eq!(node.props.get("age"), Some(&Value::Int(41)));
}

#[test]
fn test_graph_database_transaction_with_create_rel() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建两个节点
    let start = db.create_node(vec!["User"], create_test_properties("Eve", 28));
    let end = db.create_node(vec!["User"], create_test_properties("Frank", 32));

    // 开始事务
    let tx_id = db.transactions.begin_transaction().id;

    // 创建关系
    let rel_id = db.create_rel(
        start,
        end,
        "KNOWS",
        Properties::new(),
    );

    // 记录操作
    let op = TransactionOp::CreateRel {
        id: rel_id,
        start,
        end,
        typ: "KNOWS".to_string(),
        properties: Properties::new(),
    };
    db.record_operation(tx_id, op).ok();

    // 提交事务
    assert!(db.commit_transaction(tx_id).is_ok());

    // 验证关系已创建
    let rel = db.get_rel(rel_id);
    assert!(rel.is_some());
    assert_eq!(rel.unwrap().typ, "KNOWS");
}

#[test]
fn test_graph_database_all_isolation_levels() {
    let mut db = GraphDatabase::new_in_memory();

    let levels = vec![
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
        IsolationLevel::RepeatableRead,
        IsolationLevel::Serializable,
    ];

    for level in levels {
        let config = TransactionConfig::new()
            .with_isolation_level(level);
        let tx_id = db.begin_tx_with_config(config);
        assert!(db.commit_transaction(tx_id).is_ok());
    }
}
