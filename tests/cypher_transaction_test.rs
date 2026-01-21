// Cypher 事务语句测试
// 测试 BEGIN/COMMIT/ROLLBACK 语句的解析和执行

use rs_graphdb::cypher::{ast::*, parser};
use rs_graphdb::graph::db::GraphDatabase;
use rs_graphdb::storage::mem_store::MemStore;

// ==================== 解析测试 ====================

#[test]
fn test_parse_begin_transaction() {
    let queries = vec![
        "BEGIN",
        "BEGIN TRANSACTION",
        "BEGIN;",
        "BEGIN TRANSACTION;",
        "begin",
        "begin transaction",
        "START",
        "START TRANSACTION",
        "start transaction",
    ];

    for query in queries {
        let result = parser::parse_cypher(query);
        assert!(result.is_ok(), "Failed to parse: {}", query);
        match result.unwrap() {
            CypherStatement::BeginTransaction => {}
            _ => panic!("Expected BeginTransaction, got: {:?}", query),
        }
    }
}

#[test]
fn test_parse_commit() {
    let queries = vec![
        "COMMIT",
        "COMMIT TRANSACTION",
        "COMMIT;",
        "COMMIT TRANSACTION;",
        "commit",
        "commit transaction",
    ];

    for query in queries {
        let result = parser::parse_cypher(query);
        assert!(result.is_ok(), "Failed to parse: {}", query);
        match result.unwrap() {
            CypherStatement::CommitTransaction => {}
            _ => panic!("Expected CommitTransaction, got: {:?}", query),
        }
    }
}

#[test]
fn test_parse_rollback() {
    let queries = vec![
        "ROLLBACK",
        "ROLLBACK TRANSACTION",
        "ROLLBACK;",
        "ROLLBACK TRANSACTION;",
        "rollback",
        "rollback transaction",
    ];

    for query in queries {
        let result = parser::parse_cypher(query);
        assert!(result.is_ok(), "Failed to parse: {}", query);
        match result.unwrap() {
            CypherStatement::RollbackTransaction => {}
            _ => panic!("Expected RollbackTransaction, got: {:?}", query),
        }
    }
}

#[test]
fn test_parse_transaction_with_whitespace() {
    let queries = vec![
        "  BEGIN  ",
        "  COMMIT  ",
        "  ROLLBACK  ",
        "\nBEGIN\n",
        "\tCOMMIT\t",
        "\n\nROLLBACK\n\n",
    ];

    for query in queries {
        let result = parser::parse_cypher(query);
        assert!(result.is_ok(), "Failed to parse: {:?}", query);
    }
}

// ==================== 执行测试 ====================

#[test]
fn test_execute_begin_transaction() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 初始状态：没有活动事务
    assert_eq!(db.active_transaction_count(), 0);

    // 执行 BEGIN
    let stmt = parser::parse_cypher("BEGIN").unwrap();
    let result = rs_graphdb::cypher::executor::execute_statement(&mut db, &stmt);

    assert!(result.is_ok());
    match result.unwrap() {
        rs_graphdb::cypher::executor::CypherResult::TransactionStarted => {}
        _ => panic!("Expected TransactionStarted"),
    }

    // 验证事务已创建
    assert_eq!(db.active_transaction_count(), 1);
}

#[test]
fn test_execute_commit_transaction() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 开始事务
    let begin_stmt = parser::parse_cypher("BEGIN").unwrap();
    rs_graphdb::cypher::executor::execute_statement(&mut db, &begin_stmt).unwrap();
    assert_eq!(db.active_transaction_count(), 1);

    // 提交事务
    let commit_stmt = parser::parse_cypher("COMMIT").unwrap();
    let result = rs_graphdb::cypher::executor::execute_statement(&mut db, &commit_stmt);

    assert!(result.is_ok());
    match result.unwrap() {
        rs_graphdb::cypher::executor::CypherResult::TransactionCommitted => {}
        _ => panic!("Expected TransactionCommitted"),
    }

    // 验证事务已提交
    assert_eq!(db.active_transaction_count(), 0);
    assert_eq!(db.completed_transaction_count(), 1);
}

#[test]
fn test_execute_rollback_transaction() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 开始事务
    let begin_stmt = parser::parse_cypher("BEGIN").unwrap();
    rs_graphdb::cypher::executor::execute_statement(&mut db, &begin_stmt).unwrap();
    assert_eq!(db.active_transaction_count(), 1);

    // 回滚事务
    let rollback_stmt = parser::parse_cypher("ROLLBACK").unwrap();
    let result = rs_graphdb::cypher::executor::execute_statement(&mut db, &rollback_stmt);

    assert!(result.is_ok());
    match result.unwrap() {
        rs_graphdb::cypher::executor::CypherResult::TransactionRolledBack => {}
        _ => panic!("Expected TransactionRolledBack"),
    }

    // 验证事务已回滚
    assert_eq!(db.active_transaction_count(), 0);
    assert_eq!(db.completed_transaction_count(), 1);
}

#[test]
fn test_commit_without_active_transaction() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 尝试在没有活动事务的情况下提交
    let commit_stmt = parser::parse_cypher("COMMIT").unwrap();
    let result = rs_graphdb::cypher::executor::execute_statement(&mut db, &commit_stmt);

    assert!(result.is_err());
    if let Err(e) = result {
        assert!(e.contains("No active transaction"));
    }
}

#[test]
fn test_rollback_without_active_transaction() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 尝试在没有活动事务的情况下回滚
    let rollback_stmt = parser::parse_cypher("ROLLBACK").unwrap();
    let result = rs_graphdb::cypher::executor::execute_statement(&mut db, &rollback_stmt);

    assert!(result.is_err());
    if let Err(e) = result {
        assert!(e.contains("No active transaction"));
    }
}

// ==================== 综合测试 ====================

#[test]
fn test_full_transaction_lifecycle() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 开始事务
    let begin_stmt = parser::parse_cypher("BEGIN TRANSACTION").unwrap();
    let result = rs_graphdb::cypher::executor::execute_statement(&mut db, &begin_stmt);
    assert!(result.is_ok());
    assert_eq!(db.active_transaction_count(), 1);

    // 执行一些操作
    let create_stmt = parser::parse_cypher("CREATE (n:Person {name: 'Alice'})").unwrap();
    let result = rs_graphdb::cypher::executor::execute_statement(&mut db, &create_stmt);
    assert!(result.is_ok());

    // 提交事务
    let commit_stmt = parser::parse_cypher("COMMIT").unwrap();
    let result = rs_graphdb::cypher::executor::execute_statement(&mut db, &commit_stmt);
    assert!(result.is_ok());
    assert_eq!(db.active_transaction_count(), 0);
}

#[test]
fn test_multiple_transactions() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 开始第一个事务
    let begin1 = parser::parse_cypher("BEGIN").unwrap();
    rs_graphdb::cypher::executor::execute_statement(&mut db, &begin1).unwrap();
    assert_eq!(db.active_transaction_count(), 1);

    // 开始第二个事务
    let begin2 = parser::parse_cypher("BEGIN").unwrap();
    rs_graphdb::cypher::executor::execute_statement(&mut db, &begin2).unwrap();
    assert_eq!(db.active_transaction_count(), 2);

    // 提交最近的事务
    let commit = parser::parse_cypher("COMMIT").unwrap();
    rs_graphdb::cypher::executor::execute_statement(&mut db, &commit).unwrap();
    assert_eq!(db.active_transaction_count(), 1);
}

#[test]
fn test_rollback_then_begin_new() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 开始事务
    let begin = parser::parse_cypher("BEGIN").unwrap();
    rs_graphdb::cypher::executor::execute_statement(&mut db, &begin).unwrap();
    assert_eq!(db.active_transaction_count(), 1);

    // 回滚事务
    let rollback = parser::parse_cypher("ROLLBACK").unwrap();
    rs_graphdb::cypher::executor::execute_statement(&mut db, &rollback).unwrap();
    assert_eq!(db.active_transaction_count(), 0);

    // 开始新事务
    let begin2 = parser::parse_cypher("BEGIN").unwrap();
    rs_graphdb::cypher::executor::execute_statement(&mut db, &begin2).unwrap();
    assert_eq!(db.active_transaction_count(), 1);
}
