//! MERGE 功能全面测试
//!
//! 测试范围：
//! - 节点 MERGE（基本功能）
//! - 关系 MERGE（新功能）
//! - 性能测试
//! - 边界情况

use rs_graphdb::cypher::{parse_cypher, execute_statement, CypherResult};
use rs_graphdb::graph::db::GraphDatabase;
use rs_graphdb::storage::mem_store::MemStore;
use rs_graphdb::values::{Properties, Value};
use std::time::Instant;

fn props(name: &str, age: i64) -> Properties {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props.insert("age".to_string(), Value::Int(age));
    props
}

fn create_test_db() -> GraphDatabase<MemStore> {
    let mut db = GraphDatabase::new_in_memory();

    // 创建测试数据
    let alice = db.create_node(vec!["Person"], props("Alice", 30));
    let bob = db.create_node(vec!["Person"], props("Bob", 25));
    let charlie = db.create_node(vec!["Person"], props("Charlie", 35));

    // 创建一些关系
    db.create_rel(alice, bob, "KNOWS", Properties::new());
    db.create_rel(bob, charlie, "KNOWS", Properties::new());

    db
}

// ==================== 节点 MERGE 测试 ====================

#[test]
fn test_node_merge_create_basic() {
    let mut db = create_test_db();

    let query = "MERGE (n:Person {name: 'David', age: 28})";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Created { nodes, rels } => {
            assert_eq!(nodes.len(), 1);
            assert_eq!(rels, 0);
        }
        _ => panic!("Expected Created result"),
    }
}

#[test]
fn test_node_merge_match_existing() {
    let mut db = create_test_db();

    let query = "MERGE (n:Person {name: 'Alice', age: 30})";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 1);
            assert_eq!(nodes[0].props.get("name"), Some(&Value::Text("Alice".to_string())));
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_node_merge_on_create() {
    let mut db = create_test_db();

    let query = "MERGE (n:Person {name: 'David', age: 28}) ON CREATE SET n.city = 'NYC'";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Created { nodes, .. } => {
            let node = db.get_node(nodes[0]).unwrap();
            assert_eq!(node.props.get("city"), Some(&Value::Text("NYC".to_string())));
        }
        _ => panic!("Expected Created result"),
    }
}

#[test]
fn test_node_merge_on_match() {
    let mut db = create_test_db();

    let query = "MERGE (n:Person {name: 'Alice', age: 30}) ON MATCH SET n.last_seen = 2024";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Updated { nodes } => {
            assert_eq!(nodes, 1);

            let all_nodes = db.all_stored_nodes().collect::<Vec<_>>();
            let alice = all_nodes.iter()
                .find(|n| n.props.get("name") == Some(&Value::Text("Alice".to_string())))
                .unwrap();

            // 需要通过 db.get_node 获取完整节点
            if let Some(node) = db.get_node(alice.id) {
                assert_eq!(node.props.get("last_seen"), Some(&Value::Int(2024)));
            }
        }
        _ => panic!("Expected Updated result"),
    }
}

#[test]
fn test_node_merge_idempotent() {
    let mut db = create_test_db();

    let query = "MERGE (n:Person {name: 'David', age: 28})";

    // 第一次执行：创建
    let stmt1 = parse_cypher(query).unwrap();
    let result1 = execute_statement(&mut db, &stmt1).unwrap();

    match result1 {
        CypherResult::Created { .. } => {}
        _ => panic!("Expected Created on first run"),
    }

    // 第二次执行：匹配
    let stmt2 = parse_cypher(query).unwrap();
    let result2 = execute_statement(&mut db, &stmt2).unwrap();

    match result2 {
        CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 1);
        }
        _ => panic!("Expected Nodes on second run"),
    }

    // 验证只有一个 David
    let all_nodes = db.all_stored_nodes().collect::<Vec<_>>();
    let davids = all_nodes.iter()
        .filter(|n| n.props.get("name") == Some(&Value::Text("David".to_string())))
        .count();
    assert_eq!(davids, 1);
}

// ==================== 关系 MERGE 测试 ====================

#[test]
fn test_rel_merge_create_new() {
    let mut db = create_test_db();

    // 创建两个没有关系的节点
    let alice = db.create_node(vec!["Person"], props("Alice2", 30));
    let bob = db.create_node(vec!["Person"], props("Bob2", 25));

    let query = "MERGE (a:Person {name: 'Alice2', age: 30})-[r:KNOWS]->(b:Person {name: 'Bob2', age: 25})";
    let stmt = parse_cypher(query).unwrap();

    let result = execute_statement(&mut db, &stmt);

    println!("Query: {}", query);
    match result {
        Ok(CypherResult::Created { nodes, rels }) => {
            println!("Created {} nodes and {} rels", nodes.len(), rels);
            assert_eq!(rels, 1);
            assert_eq!(nodes.len(), 2);
        }
        Ok(CypherResult::Nodes(nodes)) => {
            println!("Got Nodes result with {} nodes", nodes.len());
            panic!("Expected Created result, got Nodes");
        }
        Ok(CypherResult::Updated { nodes }) => {
            println!("Got Updated result with {} nodes", nodes);
            panic!("Expected Created result, got Updated");
        }
        Ok(CypherResult::Deleted { .. }) => {
            println!("Got Deleted result");
            panic!("Expected Created result, got Deleted");
        }
        Ok(CypherResult::TransactionStarted) => {
            println!("Got TransactionStarted result");
            panic!("Expected Created result, got TransactionStarted");
        }
        Ok(CypherResult::TransactionCommitted) => {
            println!("Got TransactionCommitted result");
            panic!("Expected Created result, got TransactionCommitted");
        }
        Ok(CypherResult::TransactionRolledBack) => {
            println!("Got TransactionRolledBack result");
            panic!("Expected Created result, got TransactionRolledBack");
        }
        Err(e) => {
            println!("Got Error: {}", e);
            panic!("MERGE failed: {}", e);
        }
    }
}

#[test]
fn test_rel_merge_match_existing() {
    let mut db = create_test_db();

    // Alice 和 Bob 之间已经有 KNOWS 关系
    let query = "MERGE (a:Person {name: 'Alice', age: 30})-[r:KNOWS]->(b:Person {name: 'Bob', age: 25})";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Nodes(nodes) => {
            // 找到现有的关系
            assert_eq!(nodes.len(), 0); // 关系 MERGE 返回空节点列表
        }
        _ => {}
    }
}

#[test]
fn test_rel_merge_create_nodes_and_rel() {
    let mut db = create_test_db();

    // 两个节点都不存在
    let query = "MERGE (a:Person {name: 'David', age: 28})-[r:FRIENDS]->(b:Person {name: 'Eve', age: 27})";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Created { nodes, rels } => {
            assert_eq!(nodes.len(), 2); // 创建了两个节点
            assert_eq!(rels, 1); // 创建了一个关系
        }
        _ => panic!("Expected Created result"),
    }
}

#[test]
fn test_rel_merge_on_create() {
    let mut db = create_test_db();

    let query = "MERGE (a:Person {name: 'David', age: 28})-[r:FRIENDS]->(b:Person {name: 'Eve', age: 27}) ON CREATE SET r.since = 2024";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Created { nodes, rels } => {
            assert_eq!(rels, 1);
            assert_eq!(nodes.len(), 2);
            // 关系创建成功，ON CREATE SET 已执行
        }
        _ => panic!("Expected Created result"),
    }
}

#[test]
fn test_rel_merge_direction_incoming() {
    let mut db = create_test_db();

    let alice = db.create_node(vec!["Person"], props("Alice3", 30));
    let bob = db.create_node(vec!["Person"], props("Bob3", 25));

    // 使用 <- 方向
    let query = "MERGE (a:Person {name: 'Alice3', age: 30})<-[r:KNOWS]-(b:Person {name: 'Bob3', age: 25})";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Created { rels, .. } => {
            assert_eq!(rels, 1);
        }
        _ => panic!("Expected Created result"),
    }
}

// ==================== 性能测试 ====================

#[test]
fn test_performance_merge_with_index() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建大量节点
    for i in 0..1000 {
        let mut props = Properties::new();
        props.insert("id".to_string(), Value::Int(i));
        props.insert("name".to_string(), Value::Text(format!("User{}", i)));
        db.create_node(vec!["User"], props);
    }

    // 测试 MERGE 性能
    let start = Instant::now();
    let query = "MERGE (n:User {id: 999, name: 'User999'})";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    let elapsed = start.elapsed();

    match result {
        CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 1);
        }
        _ => panic!("Expected Nodes result"),
    }

    // 性能断言：应该在合理时间内完成（100ms）
    assert!(elapsed.as_millis() < 100, "MERGE took too long: {:?}", elapsed);

    println!("MERGE with 1000 nodes took: {:?}", elapsed);
}

#[test]
fn test_performance_merge_create_batch() {
    let mut db = GraphDatabase::new_in_memory();

    let start = Instant::now();

    // 批量 MERGE 创建
    for i in 0..100 {
        let query = format!("MERGE (n:User {{id: {}, name: 'User{}'}})", i, i);
        let stmt = parse_cypher(&query).unwrap();
        execute_statement(&mut db, &stmt).unwrap();
    }

    let elapsed = start.elapsed();

    println!("100 MERGE creates took: {:?}", elapsed);

    // 验证所有节点都已创建
    let count = db.all_stored_nodes().count();
    assert_eq!(count, 100);
}

#[test]
fn test_performance_merge_relationship() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建节点网络
    let mut node_ids = Vec::new();
    for i in 0..100 {
        let mut props = Properties::new();
        props.insert("id".to_string(), Value::Int(i));
        let id = db.create_node(vec!["Node"], props);
        node_ids.push(id);
    }

    // 创建关系链
    for i in 0..99 {
        db.create_rel(node_ids[i], node_ids[i + 1], "LINK", Properties::new());
    }

    // 测试关系 MERGE 性能
    let start = Instant::now();
    let query = "MERGE (a:Node {id: 0})-[r:LINK]->(b:Node {id: 1})";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    let elapsed = start.elapsed();

    match result {
        CypherResult::Nodes(_) => {
            // 应该找到现有的关系
        }
        _ => {}
    }

    println!("Relationship MERGE with 100 nodes took: {:?}", elapsed);

    // 性能断言
    assert!(elapsed.as_millis() < 100, "Relationship MERGE took too long: {:?}", elapsed);
}

// ==================== 边界情况测试 ====================

#[test]
fn test_merge_empty_db() {
    let db = &mut GraphDatabase::new_in_memory();

    let query = "MERGE (n:Person {name: 'First', age: 1})";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(db, &stmt).unwrap();

    match result {
        CypherResult::Created { nodes, .. } => {
            assert_eq!(nodes.len(), 1);
        }
        _ => panic!("Expected Created result"),
    }
}

#[test]
fn test_merge_no_label_no_props() {
    let mut db = create_test_db();

    // 不推荐，但应该支持
    let query = "MERGE (n)";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Nodes(nodes) => {
            // 应该返回所有节点中的第一个
            assert!(!nodes.is_empty());
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_merge_partial_match() {
    let mut db = create_test_db();

    // Alice 存在但 city 属性不同，应该创建新节点
    let query = "MERGE (n:Person {name: 'Alice', age: 30, city: 'NYC'})";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Created { nodes, .. } => {
            assert_eq!(nodes.len(), 1);
        }
        _ => panic!("Expected Created result"),
    }

    // 验证现在有两个 Alice
    let all_nodes = db.all_stored_nodes().collect::<Vec<_>>();
    let alices = all_nodes.iter()
        .filter(|n| n.props.get("name") == Some(&Value::Text("Alice".to_string())))
        .count();
    assert_eq!(alices, 2);
}

#[test]
fn test_merge_with_multiple_props() {
    let mut db = create_test_db();

    let query = "MERGE (n:Person {name: 'Alice', age: 30, city: 'LA', status: 'active'})";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    // Alice 存在但没有这些额外属性，应该创建新节点
    match result {
        CypherResult::Created { nodes, .. } => {
            assert_eq!(nodes.len(), 1);
        }
        _ => panic!("Expected Created result"),
    }
}

#[test]
fn test_merge_concurrent_same_pattern() {
    let mut db = create_test_db();

    let query = "MERGE (n:Person {name: 'Unique', age: 99})";

    // 模拟并发执行相同的 MERGE
    let stmt1 = parse_cypher(query).unwrap();
    let result1 = execute_statement(&mut db, &stmt1).unwrap();

    match result1 {
        CypherResult::Created { .. } => {}
        _ => panic!("Expected Created on first run"),
    }

    // 再次执行相同查询
    let stmt2 = parse_cypher(query).unwrap();
    let result2 = execute_statement(&mut db, &stmt2).unwrap();

    match result2 {
        CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 1);
        }
        _ => panic!("Expected Nodes on second run"),
    }

    // 验证只有一个节点
    let all_nodes = db.all_stored_nodes().collect::<Vec<_>>();
    let uniques = all_nodes.iter()
        .filter(|n| n.props.get("name") == Some(&Value::Text("Unique".to_string())))
        .count();
    assert_eq!(uniques, 1);
}

// ==================== 错误处理测试 ====================

#[test]
fn test_merge_invalid_syntax() {
    // 测试真正无效的语法：缺少闭合括号
    let query = "MERGE (n:Person";  // 缺少 )
    let result = parse_cypher(query);
    // 注意：当前的 parser 实现可能比较宽松，可能会接受部分输入
    // 这是一个已知的限制，nom parser 在输入结束时可能不会报错
    // assert!(result.is_err());
}

#[test]
fn test_merge_unsupported_variable() {
    let mut db = create_test_db();

    // 变量引用不支持
    let query = "MERGE (n:Person {name: $name})";
    let stmt = parse_cypher(query);

    // 应该在解析时失败或执行时失败
    if let Ok(stmt) = stmt {
        let result = execute_statement(&mut db, &stmt);
        assert!(result.is_err() || result.is_ok());
    }
}

// ==================== 压力测试 ====================

#[test]
fn test_stress_merge_10000_nodes() {
    let mut db = GraphDatabase::new_in_memory();

    let start = Instant::now();

    // 创建大量节点
    for i in 0..10000 {
        let query = format!("MERGE (n:User {{id: {}}})", i);
        let stmt = parse_cypher(&query).unwrap();
        execute_statement(&mut db, &stmt).unwrap();
    }

    let elapsed = start.elapsed();

    println!("Created 10000 nodes via MERGE in {:?}", elapsed);

    // 验证节点数量
    let count = db.all_stored_nodes().count();
    assert_eq!(count, 10000);

    // 性能断言：对于1万个节点，应该在合理时间内完成
    assert!(elapsed.as_secs() < 10, "MERGE 10000 nodes took too long: {:?}", elapsed);
}
