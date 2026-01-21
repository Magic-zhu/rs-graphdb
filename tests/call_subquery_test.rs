// CALL 子查询测试
// 测试 CALL { ... } 语法，用于执行内联子查询

use rs_graphdb::cypher::{parse_cypher, execute_statement};
use rs_graphdb::graph::db::GraphDatabase;
use rs_graphdb::storage::StorageEngine;
use rs_graphdb::values::{Properties, Value};

// 辅助函数：创建带属性的节点
fn create_person(db: &mut GraphDatabase<impl StorageEngine>, name: &str, age: i64) {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props.insert("age".to_string(), Value::Int(age));
    db.create_node(vec!["Person"], props);
}

#[test]
fn test_call_basic_subquery() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建测试数据
    create_person(&mut db, "Alice", 30);
    create_person(&mut db, "Bob", 25);
    create_person(&mut db, "Charlie", 35);

    // 测试 CALL { MATCH ... RETURN ... }
    let query = "CALL { MATCH (p:Person) WHERE p.age > 28 RETURN p } RETURN p";
    let stmt = parse_cypher(query).unwrap();

    let result = execute_statement(&mut db, &stmt).unwrap();

    // 应该返回 Alice (30) 和 Charlie (35)
    match result {
        rs_graphdb::cypher::CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 2);
            let names: Vec<String> = nodes.iter()
                .filter_map(|n| n.props.get("name"))
                .filter_map(|v| match v { Value::Text(s) => Some(s.clone()), _ => None })
                .collect();
            assert!(names.contains(&"Alice".to_string()));
            assert!(names.contains(&"Charlie".to_string()));
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_call_with_aggregation() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建测试数据
    create_person(&mut db, "Alice", 30);
    create_person(&mut db, "Bob", 25);
    create_person(&mut db, "Charlie", 35);

    // 测试 CALL 子查询与聚合
    let query = "CALL { MATCH (p:Person) RETURN p } RETURN count(*)";
    let stmt = parse_cypher(query).unwrap();

    let result = execute_statement(&mut db, &stmt).unwrap();

    // 验证结果
    match result {
        rs_graphdb::cypher::CypherResult::Nodes(nodes) => {
            // count(*) 返回一个节点
            assert!(!nodes.is_empty());
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_call_with_where_clause() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建测试数据
    create_person(&mut db, "Alice", 30);
    create_person(&mut db, "Bob", 25);
    create_person(&mut db, "Charlie", 35);
    create_person(&mut db, "David", 28);

    // 测试 CALL 子查询中的 WHERE 子句
    let query = "CALL { MATCH (p:Person) WHERE p.age >= 30 RETURN p } RETURN p";
    let stmt = parse_cypher(query).unwrap();

    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        rs_graphdb::cypher::CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 2);
            let ages: Vec<i64> = nodes.iter()
                .filter_map(|n| n.props.get("age"))
                .filter_map(|v| match v { Value::Int(i) => Some(*i), _ => None })
                .collect();
            assert!(ages.contains(&30));
            assert!(ages.contains(&35));
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_call_nested_query() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建测试数据
    create_person(&mut db, "Alice", 30);
    create_person(&mut db, "Bob", 25);

    // 创建关系
    let alice_id = db.all_stored_nodes().nth(0).unwrap().id;
    let bob_id = db.all_stored_nodes().nth(1).unwrap().id;
    db.create_rel(alice_id, bob_id, "KNOWS", Properties::new());

    // 测试带关系的 CALL 子查询
    let query = "CALL { MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b } RETURN a";
    let stmt = parse_cypher(query).unwrap();

    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        rs_graphdb::cypher::CypherResult::Nodes(nodes) => {
            assert!(!nodes.is_empty());
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_call_empty_result() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建测试数据（年龄都小于 40）
    create_person(&mut db, "Alice", 30);
    create_person(&mut db, "Bob", 25);

    // 测试 CALL 子查询返回空结果
    let query = "CALL { MATCH (p:Person) WHERE p.age > 40 RETURN p } RETURN p";
    let stmt = parse_cypher(query).unwrap();

    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        rs_graphdb::cypher::CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 0);
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_call_with_limit() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建测试数据
    create_person(&mut db, "Alice", 30);
    create_person(&mut db, "Bob", 25);
    create_person(&mut db, "Charlie", 35);
    create_person(&mut db, "David", 28);
    create_person(&mut db, "Eve", 32);

    // 测试 CALL 子查询中的 LIMIT
    let query = "CALL { MATCH (p:Person) RETURN p LIMIT 2 } RETURN p";
    let stmt = parse_cypher(query).unwrap();

    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        rs_graphdb::cypher::CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 2);
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_call_with_order_by() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建测试数据
    create_person(&mut db, "Alice", 30);
    create_person(&mut db, "Bob", 25);
    create_person(&mut db, "Charlie", 35);

    // 测试 CALL 子查询中的 ORDER BY
    let query = "CALL { MATCH (p:Person) RETURN p ORDER BY p.age DESC } RETURN p";
    let stmt = parse_cypher(query).unwrap();

    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        rs_graphdb::cypher::CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 3);
            // 验证降序排列：35, 30, 25
            let ages: Vec<i64> = nodes.iter()
                .filter_map(|n| n.props.get("age"))
                .filter_map(|v| match v { Value::Int(i) => Some(*i), _ => None })
                .collect();
            assert_eq!(ages, vec![35, 30, 25]);
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_call_parse_only() {
    // 测试解析各种 CALL 语句

    // 1. 基本子查询
    let query1 = "CALL { MATCH (n) RETURN n } RETURN n";
    let stmt1 = parse_cypher(query1);
    assert!(stmt1.is_ok());

    // 2. 带WHERE的子查询
    let query2 = "CALL { MATCH (n:Person) WHERE n.age > 25 RETURN n } RETURN n";
    let stmt2 = parse_cypher(query2);
    assert!(stmt2.is_ok());

    // 3. 带LIMIT的子查询
    let query3 = "CALL { MATCH (n) RETURN n LIMIT 10 } RETURN n";
    let stmt3 = parse_cypher(query3);
    assert!(stmt3.is_ok());

    // 4. 带ORDER BY的子查询
    let query4 = "CALL { MATCH (n) RETURN n ORDER BY n.name } RETURN n";
    let stmt4 = parse_cypher(query4);
    assert!(stmt4.is_ok());

    // 5. 复杂子查询
    let query5 = "CALL { MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 25 RETURN a, b ORDER BY a.name LIMIT 5 } RETURN a";
    let stmt5 = parse_cypher(query5);
    assert!(stmt5.is_ok());
}

#[test]
fn test_call_multiple_clauses() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建测试数据
    create_person(&mut db, "Alice", 30);
    create_person(&mut db, "Bob", 25);
    create_person(&mut db, "Charlie", 35);
    create_person(&mut db, "David", 40);

    // 测试 CALL 子查询带多个子句
    let query = "CALL { MATCH (p:Person) WHERE p.age >= 30 RETURN p ORDER BY p.age DESC LIMIT 2 } RETURN p";
    let stmt = parse_cypher(query).unwrap();

    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        rs_graphdb::cypher::CypherResult::Nodes(nodes) => {
            // 应该返回 David (40) 和 Charlie (35)
            assert_eq!(nodes.len(), 2);
            let ages: Vec<i64> = nodes.iter()
                .filter_map(|n| n.props.get("age"))
                .filter_map(|v| match v { Value::Int(i) => Some(*i), _ => None })
                .collect();
            assert_eq!(ages, vec![40, 35]);
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_call_return_property() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建测试数据
    create_person(&mut db, "Alice", 30);
    create_person(&mut db, "Bob", 25);

    // 测试 CALL 子查询返回特定属性
    let query = "CALL { MATCH (p:Person) RETURN p } RETURN p.name";
    let stmt = parse_cypher(query).unwrap();

    let result = execute_statement(&mut db, &stmt).unwrap();

    // 验证结果
    match result {
        rs_graphdb::cypher::CypherResult::Nodes(nodes) => {
            assert!(!nodes.is_empty());
        }
        _ => panic!("Expected Nodes result"),
    }
}
