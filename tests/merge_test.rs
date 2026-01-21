//! MERGE 语句测试
//!
//! 测试 MERGE 功能：
//! - MERGE 创建新节点
//! - MERGE 匹配现有节点
//! - MERGE 带 ON CREATE SET
//! - MERGE 带 ON MATCH SET
//! - MERGE 幂等性

use rs_graphdb::cypher::{parse_cypher, execute_statement, CypherResult};
use rs_graphdb::graph::db::GraphDatabase;
use rs_graphdb::storage::mem_store::MemStore;
use rs_graphdb::values::{Properties, Value};
use rs_graphdb::Query;

fn props(name: &str, age: i64) -> Properties {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props.insert("age".to_string(), Value::Int(age));
    props
}

fn props_with_city(name: &str, age: i64, city: &str) -> Properties {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props.insert("age".to_string(), Value::Int(age));
    props.insert("city".to_string(), Value::Text(city.to_string()));
    props
}

fn create_test_db() -> GraphDatabase<MemStore> {
    let mut db = GraphDatabase::new_in_memory();

    // 创建初始数据
    db.create_node(vec!["Person"], props("Alice", 30));
    db.create_node(vec!["Person"], props("Bob", 25));
    db.create_node(vec!["Person"], props("Charlie", 35));

    db
}

#[test]
fn test_merge_create_new_node() {
    let mut db = create_test_db();

    // MERGE 创建新节点（David 不存在）
    let query_str = "MERGE (n:Person {name: 'David', age: 28})";
    let stmt = parse_cypher(query_str).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Created { nodes, rels } => {
            assert_eq!(nodes.len(), 1);
            assert_eq!(rels, 0);

            // 验证节点已创建
            let node_id = nodes[0];
            let node = db.get_node(node_id).unwrap();
            assert_eq!(node.labels, vec!["Person"]);
            assert_eq!(node.props.get("name"), Some(&Value::Text("David".to_string())));
            assert_eq!(node.props.get("age"), Some(&Value::Int(28)));
        }
        _ => panic!("Expected Created result"),
    }
}

#[test]
fn test_merge_update_existing_node() {
    let mut db = create_test_db();

    // MERGE 匹配现有节点（Alice 已存在）
    let query_str = "MERGE (n:Person {name: 'Alice', age: 30})";
    let stmt = parse_cypher(query_str).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Nodes(nodes) => {
            // 应该匹配到 Alice
            assert_eq!(nodes.len(), 1);
            assert_eq!(nodes[0].props.get("name"), Some(&Value::Text("Alice".to_string())));
        }
        _ => panic!("Expected Nodes result"),
    }

    // 验证没有创建重复节点
    use rs_graphdb::Query;
    let all_nodes = Query::new(&db).from_label("Person").collect_nodes();
    assert_eq!(all_nodes.len(), 3); // 仍然是 3 个节点
}

#[test]
fn test_merge_with_on_create() {
    let mut db = create_test_db();

    // MERGE 带 ON CREATE SET
    let query_str = "MERGE (n:Person {name: 'David', age: 28}) ON CREATE SET n.city = 'NYC'";
    let stmt = parse_cypher(query_str).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Created { nodes, rels } => {
            assert_eq!(nodes.len(), 1);
            assert_eq!(rels, 0);

            // 验证 ON CREATE SET 生效
            let node_id = nodes[0];
            let node = db.get_node(node_id).unwrap();
            assert_eq!(node.props.get("city"), Some(&Value::Text("NYC".to_string())));
        }
        _ => panic!("Expected Created result"),
    }
}

#[test]
fn test_merge_with_on_match() {
    let mut db = create_test_db();

    // MERGE 带 ON MATCH SET
    let query_str = "MERGE (n:Person {name: 'Alice', age: 30}) ON MATCH SET n.city = 'LA'";
    let stmt = parse_cypher(query_str).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Updated { nodes } => {
            assert_eq!(nodes, 1);

            // 验证 ON MATCH SET 生效
            let all_nodes = Query::new(&db).from_label("Person").collect_nodes();
            let alice = all_nodes.iter().find(|n| n.props.get("name") == Some(&Value::Text("Alice".to_string()))).unwrap();
            assert_eq!(alice.props.get("city"), Some(&Value::Text("LA".to_string())));
        }
        _ => panic!("Expected Updated result"),
    }
}

#[test]
fn test_merge_idempotent() {
    let mut db = create_test_db();

    // 第一次 MERGE - 创建
    let query_str = "MERGE (n:Person {name: 'David', age: 28})";
    let stmt = parse_cypher(query_str).unwrap();
    let result1 = execute_statement(&mut db, &stmt).unwrap();

    match result1 {
        CypherResult::Created { nodes, .. } => {
            assert_eq!(nodes.len(), 1);
        }
        _ => panic!("Expected Created result"),
    }

    // 第二次 MERGE - 匹配（不创建新节点）
    let stmt2 = parse_cypher(query_str).unwrap();
    let result2 = execute_statement(&mut db, &stmt2).unwrap();

    match result2 {
        CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 1);
        }
        _ => panic!("Expected Nodes result"),
    }

    // 验证只有一个 David 节点
    let all_nodes = Query::new(&db).from_label("Person").collect_nodes();
    let davids: Vec<_> = all_nodes.iter()
        .filter(|n| n.props.get("name") == Some(&Value::Text("David".to_string())))
        .collect();
    assert_eq!(davids.len(), 1);
}

#[test]
fn test_merge_with_on_create_and_on_match() {
    let mut db = create_test_db();

    // MERGE 带 ON CREATE 和 ON MATCH
    let query_str = "MERGE (n:Person {name: 'David', age: 28}) ON CREATE SET n.status = 'new' ON MATCH SET n.status = 'existing'";

    // 第一次：创建
    let stmt = parse_cypher(query_str).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Created { nodes, .. } => {
            assert_eq!(nodes.len(), 1);

            let node_id = nodes[0];
            let node = db.get_node(node_id).unwrap();
            assert_eq!(node.props.get("status"), Some(&Value::Text("new".to_string())));
        }
        _ => panic!("Expected Created result"),
    }

    // 第二次：匹配
    let stmt2 = parse_cypher(query_str).unwrap();
    let result2 = execute_statement(&mut db, &stmt2).unwrap();

    match result2 {
        CypherResult::Updated { nodes } => {
            assert_eq!(nodes, 1);

            let all_nodes = Query::new(&db).from_label("Person").collect_nodes();
            let david = all_nodes.iter().find(|n| n.props.get("name") == Some(&Value::Text("David".to_string()))).unwrap();
            assert_eq!(david.props.get("status"), Some(&Value::Text("existing".to_string())));
        }
        _ => panic!("Expected Updated result"),
    }
}

#[test]
fn test_merge_parse_only_label() {
    let mut db = create_test_db();

    // MERGE 只指定标签（不推荐，但应该支持）
    let query_str = "MERGE (n:Person)";
    let stmt = parse_cypher(query_str).unwrap();

    // 这个查询应该匹配第一个 Person 节点
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Nodes(nodes) => {
            // 应该匹配到某个 Person 节点
            assert!(!nodes.is_empty());
            assert!(nodes[0].labels.contains(&"Person".to_string()));
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_merge_with_multiple_properties() {
    let mut db = create_test_db();

    // MERGE 带多个属性
    let query_str = "MERGE (n:Person {name: 'Alice', age: 30, city: 'LA'})";
    let stmt = parse_cypher(query_str).unwrap();

    // Alice 存在但 city 属性不匹配，应该创建新节点
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Created { nodes, .. } => {
            assert_eq!(nodes.len(), 1);

            // 验证新节点的所有属性
            let node_id = nodes[0];
            let node = db.get_node(node_id).unwrap();
            assert_eq!(node.props.get("name"), Some(&Value::Text("Alice".to_string())));
            assert_eq!(node.props.get("age"), Some(&Value::Int(30)));
            assert_eq!(node.props.get("city"), Some(&Value::Text("LA".to_string())));
        }
        _ => panic!("Expected Created result"),
    }

    // 验证现在有两个 Alice
    let all_nodes = Query::new(&db).from_label("Person").collect_nodes();
    let alices: Vec<_> = all_nodes.iter()
        .filter(|n| n.props.get("name") == Some(&Value::Text("Alice".to_string())))
        .collect();
    assert_eq!(alices.len(), 2);
}
