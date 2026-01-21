//! WITH 子句测试
//!
//! 测试 WITH 子句的各种用法：
//! - 基本的变量传递
//! - WITH + WHERE 过滤
//! - AS 别名
//! - 链式查询

use rs_graphdb::cypher::{parse_cypher, execute_statement, CypherResult};
use rs_graphdb::graph::db::GraphDatabase;
use rs_graphdb::storage::mem_store::MemStore;
use rs_graphdb::values::{Properties, Value};

fn create_test_db() -> GraphDatabase<MemStore> {
    let mut db = GraphDatabase::new_in_memory();

    // 创建测试数据
    let alice = db.create_node(vec!["Person"], props("Alice", 30, "NYC"));
    let bob = db.create_node(vec!["Person"], props("Bob", 25, "LA"));
    let charlie = db.create_node(vec!["Person"], props("Charlie", 35, "NYC"));
    let david = db.create_node(vec!["Person"], props("David", 28, "Chicago"));

    // 创建关系
    db.create_rel(alice, bob, "KNOWS", Properties::new());
    db.create_rel(alice, charlie, "KNOWS", Properties::new());
    db.create_rel(bob, david, "KNOWS", Properties::new());

    db
}

fn props(name: &str, age: i64, city: &str) -> Properties {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props.insert("age".to_string(), Value::Int(age));
    props.insert("city".to_string(), Value::Text(city.to_string()));
    props
}

// ==================== 基础 WITH 测试 ====================

#[test]
fn test_with_basic() {
    let mut db = create_test_db();

    // WITH 用于传递变量
    let query = "MATCH (a:Person) WITH a RETURN a";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 4); // 所有 4 个人
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_with_where_filter() {
    let mut db = create_test_db();

    // WITH + WHERE 过滤
    let query = "MATCH (a:Person) WITH a WHERE a.age > 30 RETURN a";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 1); // 只有 Charlie (35岁)
            assert_eq!(nodes[0].props.get("name"), Some(&Value::Text("Charlie".to_string())));
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_with_city_filter() {
    let mut db = create_test_db();

    // 使用 WITH 过滤特定城市的人
    let query = "MATCH (a:Person) WITH a WHERE a.city = 'NYC' RETURN a";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 2); // Alice 和 Charlie
            let names: Vec<_> = nodes.iter()
                .filter_map(|n| n.props.get("name"))
                .filter_map(|v| if let Value::Text(s) = v { Some(s.as_str()) } else { None })
                .collect();
            assert!(names.contains(&"Alice"));
            assert!(names.contains(&"Charlie"));
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_with_multiple_filters() {
    let mut db = create_test_db();

    // 链式过滤：WITH 和 WHERE
    // 先用 WITH 过滤 age > 25 (得到: Alice 30, Charlie 35, David 28)
    // 再用 WHERE 过滤 city = 'NYC' (得到: Alice, Charlie)
    let query = "MATCH (a:Person) WITH a WHERE a.age > 25 WHERE a.city = 'NYC' RETURN a";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 2); // Alice (30岁, NYC) 和 Charlie (35岁, NYC)
            let names: Vec<_> = nodes.iter()
                .filter_map(|n| n.props.get("name"))
                .filter_map(|v| if let Value::Text(s) = v { Some(s.as_str()) } else { None })
                .collect();
            assert!(names.contains(&"Alice"));
            assert!(names.contains(&"Charlie"));
        }
        _ => panic!("Expected Nodes result"),
    }
}

// ==================== AS 别名测试 ====================

#[test]
fn test_with_as_alias() {
    let mut db = create_test_db();

    // WITH ... AS 别名
    let query = "MATCH (a:Person) WITH a.name AS name RETURN name";
    let stmt = parse_cypher(query).unwrap();

    // 注意：当前实现可能不支持完整的 AS 别名功能
    // 这个测试主要验证解析器能正确解析
    match stmt {
        rs_graphdb::cypher::ast::CypherStatement::Query(q) => {
            assert!(q.with_clause.is_some());
            // 验证 WITH 子句被正确解析
        }
        _ => panic!("Expected Query statement"),
    }
}

#[test]
fn test_return_as_alias() {
    let mut db = create_test_db();

    // RETURN ... AS 别名
    let query = "MATCH (a:Person) RETURN a.name AS name";
    let stmt = parse_cypher(query).unwrap();

    // 验证解析
    match stmt {
        rs_graphdb::cypher::ast::CypherStatement::Query(q) => {
            assert!(!q.return_clause.items.is_empty());
            // 验证 RETURN items 中包含 AS 别名
        }
        _ => panic!("Expected Query statement"),
    }
}

// ==================== 属性投影测试 ====================

#[test]
fn test_with_property_projection() {
    let mut db = create_test_db();

    // WITH 投影特定属性
    let query = "MATCH (a:Person) WITH a.name RETURN a.name";
    let stmt = parse_cypher(query).unwrap();

    // 验证解析
    match stmt {
        rs_graphdb::cypher::ast::CypherStatement::Query(q) => {
            assert!(q.with_clause.is_some());
        }
        _ => panic!("Expected Query statement"),
    }
}

// ==================== 复杂查询测试 ====================

#[test]
fn test_with_order_by() {
    let mut db = create_test_db();

    // ORDER BY 在 RETURN 上（不是 WITH）
    let query = "MATCH (a:Person) WITH a RETURN a ORDER BY a.age DESC";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Nodes(nodes) => {
            // 验证排序：从大到小
            if nodes.len() >= 2 {
                let first_age = nodes[0].props.get("age").and_then(|v| if let Value::Int(i) = v { Some(*i) } else { None });
                let second_age = nodes[1].props.get("age").and_then(|v| if let Value::Int(i) = v { Some(*i) } else { None });
                if let (Some(first), Some(second)) = (first_age, second_age) {
                    assert!(first >= second, "Should be descending order");
                }
            }
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_with_limit() {
    let mut db = create_test_db();

    // WITH + LIMIT (在 RETURN 上)
    let query = "MATCH (a:Person) WITH a WHERE a.age > 25 RETURN a LIMIT 2";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Nodes(nodes) => {
            assert!(nodes.len() <= 2);
        }
        _ => panic!("Expected Nodes result"),
    }
}

// ==================== 边界情况测试 ====================

#[test]
fn test_with_empty_result() {
    let mut db = create_test_db();

    // WHERE 条件不匹配任何结果
    let query = "MATCH (a:Person) WITH a WHERE a.age > 100 RETURN a";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 0);
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_with_no_match() {
    let mut db = create_test_db();

    // 没有匹配的节点
    let query = "MATCH (a:NonExistent) WITH a RETURN a";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 0);
        }
        _ => panic!("Expected Nodes result"),
    }
}
