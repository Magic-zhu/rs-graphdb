// Cypher 聚合函数执行测试
// 测试 MIN/MAX/COUNT/COLLECT/GROUP BY 的实际执行

use rs_graphdb::cypher::{ast::*, executor, parser};
use rs_graphdb::graph::db::GraphDatabase;
use rs_graphdb::storage::mem_store::MemStore;
use rs_graphdb::values::{Properties, Value};

// 辅助函数：创建测试节点
fn create_user(db: &mut GraphDatabase<MemStore>, name: &str, age: i64, city: &str) {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props.insert("age".to_string(), Value::Int(age));
    props.insert("city".to_string(), Value::Text(city.to_string()));
    db.create_node(vec!["User"], props);
}

#[test]
fn test_execute_min_aggregation() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建测试数据
    create_user(&mut db, "Alice", 30, "New York");
    create_user(&mut db, "Bob", 25, "London");
    create_user(&mut db, "Charlie", 35, "Paris");

    // 执行查询
    let query = "MATCH (u:User) RETURN MIN(u.age)";
    let stmt = parser::parse_cypher(query).unwrap();
    let result = executor::execute_statement(&mut db, &stmt).unwrap();

    match result {
        executor::CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 1);
            let min_age = nodes[0].get("min(u.age)");
            assert_eq!(min_age, Some(&Value::Int(25)));
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_execute_max_aggregation() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    create_user(&mut db, "Alice", 30, "New York");
    create_user(&mut db, "Bob", 25, "London");
    create_user(&mut db, "Charlie", 35, "Paris");

    let query = "MATCH (u:User) RETURN MAX(u.age)";
    let stmt = parser::parse_cypher(query).unwrap();
    let result = executor::execute_statement(&mut db, &stmt).unwrap();

    match result {
        executor::CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 1);
            let max_age = nodes[0].get("max(u.age)");
            assert_eq!(max_age, Some(&Value::Int(35)));
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_execute_count_aggregation() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    create_user(&mut db, "Alice", 30, "New York");
    create_user(&mut db, "Bob", 25, "London");
    create_user(&mut db, "Charlie", 35, "Paris");

    let query = "MATCH (u:User) RETURN COUNT(*)";
    let stmt = parser::parse_cypher(query).unwrap();
    let result = executor::execute_statement(&mut db, &stmt).unwrap();

    match result {
        executor::CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 1);
            let count = nodes[0].get("count");
            assert_eq!(count, Some(&Value::Int(3)));
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_execute_group_by() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    create_user(&mut db, "Alice", 30, "New York");
    create_user(&mut db, "Bob", 25, "London");
    create_user(&mut db, "Charlie", 35, "New York");
    create_user(&mut db, "David", 28, "London");

    let query = "MATCH (u:User) RETURN u.city, COUNT(*) GROUP BY u.city";
    let stmt = parser::parse_cypher(query).unwrap();
    let result = executor::execute_statement(&mut db, &stmt).unwrap();

    match result {
        executor::CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 2); // 两个城市
            // 验证每个分组都有计数
            let total_count: i64 = nodes
                .iter()
                .filter_map(|n| n.get("count").and_then(|v| match v {
                    Value::Int(i) => Some(*i),
                    _ => None,
                }))
                .sum();
            assert_eq!(total_count, 4);
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_execute_min_max_with_group_by() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    create_user(&mut db, "Alice", 30, "New York");
    create_user(&mut db, "Bob", 25, "London");
    create_user(&mut db, "Charlie", 35, "New York");
    create_user(&mut db, "David", 20, "London");

    let query = "MATCH (u:User) RETURN u.city, MIN(u.age), MAX(u.age) GROUP BY u.city";
    let stmt = parser::parse_cypher(query).unwrap();
    let result = executor::execute_statement(&mut db, &stmt).unwrap();

    match result {
        executor::CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 2);
            // 验证每个分组都有 min 和 max
            for node in &nodes {
                assert!(node.get("min(u.age)").is_some());
                assert!(node.get("max(u.age)").is_some());
            }
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_execute_collect_aggregation() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    create_user(&mut db, "Alice", 30, "New York");
    create_user(&mut db, "Bob", 25, "London");
    create_user(&mut db, "Charlie", 35, "Paris");

    let query = "MATCH (u:User) RETURN COLLECT(u.name)";
    let stmt = parser::parse_cypher(query).unwrap();
    let result = executor::execute_statement(&mut db, &stmt).unwrap();

    match result {
        executor::CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 1);
            let names = nodes[0].get("collect(u.name)");
            assert!(names.is_some());
            if let Some(Value::List(names_list)) = names {
                assert_eq!(names_list.len(), 3);
            } else {
                panic!("Expected List value");
            }
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_execute_collect_with_group_by() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    create_user(&mut db, "Alice", 30, "New York");
    create_user(&mut db, "Bob", 25, "London");
    create_user(&mut db, "Charlie", 35, "New York");

    let query = "MATCH (u:User) RETURN u.city, COLLECT(u.name) GROUP BY u.city";
    let stmt = parser::parse_cypher(query).unwrap();
    let result = executor::execute_statement(&mut db, &stmt).unwrap();

    match result {
        executor::CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 2);
            // 验证每个分组都有收集的名称列表
            for node in &nodes {
                assert!(node.get("collect(u.name)").is_some());
                if let Some(Value::List(names)) = node.get("collect(u.name)") {
                    assert!(!names.is_empty());
                }
            }
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_execute_aggregation_with_where() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    create_user(&mut db, "Alice", 30, "New York");
    create_user(&mut db, "Bob", 25, "London");
    create_user(&mut db, "Charlie", 35, "Paris");

    let query = "MATCH (u:User) WHERE u.age > 25 RETURN COUNT(*)";
    let stmt = parser::parse_cypher(query).unwrap();
    let result = executor::execute_statement(&mut db, &stmt).unwrap();

    match result {
        executor::CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 1);
            let count = nodes[0].get("count");
            assert_eq!(count, Some(&Value::Int(2))); // Alice 和 Charlie
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_execute_aggregation_with_order_by() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    create_user(&mut db, "Alice", 30, "New York");
    create_user(&mut db, "Bob", 25, "London");
    create_user(&mut db, "Charlie", 35, "Paris");
    create_user(&mut db, "David", 28, "London");

    let query = "MATCH (u:User) RETURN u.city, COUNT(*) GROUP BY u.city ORDER BY COUNT(*) DESC";
    let stmt = parser::parse_cypher(query).unwrap();
    let result = executor::execute_statement(&mut db, &stmt).unwrap();

    match result {
        executor::CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 3); // 3个城市：London(2), New York(1), Paris(1)
            // 验证排序：London 应该在前面（2个用户）
            let first_count = nodes[0].get("count");
            assert_eq!(first_count, Some(&Value::Int(2))); // London 有2个用户
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_execute_aggregation_with_limit() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    create_user(&mut db, "Alice", 30, "New York");
    create_user(&mut db, "Bob", 25, "London");
    create_user(&mut db, "Charlie", 35, "Paris");

    let query = "MATCH (u:User) RETURN u.city, COUNT(*) GROUP BY u.city LIMIT 2";
    let stmt = parser::parse_cypher(query).unwrap();
    let result = executor::execute_statement(&mut db, &stmt).unwrap();

    match result {
        executor::CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 2);
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_execute_sum_aggregation() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    create_user(&mut db, "Alice", 30, "New York");
    create_user(&mut db, "Bob", 25, "London");
    create_user(&mut db, "Charlie", 35, "Paris");

    let query = "MATCH (u:User) RETURN SUM(u.age)";
    let stmt = parser::parse_cypher(query).unwrap();
    let result = executor::execute_statement(&mut db, &stmt).unwrap();

    match result {
        executor::CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 1);
            let sum = nodes[0].get("sum(u.age)");
            assert_eq!(sum, Some(&Value::Int(90))); // 30 + 25 + 35
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_execute_avg_aggregation() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    create_user(&mut db, "Alice", 30, "New York");
    create_user(&mut db, "Bob", 25, "London");
    create_user(&mut db, "Charlie", 35, "Paris");

    let query = "MATCH (u:User) RETURN AVG(u.age)";
    let stmt = parser::parse_cypher(query).unwrap();
    let result = executor::execute_statement(&mut db, &stmt).unwrap();

    match result {
        executor::CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 1);
            let avg = nodes[0].get("avg(u.age)");
            // 90 / 3 = 30.0
            if let Some(Value::Float(avg_val)) = avg {
                assert!((avg_val - 30.0).abs() < 0.001);
            } else {
                panic!("Expected Float value for AVG");
            }
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_execute_multiple_aggregations() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    create_user(&mut db, "Alice", 30, "New York");
    create_user(&mut db, "Bob", 25, "London");
    create_user(&mut db, "Charlie", 35, "Paris");

    let query = "MATCH (u:User) RETURN COUNT(*), SUM(u.age), AVG(u.age), MIN(u.age), MAX(u.age)";
    let stmt = parser::parse_cypher(query).unwrap();
    let result = executor::execute_statement(&mut db, &stmt).unwrap();

    match result {
        executor::CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 1);
            assert!(nodes[0].get("count").is_some());
            assert!(nodes[0].get("sum(u.age)").is_some());
            assert!(nodes[0].get("avg(u.age)").is_some());
            assert!(nodes[0].get("min(u.age)").is_some());
            assert!(nodes[0].get("max(u.age)").is_some());
        }
        _ => panic!("Expected Nodes result"),
    }
}
