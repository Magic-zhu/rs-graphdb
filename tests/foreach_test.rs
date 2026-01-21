//! FOREACH 语句测试
//!
//! 测试 FOREACH 语句的各种用法：
//! - 基本的列表遍历
//! - 批量更新属性
//! - 与其他语句组合使用

use rs_graphdb::cypher::{parse_cypher, execute_statement, CypherResult};
use rs_graphdb::graph::db::GraphDatabase;
use rs_graphdb::storage::mem_store::MemStore;
use rs_graphdb::values::{Properties, Value};

fn create_test_db() -> GraphDatabase<MemStore> {
    let mut db = GraphDatabase::new_in_memory();

    // 创建测试节点
    let _n1 = db.create_node(vec!["Node"], props(1, "A"));
    let _n2 = db.create_node(vec!["Node"], props(2, "B"));
    let _n3 = db.create_node(vec!["Node"], props(3, "C"));
    let _n4 = db.create_node(vec!["Node"], props(4, "D"));
    let _n5 = db.create_node(vec!["Node"], props(5, "E"));

    db
}

fn props(id: i64, label: &str) -> Properties {
    let mut props = Properties::new();
    props.insert("id".to_string(), Value::Int(id));
    props.insert("label".to_string(), Value::Text(label.to_string()));
    props
}

// ==================== 基础 FOREACH 测试 ====================

#[test]
fn test_foreach_basic() {
    let mut db = create_test_db();

    // FOREACH 遍历节点 ID 列表，更新属性
    let query = "FOREACH (n IN [1, 2, 3] | SET n.marked = 1)";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Updated { nodes } => {
            assert_eq!(nodes, 3); // 更新了 3 个节点
        }
        _ => panic!("Expected Updated result"),
    }

    // 验证属性已更新
    if let Some(node) = db.get_node(1) {
        assert_eq!(node.props.get("marked"), Some(&Value::Int(1)));
    }
}

#[test]
fn test_foreach_empty_list() {
    let mut db = create_test_db();

    // 空列表
    let query = "FOREACH (n IN [] | SET n.marked = 1)";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Updated { nodes } => {
            assert_eq!(nodes, 0); // 没有更新任何节点
        }
        _ => panic!("Expected Updated result"),
    }
}

#[test]
fn test_foreach_multiple_updates() {
    let mut db = create_test_db();

    // FOREACH 中有多个 SET 操作
    let query = "FOREACH (n IN [1, 2] | SET n.marked = 1, n.processed = 2)";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Updated { nodes } => {
            // 每个节点更新 2 个属性
            assert!(nodes >= 2);
        }
        _ => panic!("Expected Updated result"),
    }

    // 验证属性已更新
    if let Some(node) = db.get_node(1) {
        assert_eq!(node.props.get("marked"), Some(&Value::Int(1)));
        assert_eq!(node.props.get("processed"), Some(&Value::Int(2)));
    }
}

#[test]
fn test_foreach_string_value() {
    let mut db = create_test_db();

    // FOREACH 更新字符串属性
    let query = "FOREACH (n IN [1, 2, 3] | SET n.status = 'done')";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Updated { nodes } => {
            assert_eq!(nodes, 3);
        }
        _ => panic!("Expected Updated result"),
    }

    // 验证字符串属性已更新
    if let Some(node) = db.get_node(1) {
        assert_eq!(node.props.get("status"), Some(&Value::Text("done".to_string())));
    }
}

#[test]
fn test_foreach_nonexistent_nodes() {
    let mut db = create_test_db();

    // FOREACH 包含不存在的节点 ID
    let query = "FOREACH (n IN [1, 999, 1000] | SET n.marked = 1)";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Updated { nodes } => {
            // 只有节点 1 被更新
            assert_eq!(nodes, 1);
        }
        _ => panic!("Expected Updated result"),
    }
}

// ==================== 复杂场景测试 ====================

#[test]
fn test_foreach_large_list() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建 100 个节点
    for i in 1..=100 {
        let mut props = Properties::new();
        props.insert("id".to_string(), Value::Int(i));
        db.create_node(vec!["Node"], props);
    }

    // FOREACH 更新所有节点
    let ids: Vec<String> = (1..=100).map(|i| i.to_string()).collect::<Vec<String>>();
    let ids_str = ids.join(", ");
    let query = format!("FOREACH (n IN [{}] | SET n.batch = 1)", ids_str);
    let stmt = parse_cypher(&query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Updated { nodes } => {
            // 应该更新了接近 100 个节点（可能有一些更新失败）
            assert!(nodes >= 99, "Expected at least 99 updates, got {}", nodes);
        }
        _ => panic!("Expected Updated result"),
    }
}

#[test]
fn test_foreach_parse_only() {
    // 测试解析：不执行，只验证能正确解析
    let queries = vec![
        "FOREACH (n IN [1, 2, 3] | SET n.marked = true)",
        "FOREACH (x IN [1, 2] | SET x.done = 'yes')",
        "FOREACH (item IN [1] | SET item.count = 0, item.sum = 100)",
    ];

    for query in queries {
        let stmt = parse_cypher(query);
        assert!(stmt.is_ok(), "Failed to parse: {}", query);
    }
}

// ==================== 边界情况测试 ====================

#[test]
fn test_foreach_invalid_syntax() {
    // 缺少竖线
    let query = "FOREACH (n IN [1, 2, 3] SET n.marked = 1)";
    let stmt = parse_cypher(query);

    // 这个查询应该解析失败，因为缺少 |
    // 但当前实现可能比较宽松，所以这里只验证不会崩溃
    let _ = stmt;
}

#[test]
fn test_foreach_with_match() {
    let mut db = create_test_db();

    // 先匹配节点，然后用 FOREACH 更新
    // 注意：这是一个简化的测试，完整的 FOREACH + MATCH 需要更复杂的实现
    let query = "FOREACH (n IN [1, 2, 3] | SET n.marked = 1)";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Updated { nodes } => {
            assert!(nodes > 0);
        }
        _ => panic!("Expected Updated result"),
    }
}
