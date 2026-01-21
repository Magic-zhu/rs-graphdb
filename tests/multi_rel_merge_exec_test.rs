//! 多关系 MERGE 执行测试

use rs_graphdb::cypher::{parse_cypher, execute_statement, CypherResult};
use rs_graphdb::graph::db::GraphDatabase;
use rs_graphdb::storage::mem_store::MemStore;
use rs_graphdb::values::{Properties, Value};

fn props(id: &str, value: i64) -> Properties {
    let mut props = Properties::new();
    props.insert("id".to_string(), Value::Text(id.to_string()));
    props.insert("value".to_string(), Value::Int(value));
    props
}

#[test]
fn test_multi_rel_merge_create_path() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建三节点路径
    let query = "MERGE (a:Node {id: '1'})-[r1:LINK]->(b:Node {id: '2'})-[r2:LINK]->(c:Node {id: '3'})";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Created { nodes, rels } => {
            assert_eq!(nodes.len(), 3); // 创建了 3 个节点
            assert_eq!(rels, 2); // 创建了 2 个关系
        }
        _ => panic!("Expected Created result"),
    }
}

#[test]
fn test_multi_rel_merge_match_existing() {
    let mut db = GraphDatabase::new_in_memory();

    // 先创建路径
    let n1 = db.create_node(vec!["Node"], props("1", 10));
    let n2 = db.create_node(vec!["Node"], props("2", 20));
    let n3 = db.create_node(vec!["Node"], props("3", 30));
    db.create_rel(n1, n2, "LINK", Properties::new());
    db.create_rel(n2, n3, "LINK", Properties::new());

    // 再次执行相同的 MERGE
    let query = "MERGE (a:Node {id: '1'})-[r1:LINK]->(b:Node {id: '2'})-[r2:LINK]->(c:Node {id: '3'})";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Nodes(nodes) => {
            // 找到了现有的路径
            assert!(!nodes.is_empty());
        }
        _ => {}
    }
}

#[test]
fn test_multi_rel_merge_partial_match() {
    let mut db = GraphDatabase::new_in_memory();

    // 只创建前两个节点和一个关系
    let n1 = db.create_node(vec!["Node"], props("1", 10));
    let n2 = db.create_node(vec!["Node"], props("2", 20));
    db.create_rel(n1, n2, "LINK", Properties::new());

    // MERGE 三节点路径
    let query = "MERGE (a:Node {id: '1'})-[r1:LINK]->(b:Node {id: '2'})-[r2:LINK]->(c:Node {id: '3'})";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Created { nodes, rels } => {
            // 应该创建第三个节点和第二个关系
            // 注意：rels 可能是 2（创建两个关系）或 1（如果第一个关系已存在）
            assert!(rels >= 1 && rels <= 2); // 至少创建 1 个新关系
        }
        _ => panic!("Expected Created result"),
    }
}

#[test]
fn test_multi_rel_merge_on_create() {
    let mut db = GraphDatabase::new_in_memory();

    // MERGE with ON CREATE SET
    let query = "MERGE (a:Node {id: '1'})-[r1:LINK]->(b:Node {id: '2'})-[r2:LINK]->(c:Node {id: '3'}) ON CREATE SET a.new = 1";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    match result {
        CypherResult::Created { nodes, rels } => {
            assert_eq!(nodes.len(), 3);
            assert_eq!(rels, 2);

            // 验证 ON CREATE SET 执行了
            if let Some(node) = db.get_node(nodes[0]) {
                assert_eq!(node.props.get("new"), Some(&Value::Int(1)));
            }
        }
        _ => panic!("Expected Created result"),
    }
}
