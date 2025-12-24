use rust_graphdb::{GraphDatabase, cypher};
use rust_graphdb::values::Value;

#[test]
fn test_delete_single_node() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建节点
    let create_stmt = cypher::parse_cypher(r#"CREATE (n:User {name: "Alice"})"#).unwrap();
    cypher::execute_statement(&mut db, &create_stmt).unwrap();

    // 删除节点
    let delete_stmt = cypher::parse_cypher(r#"MATCH (n:User {name: "Alice"}) DELETE n"#).unwrap();
    let result = cypher::execute_statement(&mut db, &delete_stmt).unwrap();

    match result {
        cypher::CypherResult::Deleted { nodes, rels } => {
            assert_eq!(nodes, 1);
            assert_eq!(rels, 0);
        }
        _ => panic!("Expected Deleted result"),
    }

    // 验证节点已删除
    let query_stmt = cypher::parse_cypher(r#"MATCH (n:User) RETURN n"#).unwrap();
    let result = cypher::execute_statement(&mut db, &query_stmt).unwrap();

    match result {
        cypher::CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 0);
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_delete_node_with_relationships() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建节点和关系
    let create_stmt = cypher::parse_cypher(
        r#"CREATE (a:User {name: "Alice"})-[:FRIEND]->(b:User {name: "Bob"})"#
    ).unwrap();
    cypher::execute_statement(&mut db, &create_stmt).unwrap();

    // 删除 Alice（会同时删除关系）
    let delete_stmt = cypher::parse_cypher(r#"MATCH (n:User {name: "Alice"}) DELETE n"#).unwrap();
    let result = cypher::execute_statement(&mut db, &delete_stmt).unwrap();

    match result {
        cypher::CypherResult::Deleted { nodes, rels } => {
            assert_eq!(nodes, 1);
            assert_eq!(rels, 1); // Alice 有1条出边
        }
        _ => panic!("Expected Deleted result"),
    }

    // 验证 Bob 还在，但 Alice 已删除
    let query_stmt = cypher::parse_cypher(r#"MATCH (n:User) RETURN n"#).unwrap();
    let result = cypher::execute_statement(&mut db, &query_stmt).unwrap();

    match result {
        cypher::CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 1);
            assert_eq!(nodes[0].props.get("name"), Some(&Value::Text("Bob".to_string())));
        }
        _ => panic!("Expected Nodes result"),
    }
}

#[test]
fn test_delete_with_where() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建多个节点
    cypher::execute_statement(&mut db, &cypher::parse_cypher(r#"CREATE (n:User {name: "Alice", age: 25})"#).unwrap()).unwrap();
    cypher::execute_statement(&mut db, &cypher::parse_cypher(r#"CREATE (n:User {name: "Bob", age: 30})"#).unwrap()).unwrap();
    cypher::execute_statement(&mut db, &cypher::parse_cypher(r#"CREATE (n:User {name: "Carol", age: 35})"#).unwrap()).unwrap();

    // 删除 age > 28 的节点
    let delete_stmt = cypher::parse_cypher(r#"MATCH (n:User) WHERE n.age > 28 DELETE n"#).unwrap();
    let result = cypher::execute_statement(&mut db, &delete_stmt).unwrap();

    match result {
        cypher::CypherResult::Deleted { nodes, rels } => {
            assert_eq!(nodes, 2); // Bob 和 Carol
            assert_eq!(rels, 0);
        }
        _ => panic!("Expected Deleted result"),
    }

    // 验证只剩 Alice
    let query_stmt = cypher::parse_cypher(r#"MATCH (n:User) RETURN n"#).unwrap();
    let result = cypher::execute_statement(&mut db, &query_stmt).unwrap();

    match result {
        cypher::CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 1);
            assert_eq!(nodes[0].props.get("name"), Some(&Value::Text("Alice".to_string())));
        }
        _ => panic!("Expected Nodes result"),
    }
}
