use rust_graphdb::{GraphDatabase, cypher};
use rust_graphdb::values::Value;

#[test]
fn test_create_single_node() {
    let mut db = GraphDatabase::new_in_memory();

    let stmt = cypher::parse_cypher(r#"CREATE (n:User {name: "Alice", age: 30})"#).unwrap();

    let result = cypher::execute_statement(&mut db, &stmt).unwrap();

    match result {
        cypher::CypherResult::Created { nodes, rels } => {
            assert_eq!(nodes.len(), 1);
            assert_eq!(rels, 0);

            let node = db.get_node(nodes[0]).unwrap();
            assert_eq!(node.props.get("name"), Some(&Value::Text("Alice".to_string())));
            assert_eq!(node.props.get("age"), Some(&Value::Int(30)));
            assert!(node.has_label("User"));
        }
        _ => panic!("Expected Created result"),
    }
}

#[test]
fn test_create_relationship() {
    let mut db = GraphDatabase::new_in_memory();

    let stmt = cypher::parse_cypher(
        r#"CREATE (a:User {name: "Alice"})-[:FRIEND]->(b:User {name: "Bob"})"#
    ).unwrap();

    let result = cypher::execute_statement(&mut db, &stmt).unwrap();

    match result {
        cypher::CypherResult::Created { nodes, rels } => {
            assert_eq!(nodes.len(), 2);
            assert_eq!(rels, 1);

            let alice = db.get_node(nodes[0]).unwrap();
            let bob = db.get_node(nodes[1]).unwrap();

            assert_eq!(alice.props.get("name"), Some(&Value::Text("Alice".to_string())));
            assert_eq!(bob.props.get("name"), Some(&Value::Text("Bob".to_string())));

            // 验证关系
            let rels: Vec<_> = db.neighbors_out(nodes[0]).collect();
            assert_eq!(rels.len(), 1);
            assert_eq!(rels[0].end, nodes[1]);
            assert_eq!(rels[0].typ, "FRIEND");
        }
        _ => panic!("Expected Created result"),
    }
}

#[test]
fn test_create_chain() {
    let mut db = GraphDatabase::new_in_memory();

    let stmt = cypher::parse_cypher(
        r#"CREATE (a:User {name: "A"})-[:KNOWS]->(b:User {name: "B"})-[:KNOWS]->(c:User {name: "C"})"#
    ).unwrap();

    let result = cypher::execute_statement(&mut db, &stmt).unwrap();

    match result {
        cypher::CypherResult::Created { nodes, rels } => {
            assert_eq!(nodes.len(), 3);
            assert_eq!(rels, 2);

            // 验证 A -> B -> C 链
            let a_rels: Vec<_> = db.neighbors_out(nodes[0]).collect();
            assert_eq!(a_rels.len(), 1);
            assert_eq!(a_rels[0].end, nodes[1]);

            let b_rels: Vec<_> = db.neighbors_out(nodes[1]).collect();
            assert_eq!(b_rels.len(), 1);
            assert_eq!(b_rels[0].end, nodes[2]);
        }
        _ => panic!("Expected Created result"),
    }
}

#[test]
fn test_create_then_query() {
    let mut db = GraphDatabase::new_in_memory();

    // 先创建
    let create_stmt = cypher::parse_cypher(
        r#"CREATE (a:User {name: "Alice", age: 25})"#
    ).unwrap();
    cypher::execute_statement(&mut db, &create_stmt).unwrap();

    // 再查询
    let query_stmt = cypher::parse_cypher(
        r#"MATCH (n:User {name: "Alice"}) RETURN n"#
    ).unwrap();

    let result = cypher::execute_statement(&mut db, &query_stmt).unwrap();

    match result {
        cypher::CypherResult::Nodes(nodes) => {
            assert_eq!(nodes.len(), 1);
            assert_eq!(nodes[0].props.get("name"), Some(&Value::Text("Alice".to_string())));
            assert_eq!(nodes[0].props.get("age"), Some(&Value::Int(25)));
        }
        _ => panic!("Expected Nodes result"),
    }
}
