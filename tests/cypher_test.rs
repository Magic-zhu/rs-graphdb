use rs_graphdb::{GraphDatabase, cypher};
use rs_graphdb::values::{Properties, Value};

fn make_user(name: &str) -> Properties {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props
}

#[test]
fn test_simple_cypher_match() {
    let mut db = GraphDatabase::new_in_memory();

    let alice = db.create_node(vec!["User"], make_user("Alice"));
    let bob = db.create_node(vec!["User"], make_user("Bob"));

    db.create_rel(alice, bob, "FRIEND", Properties::new());

    // MATCH (a:User {name: "Alice"})-[:FRIEND]->(b) RETURN b
    let cypher_str = r#"MATCH (a:User {name: "Alice"})-[:FRIEND]->(b) RETURN b"#;

    let stmt = cypher::parse_cypher(cypher_str).expect("Parse failed");
    let result = match stmt {
        cypher::CypherStatement::Query(ref q) => cypher::execute_cypher(&db, q).expect("Execute failed"),
        _ => panic!("Expected Query statement"),
    };

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].id, bob);
}

#[test]
fn test_cypher_parse_only() {
    let cypher_str = r#"MATCH (a:User)-[:FRIEND]->(b:User) RETURN a, b"#;

    let stmt = cypher::parse_cypher(cypher_str).expect("Parse failed");

    match stmt {
        cypher::CypherStatement::Query(q) => {
            assert!(q.match_clause.is_some());
            assert_eq!(q.return_clause.items.len(), 2);
        }
        _ => panic!("Expected Query statement"),
    }
}
