use rust_graphdb::{GraphDatabase, cypher};
use rust_graphdb::values::{Properties, Value};

fn make_user(name: &str, age: i64) -> Properties {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props.insert("age".to_string(), Value::Int(age));
    props
}

#[test]
fn test_cypher_with_where() {
    let cypher_str = r#"MATCH (a:User) WHERE a.age > 25 RETURN a"#;
    let stmt = cypher::parse_cypher(cypher_str);
    assert!(stmt.is_ok());

    match stmt.unwrap() {
        cypher::CypherStatement::Query(q) => {
            assert!(q.where_clause.is_some());
        }
        _ => panic!("Expected Query statement"),
    }
}

#[test]
fn test_cypher_with_order_limit() {
    let cypher_str = r#"MATCH (a:User) RETURN a ORDER BY a.age DESC LIMIT 10"#;
    let stmt = cypher::parse_cypher(cypher_str);
    assert!(stmt.is_ok());

    match stmt.unwrap() {
        cypher::CypherStatement::Query(q) => {
            assert!(q.return_clause.order_by.is_some());
            assert_eq!(q.return_clause.limit, Some(10));
        }
        _ => panic!("Expected Query statement"),
    }
}

#[test]
fn test_cypher_aggregation() {
    let cypher_str = r#"MATCH (a:User) RETURN COUNT(*)"#;
    let stmt = cypher::parse_cypher(cypher_str);
    assert!(stmt.is_ok());

    match stmt.unwrap() {
        cypher::CypherStatement::Query(q) => {
            assert_eq!(q.return_clause.items.len(), 1);
        }
        _ => panic!("Expected Query statement"),
    }
}

#[test]
fn test_cypher_optional_match() {
    let cypher_str = r#"OPTIONAL MATCH (a:User)-[:FRIEND]->(b) RETURN a, b"#;
    let stmt = cypher::parse_cypher(cypher_str);
    assert!(stmt.is_ok());

    match stmt.unwrap() {
        cypher::CypherStatement::Query(q) => {
            assert!(q.match_clause.as_ref().unwrap().optional);
        }
        _ => panic!("Expected Query statement"),
    }
}
