use rs_graphdb::{GraphDatabase, cypher};
use rs_graphdb::values::{Properties, Value};

fn make_user(name: &str, age: i64) -> Properties {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props.insert("age".to_string(), Value::Int(age));
    props
}

fn main() {
    println!("🦀 Rust Graph Database - Cypher Demo\n");

    let mut db = GraphDatabase::new_in_memory();

    // 创建数据
    let alice = db.create_node(vec!["User"], make_user("Alice", 30));
    let bob = db.create_node(vec!["User"], make_user("Bob", 25));
    let carol = db.create_node(vec!["User"], make_user("Carol", 35));

    db.create_rel(alice, bob, "FRIEND", Properties::new());
    db.create_rel(alice, carol, "FRIEND", Properties::new());
    db.create_rel(bob, carol, "FRIEND", Properties::new());

    println!("✅ Created 3 users and 3 friendships\n");

    // 示例查询
    let queries = vec![
        r#"MATCH (a:User {name: "Alice"})-[:FRIEND]->(b) RETURN b"#,
        r#"MATCH (a:User) WHERE a.age > 25 RETURN a"#,
        r#"MATCH (a:User) RETURN a ORDER BY a.age DESC LIMIT 2"#,
        r#"OPTIONAL MATCH (a:User)-[:FRIEND]->(b) RETURN a"#,
    ];

    for (i, query_str) in queries.iter().enumerate() {
        println!("Query {}: {}", i + 1, query_str);

        match cypher::parse_cypher(query_str) {
            Ok(stmt) => {
                println!("  ✓ Parsed successfully");
                println!("  AST: {:#?}", stmt);

                match stmt {
                    cypher::CypherStatement::Query(ref q) => {
                        match cypher::execute_cypher(&db, q) {
                            Ok(results) => {
                                println!("  ✓ Executed: {} results", results.len());
                                for node in results.iter().take(3) {
                                    println!("    - Node {}: {:?}", node.id, node.props);
                                }
                            }
                            Err(e) => {
                                println!("  ✗ Execution error: {}", e);
                            }
                        }
                    }
                    _ => {
                        println!("  ℹ Not a query statement, skipping execution");
                    }
                }
            }
            Err(e) => {
                println!("  ✗ Parse error: {}", e);
            }
        }
        println!();
    }
}
