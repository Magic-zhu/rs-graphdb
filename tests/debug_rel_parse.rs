//! 调试关系解析

use rs_graphdb::cypher::parser::parse_cypher;

#[test]
fn debug_rel_pattern() {
    // 测试不同格式的 MERGE
    let queries = vec![
        "MERGE (a:Person)-[r:KNOWS]->(b:Person)",
        "MERGE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'})",
        "MERGE (a:Person {name: 'Alice', age: 30})-[r:KNOWS]->(b:Person {name: 'Bob', age: 25})",
    ];

    for query in queries {
        println!("\nTesting: {}", query);
        match parse_cypher(query) {
            Ok(stmt) => println!("Parsed: {:?}", stmt),
            Err(e) => println!("Parse error: {}", e),
        }
    }
}
