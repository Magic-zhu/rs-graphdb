//! 测试 MERGE 解析

use rs_graphdb::cypher::parser::parse_cypher;

#[test]
fn test_parse_rel_merge() {
    let query = "MERGE (a:Person {name: 'Alice2', age: 30})-[r:KNOWS]->(b:Person {name: 'Bob2', age: 25})";
    match parse_cypher(query) {
        Ok(stmt) => println!("Parsed: {:?}", stmt),
        Err(e) => println!("Parse error: {}", e),
    }
}