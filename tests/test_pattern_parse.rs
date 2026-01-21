//! 测试 Pattern 解析

use rs_graphdb::cypher::parser::parse_cypher;

#[test]
fn test_parse_pattern_with_rel() {
    // 测试基本的模式解析
    let query = "MATCH (a:Person {name: 'Alice', age: 30})-[r:KNOWS]->(b:Person {name: 'Bob', age: 25}) RETURN a";
    match parse_cypher(query) {
        Ok(stmt) => println!("Parsed: {:?}", stmt),
        Err(e) => println!("Parse error: {}", e),
    }
}

#[test]
fn test_parse_merge_simple_rel() {
    // 不带属性的简单关系
    let query = "MERGE (a:Person)-[r:KNOWS]->(b:Person)";
    match parse_cypher(query) {
        Ok(stmt) => println!("Parsed: {:?}", stmt),
        Err(e) => println!("Parse error: {}", e),
    }
}
