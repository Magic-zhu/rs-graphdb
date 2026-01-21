//! 多关系 MERGE 测试

use rs_graphdb::cypher::parser::parse_cypher;

#[test]
fn test_parse_multi_rel_merge() {
    // 测试解析多关系 MERGE
    let queries = vec![
        "MERGE (a)-[r1:REL1]->(b)-[r2:REL2]->(c)",
        "MERGE (a:Person)-[r1:KNOWS]->(b:Person)-[r2:KNOWS]->(c:Person)",
        "MERGE (a)-[r1:REL]->(b)-[r2:REL]->(c)-[r3:REL]->(d)",
    ];

    for query in queries {
        println!("\nTesting: {}", query);
        match parse_cypher(query) {
            Ok(stmt) => println!("Parsed: {:?}", stmt),
            Err(e) => println!("Parse error: {}", e),
        }
    }
}
