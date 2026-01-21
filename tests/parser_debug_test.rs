//! 解析器调试测试

use rs_graphdb::cypher::parse_cypher;

#[test]
fn debug_regex_parse() {
    let query_str = "MATCH (n:User) WHERE n.name =~ 'A.*' RETURN n";
    println!("Parsing: {}", query_str);
    match parse_cypher(query_str) {
        Ok(stmt) => println!("Parsed OK: {:?}", stmt),
        Err(e) => println!("Parse error: {}", e),
    }
}

#[test]
fn debug_in_parse() {
    let query_str = "MATCH (n:User) WHERE n.city IN ['NYC', 'LA'] RETURN n";
    println!("Parsing: {}", query_str);
    match parse_cypher(query_str) {
        Ok(stmt) => println!("Parsed OK: {:?}", stmt),
        Err(e) => println!("Parse error: {}", e),
    }
}

#[test]
fn debug_order_by_parse() {
    let query_str = "MATCH (n:User) ORDER BY n.city ASC RETURN n";
    println!("Parsing: {}", query_str);
    match parse_cypher(query_str) {
        Ok(stmt) => println!("Parsed OK: {:?}", stmt),
        Err(e) => println!("Parse error: {}", e),
    }
}
