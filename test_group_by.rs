use rs_graphdb::cypher::parser;

fn main() {
    let query = "MATCH (u:User) RETURN u.city, COUNT(*) GROUP BY u.city";
    println!("Testing: {}", query);
    let result = parser::parse_cypher(query);
    match result {
        Ok(stmt) => println!("Parsed successfully: {:?}", stmt),
        Err(e) => println!("Parse error: {}", e),
    }
}
