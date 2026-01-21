use rs_graphdb::cypher::parse_cypher;

#[test]
fn test_regex_parse() {
    let query = "MATCH (n:User) WHERE n.name =~ \"A.*\" RETURN n";
    match parse_cypher(query) {
        Ok(_) => println!("OK with double quotes"),
        Err(e) => println!("Error: {}", e),
    }
}
