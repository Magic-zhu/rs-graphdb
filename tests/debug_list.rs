use rs_graphdb::cypher::parse_cypher;

#[test]
fn debug_list_parse() {
    let query = "MATCH (n:User) WHERE n.city IN [\"NYC\", \"LA\"] RETURN n";
    match parse_cypher(query) {
        Ok(_) => println!("OK with double quotes"),
        Err(e) => println!("Error with double quotes: {}", e),
    }

    let query2 = "MATCH (n:User) WHERE n.city IN ['NYC', 'LA'] RETURN n";
    match parse_cypher(query2) {
        Ok(_) => println!("OK with single quotes"),
        Err(e) => println!("Error with single quotes: {}", e),
    }
}
