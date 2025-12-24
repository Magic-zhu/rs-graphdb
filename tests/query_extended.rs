use rust_graphdb::{GraphDatabase, NodeId};
use rust_graphdb::values::{Properties, Value};
use rust_graphdb::query::Query;

fn make_user(name: &str, age: i64) -> Properties {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props.insert("age".to_string(), Value::Int(age));
    props
}

#[test]
fn query_chain_with_multiple_steps() {
    let mut db = GraphDatabase::new_in_memory();

    let alice: NodeId = db.create_node(vec!["User"], make_user("Alice", 30));
    let bob: NodeId = db.create_node(vec!["User"], make_user("Bob", 25));
    let carol: NodeId = db.create_node(vec!["User"], make_user("Carol", 40));

    db.create_rel(alice, bob, "FRIEND", Properties::new());
    db.create_rel(bob, carol, "FRIEND", Properties::new());

    // MATCH (a:User {name:"Alice"})-[:FRIEND]->(b:User)-[:FRIEND]->(c:User)
    let result = Query::new(&db)
        .from_label("User")
        .where_prop_eq("name", "Alice")
        .out("FRIEND")
        .out("FRIEND")
        .where_prop_int_gt("age", 30)
        .distinct()
        .collect_nodes();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].id, carol);
}
