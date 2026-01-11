use rs_graphdb::{GraphDatabase, NodeId};
use rs_graphdb::values::{Properties, Value};
use rs_graphdb::query::Query;

fn make_user(name: &str) -> Properties {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props
}

#[test]
fn simple_query_from_label_where_out() {
    let mut db = GraphDatabase::new_in_memory();

    let alice: NodeId = db.create_node(vec!["User"], make_user("Alice"));
    let bob: NodeId = db.create_node(vec!["User"], make_user("Bob"));
    let _carol: NodeId = db.create_node(vec!["User"], make_user("Carol"));

    db.create_rel(alice, bob, "FRIEND", Properties::new());

    let result = Query::new(&db)
        .from_label("User")
        .where_prop_eq("name", "Alice")
        .out("FRIEND")
        .collect_nodes();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].id, bob);
}
