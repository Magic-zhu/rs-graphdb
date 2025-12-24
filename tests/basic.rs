use rust_graphdb::{GraphDatabase, NodeId};
use rust_graphdb::values::{Properties, Value};

fn make_props(name: &str) -> Properties {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props
}

#[test]
fn create_nodes_and_relationships() {
    let mut db = GraphDatabase::new_in_memory();

    let alice: NodeId = db.create_node(vec!["User"], make_props("Alice"));
    let bob: NodeId = db.create_node(vec!["User"], make_props("Bob"));

    let _rel = db.create_rel(alice, bob, "FRIEND", Properties::new());

    let alice_node = db.get_node(alice).expect("alice should exist");
    assert!(alice_node.has_label("User"));
    assert_eq!(
        alice_node.get("name"),
        Some(&Value::Text("Alice".to_string()))
    );

    let neighbors: Vec<_> = db.neighbors_out(alice).collect();
    assert_eq!(neighbors.len(), 1);
    assert_eq!(neighbors[0].end, bob);
    assert_eq!(neighbors[0].typ, "FRIEND".to_string());
}
