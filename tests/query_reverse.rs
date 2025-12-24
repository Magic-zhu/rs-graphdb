use rust_graphdb::{GraphDatabase, NodeId};
use rust_graphdb::values::{Properties, Value};
use rust_graphdb::query::Query;

fn make_user(name: &str) -> Properties {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props
}

#[test]
fn test_reverse_traversal_with_in() {
    let mut db = GraphDatabase::new_in_memory();

    let alice: NodeId = db.create_node(vec!["User"], make_user("Alice"));
    let bob: NodeId = db.create_node(vec!["User"], make_user("Bob"));
    let carol: NodeId = db.create_node(vec!["User"], make_user("Carol"));

    // Alice -> Bob, Carol -> Bob
    db.create_rel(alice, bob, "FRIEND", Properties::new());
    db.create_rel(carol, bob, "FRIEND", Properties::new());

    // 从 Bob 开始，沿 FRIEND 入边走，应该找到 Alice 和 Carol
    let result = Query::new(&db)
        .from_label_and_prop_eq("User", "name", "Bob")
        .in_("FRIEND")
        .distinct()
        .collect_nodes();

    assert_eq!(result.len(), 2);
    let ids: Vec<NodeId> = result.iter().map(|n| n.id).collect();
    assert!(ids.contains(&alice));
    assert!(ids.contains(&carol));
}
