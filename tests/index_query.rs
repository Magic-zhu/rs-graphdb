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
fn index_based_start_query() {
    let mut db = GraphDatabase::new_in_memory();

    let alice: NodeId = db.create_node(vec!["User"], make_user("Alice", 30));
    let bob: NodeId = db.create_node(vec!["User"], make_user("Bob", 25));
    let _carol: NodeId = db.create_node(vec!["User"], make_user("Carol", 40));

    db.create_rel(alice, bob, "FRIEND", Properties::new());

    // 使用索引直接按 label+name 找到 Alice，然后走 FRIEND 出边
    let result = Query::new(&db)
        .from_label_and_prop_eq("User", "name", "Alice")
        .out("FRIEND")
        .collect_nodes();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].id, bob);
}
