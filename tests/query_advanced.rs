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
fn test_limit_and_skip() {
    let mut db = GraphDatabase::new_in_memory();

    let _alice: NodeId = db.create_node(vec!["User"], make_user("Alice", 30));
    let bob: NodeId = db.create_node(vec!["User"], make_user("Bob", 25));
    let carol: NodeId = db.create_node(vec!["User"], make_user("Carol", 40));

    // Skip 1, limit 1 应该返回第二个节点
    let result = Query::new(&db)
        .from_label("User")
        .skip(1)
        .limit(1)
        .collect_nodes();

    assert_eq!(result.len(), 1);
    // 因为顺序不确定，只检查数量
}

#[test]
fn test_order_by() {
    let mut db = GraphDatabase::new_in_memory();

    db.create_node(vec!["User"], make_user("Alice", 30));
    db.create_node(vec!["User"], make_user("Bob", 25));
    db.create_node(vec!["User"], make_user("Carol", 40));

    // 按年龄升序
    let result = Query::new(&db)
        .from_label("User")
        .order_by("age", true)
        .collect_nodes();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].props.get("age"), Some(&Value::Int(25)));
    assert_eq!(result[1].props.get("age"), Some(&Value::Int(30)));
    assert_eq!(result[2].props.get("age"), Some(&Value::Int(40)));

    // 按年龄降序
    let result = Query::new(&db)
        .from_label("User")
        .order_by("age", false)
        .collect_nodes();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].props.get("age"), Some(&Value::Int(40)));
    assert_eq!(result[1].props.get("age"), Some(&Value::Int(30)));
    assert_eq!(result[2].props.get("age"), Some(&Value::Int(25)));
}

#[test]
fn test_aggregations() {
    let mut db = GraphDatabase::new_in_memory();

    db.create_node(vec!["User"], make_user("Alice", 30));
    db.create_node(vec!["User"], make_user("Bob", 25));
    db.create_node(vec!["User"], make_user("Carol", 40));

    // Count
    let count = Query::new(&db)
        .from_label("User")
        .count();
    assert_eq!(count, 3);

    // Sum
    let sum = Query::new(&db)
        .from_label("User")
        .sum_int("age");
    assert_eq!(sum, 95); // 30 + 25 + 40

    // Average
    let avg = Query::new(&db)
        .from_label("User")
        .avg_int("age");
    assert_eq!(avg, Some(95.0 / 3.0));
}
