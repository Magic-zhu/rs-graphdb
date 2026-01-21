// 复合索引测试
// 测试多属性索引功能

use rs_graphdb::graph::db::GraphDatabase;
use rs_graphdb::storage::StorageEngine;
use rs_graphdb::values::{Properties, Value};

// 辅助函数：创建带属性的 User 节点
fn create_user(db: &mut GraphDatabase<impl StorageEngine>, name: &str, age: i64, email: &str) {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props.insert("age".to_string(), Value::Int(age));
    props.insert("email".to_string(), Value::Text(email.to_string()));
    db.create_node(vec!["User"], props);
}

#[test]
fn test_create_composite_index() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建复合索引
    db.create_composite_index("user_name_age", "User", &["name", "age"]);

    // 验证索引已创建
    let stats = db.index_stats();
    assert_eq!(stats.1, 0); // 复合索引条目数应该是 0（还没有节点）

    // 创建一些节点
    create_user(&mut db, "Alice", 30, "alice@example.com");
    create_user(&mut db, "Bob", 25, "bob@example.com");
    create_user(&mut db, "Charlie", 30, "charlie@example.com");

    // 验证索引已更新
    let stats = db.index_stats();
    assert!(stats.1 > 0); // 应该有复合索引条目
}

#[test]
fn test_find_by_composite_index() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建复合索引
    db.create_composite_index("user_name_age", "User", &["name", "age"]);

    // 创建测试节点
    create_user(&mut db, "Alice", 30, "alice@example.com");
    create_user(&mut db, "Bob", 25, "bob@example.com");
    create_user(&mut db, "Charlie", 30, "charlie@example.com");
    create_user(&mut db, "David", 35, "david@example.com");

    // 使用复合索引查询 (name="Alice", age=30)
    let ids = db.find_by_composite_index(
        "User",
        &["name", "age"],
        &[Value::Text("Alice".to_string()), Value::Int(30)],
    );

    // 应该找到 Alice
    assert_eq!(ids.len(), 1);
    let node = db.get_node(ids[0]).unwrap();
    assert_eq!(node.props.get("name"), Some(&Value::Text("Alice".to_string())));
    assert_eq!(node.props.get("age"), Some(&Value::Int(30)));
}

#[test]
fn test_find_by_composite_index_multiple_results() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建复合索引
    db.create_composite_index("user_name_age", "User", &["name", "age"]);

    // 创建测试节点 - 多个节点有相同的 name 和 age
    create_user(&mut db, "Alice", 30, "alice1@example.com");
    create_user(&mut db, "Alice", 30, "alice2@example.com");
    create_user(&mut db, "Bob", 25, "bob@example.com");

    // 使用复合索引查询 (name="Alice", age=30)
    let ids = db.find_by_composite_index(
        "User",
        &["name", "age"],
        &[Value::Text("Alice".to_string()), Value::Int(30)],
    );

    // 应该找到两个 Alice
    assert_eq!(ids.len(), 2);

    // 验证两个节点都有相同的 name 和 age
    for id in ids {
        let node = db.get_node(id).unwrap();
        assert_eq!(node.props.get("name"), Some(&Value::Text("Alice".to_string())));
        assert_eq!(node.props.get("age"), Some(&Value::Int(30)));
    }
}

#[test]
fn test_find_by_composite_index_not_found() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建复合索引
    db.create_composite_index("user_name_age", "User", &["name", "age"]);

    // 创建测试节点
    create_user(&mut db, "Alice", 30, "alice@example.com");
    create_user(&mut db, "Bob", 25, "bob@example.com");

    // 查询不存在的组合
    let ids = db.find_by_composite_index(
        "User",
        &["name", "age"],
        &[Value::Text("Charlie".to_string()), Value::Int(35)],
    );

    // 应该找不到
    assert_eq!(ids.len(), 0);
}

#[test]
fn test_three_property_composite_index() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建三属性复合索引
    db.create_composite_index("user_name_age_email", "User", &["name", "age", "email"]);

    // 创建测试节点
    create_user(&mut db, "Alice", 30, "alice@example.com");
    create_user(&mut db, "Alice", 30, "alice2@example.com"); // 同名同年龄，不同邮箱

    // 使用复合索引查询 (name="Alice", age=30, email="alice@example.com")
    let ids = db.find_by_composite_index(
        "User",
        &["name", "age", "email"],
        &[
            Value::Text("Alice".to_string()),
            Value::Int(30),
            Value::Text("alice@example.com".to_string()),
        ],
    );

    // 应该只找到一个
    assert_eq!(ids.len(), 1);
    let node = db.get_node(ids[0]).unwrap();
    assert_eq!(node.props.get("email"), Some(&Value::Text("alice@example.com".to_string())));
}

#[test]
fn test_drop_composite_index() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建复合索引
    db.create_composite_index("user_name_age", "User", &["name", "age"]);

    // 创建节点
    create_user(&mut db, "Alice", 30, "alice@example.com");

    // 删除索引
    let dropped = db.drop_composite_index("user_name_age");
    assert!(dropped);

    // 尝试删除不存在的索引
    let dropped_again = db.drop_composite_index("user_name_age");
    assert!(!dropped_again);
}

#[test]
fn test_composite_index_with_new_nodes() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建复合索引
    db.create_composite_index("user_name_age", "User", &["name", "age"]);

    // 在创建索引之前先创建一个节点
    create_user(&mut db, "Alice", 30, "alice@example.com");

    // 创建索引后添加新节点
    create_user(&mut db, "Bob", 25, "bob@example.com");
    create_user(&mut db, "Charlie", 30, "charlie@example.com");

    // 查询应该能找到新创建的节点
    let ids = db.find_by_composite_index(
        "User",
        &["name", "age"],
        &[Value::Text("Bob".to_string()), Value::Int(25)],
    );

    assert_eq!(ids.len(), 1);
    let node = db.get_node(ids[0]).unwrap();
    assert_eq!(node.props.get("name"), Some(&Value::Text("Bob".to_string())));
}

#[test]
fn test_composite_index_partial_properties() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建复合索引
    db.create_composite_index("user_name_age", "User", &["name", "age"]);

    // 创建节点 - 有些节点缺少 age 属性
    let mut props1 = Properties::new();
    props1.insert("name".to_string(), Value::Text("Alice".to_string()));
    props1.insert("age".to_string(), Value::Int(30));
    db.create_node(vec!["User"], props1);

    let mut props2 = Properties::new();
    props2.insert("name".to_string(), Value::Text("Bob".to_string()));
    // Bob 没有 age 属性
    db.create_node(vec!["User"], props2);

    // 查询应该只找到 Alice（Bob 没有 age 属性，不会被索引）
    let ids = db.find_by_composite_index(
        "User",
        &["name", "age"],
        &[Value::Text("Alice".to_string()), Value::Int(30)],
    );

    assert_eq!(ids.len(), 1);
    let node = db.get_node(ids[0]).unwrap();
    assert_eq!(node.props.get("name"), Some(&Value::Text("Alice".to_string())));
}

#[test]
fn test_multiple_composite_indexes() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建多个复合索引
    db.create_composite_index("user_name_age", "User", &["name", "age"]);
    db.create_composite_index("user_name_email", "User", &["name", "email"]);
    db.create_composite_index("user_age_email", "User", &["age", "email"]);

    // 创建测试节点
    create_user(&mut db, "Alice", 30, "alice@example.com");

    // 使用不同的复合索引查询
    let ids1 = db.find_by_composite_index(
        "User",
        &["name", "age"],
        &[Value::Text("Alice".to_string()), Value::Int(30)],
    );
    assert_eq!(ids1.len(), 1);

    let ids2 = db.find_by_composite_index(
        "User",
        &["name", "email"],
        &[Value::Text("Alice".to_string()), Value::Text("alice@example.com".to_string())],
    );
    assert_eq!(ids2.len(), 1);

    let ids3 = db.find_by_composite_index(
        "User",
        &["age", "email"],
        &[Value::Int(30), Value::Text("alice@example.com".to_string())],
    );
    assert_eq!(ids3.len(), 1);
}

#[test]
fn test_composite_index_performance() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建复合索引
    db.create_composite_index("user_name_age", "User", &["name", "age"]);

    // 创建大量节点
    for i in 0..100 {
        create_user(
            &mut db,
            &format!("User{}", i % 10), // 10 个不同的名字
            20 + (i % 50),              // 50 个不同的年龄
            &format!("user{}@example.com", i),
        );
    }

    // 查询特定组合
    let start = std::time::Instant::now();
    let ids = db.find_by_composite_index(
        "User",
        &["name", "age"],
        &[Value::Text("User5".to_string()), Value::Int(45)],
    );
    let duration = start.elapsed();

    // 应该快速找到结果
    assert!(duration.as_millis() < 100, "查询应该在 100ms 内完成，实际耗时: {:?}", duration);

    // 应该找到 2 个节点（User5, 45 会出现 2 次）
    assert_eq!(ids.len(), 2);
}

#[test]
fn test_composite_index_empty_result() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建复合索引但不创建任何节点
    db.create_composite_index("user_name_age", "User", &["name", "age"]);

    // 查询应该返回空结果
    let ids = db.find_by_composite_index(
        "User",
        &["name", "age"],
        &[Value::Text("Alice".to_string()), Value::Int(30)],
    );

    assert_eq!(ids.len(), 0);
}

#[test]
fn test_index_stats() {
    let mut db = GraphDatabase::new_in_memory();

    // 初始状态 - 没有单属性索引条目（因为还没有节点）
    let stats = db.index_stats();
    assert_eq!(stats.0, 0); // 还没有单属性索引条目
    assert_eq!(stats.1, 0); // 还没有复合索引

    // 创建复合索引
    db.create_composite_index("user_name_age", "User", &["name", "age"]);

    // 创建节点
    create_user(&mut db, "Alice", 30, "alice@example.com");
    create_user(&mut db, "Bob", 25, "bob@example.com");

    // 检查索引统计
    let stats = db.index_stats();
    assert!(stats.0 > 0);  // 应该有单属性索引条目
    assert!(stats.1 > 0);  // 应该有复合索引条目
}
