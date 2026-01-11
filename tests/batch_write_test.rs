use rs_graphdb::{GraphDatabase};
use rs_graphdb::storage::sled_store::SledStore;
use rs_graphdb::values::{Properties, Value};

fn make_props(name: &str) -> Properties {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props
}

#[test]
fn test_batch_create_nodes_memory() {
    let mut db = GraphDatabase::new_in_memory();

    // 准备批量节点数据
    let nodes = vec![
        (vec!["User".to_string()], make_props("Alice")),
        (vec!["User".to_string()], make_props("Bob")),
        (vec!["User".to_string()], make_props("Charlie")),
        (vec!["Admin".to_string()], make_props("Admin")),
    ];

    // 批量创建节点
    let ids = db.batch_create_nodes(nodes);

    // 验证创建了4个节点
    assert_eq!(ids.len(), 4);

    // 验证ID是连续的
    assert_eq!(ids[0] + 1, ids[1]);
    assert_eq!(ids[1] + 1, ids[2]);
    assert_eq!(ids[2] + 1, ids[3]);

    // 验证每个节点的属性
    let alice = db.get_node(ids[0]).expect("Alice should exist");
    assert!(alice.has_label("User"));
    assert_eq!(alice.get("name"), Some(&Value::Text("Alice".to_string())));

    let bob = db.get_node(ids[1]).expect("Bob should exist");
    assert_eq!(bob.get("name"), Some(&Value::Text("Bob".to_string())));

    let admin = db.get_node(ids[3]).expect("Admin should exist");
    assert!(admin.has_label("Admin"));
    assert_eq!(admin.get("name"), Some(&Value::Text("Admin".to_string())));
}

#[test]
fn test_batch_create_rels_memory() {
    let mut db = GraphDatabase::new_in_memory();

    // 先创建一些节点
    let alice = db.create_node(vec!["User"], make_props("Alice"));
    let bob = db.create_node(vec!["User"], make_props("Bob"));
    let charlie = db.create_node(vec!["User"], make_props("Charlie"));

    // 准备批量关系数据
    let rels = vec![
        (alice, bob, "FRIEND".to_string(), Properties::new()),
        (bob, charlie, "FRIEND".to_string(), Properties::new()),
        (alice, charlie, "FOLLOWS".to_string(), Properties::new()),
    ];

    // 批量创建关系
    let rel_ids = db.batch_create_rels(rels);

    // 验证创建了3个关系
    assert_eq!(rel_ids.len(), 3);

    // 验证ID是连续的
    assert_eq!(rel_ids[0] + 1, rel_ids[1]);
    assert_eq!(rel_ids[1] + 1, rel_ids[2]);

    // 验证关系是否正确创建
    let alice_out: Vec<_> = db.neighbors_out(alice).collect();
    assert_eq!(alice_out.len(), 2);

    let bob_out: Vec<_> = db.neighbors_out(bob).collect();
    assert_eq!(bob_out.len(), 1);
    assert_eq!(bob_out[0].end, charlie);
}

#[test]
fn test_batch_vs_single_create_consistency() {
    let mut db1 = GraphDatabase::new_in_memory();
    let mut db2 = GraphDatabase::new_in_memory();

    // 使用单个创建
    let id1 = db1.create_node(vec!["User"], make_props("Test1"));
    let id2 = db1.create_node(vec!["User"], make_props("Test2"));

    // 使用批量创建
    let ids = db2.batch_create_nodes(vec![
        (vec!["User".to_string()], make_props("Test1")),
        (vec!["User".to_string()], make_props("Test2")),
    ]);

    // 验证结果一致
    assert_eq!(ids.len(), 2);
    assert_eq!(ids[0], id1);
    assert_eq!(ids[1], id2);

    // 验证数据一致性
    let node1_db1 = db1.get_node(id1).unwrap();
    let node1_db2 = db2.get_node(ids[0]).unwrap();
    assert_eq!(node1_db1.props, node1_db2.props);
    assert_eq!(node1_db1.labels, node1_db2.labels);
}

#[test]
fn test_batch_large_dataset() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建大量节点
    let count = 1000;
    let nodes: Vec<_> = (0..count)
        .map(|i| (vec!["Node".to_string()], {
            let mut props = Properties::new();
            props.insert("id".to_string(), Value::Int(i as i64));
            props.insert("name".to_string(), Value::Text(format!("Node_{}", i)));
            props
        }))
        .collect();

    let start = std::time::Instant::now();
    let ids = db.batch_create_nodes(nodes);
    let duration = start.elapsed();

    println!("批量创建 {} 个节点耗时: {:?}", count, duration);

    assert_eq!(ids.len(), count);

    // 验证部分数据
    for i in 0..10 {
        let node = db.get_node(ids[i]).expect("Node should exist");
        assert_eq!(node.get("id"), Some(&Value::Int(i as i64)));
        assert_eq!(node.get("name"), Some(&Value::Text(format!("Node_{}", i))));
    }
}

#[test]
fn test_batch_empty_dataset() {
    let mut db = GraphDatabase::new_in_memory();

    // 空批量操作
    let ids = db.batch_create_nodes(vec![]);
    assert_eq!(ids.len(), 0);

    let rel_ids = db.batch_create_rels(vec![]);
    assert_eq!(rel_ids.len(), 0);
}

#[test]
fn test_batch_sled_persistence() {
    let dir = std::env::temp_dir().join("batch_test_db");
    let _ = std::fs::remove_dir_all(&dir);

    {
        // 创建数据库并写入数据
        let engine = SledStore::new(&dir).unwrap();
        let mut db = GraphDatabase::from_engine(engine);

        let nodes = vec![
            (vec!["User".to_string()], make_props("Alice")),
            (vec!["User".to_string()], make_props("Bob")),
            (vec!["Admin".to_string()], make_props("Admin")),
        ];

        let ids = db.batch_create_nodes(nodes);
        assert_eq!(ids.len(), 3);
    }

    {
        // 重新打开数据库验证持久化
        let engine = SledStore::new(&dir).unwrap();
        let db = GraphDatabase::from_engine(engine);

        // 验证节点是否存在
        let all_nodes: Vec<_> = db.all_stored_nodes().collect();
        assert_eq!(all_nodes.len(), 3);

        // 验证数据内容
        let alice = all_nodes.iter().find(|n| {
            n.props.get("name") == Some(&Value::Text("Alice".to_string()))
        });
        assert!(alice.is_some());
    }

    let _ = std::fs::remove_dir_all(&dir);
}
