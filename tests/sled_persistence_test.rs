use rust_graphdb::{GraphDatabase, values::{Properties, Value}};
use rust_graphdb::storage::sled_store::SledStore;
use std::fs;

fn make_user(name: &str) -> Properties {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props
}

#[test]
fn test_sled_persistence() {
    let db_path = "./test_db_sled";

    // 清理之前的测试数据
    let _ = fs::remove_dir_all(db_path);

    // 第一次：创建数据
    {
        let store = SledStore::new(db_path).unwrap();
        let mut db = GraphDatabase::from_engine(store);

        let alice = db.create_node(vec!["User"], make_user("Alice"));
        let bob = db.create_node(vec!["User"], make_user("Bob"));

        db.create_rel(alice, bob, "FRIEND", Properties::new());

        // 强制刷新到磁盘
        db.flush().unwrap();
    }

    // 第二次：重新打开，验证数据持久化
    {
        let store = SledStore::new(db_path).unwrap();
        let db = GraphDatabase::from_engine(store);

        let alice_node = db.get_node(0).expect("Alice should exist");
        assert_eq!(
            alice_node.props.get("name"),
            Some(&Value::Text("Alice".to_string()))
        );

        let bob_node = db.get_node(1).expect("Bob should exist");
        assert_eq!(
            bob_node.props.get("name"),
            Some(&Value::Text("Bob".to_string()))
        );

        let rels: Vec<_> = db.neighbors_out(0).collect();
        assert_eq!(rels.len(), 1);
        assert_eq!(rels[0].end, 1);
    }

    // 清理测试数据
    let _ = fs::remove_dir_all(db_path);
}
