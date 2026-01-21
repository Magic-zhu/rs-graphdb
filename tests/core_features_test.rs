// 核心功能测试：事务、更新 API、WHERE 增强

use rs_graphdb::graph::db::GraphDatabase;
use rs_graphdb::storage::mem_store::MemStore;
use rs_graphdb::values::{Properties, Value};
use rs_graphdb::cypher::{parse_cypher, executor};
use rs_graphdb::storage::TxHandle;

// ========== 事务支持测试 ==========

#[test]
fn test_transaction_begin_commit() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 开始事务
    let tx_handle = db.begin_tx();
    assert!(tx_handle.is_ok(), "Should be able to begin transaction");
    let tx = tx_handle.unwrap();

    // 在事务外创建一个节点（验证事务隔离）
    let node_id_1 = db.create_node(vec!["User"], {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Alice".to_string()));
        props
    });

    // 提交事务（空事务）
    let result = db.commit_tx(tx);
    assert!(result.is_ok(), "Should be able to commit empty transaction");

    // 验证节点仍然存在
    let node = db.get_node(node_id_1);
    assert!(node.is_some(), "Node should still exist after tx commit");
}

#[test]
fn test_transaction_rollback() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个节点
    let node_id = db.create_node(vec!["User"], {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Bob".to_string()));
        props
    });

    // 开始事务
    let tx_handle = db.begin_tx().unwrap();

    // 回滚事务
    let result = db.rollback_tx(tx_handle);
    assert!(result.is_ok(), "Should be able to rollback transaction");

    // 验证节点仍然存在（事务没有做任何修改）
    let node = db.get_node(node_id);
    assert!(node.is_some(), "Node should still exist after rollback");
}

#[test]
fn test_transaction_double_commit_fails() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 开始事务
    let tx_handle = db.begin_tx().unwrap();

    // 第一次提交
    let result1 = db.commit_tx(tx_handle);
    assert!(result1.is_ok(), "First commit should succeed");

    // 第二次提交应该失败
    let result2 = db.commit_tx(tx_handle);
    assert!(result2.is_err(), "Second commit should fail");
}

#[test]
fn test_transaction_invalid_handle_fails() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 使用无效的事务句柄
    let invalid_tx = TxHandle(999);

    let result = db.commit_tx(invalid_tx);
    assert!(result.is_err(), "Commit with invalid handle should fail");

    let result = db.rollback_tx(invalid_tx);
    assert!(result.is_err(), "Rollback with invalid handle should fail");
}

// ========== 存储层更新 API 测试 ==========

#[test]
fn test_update_node_props() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建节点
    let node_id = db.create_node(vec!["User"], {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Charlie".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        props
    });

    // 更新节点属性
    let mut new_props = Properties::new();
    new_props.insert("age".to_string(), Value::Int(31));
    new_props.insert("city".to_string(), Value::Text("NYC".to_string()));

    let result = db.update_node_props(node_id, new_props);
    assert!(result, "Should be able to update node properties");

    // 验证更新
    let node = db.get_node(node_id).unwrap();
    assert_eq!(node.props.get("age"), Some(&Value::Int(31)), "Age should be updated");
    assert_eq!(node.props.get("city"), Some(&Value::Text("NYC".to_string())), "City should be added");
    assert_eq!(node.props.get("name"), Some(&Value::Text("Charlie".to_string())), "Name should be preserved");
}

#[test]
fn test_update_node_props_nonexistent() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 尝试更新不存在的节点
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text("Test".to_string()));

    let result = db.update_node_props(999, props);
    assert!(!result, "Should fail to update nonexistent node");
}

#[test]
fn test_update_rel_props() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建两个节点和关系
    let node1 = db.create_node(vec!["User"], Properties::new());
    let node2 = db.create_node(vec!["User"], Properties::new());

    let rel_id = db.create_rel(node1, node2, "FRIEND", {
        let mut props = Properties::new();
        props.insert("since".to_string(), Value::Text("2020".to_string()));
        props
    });

    // 更新关系属性
    let mut new_props = Properties::new();
    new_props.insert("since".to_string(), Value::Text("2021".to_string()));
    new_props.insert("strength".to_string(), Value::Text("strong".to_string()));

    let result = db.update_rel_props(rel_id, new_props);
    assert!(result, "Should be able to update relationship properties");

    // 验证更新
    let rel = db.get_rel(rel_id).unwrap();
    assert_eq!(rel.props.get("since"), Some(&Value::Text("2021".to_string())), "Since should be updated");
    assert_eq!(rel.props.get("strength"), Some(&Value::Text("strong".to_string())), "Strength should be added");
}

#[test]
fn test_update_rel_props_nonexistent() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 尝试更新的不存在的关系
    let mut props = Properties::new();
    props.insert("test".to_string(), Value::Text("Test".to_string()));

    let result = db.update_rel_props(999, props);
    assert!(!result, "Should fail to update nonexistent relationship");
}

// ========== WHERE 子句增强测试 ==========

#[test]
fn test_where_and_condition() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建测试数据
    db.create_node(vec!["User"], {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        props
    });

    db.create_node(vec!["User"], {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Bob".to_string()));
        props.insert("age".to_string(), Value::Int(25));
        props
    });

    db.create_node(vec!["User"], {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Charlie".to_string()));
        props.insert("age".to_string(), Value::Int(35));
        props
    });

    // 测试 AND 条件
    let query_str = "MATCH (n:User) WHERE n.age > 25 AND n.age < 35 RETURN n";
    let stmt = parse_cypher(query_str).unwrap();

    if let executor::CypherResult::Nodes(nodes) = executor::execute_statement(&mut db, &stmt).unwrap() {
        assert_eq!(nodes.len(), 1, "Should find one node matching both conditions");
        assert_eq!(nodes[0].props.get("name"), Some(&Value::Text("Alice".to_string())));
    } else {
        panic!("Should return nodes");
    }
}

#[test]
fn test_where_or_condition() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建测试数据
    db.create_node(vec!["User"], {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(20));
        props
    });

    db.create_node(vec!["User"], {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Bob".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        props
    });

    db.create_node(vec!["User"], {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Charlie".to_string()));
        props.insert("age".to_string(), Value::Int(25));
        props
    });

    // 测试 OR 条件
    let query_str = "MATCH (n:User) WHERE n.age = 20 OR n.age = 30 RETURN n";
    let stmt = parse_cypher(query_str).unwrap();

    if let executor::CypherResult::Nodes(nodes) = executor::execute_statement(&mut db, &stmt).unwrap() {
        assert_eq!(nodes.len(), 2, "Should find two nodes matching OR condition");
        let names: Vec<_> = nodes.iter()
            .filter_map(|n| n.props.get("name"))
            .collect();
        assert!(names.contains(&&Value::Text("Alice".to_string())));
        assert!(names.contains(&&Value::Text("Bob".to_string())));
    } else {
        panic!("Should return nodes");
    }
}

#[test]
fn test_where_complex_and_or() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建测试数据
    db.create_node(vec!["User"], {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(20));
        props.insert("city".to_string(), Value::Text("NYC".to_string()));
        props
    });

    db.create_node(vec!["User"], {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Bob".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        props.insert("city".to_string(), Value::Text("NYC".to_string()));
        props
    });

    db.create_node(vec!["User"], {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Charlie".to_string()));
        props.insert("age".to_string(), Value::Int(25));
        props.insert("city".to_string(), Value::Text("LA".to_string()));
        props
    });

    // 测试复杂 AND/OR 组合
    let query_str = "MATCH (n:User) WHERE n.city = \"NYC\" AND (n.age = 20 OR n.age = 30) RETURN n";
    let stmt = parse_cypher(query_str).unwrap();

    if let executor::CypherResult::Nodes(nodes) = executor::execute_statement(&mut db, &stmt).unwrap() {
        assert_eq!(nodes.len(), 2, "Should find two nodes matching complex condition");
        let names: Vec<_> = nodes.iter()
            .filter_map(|n| n.props.get("name"))
            .collect();
        assert!(names.contains(&&Value::Text("Alice".to_string())));
        assert!(names.contains(&&Value::Text("Bob".to_string())));
    } else {
        panic!("Should return nodes");
    }
}

#[test]
fn test_where_gte_lte() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建测试数据
    db.create_node(vec!["User"], {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Alice".to_string()));
        props.insert("score".to_string(), Value::Int(85));
        props
    });

    db.create_node(vec!["User"], {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Bob".to_string()));
        props.insert("score".to_string(), Value::Int(90));
        props
    });

    db.create_node(vec!["User"], {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Charlie".to_string()));
        props.insert("score".to_string(), Value::Int(95));
        props
    });

    // 测试 >= 和 <=
    let query_str = "MATCH (n:User) WHERE n.score >= 85 AND n.score <= 90 RETURN n";
    let stmt = parse_cypher(query_str).unwrap();

    if let executor::CypherResult::Nodes(nodes) = executor::execute_statement(&mut db, &stmt).unwrap() {
        assert_eq!(nodes.len(), 2, "Should find two nodes in range");
        let names: Vec<_> = nodes.iter()
            .filter_map(|n| n.props.get("name"))
            .collect();
        assert!(names.contains(&&Value::Text("Alice".to_string())));
        assert!(names.contains(&&Value::Text("Bob".to_string())));
    } else {
        panic!("Should return nodes");
    }
}

#[test]
fn test_where_ne() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建测试数据
    db.create_node(vec!["User"], {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Alice".to_string()));
        props.insert("status".to_string(), Value::Text("active".to_string()));
        props
    });

    db.create_node(vec!["User"], {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Bob".to_string()));
        props.insert("status".to_string(), Value::Text("inactive".to_string()));
        props
    });

    // 测试 <>
    let query_str = "MATCH (n:User) WHERE n.status <> \"inactive\" RETURN n";
    let stmt = parse_cypher(query_str).unwrap();

    if let executor::CypherResult::Nodes(nodes) = executor::execute_statement(&mut db, &stmt).unwrap() {
        assert_eq!(nodes.len(), 1, "Should find one node with status not equal to inactive");
        assert_eq!(nodes[0].props.get("name"), Some(&Value::Text("Alice".to_string())));
    } else {
        panic!("Should return nodes");
    }
}

// ========== 综合测试 ==========

#[test]
fn test_update_with_transaction() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建初始节点
    let node_id = db.create_node(vec!["User"], {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Test".to_string()));
        props.insert("counter".to_string(), Value::Int(0));
        props
    });

    // 开始事务
    let tx = db.begin_tx().unwrap();

    // 准备更新属性
    let mut update_props = Properties::new();
    update_props.insert("counter".to_string(), Value::Int(1));

    // 提交事务（当前实现中事务是空的，直接提交）
    db.commit_tx(tx).unwrap();

    // 应用更新
    db.update_node_props(node_id, update_props);

    // 验证更新
    let node = db.get_node(node_id).unwrap();
    assert_eq!(node.props.get("counter"), Some(&Value::Int(1)));
}
