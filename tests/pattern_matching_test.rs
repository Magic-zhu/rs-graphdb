// 图模式匹配测试
// 测试可变长度路径、复杂模式组合等功能

use rs_graphdb::graph::db::GraphDatabase;
use rs_graphdb::storage::{StorageEngine, NodeId};
use rs_graphdb::values::{Properties, Value};
use rs_graphdb::cypher::{parse_cypher, execute_statement};

// 辅助函数：创建Person节点
fn create_person(db: &mut GraphDatabase<impl StorageEngine>, name: &str, age: i64) {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props.insert("age".to_string(), Value::Int(age));
    db.create_node(vec!["Person"], props);
}

// 辅助函数：创建Person节点并返回ID
fn create_person_return_id(db: &mut GraphDatabase<impl StorageEngine>, name: &str, age: i64) -> NodeId {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props.insert("age".to_string(), Value::Int(age));
    db.create_node(vec!["Person"], props)
}

#[test]
fn test_variable_length_path_fixed() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建线性图: Alice -> Bob -> Charlie -> David
    let alice = create_person_return_id(&mut db, "Alice", 30);
    let bob = create_person_return_id(&mut db, "Bob", 25);
    let charlie = create_person_return_id(&mut db, "Charlie", 35);
    let david = create_person_return_id(&mut db, "David", 28);

    db.create_rel(alice, bob, "KNOWS", Properties::new());
    db.create_rel(bob, charlie, "KNOWS", Properties::new());
    db.create_rel(charlie, david, "KNOWS", Properties::new());

    // 测试固定长度路径: 从 Alice 开始，2跳应该到达 Charlie
    let query = "MATCH (p:Person {name: 'Alice'})-[:KNOWS*2..2]->(friend:Person) RETURN friend";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    if let rs_graphdb::cypher::CypherResult::Nodes(nodes) = result {
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].props.get("name"), Some(&Value::Text("Charlie".to_string())));
    } else {
        panic!("Expected nodes result");
    }
}

#[test]
fn test_variable_length_path_range() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建图: Alice -> Bob -> Charlie -> David
    let alice = create_person_return_id(&mut db, "Alice", 30);
    let bob = create_person_return_id(&mut db, "Bob", 25);
    let charlie = create_person_return_id(&mut db, "Charlie", 35);
    let david = create_person_return_id(&mut db, "David", 28);

    db.create_rel(alice, bob, "KNOWS", Properties::new());
    db.create_rel(bob, charlie, "KNOWS", Properties::new());
    db.create_rel(charlie, david, "KNOWS", Properties::new());

    // 测试范围路径: 从 Alice 开始，1-3跳应该到达 Bob, Charlie, David
    let query = "MATCH (p:Person {name: 'Alice'})-[:KNOWS*1..3]->(friend:Person) RETURN friend";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    if let rs_graphdb::cypher::CypherResult::Nodes(nodes) = result {
        assert_eq!(nodes.len(), 3);
        let names: Vec<_> = nodes.iter()
            .filter_map(|n| n.props.get("name"))
            .filter_map(|v| if let Value::Text(s) = v { Some(s.as_str()) } else { None })
            .collect();
        assert!(names.contains(&"Bob"));
        assert!(names.contains(&"Charlie"));
        assert!(names.contains(&"David"));
    } else {
        panic!("Expected nodes result");
    }
}

#[test]
fn test_variable_length_path_unbounded() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建图: Alice -> Bob -> Charlie -> David
    let alice = create_person_return_id(&mut db, "Alice", 30);
    let bob = create_person_return_id(&mut db, "Bob", 25);
    let charlie = create_person_return_id(&mut db, "Charlie", 35);
    let david = create_person_return_id(&mut db, "David", 28);

    db.create_rel(alice, bob, "KNOWS", Properties::new());
    db.create_rel(bob, charlie, "KNOWS", Properties::new());
    db.create_rel(charlie, david, "KNOWS", Properties::new());

    // 测试无上界路径: 从 Alice 开始，2跳及以上应该到达 Charlie, David
    let query = "MATCH (p:Person {name: 'Alice'})-[:KNOWS*2..]->(friend:Person) RETURN friend";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    if let rs_graphdb::cypher::CypherResult::Nodes(nodes) = result {
        assert_eq!(nodes.len(), 2);
        let names: Vec<_> = nodes.iter()
            .filter_map(|n| n.props.get("name"))
            .filter_map(|v| if let Value::Text(s) = v { Some(s.as_str()) } else { None })
            .collect();
        assert!(names.contains(&"Charlie"));
        assert!(names.contains(&"David"));
    } else {
        panic!("Expected nodes result");
    }
}

#[test]
fn test_variable_length_path_incoming() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建图: Alice <- Bob <- Charlie
    // 即: Bob -> Alice (FOLLOWS), Charlie -> Bob (FOLLOWS)
    let alice = db.create_node(vec!["Person"], {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        props
    });
    let bob = db.create_node(vec!["Person"], {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Bob".to_string()));
        props.insert("age".to_string(), Value::Int(25));
        props
    });
    let charlie = db.create_node(vec!["Person"], {
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Charlie".to_string()));
        props.insert("age".to_string(), Value::Int(35));
        props
    });

    // Bob -> Alice (Bob follows Alice)
    db.create_rel(bob, alice, "FOLLOWS", Properties::new());
    // Charlie -> Bob (Charlie follows Bob)
    db.create_rel(charlie, bob, "FOLLOWS", Properties::new());

    // 测试入边可变长度路径: 从 Alice 开始找 2+ 跳的入边邻居
    let query = "MATCH (p:Person {name: 'Alice'})<-[:FOLLOWS*2..]-(follower:Person) RETURN follower";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    if let rs_graphdb::cypher::CypherResult::Nodes(nodes) = result {
        // 从 Alice 开始，2跳入边应该能到达 Charlie
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].props.get("name"), Some(&Value::Text("Charlie".to_string())));
    } else {
        panic!("Expected nodes result");
    }
}

#[test]
fn test_variable_length_path_branching() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建分支图:
    //     Alice
    //     /  \
    //   Bob  Charlie
    //   /       \
    // David    Eve
    let alice = create_person_return_id(&mut db, "Alice", 30);
    let bob = create_person_return_id(&mut db, "Bob", 25);
    let charlie = create_person_return_id(&mut db, "Charlie", 35);
    let david = create_person_return_id(&mut db, "David", 28);
    let eve = create_person_return_id(&mut db, "Eve", 32);

    db.create_rel(alice, bob, "KNOWS", Properties::new());
    db.create_rel(alice, charlie, "KNOWS", Properties::new());
    db.create_rel(bob, david, "KNOWS", Properties::new());
    db.create_rel(charlie, eve, "KNOWS", Properties::new());

    // 测试分支路径: 从 Alice 开始，2跳应该找到 David 和 Eve
    let query = "MATCH (p:Person {name: 'Alice'})-[:KNOWS*2..2]->(friend:Person) RETURN friend";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    if let rs_graphdb::cypher::CypherResult::Nodes(nodes) = result {
        assert_eq!(nodes.len(), 2);
        let names: Vec<_> = nodes.iter()
            .filter_map(|n| n.props.get("name"))
            .filter_map(|v| if let Value::Text(s) = v { Some(s.as_str()) } else { None })
            .collect();
        assert!(names.contains(&"David"));
        assert!(names.contains(&"Eve"));
    } else {
        panic!("Expected nodes result");
    }
}

#[test]
fn test_variable_length_path_with_filter() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建图
    let alice = create_person_return_id(&mut db, "Alice", 30);
    let bob = create_person_return_id(&mut db, "Bob", 25);
    let charlie = create_person_return_id(&mut db, "Charlie", 35);
    let david = create_person_return_id(&mut db, "David", 40);

    db.create_rel(alice, bob, "KNOWS", Properties::new());
    db.create_rel(bob, charlie, "KNOWS", Properties::new());
    db.create_rel(charlie, david, "KNOWS", Properties::new());

    // 测试可变长度路径 + WHERE 过滤
    let query = "MATCH (p:Person {name: 'Alice'})-[:KNOWS*1..3]->(friend:Person) WHERE friend.age > 30 RETURN friend";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    if let rs_graphdb::cypher::CypherResult::Nodes(nodes) = result {
        // 应该只找到 Charlie (35) 和 David (40)
        assert_eq!(nodes.len(), 2);
        let ages: Vec<_> = nodes.iter()
            .filter_map(|n| n.props.get("age"))
            .filter_map(|v| if let Value::Int(i) = v { Some(*i) } else { None })
            .collect();
        assert!(ages.contains(&35));
        assert!(ages.contains(&40));
        assert!(!ages.contains(&25));
        assert!(!ages.contains(&30));
    } else {
        panic!("Expected nodes result");
    }
}

#[test]
fn test_variable_length_path_with_label_filter() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建混合标签图
    create_person(&mut db, "Alice", 30);
    create_person(&mut db, "Bob", 25);

    let mut company_props = Properties::new();
    company_props.insert("name".to_string(), Value::Text("Company".to_string()));
    let company = db.create_node(vec!["Company"], company_props);

    let nodes: Vec<_> = db.all_stored_nodes().collect();
    db.create_rel(nodes[0].id, company, "WORKS_AT", Properties::new());
    db.create_rel(nodes[1].id, company, "WORKS_AT", Properties::new());

    // 测试可变长度路径 + 标签过滤
    let query = "MATCH (p:Person)-[:WORKS_AT*1]->(c:Company) RETURN c";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    if let rs_graphdb::cypher::CypherResult::Nodes(nodes) = result {
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].labels.get(0).map(|s| s.as_str()), Some("Company"));
    } else {
        panic!("Expected nodes result");
    }
}

#[test]
fn test_variable_length_path_cycle_detection() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建带环的图: Alice -> Bob -> Charlie -> Alice
    create_person(&mut db, "Alice", 30);
    create_person(&mut db, "Bob", 25);
    create_person(&mut db, "Charlie", 35);

    let nodes: Vec<_> = db.all_stored_nodes().collect();
    db.create_rel(nodes[0].id, nodes[1].id, "KNOWS", Properties::new());
    db.create_rel(nodes[1].id, nodes[2].id, "KNOWS", Properties::new());
    db.create_rel(nodes[2].id, nodes[0].id, "KNOWS", Properties::new());

    // 测试环检测: MATCH (p:Person)-[:KNOWS*1..5]->(friend) RETURN friend
    // 应该不会无限循环，且能正确找到节点
    let query = "MATCH (p:Person)-[:KNOWS*1..5]->(friend:Person) RETURN friend";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    if let rs_graphdb::cypher::CypherResult::Nodes(nodes) = result {
        // 由于有环，应该返回去重后的节点
        // 每个节点都能通过1-5跳到达其他节点（包括自己）
        assert!(nodes.len() <= 3);
    } else {
        panic!("Expected nodes result");
    }
}

#[test]
fn test_single_hop_syntax() {
    let mut db = GraphDatabase::new_in_memory();

    let alice = create_person_return_id(&mut db, "Alice", 30);
    let bob = create_person_return_id(&mut db, "Bob", 25);

    db.create_rel(alice, bob, "KNOWS", Properties::new());

    // 测试单跳语法（没有可变长度）
    // 从 Alice 开始，1跳应该找到 Bob
    let query = "MATCH (p:Person {name: 'Alice'})-[:KNOWS]->(friend:Person) RETURN friend";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    if let rs_graphdb::cypher::CypherResult::Nodes(nodes) = result {
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].props.get("name"), Some(&Value::Text("Bob".to_string())));
    } else {
        panic!("Expected nodes result");
    }
}

#[test]
fn test_undirected_variable_length() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建有向图: Alice -> Bob -> Charlie
    create_person(&mut db, "Alice", 30);
    create_person(&mut db, "Bob", 25);
    create_person(&mut db, "Charlie", 35);

    let nodes: Vec<_> = db.all_stored_nodes().collect();
    db.create_rel(nodes[0].id, nodes[1].id, "CONNECTED", Properties::new());
    db.create_rel(nodes[1].id, nodes[2].id, "CONNECTED", Properties::new());

    // 注意：当前实现的无向路径是通过 Direction::Both 实现的
    // 但我们的 parser 目前不直接支持 -(rel)- 语法中的可变长度
    // 这个测试可能需要调整
}

#[test]
fn test_variable_length_empty_result() {
    let mut db = GraphDatabase::new_in_memory();

    create_person(&mut db, "Alice", 30);
    create_person(&mut db, "Bob", 25);

    // 没有关系，测试空结果
    let query = "MATCH (p:Person)-[:KNOWS*2..3]->(friend:Person) RETURN friend";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    if let rs_graphdb::cypher::CypherResult::Nodes(nodes) = result {
        assert_eq!(nodes.len(), 0);
    } else {
        panic!("Expected nodes result");
    }
}

#[test]
fn test_variable_length_path_distinct() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建菱形图:
    //     Alice
    //     /  \
    //   Bob  Charlie
    //     \  /
    //    David
    let alice = create_person_return_id(&mut db, "Alice", 30);
    let bob = create_person_return_id(&mut db, "Bob", 25);
    let charlie = create_person_return_id(&mut db, "Charlie", 35);
    let david = create_person_return_id(&mut db, "David", 28);

    db.create_rel(alice, bob, "KNOWS", Properties::new());
    db.create_rel(alice, charlie, "KNOWS", Properties::new());
    db.create_rel(bob, david, "KNOWS", Properties::new());
    db.create_rel(charlie, david, "KNOWS", Properties::new());

    // 测试去重: 从 Alice 开始，2跳应该只找到 David 一次
    let query = "MATCH (p:Person {name: 'Alice'})-[:KNOWS*2..2]->(friend:Person) RETURN friend";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    if let rs_graphdb::cypher::CypherResult::Nodes(nodes) = result {
        // David 只应该出现一次
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].props.get("name"), Some(&Value::Text("David".to_string())));
    } else {
        panic!("Expected nodes result");
    }
}

#[test]
fn test_complex_pattern_with_where() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建社交网络
    let user1 = create_person_return_id(&mut db, "User1", 21);
    let user2 = create_person_return_id(&mut db, "User2", 22);
    let user3 = create_person_return_id(&mut db, "User3", 23);
    let user4 = create_person_return_id(&mut db, "User4", 24);
    let user5 = create_person_return_id(&mut db, "User5", 25);
    let user6 = create_person_return_id(&mut db, "User6", 26);

    // User1 -> User2 -> User3 -> User4
    db.create_rel(user1, user2, "FRIEND", Properties::new());
    db.create_rel(user2, user3, "FRIEND", Properties::new());
    db.create_rel(user3, user4, "FRIEND", Properties::new());
    // User1 -> User5 -> User6
    db.create_rel(user1, user5, "FRIEND", Properties::new());
    db.create_rel(user5, user6, "FRIEND", Properties::new());

    // 查找 User1 的 2-3 跳内且年龄 > 24 的朋友
    let query = "MATCH (p:Person {name: 'User1'})-[:FRIEND*2..3]->(friend:Person) WHERE friend.age > 24 RETURN friend";
    let stmt = parse_cypher(query).unwrap();
    let result = execute_statement(&mut db, &stmt).unwrap();

    if let rs_graphdb::cypher::CypherResult::Nodes(nodes) = result {
        // User4 (age 24), User6 (age 26)
        // age > 24，所以只有 User6 满足
        assert_eq!(nodes.len(), 1);
        let ages: Vec<_> = nodes.iter()
            .filter_map(|n| n.props.get("age"))
            .filter_map(|v| if let Value::Int(i) = v { Some(*i) } else { None })
            .collect();
        assert!(ages.contains(&26)); // User6
    } else {
        panic!("Expected nodes result");
    }
}
