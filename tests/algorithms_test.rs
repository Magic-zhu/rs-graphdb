use rust_graphdb::{GraphDatabase, algorithms};
use rust_graphdb::values::{Properties, Value};

fn make_user(name: &str) -> Properties {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props
}

#[test]
fn test_shortest_path_bfs() {
    let mut db = GraphDatabase::new_in_memory();

    let a = db.create_node(vec!["User"], make_user("A"));
    let b = db.create_node(vec!["User"], make_user("B"));
    let c = db.create_node(vec!["User"], make_user("C"));
    let d = db.create_node(vec!["User"], make_user("D"));

    // A -> B -> D
    // A -> C -> D
    db.create_rel(a, b, "KNOWS", Properties::new());
    db.create_rel(b, d, "KNOWS", Properties::new());
    db.create_rel(a, c, "KNOWS", Properties::new());
    db.create_rel(c, d, "KNOWS", Properties::new());

    let path = algorithms::bfs_shortest_path(&db, a, d);
    assert!(path.is_some());

    let p = path.unwrap();
    assert_eq!(p.len(), 3); // A -> B -> D 或 A -> C -> D
    assert_eq!(p[0], a);
    assert_eq!(p[2], d);
}

#[test]
fn test_dijkstra() {
    let mut db = GraphDatabase::new_in_memory();

    let a = db.create_node(vec!["User"], make_user("A"));
    let b = db.create_node(vec!["User"], make_user("B"));
    let c = db.create_node(vec!["User"], make_user("C"));

    db.create_rel(a, b, "KNOWS", Properties::new());
    db.create_rel(b, c, "KNOWS", Properties::new());

    let result = algorithms::dijkstra(&db, a, c);
    assert!(result.is_some());

    let (path, cost) = result.unwrap();
    assert_eq!(path.len(), 3);
    assert_eq!(cost, 2);
}

#[test]
fn test_degree_centrality() {
    let mut db = GraphDatabase::new_in_memory();

    let a = db.create_node(vec!["User"], make_user("A"));
    let b = db.create_node(vec!["User"], make_user("B"));
    let c = db.create_node(vec!["User"], make_user("C"));

    db.create_rel(a, b, "KNOWS", Properties::new());
    db.create_rel(a, c, "KNOWS", Properties::new());
    db.create_rel(b, c, "KNOWS", Properties::new());

    let centrality = algorithms::degree_centrality(&db);

    assert!(centrality.contains_key(&a));
    assert!(centrality.contains_key(&b));
    assert!(centrality.contains_key(&c));

    // A 的度最高（2个出边）
    assert!(centrality[&a] >= centrality[&c]);
}

#[test]
fn test_connected_components() {
    let mut db = GraphDatabase::new_in_memory();

    // 第一个连通分量
    let a = db.create_node(vec!["User"], make_user("A"));
    let b = db.create_node(vec!["User"], make_user("B"));

    // 第二个连通分量
    let c = db.create_node(vec!["User"], make_user("C"));
    let d = db.create_node(vec!["User"], make_user("D"));

    db.create_rel(a, b, "KNOWS", Properties::new());
    db.create_rel(c, d, "KNOWS", Properties::new());

    let components = algorithms::connected_components(&db);

    // 应该有2个连通分量
    let comp_a = components[&a];
    let comp_b = components[&b];
    let comp_c = components[&c];
    let comp_d = components[&d];

    assert_eq!(comp_a, comp_b);
    assert_eq!(comp_c, comp_d);
    assert_ne!(comp_a, comp_c);
}

#[test]
fn test_pagerank_basic() {
    let mut db = GraphDatabase::new_in_memory();

    let a = db.create_node(vec!["User"], make_user("A"));
    let b = db.create_node(vec!["User"], make_user("B"));
    let c = db.create_node(vec!["User"], make_user("C"));

    // A -> B, A -> C, B -> C
    db.create_rel(a, b, "LINK", Properties::new());
    db.create_rel(a, c, "LINK", Properties::new());
    db.create_rel(b, c, "LINK", Properties::new());

    let ranks = algorithms::pagerank(&db, 0.85, 20);

    // 所有节点都应该有 rank
    assert!(ranks.contains_key(&a));
    assert!(ranks.contains_key(&b));
    assert!(ranks.contains_key(&c));

    // 排名应该非负，且总和接近 1
    let sum: f64 = ranks.values().sum();
    assert!((sum - 1.0).abs() < 1e-6);

    // C 有最多入边，rank 应该最高
    let rank_a = ranks[&a];
    let rank_b = ranks[&b];
    let rank_c = ranks[&c];
    assert!(rank_c > rank_b);
    assert!(rank_b > rank_a || (rank_b - rank_a).abs() < 1e-6);
}

#[test]
fn test_louvain_basic() {
    let mut db = GraphDatabase::new_in_memory();

    // 社区 1: A-B-C
    let a = db.create_node(vec!["User"], make_user("A"));
    let b = db.create_node(vec!["User"], make_user("B"));
    let c = db.create_node(vec!["User"], make_user("C"));

    db.create_rel(a, b, "KNOWS", Properties::new());
    db.create_rel(b, c, "KNOWS", Properties::new());
    db.create_rel(a, c, "KNOWS", Properties::new());

    // 社区 2: D-E-F
    let d = db.create_node(vec!["User"], make_user("D"));
    let e = db.create_node(vec!["User"], make_user("E"));
    let f = db.create_node(vec!["User"], make_user("F"));

    db.create_rel(d, e, "KNOWS", Properties::new());
    db.create_rel(e, f, "KNOWS", Properties::new());
    db.create_rel(d, f, "KNOWS", Properties::new());

    let communities = algorithms::louvain(&db, 10);

    // 所有节点都应该有社区分配
    for node in &[a, b, c, d, e, f] {
        assert!(communities.contains_key(node));
    }

    let comm_a = communities[&a];
    let comm_b = communities[&b];
    let comm_c = communities[&c];
    let comm_d = communities[&d];
    let comm_e = communities[&e];
    let comm_f = communities[&f];

    // A/B/C 应在同一社区，D/E/F 在另一个社区
    assert_eq!(comm_a, comm_b);
    assert_eq!(comm_b, comm_c);
    assert_eq!(comm_d, comm_e);
    assert_eq!(comm_e, comm_f);
    assert_ne!(comm_a, comm_d);
}
