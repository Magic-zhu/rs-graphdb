//! 三角计数算法测试

use rs_graphdb::{GraphDatabase, algorithms};
use rs_graphdb::values::{Properties, Value};

fn props(name: &str) -> Properties {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props
}

#[test]
fn test_empty_graph() {
    let db = GraphDatabase::new_in_memory();

    let count = algorithms::count_triangles(&db);
    assert_eq!(count, 0);
}

#[test]
fn test_single_triangle() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建三角形: A - B - C - A
    let a = db.create_node(vec!["Node"], props("A"));
    let b = db.create_node(vec!["Node"], props("B"));
    let c = db.create_node(vec!["Node"], props("C"));

    db.create_rel(a, b, "KNOWS", Properties::new());
    db.create_rel(b, c, "KNOWS", Properties::new());
    db.create_rel(c, a, "KNOWS", Properties::new());

    let count = algorithms::count_triangles(&db);
    assert_eq!(count, 1);
}

#[test]
fn test_count_triangles_for_node() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建三角形: A - B - C - A
    let a = db.create_node(vec!["Node"], props("A"));
    let b = db.create_node(vec!["Node"], props("B"));
    let c = db.create_node(vec!["Node"], props("C"));

    db.create_rel(a, b, "KNOWS", Properties::new());
    db.create_rel(b, c, "KNOWS", Properties::new());
    db.create_rel(c, a, "KNOWS", Properties::new());

    // 每个节点参与 1 个三角形
    assert_eq!(algorithms::count_triangles_for_node(&db, a), 1);
    assert_eq!(algorithms::count_triangles_for_node(&db, b), 1);
    assert_eq!(algorithms::count_triangles_for_node(&db, c), 1);
}

#[test]
fn test_multiple_triangles() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建 K4 (完全图，4个节点)
    let nodes = vec![
        db.create_node(vec!["Node"], props("A")),
        db.create_node(vec!["Node"], props("B")),
        db.create_node(vec!["Node"], props("C")),
        db.create_node(vec!["Node"], props("D")),
    ];

    // K4 有 4 个三角形
    // ABC, ABD, ACD, BCD
    for i in 0..4 {
        for j in (i+1)..4 {
            db.create_rel(nodes[i], nodes[j], "KNOWS", Properties::new());
        }
    }

    let count = algorithms::count_triangles(&db);
    assert_eq!(count, 4);
}

#[test]
fn test_clustering_coefficient() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建三角形: A - B - C - A
    let a = db.create_node(vec!["Node"], props("A"));
    let b = db.create_node(vec!["Node"], props("B"));
    let c = db.create_node(vec!["Node"], props("C"));

    db.create_rel(a, b, "KNOWS", Properties::new());
    db.create_rel(b, c, "KNOWS", Properties::new());
    db.create_rel(c, a, "KNOWS", Properties::new());

    // 完全聚类：聚类系数 = 1.0
    let cc_a = algorithms::local_clustering_coefficient(&db, a);
    let cc_b = algorithms::local_clustering_coefficient(&db, b);
    let cc_c = algorithms::local_clustering_coefficient(&db, c);

    assert_eq!(cc_a, 1.0);
    assert_eq!(cc_b, 1.0);
    assert_eq!(cc_c, 1.0);

    // 全局聚类系数也应该是 1.0
    let global_cc = algorithms::global_clustering_coefficient(&db);
    assert_eq!(global_cc, 1.0);
}

#[test]
fn test_no_clustering() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建星形图（中心节点连接所有其他节点，但其他节点之间不连接）
    let center = db.create_node(vec!["Node"], props("Center"));
    let leaf1 = db.create_node(vec!["Node"], props("Leaf1"));
    let leaf2 = db.create_node(vec!["Node"], props("Leaf2"));
    let leaf3 = db.create_node(vec!["Node"], props("Leaf3"));

    db.create_rel(center, leaf1, "KNOWS", Properties::new());
    db.create_rel(center, leaf2, "KNOWS", Properties::new());
    db.create_rel(center, leaf3, "KNOWS", Properties::new());

    // 叶节点聚类系数 = 0（没有邻居之间的连接）
    let cc_leaf1 = algorithms::local_clustering_coefficient(&db, leaf1);
    assert_eq!(cc_leaf1, 0.0);

    // 中心节点聚类系数 = 0（邻居之间没有连接）
    let cc_center = algorithms::local_clustering_coefficient(&db, center);
    assert_eq!(cc_center, 0.0);

    // 全局聚类系数 = 0
    let global_cc = algorithms::global_clustering_coefficient(&db);
    assert_eq!(global_cc, 0.0);
}

#[test]
fn test_count_all_nodes() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建三角形: A - B - C - A
    let a = db.create_node(vec!["Node"], props("A"));
    let b = db.create_node(vec!["Node"], props("B"));
    let c = db.create_node(vec!["Node"], props("C"));

    db.create_rel(a, b, "KNOWS", Properties::new());
    db.create_rel(b, c, "KNOWS", Properties::new());
    db.create_rel(c, a, "KNOWS", Properties::new());

    let all_triangles = algorithms::count_triangles_all_nodes(&db);

    // 每个节点参与 1 个三角形
    assert_eq!(all_triangles.len(), 3);
    assert_eq!(all_triangles.get(&a), Some(&1));
    assert_eq!(all_triangles.get(&b), Some(&1));
    assert_eq!(all_triangles.get(&c), Some(&1));
}

#[test]
fn test_partial_clustering() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建两个三角形共享一条边
    // Triangle 1: A-B-C-A
    // Triangle 2: A-B-D-A
    let a = db.create_node(vec!["Node"], props("A"));
    let b = db.create_node(vec!["Node"], props("B"));
    let c = db.create_node(vec!["Node"], props("C"));
    let d = db.create_node(vec!["Node"], props("D"));

    // Triangle 1
    db.create_rel(a, b, "KNOWS", Properties::new());
    db.create_rel(b, c, "KNOWS", Properties::new());
    db.create_rel(c, a, "KNOWS", Properties::new());

    // Triangle 2
    db.create_rel(a, d, "KNOWS", Properties::new());
    db.create_rel(d, b, "KNOWS", Properties::new());

    // 总三角形数 = 2
    let total = algorithms::count_triangles(&db);
    assert_eq!(total, 2);

    // 节点 A 和 B 参与两个三角形
    assert_eq!(algorithms::count_triangles_for_node(&db, a), 2);
    assert_eq!(algorithms::count_triangles_for_node(&db, b), 2);

    // 节点 C 和 D 各参与一个三角形
    assert_eq!(algorithms::count_triangles_for_node(&db, c), 1);
    assert_eq!(algorithms::count_triangles_for_node(&db, d), 1);
}

#[test]
fn test_performance_100_nodes() {
    let mut db = GraphDatabase::new_in_memory();

    // 创建 100 个节点的随机图
    let nodes: Vec<_> = (0..100)
        .map(|_| {
            let mut props = Properties::new();
            props.insert("id".to_string(), Value::Int(0));
            db.create_node(vec!["Node"], props)
        })
        .collect();

    // 创建一些随机边（每个节点平均连接 10 个其他节点）
    use std::time::Instant;
    let start = Instant::now();

    for i in 0..100 {
        for j in (i+1)..std::cmp::min(i+11, 100) {
            db.create_rel(nodes[i], nodes[j], "KNOWS", Properties::new());
        }
    }

    let count = algorithms::count_triangles(&db);
    let elapsed = start.elapsed();

    println!("100 nodes, {} triangles, {:?}", count, elapsed);

    // 验证结果合理性
    assert!(count > 0);

    // 性能断言：应该在合理时间内完成
    assert!(elapsed.as_secs() < 5, "Triangle counting took too long: {:?}", elapsed);
}
