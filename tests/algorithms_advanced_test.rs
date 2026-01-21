// 新增算法测试：SCC、K-Core、A*
// 测试强连通分量、K-核心分解和A*搜索算法

use rs_graphdb::{GraphDatabase, algorithms};
use rs_graphdb::storage::mem_store::MemStore;
use rs_graphdb::values::{Properties, Value};

fn make_node_with_pos(name: &str, x: i64, y: i64) -> Properties {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props.insert("x".to_string(), Value::Int(x));
    props.insert("y".to_string(), Value::Int(y));
    props
}

// ==================== 强连通分量 (SCC) 测试 ====================

#[test]
fn test_scc_simple_cycle() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个简单环: A -> B -> C -> A
    let a = db.create_node(vec!["User"], Properties::new());
    let b = db.create_node(vec!["User"], Properties::new());
    let c = db.create_node(vec!["User"], Properties::new());

    db.create_rel(a, b, "EDGE", Properties::new());
    db.create_rel(b, c, "EDGE", Properties::new());
    db.create_rel(c, a, "EDGE", Properties::new());

    let sccs = algorithms::strongly_connected_components(&db);

    // 所有节点应该在同一 SCC 中
    assert_eq!(sccs[&a], sccs[&b]);
    assert_eq!(sccs[&b], sccs[&c]);

    // 应该只有一个 SCC
    assert_eq!(algorithms::count_scc(&db), 1);
}

#[test]
fn test_scc_dag() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个 DAG: A -> B -> C, A -> D -> C
    let a = db.create_node(vec!["User"], Properties::new());
    let b = db.create_node(vec!["User"], Properties::new());
    let c = db.create_node(vec!["User"], Properties::new());
    let d = db.create_node(vec!["User"], Properties::new());

    db.create_rel(a, b, "EDGE", Properties::new());
    db.create_rel(b, c, "EDGE", Properties::new());
    db.create_rel(a, d, "EDGE", Properties::new());
    db.create_rel(d, c, "EDGE", Properties::new());

    let sccs = algorithms::strongly_connected_components(&db);

    // DAG 中每个节点都是自己的 SCC
    // 所有 SCC ID 应该不同
    let unique_sccs: std::collections::HashSet<_> = sccs.values().collect();
    assert_eq!(unique_sccs.len(), 4);
}

#[test]
fn test_scc_multiple_components() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // SCC 1: A <-> B (双向连接)
    let a = db.create_node(vec!["User"], Properties::new());
    let b = db.create_node(vec!["User"], Properties::new());
    db.create_rel(a, b, "EDGE", Properties::new());
    db.create_rel(b, a, "EDGE", Properties::new());

    // SCC 2: C <-> D (双向连接)
    let c = db.create_node(vec!["User"], Properties::new());
    let d = db.create_node(vec!["User"], Properties::new());
    db.create_rel(c, d, "EDGE", Properties::new());
    db.create_rel(d, c, "EDGE", Properties::new());

    // A -> C (单向连接，不形成环)
    db.create_rel(a, c, "EDGE", Properties::new());

    let sccs = algorithms::strongly_connected_components(&db);

    // A 和 B 在同一 SCC
    assert_eq!(sccs[&a], sccs[&b]);

    // C 和 D 在同一 SCC
    assert_eq!(sccs[&c], sccs[&d]);

    // 两个 SCC 不同
    assert_ne!(sccs[&a], sccs[&c]);

    // 应该有 2 个 SCC
    assert_eq!(algorithms::count_scc(&db), 2);
}

#[test]
fn test_is_strongly_connected() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个强连通图: A -> B -> C -> A
    let a = db.create_node(vec!["User"], Properties::new());
    let b = db.create_node(vec!["User"], Properties::new());
    let c = db.create_node(vec!["User"], Properties::new());

    db.create_rel(a, b, "EDGE", Properties::new());
    db.create_rel(b, c, "EDGE", Properties::new());
    db.create_rel(c, a, "EDGE", Properties::new());

    assert!(algorithms::is_strongly_connected(&db));
}

#[test]
fn test_scc_groups() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个环 A-B-C-A 和孤立节点 D
    let a = db.create_node(vec!["User"], Properties::new());
    let b = db.create_node(vec!["User"], Properties::new());
    let c = db.create_node(vec!["User"], Properties::new());
    let d = db.create_node(vec!["User"], Properties::new());

    db.create_rel(a, b, "EDGE", Properties::new());
    db.create_rel(b, c, "EDGE", Properties::new());
    db.create_rel(c, a, "EDGE", Properties::new());

    let groups = algorithms::get_scc_groups(&db);

    // 应该有 2 个 SCC 组
    assert_eq!(groups.len(), 2);

    // 一个组包含 3 个节点 (A, B, C)
    let has_large_group = groups.values().any(|v| v.len() == 3);
    assert!(has_large_group);

    // 一个组包含 1 个节点 (D)
    let has_single_node = groups.values().any(|v| v.len() == 1);
    assert!(has_single_node);
}

#[test]
fn test_scc_size_distribution() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建两个三角形环
    let nodes: Vec<_> = (0..6).map(|_| {
        db.create_node(vec!["User"], Properties::new())
    }).collect();

    // 第一个三角形: 0 -> 1 -> 2 -> 0
    db.create_rel(nodes[0], nodes[1], "EDGE", Properties::new());
    db.create_rel(nodes[1], nodes[2], "EDGE", Properties::new());
    db.create_rel(nodes[2], nodes[0], "EDGE", Properties::new());

    // 第二个三角形: 3 -> 4 -> 5 -> 3
    db.create_rel(nodes[3], nodes[4], "EDGE", Properties::new());
    db.create_rel(nodes[4], nodes[5], "EDGE", Properties::new());
    db.create_rel(nodes[5], nodes[3], "EDGE", Properties::new());

    let sizes = algorithms::scc_size_distribution(&db);

    // 应该有 2 个 SCC，大小分别为 3 和 3
    assert_eq!(sizes.len(), 2);
    assert_eq!(sizes[0], 3);
    assert_eq!(sizes[1], 3);
}

// ==================== K-Core 测试 ====================

#[test]
fn test_k_core_triangle() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个三角形: A-B-C (每个节点度数为2)
    let a = db.create_node(vec!["User"], Properties::new());
    let b = db.create_node(vec!["User"], Properties::new());
    let c = db.create_node(vec!["User"], Properties::new());

    db.create_rel(a, b, "EDGE", Properties::new());
    db.create_rel(b, c, "EDGE", Properties::new());
    db.create_rel(c, a, "EDGE", Properties::new());

    let k_cores = algorithms::k_core_decomposition(&db);

    // 所有节点都应该在 2-core 中
    assert!(k_cores[&a] >= 2);
    assert!(k_cores[&b] >= 2);
    assert!(k_cores[&c] >= 2);
}

#[test]
fn test_k_core_with_leaf() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个三角形 A-B-C，加上一个叶子节点 D 连接到 A
    let a = db.create_node(vec!["User"], Properties::new());
    let b = db.create_node(vec!["User"], Properties::new());
    let c = db.create_node(vec!["User"], Properties::new());
    let d = db.create_node(vec!["User"], Properties::new());

    // 三角形
    db.create_rel(a, b, "EDGE", Properties::new());
    db.create_rel(b, c, "EDGE", Properties::new());
    db.create_rel(c, a, "EDGE", Properties::new());

    // 叶子
    db.create_rel(a, d, "EDGE", Properties::new());

    let k_cores = algorithms::k_core_decomposition(&db);

    // A, B, C 应该至少在 2-core 中
    assert!(k_cores[&a] >= 2);
    assert!(k_cores[&b] >= 2);
    assert!(k_cores[&c] >= 2);

    // D 应该在 1-core 中
    assert_eq!(k_cores[&d], 1);
}

#[test]
fn test_get_k_core() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个完全图 K4
    let nodes: Vec<_> = (0..4).map(|_| {
        db.create_node(vec!["User"], Properties::new())
    }).collect();

    for i in 0..4 {
        for j in (i + 1)..4 {
            db.create_rel(nodes[i], nodes[j], "EDGE", Properties::new());
        }
    }

    // 获取 3-core（应该包含所有节点）
    let k3_core = algorithms::get_k_core(&db, 3);
    assert_eq!(k3_core.len(), 4);

    // 获取 4-core（应该为空）
    let k4_core = algorithms::get_k_core(&db, 4);
    assert_eq!(k4_core.len(), 0);
}

#[test]
fn test_max_core_number() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个三角形
    let a = db.create_node(vec!["User"], Properties::new());
    let b = db.create_node(vec!["User"], Properties::new());
    let c = db.create_node(vec!["User"], Properties::new());

    db.create_rel(a, b, "EDGE", Properties::new());
    db.create_rel(b, c, "EDGE", Properties::new());
    db.create_rel(c, a, "EDGE", Properties::new());

    // 三角形的最大核心值是 2
    assert_eq!(algorithms::max_core_number(&db), 2);
}

#[test]
fn test_k_core_line_graph() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一条线: A -> B -> C -> D
    let nodes: Vec<_> = (0..4).map(|_| {
        db.create_node(vec!["User"], Properties::new())
    }).collect();

    for i in 0..3 {
        db.create_rel(nodes[i], nodes[i + 1], "EDGE", Properties::new());
    }

    let k_cores = algorithms::k_core_decomposition(&db);

    // 在线图中，所有节点的 k-core 值都是 1
    for &node in &nodes {
        assert_eq!(k_cores[&node], 1);
    }

    assert_eq!(algorithms::max_core_number(&db), 1);
}

// ==================== A* 算法测试 ====================

#[test]
fn test_astar_simple_path() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    let a = db.create_node(vec!["User"], make_node_with_pos("A", 0, 0));
    let b = db.create_node(vec!["User"], make_node_with_pos("B", 10, 0));
    let c = db.create_node(vec!["User"], make_node_with_pos("C", 10, 10));

    db.create_rel(a, b, "ROAD", Properties::new());
    db.create_rel(b, c, "ROAD", Properties::new());

    // 使用欧几里得距离作为启发式
    let heuristic = |node: u64| -> f64 {
        if let Some(n) = db.get_node(node) {
            let x = n.get("x").and_then(|v| match v {
                Value::Int(i) => Some(*i as f64),
                _ => None,
            }).unwrap_or(0.0);
            let y = n.get("y").and_then(|v| match v {
                Value::Int(i) => Some(*i as f64),
                _ => None,
            }).unwrap_or(0.0);
            ((x - 10.0).powi(2) + (y - 10.0).powi(2)).sqrt()
        } else {
            0.0
        }
    };

    let path = algorithms::astar(&db, a, c, heuristic, &|_, _| 1.0);

    assert!(path.is_some());
    let p = path.unwrap();
    assert_eq!(p.len(), 3); // A -> B -> C
    assert_eq!(p[0], a);
    assert_eq!(p[2], c);
}

#[test]
fn test_astar_euclidean() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    let a = db.create_node(vec!["User"], make_node_with_pos("A", 0, 0));
    let b = db.create_node(vec!["User"], make_node_with_pos("B", 3, 4));
    let c = db.create_node(vec!["User"], make_node_with_pos("C", 6, 0));

    db.create_rel(a, b, "ROAD", Properties::new());
    db.create_rel(b, c, "ROAD", Properties::new());

    let get_pos = |node: u64| -> (f64, f64) {
        if let Some(n) = db.get_node(node) {
            let x = n.get("x").and_then(|v| match v {
                Value::Int(i) => Some(*i as f64),
                _ => None,
            }).unwrap_or(0.0);
            let y = n.get("y").and_then(|v| match v {
                Value::Int(i) => Some(*i as f64),
                _ => None,
            }).unwrap_or(0.0);
            (x, y)
        } else {
            (0.0, 0.0)
        }
    };

    let path = algorithms::astar_euclidean(&db, a, c, get_pos, &|_, _| 1.0);

    assert!(path.is_some());
    let p = path.unwrap();
    assert_eq!(p.len(), 3);
}

#[test]
fn test_astar_manhattan() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    let a = db.create_node(vec!["User"], make_node_with_pos("A", 0, 0));
    let b = db.create_node(vec!["User"], make_node_with_pos("B", 5, 0));
    let c = db.create_node(vec!["User"], make_node_with_pos("C", 10, 0));

    db.create_rel(a, b, "ROAD", Properties::new());
    db.create_rel(b, c, "ROAD", Properties::new());

    let get_pos = |node: u64| -> (f64, f64) {
        if let Some(n) = db.get_node(node) {
            let x = n.get("x").and_then(|v| match v {
                Value::Int(i) => Some(*i as f64),
                _ => None,
            }).unwrap_or(0.0);
            let y = n.get("y").and_then(|v| match v {
                Value::Int(i) => Some(*i as f64),
                _ => None,
            }).unwrap_or(0.0);
            (x, y)
        } else {
            (0.0, 0.0)
        }
    };

    let path = algorithms::astar_manhattan(&db, a, c, get_pos, &|_, _| 1.0);

    assert!(path.is_some());
    let p = path.unwrap();
    assert_eq!(p.len(), 3);
}

#[test]
fn test_astar_no_path() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    let a = db.create_node(vec!["User"], make_node_with_pos("A", 0, 0));
    let b = db.create_node(vec!["User"], make_node_with_pos("B", 10, 0));

    // A 和 B 之间没有路径

    let get_pos = |node: u64| -> (f64, f64) {
        if let Some(n) = db.get_node(node) {
            let x = n.get("x").and_then(|v| match v {
                Value::Int(i) => Some(*i as f64),
                _ => None,
            }).unwrap_or(0.0);
            let y = n.get("y").and_then(|v| match v {
                Value::Int(i) => Some(*i as f64),
                _ => None,
            }).unwrap_or(0.0);
            (x, y)
        } else {
            (0.0, 0.0)
        }
    };

    let path = algorithms::astar_euclidean(&db, a, b, get_pos, &|_, _| 1.0);

    assert!(path.is_none());
}

#[test]
fn test_astar_same_node() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    let a = db.create_node(vec!["User"], make_node_with_pos("A", 0, 0));

    let get_pos = |node: u64| -> (f64, f64) {
        if let Some(n) = db.get_node(node) {
            let x = n.get("x").and_then(|v| match v {
                Value::Int(i) => Some(*i as f64),
                _ => None,
            }).unwrap_or(0.0);
            let y = n.get("y").and_then(|v| match v {
                Value::Int(i) => Some(*i as f64),
                _ => None,
            }).unwrap_or(0.0);
            (x, y)
        } else {
            (0.0, 0.0)
        }
    };

    let path = algorithms::astar_euclidean(&db, a, a, get_pos, &|_, _| 1.0);

    assert!(path.is_some());
    let p = path.unwrap();
    assert_eq!(p.len(), 1);
    assert_eq!(p[0], a);
}

#[test]
fn test_astar_multiple_paths() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个网格图，有多个路径
    // (0,0) -> (1,0) -> (2,0)
    // (0,0) -> (0,1) -> (2,1)
    // (1,0) -> (2,0)
    // (1,0) -> (2,1)
    // (0,1) -> (2,1)
    let start = db.create_node(vec!["User"], make_node_with_pos("Start", 0, 0));
    let n1 = db.create_node(vec!["User"], make_node_with_pos("N1", 1, 0));
    let n2 = db.create_node(vec!["User"], make_node_with_pos("N2", 0, 1));
    let end = db.create_node(vec!["User"], make_node_with_pos("End", 2, 1));

    db.create_rel(start, n1, "EDGE", Properties::new());
    db.create_rel(start, n2, "EDGE", Properties::new());
    db.create_rel(n1, end, "EDGE", Properties::new());
    db.create_rel(n2, end, "EDGE", Properties::new());
    db.create_rel(n1, n2, "EDGE", Properties::new());

    let get_pos = |node: u64| -> (f64, f64) {
        if let Some(n) = db.get_node(node) {
            let x = n.get("x").and_then(|v| match v {
                Value::Int(i) => Some(*i as f64),
                _ => None,
            }).unwrap_or(0.0);
            let y = n.get("y").and_then(|v| match v {
                Value::Int(i) => Some(*i as f64),
                _ => None,
            }).unwrap_or(0.0);
            (x, y)
        } else {
            (0.0, 0.0)
        }
    };

    // 使用曼哈顿距离（网格图的最佳启发式）
    let path = algorithms::astar_manhattan(&db, start, end, get_pos, &|_, _| 1.0);

    assert!(path.is_some());
    let p = path.unwrap();
    // 应该找到最短路径
    assert!(p.len() <= 4);
    assert_eq!(p[0], start);
    assert_eq!(p[p.len() - 1], end);
}
