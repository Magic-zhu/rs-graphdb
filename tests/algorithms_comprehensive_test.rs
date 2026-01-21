// 图算法综合测试
// 测试三角计数、聚类系数、PageRank、社区检测等算法

use rs_graphdb::{GraphDatabase, algorithms};
use rs_graphdb::storage::mem_store::MemStore;
use rs_graphdb::values::{Properties, Value};

fn make_user(name: &str) -> Properties {
    let mut props = Properties::new();
    props.insert("name".to_string(), Value::Text(name.to_string()));
    props
}

// ==================== 三角计数测试 ====================

#[test]
fn test_count_triangles_simple() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个简单的三角形: A - B - C - A
    let a = db.create_node(vec!["User"], make_user("A"));
    let b = db.create_node(vec!["User"], make_user("B"));
    let c = db.create_node(vec!["User"], make_user("C"));

    db.create_rel(a, b, "KNOWS", Properties::new());
    db.create_rel(b, c, "KNOWS", Properties::new());
    db.create_rel(c, a, "KNOWS", Properties::new());

    let count = algorithms::count_triangles(&db);
    assert_eq!(count, 1);
}

#[test]
fn test_count_triangles_multiple() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建两个三角形共享一条边
    // Triangle 1: A - B - C - A
    // Triangle 2: A - B - D - A
    let a = db.create_node(vec!["User"], make_user("A"));
    let b = db.create_node(vec!["User"], make_user("B"));
    let c = db.create_node(vec!["User"], make_user("C"));
    let d = db.create_node(vec!["User"], make_user("D"));

    db.create_rel(a, b, "KNOWS", Properties::new());
    db.create_rel(b, c, "KNOWS", Properties::new());
    db.create_rel(c, a, "KNOWS", Properties::new());

    db.create_rel(a, b, "KNOWS", Properties::new());
    db.create_rel(b, d, "KNOWS", Properties::new());
    db.create_rel(d, a, "KNOWS", Properties::new());

    let count = algorithms::count_triangles(&db);
    assert_eq!(count, 2);
}

#[test]
fn test_count_triangles_no_triangles() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一条线: A - B - C - D (没有三角形)
    let a = db.create_node(vec!["User"], make_user("A"));
    let b = db.create_node(vec!["User"], make_user("B"));
    let c = db.create_node(vec!["User"], make_user("C"));
    let d = db.create_node(vec!["User"], make_user("D"));

    db.create_rel(a, b, "KNOWS", Properties::new());
    db.create_rel(b, c, "KNOWS", Properties::new());
    db.create_rel(c, d, "KNOWS", Properties::new());

    let count = algorithms::count_triangles(&db);
    assert_eq!(count, 0);
}

#[test]
fn test_count_triangles_for_node() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建两个三角形:
    // Triangle 1: A - B - C - A
    // Triangle 2: A - C - D - A
    // A 参与 2 个三角形，C 参与 2 个三角形，B 和 D 各参与 1 个
    let a = db.create_node(vec!["User"], make_user("A"));
    let b = db.create_node(vec!["User"], make_user("B"));
    let c = db.create_node(vec!["User"], make_user("C"));
    let d = db.create_node(vec!["User"], make_user("D"));

    db.create_rel(a, b, "KNOWS", Properties::new());
    db.create_rel(b, c, "KNOWS", Properties::new());
    db.create_rel(c, a, "KNOWS", Properties::new());

    db.create_rel(a, c, "KNOWS", Properties::new());
    db.create_rel(c, d, "KNOWS", Properties::new());
    db.create_rel(d, a, "KNOWS", Properties::new());

    assert_eq!(algorithms::count_triangles_for_node(&db, a), 2);
    assert_eq!(algorithms::count_triangles_for_node(&db, b), 1);
    assert_eq!(algorithms::count_triangles_for_node(&db, c), 2);
    assert_eq!(algorithms::count_triangles_for_node(&db, d), 1);
}

#[test]
fn test_count_triangles_all_nodes() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个三角形: A - B - C - A
    let a = db.create_node(vec!["User"], make_user("A"));
    let b = db.create_node(vec!["User"], make_user("B"));
    let c = db.create_node(vec!["User"], make_user("C"));

    db.create_rel(a, b, "KNOWS", Properties::new());
    db.create_rel(b, c, "KNOWS", Properties::new());
    db.create_rel(c, a, "KNOWS", Properties::new());

    let triangles = algorithms::count_triangles_all_nodes(&db);

    assert_eq!(triangles.len(), 3);
    assert_eq!(triangles[&a], 1);
    assert_eq!(triangles[&b], 1);
    assert_eq!(triangles[&c], 1);
}

#[test]
fn test_local_clustering_coefficient() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个完全连接的三角形: A - B - C - A
    let a = db.create_node(vec!["User"], make_user("A"));
    let b = db.create_node(vec!["User"], make_user("B"));
    let c = db.create_node(vec!["User"], make_user("C"));

    db.create_rel(a, b, "KNOWS", Properties::new());
    db.create_rel(b, c, "KNOWS", Properties::new());
    db.create_rel(c, a, "KNOWS", Properties::new());

    // 在完全连接的三角形中，每个节点的聚类系数都是 1.0
    let cc_a = algorithms::local_clustering_coefficient(&db, a);
    let cc_b = algorithms::local_clustering_coefficient(&db, b);
    let cc_c = algorithms::local_clustering_coefficient(&db, c);

    assert!((cc_a - 1.0).abs() < 1e-6);
    assert!((cc_b - 1.0).abs() < 1e-6);
    assert!((cc_c - 1.0).abs() < 1e-6);
}

#[test]
fn test_local_clustering_coefficient_partial() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个部分连接的图: A 连接到 B 和 C，但 B 和 C 不相连
    let a = db.create_node(vec!["User"], make_user("A"));
    let b = db.create_node(vec!["User"], make_user("B"));
    let c = db.create_node(vec!["User"], make_user("C"));

    db.create_rel(a, b, "KNOWS", Properties::new());
    db.create_rel(a, c, "KNOWS", Properties::new());
    // B 和 C 之间没有连接

    // A 的聚类系数是 0.0（B 和 C 不相连）
    let cc_a = algorithms::local_clustering_coefficient(&db, a);
    assert_eq!(cc_a, 0.0);
}

#[test]
fn test_global_clustering_coefficient() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个完全连接的三角形: A - B - C - A
    let a = db.create_node(vec!["User"], make_user("A"));
    let b = db.create_node(vec!["User"], make_user("B"));
    let c = db.create_node(vec!["User"], make_user("C"));

    db.create_rel(a, b, "KNOWS", Properties::new());
    db.create_rel(b, c, "KNOWS", Properties::new());
    db.create_rel(c, a, "KNOWS", Properties::new());

    let cc = algorithms::global_clustering_coefficient(&db);
    assert!((cc - 1.0).abs() < 1e-6);
}

#[test]
fn test_global_clustering_coefficient_empty() {
    let db = GraphDatabase::<MemStore>::new_in_memory();
    let cc = algorithms::global_clustering_coefficient(&db);
    assert_eq!(cc, 0.0);
}

// ==================== PageRank 测试 ====================

#[test]
fn test_pagerank_star_graph() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 星形图: 中心节点 A 连接到所有其他节点
    let center = db.create_node(vec!["User"], make_user("Center"));
    let nodes: Vec<_> = (0..5).map(|_| {
        db.create_node(vec!["User"], make_user("Leaf"))
    }).collect();

    for &node in &nodes {
        db.create_rel(center, node, "LINK", Properties::new());
    }

    let ranks = algorithms::pagerank(&db, 0.85, 20);

    // 在星形图中，中心节点有最高的出度，但因为所有边都是单向的
    // 叶节点没有出边，会随机跳转，所以中心节点的 rank 可能不是最高的
    // 我们只验证所有节点都有 rank
    assert!(ranks.contains_key(&center));
    for &node in &nodes {
        assert!(ranks.contains_key(&node));
    }

    // Rank 总和应该接近 1
    let sum: f64 = ranks.values().sum();
    assert!((sum - 1.0).abs() < 1e-6);
}

#[test]
fn test_pagerank_dangling_nodes() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建有悬挂节点的图
    let a = db.create_node(vec!["User"], make_user("A"));
    let b = db.create_node(vec!["User"], make_user("B"));
    let c = db.create_node(vec!["User"], make_user("C"));

    // A -> B, C 是悬挂节点（没有出边）
    db.create_rel(a, b, "LINK", Properties::new());

    let ranks = algorithms::pagerank(&db, 0.85, 20);

    // 所有节点都应该有 rank
    assert!(ranks.contains_key(&a));
    assert!(ranks.contains_key(&b));
    assert!(ranks.contains_key(&c));

    // Rank 总和应该接近 1
    let sum: f64 = ranks.values().sum();
    assert!((sum - 1.0).abs() < 1e-6);
}

// ==================== 社区检测测试 ====================

#[test]
fn test_connected_components_disconnected() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 两个完全断开的子图
    let a1 = db.create_node(vec!["User"], make_user("A1"));
    let a2 = db.create_node(vec!["User"], make_user("A2"));

    let b1 = db.create_node(vec!["User"], make_user("B1"));
    let b2 = db.create_node(vec!["User"], make_user("B2"));

    db.create_rel(a1, a2, "KNOWS", Properties::new());
    db.create_rel(b1, b2, "KNOWS", Properties::new());

    let components = algorithms::connected_components(&db);

    // A1 和 A2 应该在同一分量
    assert_eq!(components[&a1], components[&a2]);

    // B1 和 B2 应该在同一分量
    assert_eq!(components[&b1], components[&b2]);

    // 两个分量应该不同
    assert_ne!(components[&a1], components[&b1]);
}

#[test]
fn test_louvain_two_communities() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 社区 1: 密集连接的 A-B-C
    let a = db.create_node(vec!["User"], make_user("A"));
    let b = db.create_node(vec!["User"], make_user("B"));
    let c = db.create_node(vec!["User"], make_user("C"));

    db.create_rel(a, b, "KNOWS", Properties::new());
    db.create_rel(b, c, "KNOWS", Properties::new());
    db.create_rel(a, c, "KNOWS", Properties::new());

    // 社区 2: 密集连接的 D-E-F
    let d = db.create_node(vec!["User"], make_user("D"));
    let e = db.create_node(vec!["User"], make_user("E"));
    let f = db.create_node(vec!["User"], make_user("F"));

    db.create_rel(d, e, "KNOWS", Properties::new());
    db.create_rel(e, f, "KNOWS", Properties::new());
    db.create_rel(d, f, "KNOWS", Properties::new());

    // 只有一条弱连接连接两个社区
    db.create_rel(c, d, "KNOWS", Properties::new());

    let communities = algorithms::louvain(&db, 10);

    // A/B/C 应该在同一社区
    let comm_a = communities[&a];
    let comm_b = communities[&b];
    let comm_c = communities[&c];
    assert_eq!(comm_a, comm_b);
    assert_eq!(comm_b, comm_c);

    // D/E/F 应该在同一社区
    let comm_d = communities[&d];
    let comm_e = communities[&e];
    let comm_f = communities[&f];
    assert_eq!(comm_d, comm_e);
    assert_eq!(comm_e, comm_f);

    // 两个社区应该不同（虽然有一条弱连接）
    assert_ne!(comm_a, comm_d);
}

// ==================== 中心性测试 ====================

#[test]
fn test_degree_centrality_hub() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个中心节点连接到多个其他节点
    let hub = db.create_node(vec!["User"], make_user("Hub"));
    let nodes: Vec<_> = (0..5).map(|_| {
        db.create_node(vec!["User"], make_user("Node"))
    }).collect();

    for &node in &nodes {
        db.create_rel(hub, node, "KNOWS", Properties::new());
    }

    let centrality = algorithms::degree_centrality(&db);

    // Hub 的度中心性应该最高
    let hub_centrality = centrality[&hub];
    for &node in &nodes {
        let node_centrality = centrality[&node];
        assert!(hub_centrality > node_centrality);
    }
}

#[test]
fn test_betweenness_centrality_simple() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个简单的桥接图: A - B - C
    // B 在所有最短路径上，应该有最高的介数中心性
    let a = db.create_node(vec!["User"], make_user("A"));
    let b = db.create_node(vec!["User"], make_user("B"));
    let c = db.create_node(vec!["User"], make_user("C"));

    db.create_rel(a, b, "KNOWS", Properties::new());
    db.create_rel(b, c, "KNOWS", Properties::new());

    let centrality = algorithms::betweenness_centrality(&db);

    // B 的介数中心性应该最高（它在 A 到 C 的最短路径上）
    let b_centrality = centrality[&b];
    let a_centrality = centrality[&a];
    let c_centrality = centrality[&c];

    assert!(b_centrality > a_centrality);
    assert!(b_centrality > c_centrality);
}

// ==================== 遍历算法测试 ====================

#[test]
fn test_bfs_level_by_level() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个 3 层的树形结构
    let root = db.create_node(vec!["User"], make_user("Root"));

    let level1: Vec<_> = (0..3).map(|_| {
        db.create_node(vec!["User"], make_user("L1"))
    }).collect();

    let level2: Vec<_> = (0..6).map(|_| {
        db.create_node(vec!["User"], make_user("L2"))
    }).collect();

    // 连接 root 到 level1
    for &node in &level1 {
        db.create_rel(root, node, "PARENT", Properties::new());
    }

    // 连接 level1 到 level2
    for (i, &l1) in level1.iter().enumerate() {
        for j in 0..2 {
            db.create_rel(l1, level2[i * 2 + j], "PARENT", Properties::new());
        }
    }

    let visited = algorithms::bfs(&db, root, Some(2));

    // 应该访问 root 和 level1 的所有节点（深度限制为2）
    assert!(visited.contains(&root));
    for &node in &level1 {
        assert!(visited.contains(&node));
    }
}

#[test]
fn test_dfs_depth_first() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个简单的图: A -> B, A -> C, B -> D
    let a = db.create_node(vec!["User"], make_user("A"));
    let b = db.create_node(vec!["User"], make_user("B"));
    let c = db.create_node(vec!["User"], make_user("C"));
    let d = db.create_node(vec!["User"], make_user("D"));

    db.create_rel(a, b, "EDGE", Properties::new());
    db.create_rel(a, c, "EDGE", Properties::new());
    db.create_rel(b, d, "EDGE", Properties::new());

    let visited = algorithms::dfs(&db, a, None);

    // DFS 应该访问所有可达节点
    assert!(visited.contains(&a));
    assert!(visited.contains(&b));
    assert!(visited.contains(&c));
    assert!(visited.contains(&d));
}

#[test]
fn test_all_simple_paths() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个菱形图: A -> B, A -> C, B -> D, C -> D
    let a = db.create_node(vec!["User"], make_user("A"));
    let b = db.create_node(vec!["User"], make_user("B"));
    let c = db.create_node(vec!["User"], make_user("C"));
    let d = db.create_node(vec!["User"], make_user("D"));

    db.create_rel(a, b, "EDGE", Properties::new());
    db.create_rel(a, c, "EDGE", Properties::new());
    db.create_rel(b, d, "EDGE", Properties::new());
    db.create_rel(c, d, "EDGE", Properties::new());

    let paths = algorithms::all_simple_paths(&db, a, d, None);

    // 应该找到两条路径: A->B->D 和 A->C->D
    assert_eq!(paths.len(), 2);

    // 验证路径长度
    for path in &paths {
        assert_eq!(path.len(), 3); // A -> X -> D
        assert_eq!(path[0], a);
        assert_eq!(path[2], d);
    }
}

#[test]
fn test_shortest_path_by_rel_type() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个图，有两种关系类型
    let a = db.create_node(vec!["User"], make_user("A"));
    let b = db.create_node(vec!["User"], make_user("B"));
    let c = db.create_node(vec!["User"], make_user("C"));

    db.create_rel(a, b, "FRIEND", Properties::new());
    db.create_rel(b, c, "COLLEAGUE", Properties::new());
    db.create_rel(a, c, "FRIEND", Properties::new());

    // 通过 FRIEND 关系从 A 到 C
    let path = algorithms::bfs_shortest_path_by_rel_type(&db, a, c, Some(&["FRIEND"]));

    assert!(path.is_some());
    let p = path.unwrap();
    assert_eq!(p.len(), 2); // A -> C (直接通过 FRIEND)
    assert_eq!(p[0], a);
    assert_eq!(p[1], c);
}

// ==================== 最短路径测试 ====================

#[test]
fn test_dijkstra_weighted() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个加权图（通过属性模拟）
    let a = db.create_node(vec!["User"], make_user("A"));
    let b = db.create_node(vec!["User"], make_user("B"));
    let c = db.create_node(vec!["User"], make_user("C"));
    let d = db.create_node(vec!["User"], make_user("D"));

    // A -> B -> D (cost = 2)
    db.create_rel(a, b, "ROAD", Properties::new());
    db.create_rel(b, d, "ROAD", Properties::new());

    // A -> C -> D (cost = 2)
    db.create_rel(a, c, "ROAD", Properties::new());
    db.create_rel(c, d, "ROAD", Properties::new());

    // A -> D 直接连接 (cost = 1)
    db.create_rel(a, d, "ROAD", Properties::new());

    let result = algorithms::dijkstra(&db, a, d);

    assert!(result.is_some());
    let (path, cost) = result.unwrap();

    // 最短路径应该是 A -> D
    assert_eq!(path.len(), 2);
    assert_eq!(path[0], a);
    assert_eq!(path[1], d);
    assert_eq!(cost, 1);
}

#[test]
fn test_variable_length_path() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个多路径图: A -> B -> D, A -> C -> D
    let a = db.create_node(vec!["User"], make_user("A"));
    let b = db.create_node(vec!["User"], make_user("B"));
    let c = db.create_node(vec!["User"], make_user("C"));
    let d = db.create_node(vec!["User"], make_user("D"));

    db.create_rel(a, b, "LINK", Properties::new());
    db.create_rel(b, d, "LINK", Properties::new());
    db.create_rel(a, c, "LINK", Properties::new());
    db.create_rel(c, d, "LINK", Properties::new());

    // 查找从 A 到 D 的路径，2-3 跳
    // 最短路径是 2 跳 (A->B->D 或 A->C->D)
    let paths = algorithms::variable_length_path(&db, a, d, 2, 3);

    // 应该找到路径: A -> B -> D 和 A -> C -> D (都是 2 跳)
    assert!(!paths.is_empty());

    for path in &paths {
        let len = path.len();
        assert!(len >= 3 && len <= 4); // 2-3 跳意味着 3-4 个节点
        assert_eq!(path[0], a);
        assert_eq!(path[path.len() - 1], d);
    }
}

// ==================== 复杂图测试 ====================

#[test]
fn test_complete_graph() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个 5 节点的完全图
    let n = 5;
    let nodes: Vec<_> = (0..n).map(|i| {
        db.create_node(vec!["User"], make_user(&format!("Node{}", i)))
    }).collect();

    // 在完全图中，每对节点之间都有边（双向）
    for i in 0..n {
        for j in 0..n {
            if i != j {
                db.create_rel(nodes[i], nodes[j], "EDGE", Properties::new());
            }
        }
    }

    // 双向完全图中的三角形数量应该是 n * C(n-1, 2) = 5 * 6 = 30
    // 但因为是无向计数，应该是 C(5,3) = 10（每个三角形只计数一次）
    let triangles = algorithms::count_triangles(&db);
    assert_eq!(triangles, 10);

    // 完全图的聚类系数应该是 1.0
    let cc = algorithms::global_clustering_coefficient(&db);
    assert!((cc - 1.0).abs() < 1e-6);

    // 在双向完全图中，PageRank 应该均匀分布
    // 因为每个节点的入度和出度都相同
    let ranks = algorithms::pagerank(&db, 0.85, 100); // 增加迭代次数以收敛
    let avg_rank = 1.0 / (n as f64);

    // 验证 rank 总和是 1
    let sum: f64 = ranks.values().sum();
    assert!((sum - 1.0).abs() < 1e-6);

    // 在双向完全图中，每个节点的 PageRank 应该非常接近
    for &node in &nodes {
        let rank = ranks[&node];
        // 由于数值计算的精度限制，允许一些误差
        assert!((rank - avg_rank).abs() < 0.01, "Node rank {} vs avg {}", rank, avg_rank);
    }
}

#[test]
fn test_cycle_graph() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个环形图: A -> B -> C -> D -> E -> A
    let n = 5;
    let nodes: Vec<_> = (0..n).map(|i| {
        db.create_node(vec!["User"], make_user(&format!("Node{}", i)))
    }).collect();

    for i in 0..n {
        let next = (i + 1) % n;
        db.create_rel(nodes[i], nodes[next], "NEXT", Properties::new());
    }

    // 环形图没有三角形
    let triangles = algorithms::count_triangles(&db);
    assert_eq!(triangles, 0);

    // 聚类系数应该是 0.0
    let cc = algorithms::global_clustering_coefficient(&db);
    assert_eq!(cc, 0.0);

    // 应该只有一个连通分量
    let components = algorithms::connected_components(&db);
    let unique_components: std::collections::HashSet<_> = components.values().collect();
    assert_eq!(unique_components.len(), 1);
}

#[test]
fn test_empty_graph() {
    let db = GraphDatabase::<MemStore>::new_in_memory();

    // 空图的测试
    assert_eq!(algorithms::count_triangles(&db), 0);
    assert_eq!(algorithms::global_clustering_coefficient(&db), 0.0);

    let ranks = algorithms::pagerank(&db, 0.85, 20);
    assert!(ranks.is_empty());

    let components = algorithms::connected_components(&db);
    assert!(components.is_empty());
}
