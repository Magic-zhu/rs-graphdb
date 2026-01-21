// 所有最短路径 (All Shortest Paths) 测试
// 测试找到从起点到终点的所有最短路径

use rs_graphdb::{GraphDatabase, algorithms};
use rs_graphdb::storage::mem_store::MemStore;
use rs_graphdb::values::Properties;

// ==================== 基础测试 ====================

#[test]
fn test_all_shortest_paths_simple() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建简单路径: A -> B -> C
    let a = db.create_node(vec!["Node"], Properties::new());
    let b = db.create_node(vec!["Node"], Properties::new());
    let c = db.create_node(vec!["Node"], Properties::new());

    db.create_rel(a, b, "EDGE", Properties::new());
    db.create_rel(b, c, "EDGE", Properties::new());

    let paths = algorithms::all_shortest_paths(&db, a, c);

    // 应该只有一条最短路径
    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0], vec![a, b, c]);
}

#[test]
fn test_all_shortest_paths_multiple() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建多条路径: A->B->D 和 A->C->D
    let a = db.create_node(vec!["Node"], Properties::new());
    let b = db.create_node(vec!["Node"], Properties::new());
    let c = db.create_node(vec!["Node"], Properties::new());
    let d = db.create_node(vec!["Node"], Properties::new());

    db.create_rel(a, b, "EDGE", Properties::new());
    db.create_rel(b, d, "EDGE", Properties::new());
    db.create_rel(a, c, "EDGE", Properties::new());
    db.create_rel(c, d, "EDGE", Properties::new());

    let paths = algorithms::all_shortest_paths(&db, a, d);

    // 应该找到两条最短路径
    assert_eq!(paths.len(), 2);
    assert!(paths.iter().all(|p| p.len() == 3));

    // 检查路径是否正确
    let path1: Vec<_> = paths[0].iter().collect();
    let path2: Vec<_> = paths[1].iter().collect();
    assert!(path1.contains(&&b) || path2.contains(&&b));
    assert!(path1.contains(&&c) || path2.contains(&&c));
}

#[test]
fn test_all_shortest_paths_direct_and_indirect() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // A -> C (直接) 和 A -> B -> C (间接)
    let a = db.create_node(vec!["Node"], Properties::new());
    let b = db.create_node(vec!["Node"], Properties::new());
    let c = db.create_node(vec!["Node"], Properties::new());

    db.create_rel(a, c, "EDGE", Properties::new());  // 直接路径
    db.create_rel(a, b, "EDGE", Properties::new());
    db.create_rel(b, c, "EDGE", Properties::new());  // 间接路径

    let paths = algorithms::all_shortest_paths(&db, a, c);

    // 只有直接路径是最短路径
    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0], vec![a, c]);
}

#[test]
fn test_all_shortest_paths_same_node() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    let a = db.create_node(vec!["Node"], Properties::new());

    let paths = algorithms::all_shortest_paths(&db, a, a);

    // 起点和终点相同时，返回包含该节点的路径
    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0], vec![a]);
}

#[test]
fn test_all_shortest_paths_no_path() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    let a = db.create_node(vec!["Node"], Properties::new());
    let b = db.create_node(vec!["Node"], Properties::new());

    // A 和 B 之间没有连接

    let paths = algorithms::all_shortest_paths(&db, a, b);

    // 没有路径时返回空列表
    assert_eq!(paths.len(), 0);
}

#[test]
fn test_all_shortest_paths_triangle() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个菱形图：两条长度相同的路径
    // A -> B -> D
    // A -> C -> D
    let a = db.create_node(vec!["Node"], Properties::new());
    let b = db.create_node(vec!["Node"], Properties::new());
    let c = db.create_node(vec!["Node"], Properties::new());
    let d = db.create_node(vec!["Node"], Properties::new());

    db.create_rel(a, b, "EDGE", Properties::new());
    db.create_rel(b, d, "EDGE", Properties::new());
    db.create_rel(a, c, "EDGE", Properties::new());
    db.create_rel(c, d, "EDGE", Properties::new());

    let paths = algorithms::all_shortest_paths(&db, a, d);

    // 应该找到两条最短路径，长度都是 3
    assert_eq!(paths.len(), 2);
    assert!(paths.iter().all(|p| p.len() == 3));

    // 验证两条路径分别是 A->B->D 和 A->C->D
    let has_path_via_b = paths.iter().any(|p| p == &vec![a, b, d]);
    let has_path_via_c = paths.iter().any(|p| p == &vec![a, c, d]);
    assert!(has_path_via_b);
    assert!(has_path_via_c);
}

#[test]
fn test_all_shortest_paths_diamond() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 菱形图: A -> B, A -> C, B -> D, C -> D
    let a = db.create_node(vec!["Node"], Properties::new());
    let b = db.create_node(vec!["Node"], Properties::new());
    let c = db.create_node(vec!["Node"], Properties::new());
    let d = db.create_node(vec!["Node"], Properties::new());

    db.create_rel(a, b, "EDGE", Properties::new());
    db.create_rel(a, c, "EDGE", Properties::new());
    db.create_rel(b, d, "EDGE", Properties::new());
    db.create_rel(c, d, "EDGE", Properties::new());

    let paths = algorithms::all_shortest_paths(&db, a, d);

    // 两条最短路径
    assert_eq!(paths.len(), 2);
    assert!(paths.iter().all(|p| p.len() == 3));
}

// ==================== 复杂图测试 ====================

#[test]
fn test_all_shortest_paths_multiple_intermediate() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // A -> B1, A -> B2, A -> B3
    // B1 -> C, B2 -> C, B3 -> C
    let a = db.create_node(vec!["Node"], Properties::new());
    let b1 = db.create_node(vec!["Node"], Properties::new());
    let b2 = db.create_node(vec!["Node"], Properties::new());
    let b3 = db.create_node(vec!["Node"], Properties::new());
    let c = db.create_node(vec!["Node"], Properties::new());

    db.create_rel(a, b1, "EDGE", Properties::new());
    db.create_rel(a, b2, "EDGE", Properties::new());
    db.create_rel(a, b3, "EDGE", Properties::new());
    db.create_rel(b1, c, "EDGE", Properties::new());
    db.create_rel(b2, c, "EDGE", Properties::new());
    db.create_rel(b3, c, "EDGE", Properties::new());

    let paths = algorithms::all_shortest_paths(&db, a, c);

    // 三条最短路径
    assert_eq!(paths.len(), 3);
    assert!(paths.iter().all(|p| p.len() == 3));
}

#[test]
fn test_all_shortest_paths_with_longer_path() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // A -> B -> C -> D (长度 3)
    // A -> E -> D (长度 2，但 E 后创建)
    let a = db.create_node(vec!["Node"], Properties::new());
    let b = db.create_node(vec!["Node"], Properties::new());
    let c = db.create_node(vec!["Node"], Properties::new());
    let d = db.create_node(vec!["Node"], Properties::new());
    let e = db.create_node(vec!["Node"], Properties::new());

    db.create_rel(a, b, "EDGE", Properties::new());
    db.create_rel(b, c, "EDGE", Properties::new());
    db.create_rel(c, d, "EDGE", Properties::new());
    db.create_rel(a, e, "EDGE", Properties::new());
    db.create_rel(e, d, "EDGE", Properties::new());

    let paths = algorithms::all_shortest_paths(&db, a, d);

    // 只有 A->E->D 是最短路径（长度 3）
    // A->B->C->D 长度为 4
    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0].len(), 3);
}

// ==================== 带关系类型过滤的测试 ====================

#[test]
fn test_all_shortest_paths_by_rel_type() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    let a = db.create_node(vec!["Node"], Properties::new());
    let b = db.create_node(vec!["Node"], Properties::new());
    let c = db.create_node(vec!["Node"], Properties::new());

    db.create_rel(a, b, "FRIEND", Properties::new());
    db.create_rel(b, c, "KNOWS", Properties::new());
    db.create_rel(a, c, "FRIEND", Properties::new());

    // 只考虑 FRIEND 类型的边
    let paths = algorithms::all_shortest_paths_by_rel_type(&db, a, c, Some(&["FRIEND"]));

    // 应该只找到直接路径 A->C
    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0], vec![a, c]);
}

#[test]
fn test_all_shortest_paths_by_rel_type_multiple() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    let a = db.create_node(vec!["Node"], Properties::new());
    let b = db.create_node(vec!["Node"], Properties::new());
    let c = db.create_node(vec!["Node"], Properties::new());
    let d = db.create_node(vec!["Node"], Properties::new());

    db.create_rel(a, b, "FRIEND", Properties::new());
    db.create_rel(b, d, "FRIEND", Properties::new());
    db.create_rel(a, c, "FRIEND", Properties::new());
    db.create_rel(c, d, "KNOWS", Properties::new());

    // 只考虑 FRIEND 类型
    let paths = algorithms::all_shortest_paths_by_rel_type(&db, a, d, Some(&["FRIEND"]));

    // 应该只找到 A->B->D
    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0], vec![a, b, d]);
}

#[test]
fn test_all_shortest_paths_by_rel_type_multiple_types() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    let a = db.create_node(vec!["Node"], Properties::new());
    let b = db.create_node(vec!["Node"], Properties::new());
    let c = db.create_node(vec!["Node"], Properties::new());
    let d = db.create_node(vec!["Node"], Properties::new());

    db.create_rel(a, b, "FRIEND", Properties::new());
    db.create_rel(b, d, "COLLEAGUE", Properties::new());
    db.create_rel(a, c, "KNOWS", Properties::new());
    db.create_rel(c, d, "KNOWS", Properties::new());

    // 考虑 FRIEND 和 COLLEAGUE 类型
    let paths = algorithms::all_shortest_paths_by_rel_type(
        &db, a, d,
        Some(&["FRIEND", "COLLEAGUE"])
    );

    // 应该找到 A->B->D
    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0], vec![a, b, d]);
}

#[test]
fn test_all_shortest_paths_by_rel_type_no_filter() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建两条长度相同的路径
    let a = db.create_node(vec!["Node"], Properties::new());
    let b = db.create_node(vec!["Node"], Properties::new());
    let c = db.create_node(vec!["Node"], Properties::new());
    let d = db.create_node(vec!["Node"], Properties::new());

    db.create_rel(a, b, "FRIEND", Properties::new());
    db.create_rel(b, d, "KNOWS", Properties::new());
    db.create_rel(a, c, "FRIEND", Properties::new());
    db.create_rel(c, d, "COLLEAGUE", Properties::new());

    // None 表示不考虑类型过滤
    let paths = algorithms::all_shortest_paths_by_rel_type(&db, a, d, None);

    // 应该找到两条长度相同的路径：A->B->D 和 A->C->D
    assert_eq!(paths.len(), 2);
    assert!(paths.iter().all(|p| p.len() == 3));
}

// ==================== 辅助函数测试 ====================

#[test]
fn test_count_all_shortest_paths() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    let a = db.create_node(vec!["Node"], Properties::new());
    let b = db.create_node(vec!["Node"], Properties::new());
    let c = db.create_node(vec!["Node"], Properties::new());
    let d = db.create_node(vec!["Node"], Properties::new());

    db.create_rel(a, b, "EDGE", Properties::new());
    db.create_rel(b, d, "EDGE", Properties::new());
    db.create_rel(a, c, "EDGE", Properties::new());
    db.create_rel(c, d, "EDGE", Properties::new());

    let count = algorithms::count_all_shortest_paths(&db, a, d);

    assert_eq!(count, 2);
}

#[test]
fn test_has_path() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    let a = db.create_node(vec!["Node"], Properties::new());
    let b = db.create_node(vec!["Node"], Properties::new());
    let c = db.create_node(vec!["Node"], Properties::new());

    db.create_rel(a, b, "EDGE", Properties::new());
    // b 和 c 之间没有连接

    assert!(algorithms::has_path(&db, a, b));
    assert!(!algorithms::has_path(&db, a, c));
}

#[test]
fn test_has_path_same_node() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    let a = db.create_node(vec!["Node"], Properties::new());

    // 节点到自身应该有路径
    assert!(algorithms::has_path(&db, a, a));
}

#[test]
fn test_has_path_indirect() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    let a = db.create_node(vec!["Node"], Properties::new());
    let b = db.create_node(vec!["Node"], Properties::new());
    let c = db.create_node(vec!["Node"], Properties::new());

    db.create_rel(a, b, "EDGE", Properties::new());
    db.create_rel(b, c, "EDGE", Properties::new());

    // A 到 C 有间接路径
    assert!(algorithms::has_path(&db, a, c));
}

// ==================== 边界情况测试 ====================

#[test]
fn test_all_shortest_paths_empty_graph() {
    let db = GraphDatabase::<MemStore>::new_in_memory();

    // 空图，尝试查询不存在的节点
    // 这实际上不会发生，因为没有节点
    // 但我们可以测试一个只有单个节点的图
    let mut db = GraphDatabase::<MemStore>::new_in_memory();
    let a = db.create_node(vec!["Node"], Properties::new());

    let paths = algorithms::all_shortest_paths(&db, a, a);
    assert_eq!(paths.len(), 1);
}

#[test]
fn test_all_shortest_paths_disconnected_components() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 两个不连通的分量
    let a = db.create_node(vec!["Node"], Properties::new());
    let b = db.create_node(vec!["Node"], Properties::new());
    let c = db.create_node(vec!["Node"], Properties::new());
    let d = db.create_node(vec!["Node"], Properties::new());

    db.create_rel(a, b, "EDGE", Properties::new());
    db.create_rel(c, d, "EDGE", Properties::new());

    // A 和 C 之间没有路径
    let paths = algorithms::all_shortest_paths(&db, a, c);
    assert_eq!(paths.len(), 0);
}

#[test]
fn test_all_shortest_paths_complex_graph() {
    let mut db = GraphDatabase::<MemStore>::new_in_memory();

    // 创建一个更复杂的图
    // A -> B, A -> C
    // B -> D, B -> E
    // C -> D, C -> F
    // D -> G
    // E -> G
    // F -> G
    let a = db.create_node(vec!["Node"], Properties::new());
    let b = db.create_node(vec!["Node"], Properties::new());
    let c = db.create_node(vec!["Node"], Properties::new());
    let d = db.create_node(vec!["Node"], Properties::new());
    let e = db.create_node(vec!["Node"], Properties::new());
    let f = db.create_node(vec!["Node"], Properties::new());
    let g = db.create_node(vec!["Node"], Properties::new());

    db.create_rel(a, b, "EDGE", Properties::new());
    db.create_rel(a, c, "EDGE", Properties::new());
    db.create_rel(b, d, "EDGE", Properties::new());
    db.create_rel(b, e, "EDGE", Properties::new());
    db.create_rel(c, d, "EDGE", Properties::new());
    db.create_rel(c, f, "EDGE", Properties::new());
    db.create_rel(d, g, "EDGE", Properties::new());
    db.create_rel(e, g, "EDGE", Properties::new());
    db.create_rel(f, g, "EDGE", Properties::new());

    let paths = algorithms::all_shortest_paths(&db, a, g);

    // 应该有多条最短路径，所有路径长度应该相同
    assert!(paths.len() > 0);
    let first_len = paths[0].len();
    assert!(paths.iter().all(|p| p.len() == first_len));
}
