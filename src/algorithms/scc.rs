//! 强连通分量 (Strongly Connected Components, SCC) 算法
//!
//! 用于分析有向图的强连通性

use crate::graph::db::GraphDatabase;
use crate::storage::{NodeId, StorageEngine};
use std::collections::{HashMap, HashSet, VecDeque};

/// 计算有向图的所有强连通分量
///
/// # 算法说明
///
/// 使用 Kosaraju 算法：
/// 1. 对原图进行 DFS，记录节点的完成时间（后序遍历）
/// 2. 反转图中的所有边
/// 3. 按照完成时间递减的顺序，对反转图进行 DFS
/// 4. 每次 DFS 访问的节点集构成一个强连通分量
///
/// # 复杂度
///
/// - 时间复杂度: O(|V| + |E|)
/// - 空间复杂度: O(|V| + |E|)
///
/// # 示例
///
/// ```
/// use rs_graphdb::algorithms::strongly_connected_components;
/// use rs_graphdb::graph::db::GraphDatabase;
/// use rs_graphdb::storage::mem_store::MemStore;
/// use rs_graphdb::values::Properties;
///
/// let mut db = GraphDatabase::<MemStore>::new_in_memory();
///
/// // 创建一个有向图: A -> B -> C, C -> A (形成 SCC1), D -> E (形成 SCC2)
/// let a = db.create_node(vec![], Properties::new());
/// let b = db.create_node(vec![], Properties::new());
/// let c = db.create_node(vec![], Properties::new());
/// let d = db.create_node(vec![], Properties::new());
/// let e = db.create_node(vec![], Properties::new());
///
/// db.create_rel(a, b, "EDGE", Properties::new());
/// db.create_rel(b, c, "EDGE", Properties::new());
/// db.create_rel(c, a, "EDGE", Properties::new());
/// db.create_rel(d, e, "EDGE", Properties::new());
///
/// let sccs = strongly_connected_components(&db);
///
/// // A, B, C 应该在同一 SCC
/// assert_eq!(sccs[&a], sccs[&b]);
/// assert_eq!(sccs[&b], sccs[&c]);
///
/// // D, E 应该在同一 SCC
/// assert_eq!(sccs[&d], sccs[&e]);
///
/// // 两个 SCC 应该不同
/// assert_ne!(sccs[&a], sccs[&d]);
/// ```
pub fn strongly_connected_components<E: StorageEngine>(
    db: &GraphDatabase<E>,
) -> HashMap<NodeId, usize> {
    // 收集所有节点
    let nodes: Vec<NodeId> = db.all_stored_nodes().map(|n| n.id).collect();

    if nodes.is_empty() {
        return HashMap::new();
    }

    // 第一步：DFS 获取完成顺序
    let mut finish_order = Vec::new();
    let mut visited = HashSet::new();

    for &node in &nodes {
        if !visited.contains(&node) {
            dfs_for_finish_order(db, node, &mut visited, &mut finish_order);
        }
    }

    // 第二步：反转图并按完成顺序进行 DFS
    let mut scc_id = 0;
    let mut scc_map: HashMap<NodeId, usize> = HashMap::new();
    let mut assigned = HashSet::new();

    // 按完成时间递减的顺序处理节点
    for &node in finish_order.iter().rev() {
        if !assigned.contains(&node) {
            dfs_on_reversed_graph(db, node, scc_id, &mut assigned, &mut scc_map);
            scc_id += 1;
        }
    }

    scc_map
}

/// DFS 获取节点的完成顺序（用于 Kosaraju 算法第一步）
fn dfs_for_finish_order<E: StorageEngine>(
    db: &GraphDatabase<E>,
    node: NodeId,
    visited: &mut HashSet<NodeId>,
    finish_order: &mut Vec<NodeId>,
) {
    visited.insert(node);

    // 递归访问所有未访问的邻居
    for rel in db.engine.outgoing_rels(node) {
        let neighbor = rel.end;
        if !visited.contains(&neighbor) {
            dfs_for_finish_order(db, neighbor, visited, finish_order);
        }
    }

    // 所有邻居都访问完毕，记录完成顺序
    finish_order.push(node);
}

/// 在反转图上进行 DFS（用于 Kosaraju 算法第二步）
fn dfs_on_reversed_graph<E: StorageEngine>(
    db: &GraphDatabase<E>,
    node: NodeId,
    scc_id: usize,
    assigned: &mut HashSet<NodeId>,
    scc_map: &mut HashMap<NodeId, usize>,
) {
    assigned.insert(node);
    scc_map.insert(node, scc_id);

    // 在反转图上访问邻居（即原图中指向该节点的节点）
    for rel in db.engine.incoming_rels(node) {
        let neighbor = rel.start;
        if !assigned.contains(&neighbor) {
            dfs_on_reversed_graph(db, neighbor, scc_id, assigned, scc_map);
        }
    }
}

/// 获取强连通分量的数量
///
/// 返回图中强连通分量的总数
///
/// # 示例
///
/// ```
/// use rs_graphdb::algorithms::count_scc;
/// use rs_graphdb::graph::db::GraphDatabase;
/// use rs_graphdb::storage::mem_store::MemStore;
/// use rs_graphdb::values::Properties;
///
/// let mut db = GraphDatabase::<MemStore>::new_in_memory();
///
/// let a = db.create_node(vec![], Properties::new());
/// let b = db.create_node(vec![], Properties::new());
/// let c = db.create_node(vec![], Properties::new());
///
/// db.create_rel(a, b, "EDGE", Properties::new());
/// db.create_rel(b, c, "EDGE", Properties::new());
/// db.create_rel(c, a, "EDGE", Properties::new());
///
/// // A, B, C 形成一个 SCC
/// assert_eq!(count_scc(&db), 1);
/// ```
pub fn count_scc<E: StorageEngine>(db: &GraphDatabase<E>) -> usize {
    let scc_map = strongly_connected_components(db);
    let unique_sccs: HashSet<usize> = scc_map.values().cloned().collect();
    unique_sccs.len()
}

/// 获取每个强连通分量的节点列表
///
/// 返回一个 HashMap，键为 SCC ID，值为属于该 SCC 的节点列表
///
/// # 示例
///
/// ```
/// use rs_graphdb::algorithms::get_scc_groups;
/// use rs_graphdb::graph::db::GraphDatabase;
/// use rs_graphdb::storage::mem_store::MemStore;
/// use rs_graphdb::values::Properties;
///
/// let mut db = GraphDatabase::<MemStore>::new_in_memory();
///
/// let a = db.create_node(vec![], Properties::new());
/// let b = db.create_node(vec![], Properties::new());
/// let c = db.create_node(vec![], Properties::new());
/// let d = db.create_node(vec![], Properties::new());
///
/// // A-B-C 形成 SCC，D 是单独的 SCC
/// db.create_rel(a, b, "EDGE", Properties::new());
/// db.create_rel(b, c, "EDGE", Properties::new());
/// db.create_rel(c, a, "EDGE", Properties::new());
///
/// let groups = get_scc_groups(&db);
///
/// // 应该有 2 个 SCC
/// assert_eq!(groups.len(), 2);
/// ```
pub fn get_scc_groups<E: StorageEngine>(
    db: &GraphDatabase<E>,
) -> HashMap<usize, Vec<NodeId>> {
    let scc_map = strongly_connected_components(db);
    let mut groups: HashMap<usize, Vec<NodeId>> = HashMap::new();

    for (node, scc_id) in scc_map {
        groups.entry(scc_id).or_default().push(node);
    }

    groups
}

/// 检查图是否是强连通的
///
/// 如果整个图是一个强连通分量，则返回 true
///
/// # 示例
///
/// ```
/// use rs_graphdb::algorithms::is_strongly_connected;
/// use rs_graphdb::graph::db::GraphDatabase;
/// use rs_graphdb::storage::mem_store::MemStore;
/// use rs_graphdb::values::Properties;
///
/// let mut db = GraphDatabase::<MemStore>::new_in_memory();
///
/// let a = db.create_node(vec![], Properties::new());
/// let b = db.create_node(vec![], Properties::new());
/// let c = db.create_node(vec![], Properties::new());
///
/// db.create_rel(a, b, "EDGE", Properties::new());
/// db.create_rel(b, c, "EDGE", Properties::new());
/// db.create_rel(c, a, "EDGE", Properties::new());
///
/// // A-B-C 形成环，图是强连通的
/// assert!(is_strongly_connected(&db));
/// ```
pub fn is_strongly_connected<E: StorageEngine>(db: &GraphDatabase<E>) -> bool {
    count_scc(db) == 1
}

/// 计算强连通分量的大小分布
///
/// 返回一个 Vec，其中每个元素表示一个 SCC 的大小（节点数量）
///
/// # 示例
///
/// ```
/// use rs_graphdb::algorithms::scc_size_distribution;
/// use rs_graphdb::graph::db::GraphDatabase;
/// use rs_graphdb::storage::mem_store::MemStore;
/// use rs_graphdb::values::Properties;
///
/// let mut db = GraphDatabase::<MemStore>::new_in_memory();
///
/// let a = db.create_node(vec![], Properties::new());
/// let b = db.create_node(vec![], Properties::new());
/// let c = db.create_node(vec![], Properties::new());
/// let d = db.create_node(vec![], Properties::new());
///
/// // A-B-C 形成 SCC (3 nodes), D 单独 (1 node)
/// db.create_rel(a, b, "EDGE", Properties::new());
/// db.create_rel(b, c, "EDGE", Properties::new());
/// db.create_rel(c, a, "EDGE", Properties::new());
///
/// let sizes = scc_size_distribution(&db);
///
/// // 应该有 [3, 1] 或 [1, 3]
/// assert_eq!(sizes.len(), 2);
/// assert!(sizes.contains(&3));
/// assert!(sizes.contains(&1));
/// ```
pub fn scc_size_distribution<E: StorageEngine>(
    db: &GraphDatabase<E>,
) -> Vec<usize> {
    let groups = get_scc_groups(db);
    let mut sizes: Vec<usize> = groups.values().map(|v| v.len()).collect();
    sizes.sort();
    sizes
}
