//! 三角计数算法
//!
//! 用于发现图中的三角形结构，常用于社交网络分析中的聚类系数计算。

use crate::graph::db::GraphDatabase;
use crate::storage::{NodeId, StorageEngine};
use std::collections::{HashMap, HashSet};

/// 计算图中三角形的总数
///
/// # 算法说明
///
/// 对于无向图，三角形的数量可以通过以下方式计算：
/// 1. 对每个节点 u
/// 2. 对 u 的每对邻居 (v, w)，检查 v 和 w 是否相连
/// 3. 确保 u < v < w（按节点ID排序），这样每个三角形只被计数一次
/// 4. 如果 v 和 w 相连，则 (u, v, w) 形成一个三角形
///
/// # 复杂度
///
/// - 时间复杂度: O(|V| * d^2)，其中 d 是平均度数
/// - 空间复杂度: O(|V| + |E|)
///
/// # 示例
///
/// ```
/// use rs_graphdb::algorithms::count_triangles;
/// use rs_graphdb::graph::db::GraphDatabase;
/// use rs_graphdb::storage::mem_store::MemStore;
/// use rs_graphdb::values::{Properties, Value};
///
/// let mut db = GraphDatabase::<MemStore>::new_in_memory();
///
/// // 创建一个三角形: (1) - (2) - (3) - (1)
/// let n1 = db.create_node(vec![], Properties::new());
/// let n2 = db.create_node(vec![], Properties::new());
/// let n3 = db.create_node(vec![], Properties::new());
///
/// db.create_rel(n1, n2, "EDGE", Properties::new());
/// db.create_rel(n2, n3, "EDGE", Properties::new());
/// db.create_rel(n3, n1, "EDGE", Properties::new());
///
/// let total = count_triangles(&db);
/// assert_eq!(total, 1);
/// ```
pub fn count_triangles<E: StorageEngine>(db: &GraphDatabase<E>) -> usize {
    let mut count = 0;

    // 对每个节点
    for stored_node in db.all_stored_nodes() {
        let u = stored_node.id;

        // 获取 u 的所有邻居
        let neighbors_u = get_neighbors(db, u);
        let neighbor_list: Vec<NodeId> = neighbors_u.iter().copied().collect();

        // 对每对邻居 (v, w)
        for (i, v) in neighbor_list.iter().enumerate() {
            for w in neighbor_list.iter().skip(i + 1) {
                // 确保顺序：u < v < w，避免重复计数
                // 这样确保每个三角形只被计数一次（当处理最小节点 u 时）
                if u >= *v || u >= *w {
                    continue;
                }

                // 检查 v 和 w 是否相连
                if has_edge(db, *v, *w) {
                    count += 1;
                }
            }
        }
    }

    // 由于有 u < v < w 的约束，每个三角形只被计数一次
    count
}

/// 计算某个节点参与的三角形数量
///
/// # 算法说明
///
/// 对于节点 u：
/// 1. 获取 u 的所有邻居
/// 2. 对每对邻居 (v, w)，检查 v 和 w 是否相连
/// 3. 如果相连，则 (u, v, w) 形成一个三角形
///
/// # 示例
///
/// ```
/// use rs_graphdb::algorithms::count_triangles_for_node;
/// use rs_graphdb::graph::db::GraphDatabase;
/// use rs_graphdb::storage::mem_store::MemStore;
/// use rs_graphdb::values::{Properties, Value};
///
/// let mut db = GraphDatabase::<MemStore>::new_in_memory();
///
/// // 创建一个三角形: (n1) - (n2) - (n3) - (n1)
/// let n1 = db.create_node(vec![], Properties::new());
/// let n2 = db.create_node(vec![], Properties::new());
/// let n3 = db.create_node(vec![], Properties::new());
///
/// db.create_rel(n1, n2, "EDGE", Properties::new());
/// db.create_rel(n2, n3, "EDGE", Properties::new());
/// db.create_rel(n3, n1, "EDGE", Properties::new());
///
/// // n1 参与了 1 个三角形
/// let triangles = count_triangles_for_node(&db, n1);
/// assert_eq!(triangles, 1);
/// ```
pub fn count_triangles_for_node<E: StorageEngine>(
    db: &GraphDatabase<E>,
    node_id: NodeId,
) -> usize {
    let neighbors = get_neighbors(db, node_id);
    let mut count = 0;

    // 将邻居转换为 Vec 以便索引
    let neighbor_list: Vec<NodeId> = neighbors.iter().copied().collect();

    // 对每对邻居 (v, w)
    for (i, v) in neighbor_list.iter().enumerate() {
        for w in neighbor_list.iter().skip(i + 1) {
            // 检查 v 和 w 是否相连
            if has_edge(db, *v, *w) {
                count += 1;
            }
        }
    }

    count
}

/// 计算所有节点的三角形数量
///
/// 返回一个 HashMap，键为节点 ID，值为该节点参与的三角形数量
///
/// # 示例
///
/// ```
/// use rs_graphdb::algorithms::count_triangles_all_nodes;
/// use rs_graphdb::graph::db::GraphDatabase;
/// use rs_graphdb::storage::mem_store::MemStore;
/// use rs_graphdb::values::Properties;
///
/// let mut db = GraphDatabase::<MemStore>::new_in_memory();
///
/// // 创建一个三角形: (n1) - (n2) - (n3) - (n1)
/// let n1 = db.create_node(vec![], Properties::new());
/// let n2 = db.create_node(vec![], Properties::new());
/// let n3 = db.create_node(vec![], Properties::new());
///
/// db.create_rel(n1, n2, "EDGE", Properties::new());
/// db.create_rel(n2, n3, "EDGE", Properties::new());
/// db.create_rel(n3, n1, "EDGE", Properties::new());
///
/// let triangles = count_triangles_all_nodes(&db);
/// // 每个节点都参与了 1 个三角形
/// assert_eq!(triangles.len(), 3);
/// assert_eq!(triangles[&n1], 1);
/// ```
pub fn count_triangles_all_nodes<E: StorageEngine>(
    db: &GraphDatabase<E>,
) -> HashMap<NodeId, usize> {
    let mut result = HashMap::new();

    for stored_node in db.all_stored_nodes() {
        let node_id = stored_node.id;
        let count = count_triangles_for_node(db, node_id);
        result.insert(node_id, count);
    }

    result
}

/// 计算图的局部聚类系数（Local Clustering Coefficient）
///
/// 聚类系数衡量节点的邻居之间互连的程度
///
/// C = (2 * 实际三角形数量) / (度 * (度 - 1))
///
/// # 示例
///
/// ```
/// use rs_graphdb::algorithms::local_clustering_coefficient;
/// use rs_graphdb::graph::db::GraphDatabase;
/// use rs_graphdb::storage::mem_store::MemStore;
/// use rs_graphdb::values::Properties;
///
/// let mut db = GraphDatabase::<MemStore>::new_in_memory();
///
/// // 创建一个三角形: (n1) - (n2) - (n3) - (n1)
/// let n1 = db.create_node(vec![], Properties::new());
/// let n2 = db.create_node(vec![], Properties::new());
/// let n3 = db.create_node(vec![], Properties::new());
///
/// db.create_rel(n1, n2, "EDGE", Properties::new());
/// db.create_rel(n2, n3, "EDGE", Properties::new());
/// db.create_rel(n3, n1, "EDGE", Properties::new());
///
/// // n1 的聚类系数是 1.0（它的所有邻居都互相连接）
/// let cc = local_clustering_coefficient(&db, n1);
/// assert_eq!(cc, 1.0);
/// ```
pub fn local_clustering_coefficient<E: StorageEngine>(
    db: &GraphDatabase<E>,
    node_id: NodeId,
) -> f64 {
    let degree = get_neighbors(db, node_id).len();

    if degree < 2 {
        return 0.0;
    }

    let triangles = count_triangles_for_node(db, node_id);
    let possible_triangles = degree * (degree - 1) / 2;

    if possible_triangles == 0 {
        return 0.0;
    }

    (triangles as f64) / (possible_triangles as f64)
}

/// 计算全局平均聚类系数（Average Clustering Coefficient）
///
/// 全局聚类系数是所有节点局部聚类系数的平均值
///
/// # 示例
///
/// ```
/// use rs_graphdb::algorithms::global_clustering_coefficient;
/// use rs_graphdb::graph::db::GraphDatabase;
/// use rs_graphdb::storage::mem_store::MemStore;
/// use rs_graphdb::values::Properties;
///
/// let mut db = GraphDatabase::<MemStore>::new_in_memory();
///
/// // 创建一个三角形: (n1) - (n2) - (n3) - (n1)
/// let n1 = db.create_node(vec![], Properties::new());
/// let n2 = db.create_node(vec![], Properties::new());
/// let n3 = db.create_node(vec![], Properties::new());
///
/// db.create_rel(n1, n2, "EDGE", Properties::new());
/// db.create_rel(n2, n3, "EDGE", Properties::new());
/// db.create_rel(n3, n1, "EDGE", Properties::new());
///
/// // 全局聚类系数是 1.0（所有节点的聚类系数都是 1.0）
/// let cc = global_clustering_coefficient(&db);
/// assert_eq!(cc, 1.0);
/// ```
pub fn global_clustering_coefficient<E: StorageEngine>(
    db: &GraphDatabase<E>,
) -> f64 {
    let mut sum = 0.0;
    let mut count = 0;

    for stored_node in db.all_stored_nodes() {
        let node_id = stored_node.id;
        let cc = local_clustering_coefficient(db, node_id);
        sum += cc;
        count += 1;
    }

    if count == 0 {
        return 0.0;
    }

    sum / (count as f64)
}

// ============ 辅助函数 ============

/// 获取节点的所有邻居
fn get_neighbors<E: StorageEngine>(
    db: &GraphDatabase<E>,
    node_id: NodeId,
) -> HashSet<NodeId> {
    let mut neighbors = HashSet::new();

    // 获取出边（排除自环）
    for rel in db.engine.outgoing_rels(node_id) {
        if rel.end != node_id {
            neighbors.insert(rel.end);
        }
    }

    // 获取入边（排除自环）
    for rel in db.engine.incoming_rels(node_id) {
        if rel.start != node_id {
            neighbors.insert(rel.start);
        }
    }

    neighbors
}

/// 检查两个节点之间是否有边相连
fn has_edge<E: StorageEngine>(
    db: &GraphDatabase<E>,
    u: NodeId,
    v: NodeId,
) -> bool {
    // 检查 u -> v
    for rel in db.engine.outgoing_rels(u) {
        if rel.end == v {
            return true;
        }
    }

    // 检查 v -> u
    for rel in db.engine.outgoing_rels(v) {
        if rel.end == u {
            return true;
        }
    }

    false
}
