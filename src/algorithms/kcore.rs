//! K-核心分解 (K-Core Decomposition) 算法
//!
//! 用于分析图的连通性和层次结构

use crate::graph::db::GraphDatabase;
use crate::storage::{NodeId, StorageEngine};
use std::collections::{HashMap, HashSet};

/// 计算图中每个节点的 K-核心值
///
/// # 算法说明
///
/// K-核心是图的极大子图，其中每个节点的度数至少为 k。
/// k-core 值表示节点所属的最大核心。
///
/// 算法步骤（剥皮算法）：
/// 1. 初始化所有节点的 k-core 值为其度数
/// 2. 递归移除度数小于当前 k 值的节点
/// 3. 更新剩余节点的度数
/// 4. 重复直到没有节点可以移除
///
/// # 复杂度
///
/// - 时间复杂度: O(|V| + |E|)
/// - 空间复杂度: O(|V| + |E|)
///
/// # 示例
///
/// ```
/// use rs_graphdb::algorithms::k_core_decomposition;
/// use rs_graphdb::graph::db::GraphDatabase;
/// use rs_graphdb::storage::mem_store::MemStore;
/// use rs_graphdb::values::Properties;
///
/// let mut db = GraphDatabase::<MemStore>::new_in_memory();
///
/// // 创建一个图：
/// // 完全连接的三角形 A-B-C (每个节点度数为2)
/// // D 连接到 A (度数为1)
/// let a = db.create_node(vec![], Properties::new());
/// let b = db.create_node(vec![], Properties::new());
/// let c = db.create_node(vec![], Properties::new());
/// let d = db.create_node(vec![], Properties::new());
///
/// db.create_rel(a, b, "EDGE", Properties::new());
/// db.create_rel(b, c, "EDGE", Properties::new());
/// db.create_rel(c, a, "EDGE", Properties::new());
/// db.create_rel(a, d, "EDGE", Properties::new());
///
/// let k_cores = k_core_decomposition(&db);
///
/// // A, B, C 应该在 2-core 中
/// assert!(k_cores[&a] >= 2);
/// assert!(k_cores[&b] >= 2);
/// assert!(k_cores[&c] >= 2);
///
/// // D 应该在 1-core 中
/// assert_eq!(k_cores[&d], 1);
/// ```
pub fn k_core_decomposition<E: StorageEngine>(
    db: &GraphDatabase<E>,
) -> HashMap<NodeId, usize> {
    let mut k_core_values: HashMap<NodeId, usize> = HashMap::new();
    let mut remaining_nodes: HashSet<NodeId> = HashSet::new();
    let mut degrees: HashMap<NodeId, usize> = HashMap::new();

    // 初始化：收集所有节点并计算度数
    for stored_node in db.all_stored_nodes() {
        let node_id = stored_node.id;
        let degree = get_node_degree(db, node_id);
        degrees.insert(node_id, degree);
        remaining_nodes.insert(node_id);
    }

    // 剥皮算法
    let mut current_k = 1;

    while !remaining_nodes.is_empty() {
        let mut removed = true;

        while removed {
            removed = false;
            let nodes_to_remove: Vec<NodeId> = remaining_nodes
                .iter()
                .filter(|&&node| degrees[&node] < current_k)
                .copied()
                .collect();

            if !nodes_to_remove.is_empty() {
                removed = true;

                // 移除这些节点并更新邻居的度数
                for node in nodes_to_remove {
                    k_core_values.insert(node, current_k - 1);

                    // 更新邻居的度数
                    for rel in db.engine.outgoing_rels(node) {
                        let neighbor = rel.end;
                        if remaining_nodes.contains(&neighbor) {
                            if let Some(degree) = degrees.get_mut(&neighbor) {
                                *degree = degree.saturating_sub(1);
                            }
                        }
                    }

                    for rel in db.engine.incoming_rels(node) {
                        let neighbor = rel.start;
                        if remaining_nodes.contains(&neighbor) {
                            if let Some(degree) = degrees.get_mut(&neighbor) {
                                *degree = degree.saturating_sub(1);
                            }
                        }
                    }

                    remaining_nodes.remove(&node);
                    degrees.remove(&node);
                }
            }
        }

        current_k += 1;
    }

    k_core_values
}

/// 获取指定 k 值的 k-core 中的所有节点
///
/// 返回所有 k-core 值大于等于 k 的节点
///
/// # 示例
///
/// ```
/// use rs_graphdb::algorithms::get_k_core;
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
/// // 创建三角形
/// db.create_rel(a, b, "EDGE", Properties::new());
/// db.create_rel(b, c, "EDGE", Properties::new());
/// db.create_rel(c, a, "EDGE", Properties::new());
///
/// let k2_core = get_k_core(&db, 2);
///
/// // 三角形中的所有节点都在 2-core 中
/// assert_eq!(k2_core.len(), 3);
/// assert!(k2_core.contains(&a));
/// assert!(k2_core.contains(&b));
/// assert!(k2_core.contains(&c));
/// ```
pub fn get_k_core<E: StorageEngine>(
    db: &GraphDatabase<E>,
    k: usize,
) -> HashSet<NodeId> {
    let k_core_values = k_core_decomposition(db);
    k_core_values
        .into_iter()
        .filter(|(_, core_value)| *core_value >= k)
        .map(|(node, _)| node)
        .collect()
}

/// 获取图的最大 k-core 值
///
/// 返回图中存在的最大核心值
///
/// # 示例
///
/// ```
/// use rs_graphdb::algorithms::max_core_number;
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
/// // 创建三角形（2-core）
/// db.create_rel(a, b, "EDGE", Properties::new());
/// db.create_rel(b, c, "EDGE", Properties::new());
/// db.create_rel(c, a, "EDGE", Properties::new());
///
/// assert_eq!(max_core_number(&db), 2);
/// ```
pub fn max_core_number<E: StorageEngine>(db: &GraphDatabase<E>) -> usize {
    let k_core_values = k_core_decomposition(db);
    k_core_values
        .values()
        .max()
        .copied()
        .unwrap_or(0)
}

/// 计算节点的度数（考虑入边和出边）
fn get_node_degree<E: StorageEngine>(
    db: &GraphDatabase<E>,
    node_id: NodeId,
) -> usize {
    let mut degree = 0;

    // 统计出边
    for _rel in db.engine.outgoing_rels(node_id) {
        degree += 1;
    }

    // 统计入边
    for _rel in db.engine.incoming_rels(node_id) {
        degree += 1;
    }

    degree
}
