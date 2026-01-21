//! A* 启发式搜索算法
//!
//! 用于在加权图中找到最短路径

use crate::graph::db::GraphDatabase;
use crate::storage::{NodeId, StorageEngine};
use std::collections::{HashMap, HashSet, BinaryHeap};
use std::cmp::Ordering;

/// A* 启发式搜索算法
///
/// # 算法说明
///
/// A* 是一种启发式搜索算法，结合了 Dijkstra 的保证最优性和贪心搜索的效率。
/// f(n) = g(n) + h(n)，其中：
/// - g(n) 是从起点到节点 n 的实际代价
/// - h(n) 是从节点 n 到终点的启发式估计代价
///
/// 启发式函数必须满足：
/// - 可采纳性（Admissibility）：h(n) ≤ h*(n)，即不会高估实际代价
/// - 一致性（Consistency）：h(n) ≤ c(n, n') + h(n')
///
/// # 复杂度
///
/// - 时间复杂度: O(b^d)，其中 b 是分支因子，d 是解深度
/// - 空间复杂度: O(b^d)
///
/// # 参数
///
/// - `db`: 图数据库
/// - `start`: 起始节点 ID
/// - `end`: 目标节点 ID
/// - `heuristic`: 启发式函数，接收节点 ID 返回到目标的估计代价
/// - `cost_fn`: 代价函数，接收两个节点 ID 返回它们之间的边代价
///
/// # 示例
///
/// ```
/// use rs_graphdb::algorithms::astar;
/// use rs_graphdb::graph::db::GraphDatabase;
/// use rs_graphdb::storage::mem_store::MemStore;
/// use rs_graphdb::values::Properties;
///
/// let mut db = GraphDatabase::<MemStore>::new_in_memory();
///
/// let a = db.create_node(vec![], {
///     let mut props = Properties::new();
///     props.insert("x".to_string(), rs_graphdb::values::Value::Int(0));
///     props.insert("y".to_string(), rs_graphdb::values::Value::Int(0));
///     props
/// });
///
/// let b = db.create_node(vec![], {
///     let mut props = Properties::new();
///     props.insert("x".to_string(), rs_graphdb::values::Value::Int(10));
///     props.insert("y".to_string(), rs_graphdb::values::Value::Int(0));
///     props
/// });
///
/// db.create_rel(a, b, "ROAD", Properties::new());
///
/// // 使用曼哈顿距离作为启发式
/// let heuristic = |node: u64| -> f64 {
///     if let Some(node) = db.get_node(node) {
///         let x1 = node.get("x").and_then(|v| match v {
///             rs_graphdb::values::Value::Int(i) => Some(*i as f64),
///             _ => None,
///         }).unwrap_or(0.0);
///         let y1 = node.get("y").and_then(|v| match v {
///             rs_graphdb::values::Value::Int(i) => Some(*i as f64),
///             _ => None,
///         }).unwrap_or(0.0);
///         ((x1 - 10.0).abs() + (y1 - 0.0).abs())
///     } else {
///         0.0
///     }
/// };
///
/// let path = astar(&db, a, b, heuristic, &|_, _| 1.0);
/// assert!(path.is_some());
/// ```
pub fn astar<E: StorageEngine, F, H>(
    db: &GraphDatabase<E>,
    start: NodeId,
    end: NodeId,
    heuristic: H,
    cost_fn: F,
) -> Option<Vec<NodeId>>
where
    F: Fn(NodeId, NodeId) -> f64,
    H: Fn(NodeId) -> f64 + Copy,
{
    // 如果起点就是终点
    if start == end {
        return Some(vec![start]);
    }

    // 优先队列，存储 (f_score, g_score, node, parent)
    let mut open_set = BinaryHeap::new();
    let mut open_set_members: HashSet<NodeId> = HashSet::new();
    let mut closed_set: HashSet<NodeId> = HashSet::new();
    let mut g_score: HashMap<NodeId, f64> = HashMap::new();
    let mut parents: HashMap<NodeId, NodeId> = HashMap::new();

    // 初始化起点
    g_score.insert(start, 0.0);
    let h_start = heuristic(start);
    open_set.push(AStarNode {
        f_score: h_start,
        g_score: 0.0,
        node: start,
    });
    open_set_members.insert(start);

    while let Some(current) = open_set.pop() {
        let current_node = current.node;

        // 如果已经处理过这个节点，跳过
        if !open_set_members.remove(&current_node) {
            continue;
        }

        // 到达目标
        if current_node == end {
            return reconstruct_path(&parents, start, end);
        }

        closed_set.insert(current_node);

        // 探索邻居
        let neighbors = get_all_neighbors(db, current_node);
        for neighbor in neighbors {
            if closed_set.contains(&neighbor) {
                continue;
            }

            // 计算从起点经由 current 到 neighbor 的代价
            let tentative_g = g_score[&current_node] + cost_fn(current_node, neighbor);

            // 如果这是一个新节点，或者找到了更好的路径
            let is_better = !g_score.contains_key(&neighbor)
                || tentative_g < g_score[&neighbor];

            if is_better {
                parents.insert(neighbor, current_node);
                g_score.insert(neighbor, tentative_g);

                let f_score = tentative_g + heuristic(neighbor);

                open_set.push(AStarNode {
                    f_score,
                    g_score: tentative_g,
                    node: neighbor,
                });
                open_set_members.insert(neighbor);
            }
        }
    }

    // 没有找到路径
    None
}

/// 使用欧几里得距离的 A* 搜索（适用于空间数据）
///
/// # 参数
///
/// - `db`: 图数据库
/// - `start`: 起始节点 ID
/// - `end`: 目标节点 ID
/// - `get_pos`: 获取节点位置的函数，返回 (x, y) 坐标
/// - `cost_fn`: 边代价函数
///
/// # 示例
///
/// ```
/// use rs_graphdb::algorithms::astar_euclidean;
/// use rs_graphdb::graph::db::GraphDatabase;
/// use rs_graphdb::storage::mem_store::MemStore;
/// use rs_graphdb::values::Properties;
///
/// let mut db = GraphDatabase::<MemStore>::new_in_memory();
///
/// let a = db.create_node(vec![], {
///     let mut props = Properties::new();
///     props.insert("x".to_string(), rs_graphdb::values::Value::Int(0));
///     props.insert("y".to_string(), rs_graphdb::values::Value::Int(0));
///     props
/// });
///
/// let b = db.create_node(vec![], {
///     let mut props = Properties::new();
///     props.insert("x".to_string(), rs_graphdb::values::Value::Int(100));
///     props.insert("y".to_string(), rs_graphdb::values::Value::Int(100));
///     props
/// });
///
/// db.create_rel(a, b, "ROAD", Properties::new());
///
/// let get_pos = |node: u64| -> (f64, f64) {
///     if let Some(n) = db.get_node(node) {
///         let x = n.get("x").and_then(|v| match v {
///             rs_graphdb::values::Value::Int(i) => Some(*i as f64),
///             _ => None,
///         }).unwrap_or(0.0);
///         let y = n.get("y").and_then(|v| match v {
///             rs_graphdb::values::Value::Int(i) => Some(*i as f64),
///             _ => None,
///         }).unwrap_or(0.0);
///         (x, y)
///     } else {
///         (0.0, 0.0)
///     }
/// };
///
/// let path = astar_euclidean(&db, a, b, get_pos, &|_, _| 1.0);
/// assert!(path.is_some());
/// ```
pub fn astar_euclidean<E: StorageEngine, F, P>(
    db: &GraphDatabase<E>,
    start: NodeId,
    end: NodeId,
    get_pos: P,
    cost_fn: F,
) -> Option<Vec<NodeId>>
where
    F: Fn(NodeId, NodeId) -> f64,
    P: Fn(NodeId) -> (f64, f64) + Copy,
{
    let target_pos = get_pos(end);

    let heuristic = move |node: NodeId| -> f64 {
        let pos = get_pos(node);
        let dx = pos.0 - target_pos.0;
        let dy = pos.1 - target_pos.1;
        (dx * dx + dy * dy).sqrt()
    };

    astar(db, start, end, heuristic, cost_fn)
}

/// 使用曼哈顿距离的 A* 搜索（适用于网格数据）
///
/// # 参数
///
/// - `db`: 图数据库
/// - `start`: 起始节点 ID
/// - `end`: 目标节点 ID
/// - `get_pos`: 获取节点位置的函数，返回 (x, y) 坐标
/// - `cost_fn`: 边代价函数
pub fn astar_manhattan<E: StorageEngine, F, P>(
    db: &GraphDatabase<E>,
    start: NodeId,
    end: NodeId,
    get_pos: P,
    cost_fn: F,
) -> Option<Vec<NodeId>>
where
    F: Fn(NodeId, NodeId) -> f64,
    P: Fn(NodeId) -> (f64, f64) + Copy,
{
    let target_pos = get_pos(end);

    let heuristic = move |node: NodeId| -> f64 {
        let pos = get_pos(node);
        let dx = (pos.0 - target_pos.0).abs();
        let dy = (pos.1 - target_pos.1).abs();
        dx + dy
    };

    astar(db, start, end, heuristic, cost_fn)
}

// ============ 辅助结构和函数 ============

#[derive(Debug, Clone, Copy)]
struct AStarNode {
    f_score: f64,
    g_score: f64,
    node: NodeId,
}

// 实现排序：优先级队列会优先弹出 f_score 最小的节点
impl PartialEq for AStarNode {
    fn eq(&self, other: &Self) -> bool {
        self.node == other.node
    }
}

impl Eq for AStarNode {}

impl PartialOrd for AStarNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AStarNode {
    fn cmp(&self, other: &Self) -> Ordering {
        // 注意：BinaryHeap 是最大堆，所以我们需要反转排序
        // 使用 f_score 作为主排序键，g_score 作为次排序键
        match other.f_score.partial_cmp(&self.f_score).unwrap() {
            Ordering::Equal => {
                match other.g_score.partial_cmp(&self.g_score).unwrap() {
                    Ordering::Equal => self.node.cmp(&other.node),
                    other => other,
                }
            }
            other => other,
        }
    }
}

/// 获取节点的所有邻居（出边和入边）
fn get_all_neighbors<E: StorageEngine>(
    db: &GraphDatabase<E>,
    node_id: NodeId,
) -> Vec<NodeId> {
    let mut neighbors = Vec::new();
    let mut seen = HashSet::new();

    // 获取出边邻居
    for rel in db.engine.outgoing_rels(node_id) {
        if seen.insert(rel.end) {
            neighbors.push(rel.end);
        }
    }

    // 获取入边邻居
    for rel in db.engine.incoming_rels(node_id) {
        if seen.insert(rel.start) {
            neighbors.push(rel.start);
        }
    }

    neighbors
}

/// 从父节点映射重建路径
fn reconstruct_path(
    parents: &HashMap<NodeId, NodeId>,
    start: NodeId,
    end: NodeId,
) -> Option<Vec<NodeId>> {
    let mut path = vec![end];
    let mut current = end;

    while current != start {
        match parents.get(&current) {
            Some(&parent) => {
                path.push(parent);
                current = parent;
            }
            None => return None,
        }
    }

    path.reverse();
    Some(path)
}
