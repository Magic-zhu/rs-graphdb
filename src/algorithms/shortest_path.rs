use crate::graph::db::GraphDatabase;
use crate::storage::{NodeId, StorageEngine};
use std::collections::{HashMap, HashSet, VecDeque, BinaryHeap};
use std::cmp::Ordering;

/// 所有最短路径（BFS 层级遍历，找到所有最短路径）
///
/// # 算法说明
///
/// 使用 BFS 分层遍历，找到从起点到终点的所有最短路径。
/// 算法步骤：
/// 1. 使用 BFS 进行层级遍历，记录每个节点到起点的距离
/// 2. 从终点开始回溯，只保留距离为 dist-1 的父节点
/// 3. 递归构建所有可能的最短路径
///
/// # 复杂度
///
/// - 时间复杂度: O(|V| + |E| + P)，其中 P 是最短路径的数量
/// - 空间复杂度: O(|V| + |E|)
///
/// # 参数
///
/// - `db`: 图数据库
/// - `start`: 起始节点 ID
/// - `end`: 目标节点 ID
///
/// # 返回
///
/// 返回所有最短路径的列表，每条路径是一个节点 ID 的向量。
/// 如果不存在路径，返回空列表。
///
/// # 示例
///
/// ```
/// use rs_graphdb::algorithms::all_shortest_paths;
/// use rs_graphdb::graph::db::GraphDatabase;
/// use rs_graphdb::storage::mem_store::MemStore;
/// use rs_graphdb::values::Properties;
///
/// let mut db = GraphDatabase::<MemStore>::new_in_memory();
///
/// let a = db.create_node(vec!["Node"], Properties::new());
/// let b = db.create_node(vec!["Node"], Properties::new());
/// let c = db.create_node(vec!["Node"], Properties::new());
/// let d = db.create_node(vec!["Node"], Properties::new());
///
/// // 创建多条路径: A->B->D 和 A->C->D
/// db.create_rel(a, b, "EDGE", Properties::new());
/// db.create_rel(b, d, "EDGE", Properties::new());
/// db.create_rel(a, c, "EDGE", Properties::new());
/// db.create_rel(c, d, "EDGE", Properties::new());
///
/// let paths = all_shortest_paths(&db, a, d);
///
/// // 应该找到两条最短路径，长度都是 3
/// assert_eq!(paths.len(), 2);
/// assert!(paths.iter().all(|p| p.len() == 3));
/// ```
pub fn all_shortest_paths<E: StorageEngine>(
    db: &GraphDatabase<E>,
    start: NodeId,
    end: NodeId,
) -> Vec<Vec<NodeId>> {
    // 特殊情况：起点和终点相同
    if start == end {
        return vec![vec![start]];
    }

    // 第一步：BFS 计算每个节点到起点的最短距离
    let mut distance: HashMap<NodeId, usize> = HashMap::new();
    let mut parents: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
    let mut queue = VecDeque::new();

    distance.insert(start, 0);
    queue.push_back(start);

    while let Some(current) = queue.pop_front() {
        let current_dist = distance[&current];

        // 如果已经到达终点且当前距离大于最短距离，停止探索该节点
        // 但要继续处理队列中已有的节点（可能还有其他最短路径）
        if let Some(&end_dist) = distance.get(&end) {
            if current_dist > end_dist {
                continue;
            }
        }

        // 探索所有出边邻居
        for rel in db.engine.outgoing_rels(current) {
            let neighbor = rel.end;

            match distance.get(&neighbor) {
                None => {
                    // 第一次访问该节点
                    distance.insert(neighbor, current_dist + 1);
                    parents.entry(neighbor).or_default().push(current);
                    queue.push_back(neighbor);
                }
                Some(&dist) if dist == current_dist + 1 => {
                    // 已经访问过，且距离相同，添加另一个父节点
                    parents.entry(neighbor).or_default().push(current);
                }
                _ => {
                    // 距离更长，忽略
                }
            }
        }
    }

    // 如果终点不可达，返回空列表
    if !distance.contains_key(&end) {
        return Vec::new();
    }

    // 第二步：从终点回溯，构建所有最短路径
    let mut paths = Vec::new();
    let mut current_path = Vec::new();
    build_all_paths(&parents, end, start, &mut current_path, &mut paths);

    paths
}

/// 递归构建所有路径
///
/// 从终点开始，通过 parents 映射回溯到起点，构建所有可能的路径
fn build_all_paths(
    parents: &HashMap<NodeId, Vec<NodeId>>,
    current: NodeId,
    start: NodeId,
    current_path: &mut Vec<NodeId>,
    all_paths: &mut Vec<Vec<NodeId>>,
) {
    current_path.push(current);

    if current == start {
        // 到达起点，构建完整路径并反转
        let mut path = current_path.clone();
        path.reverse();
        all_paths.push(path);
    } else if let Some(parents_list) = parents.get(&current) {
        // 递归处理所有父节点
        for &parent in parents_list {
            build_all_paths(parents, parent, start, current_path, all_paths);
        }
    }

    current_path.pop();
}

/// 带关系类型过滤的所有最短路径
///
/// # 参数
///
/// - `db`: 图数据库
/// - `start`: 起始节点 ID
/// - `end`: 目标节点 ID
/// - `rel_types`: 关系类型过滤器，None 表示不考虑类型
///
/// # 示例
///
/// ```
/// use rs_graphdb::algorithms::all_shortest_paths_by_rel_type;
/// use rs_graphdb::graph::db::GraphDatabase;
/// use rs_graphdb::storage::mem_store::MemStore;
/// use rs_graphdb::values::Properties;
///
/// let mut db = GraphDatabase::<MemStore>::new_in_memory();
///
/// let a = db.create_node(vec!["Node"], Properties::new());
/// let b = db.create_node(vec!["Node"], Properties::new());
/// let c = db.create_node(vec!["Node"], Properties::new());
///
/// db.create_rel(a, b, "FRIEND", Properties::new());
/// db.create_rel(b, c, "KNOWS", Properties::new());
/// db.create_rel(a, c, "FRIEND", Properties::new());
///
/// // 只考虑 FRIEND 类型的边
/// let paths = all_shortest_paths_by_rel_type(&db, a, c, Some(&["FRIEND"]));
///
/// // 应该只找到直接路径 A->C
/// assert_eq!(paths.len(), 1);
/// assert_eq!(paths[0], vec![a, c]);
/// ```
pub fn all_shortest_paths_by_rel_type<E: StorageEngine>(
    db: &GraphDatabase<E>,
    start: NodeId,
    end: NodeId,
    rel_types: Option<&[&str]>,
) -> Vec<Vec<NodeId>> {
    let rel_types_set: Option<HashSet<&str>> = rel_types.map(|types| types.iter().cloned().collect());

    // 特殊情况：起点和终点相同
    if start == end {
        return vec![vec![start]];
    }

    // 第一步：BFS 计算每个节点到起点的最短距离
    let mut distance: HashMap<NodeId, usize> = HashMap::new();
    let mut parents: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
    let mut queue = VecDeque::new();

    distance.insert(start, 0);
    queue.push_back(start);

    while let Some(current) = queue.pop_front() {
        let current_dist = distance[&current];

        // 如果已经到达终点且当前距离大于最短距离，停止探索该节点
        // 但要继续处理队列中已有的节点（可能还有其他最短路径）
        if let Some(&end_dist) = distance.get(&end) {
            if current_dist > end_dist {
                continue;
            }
        }

        // 探索所有出边邻居（考虑关系类型过滤）
        for rel in db.engine.outgoing_rels(current) {
            // 关系类型过滤
            if let Some(ref types) = rel_types_set {
                if !types.contains(rel.typ.as_str()) {
                    continue;
                }
            }

            let neighbor = rel.end;

            match distance.get(&neighbor) {
                None => {
                    // 第一次访问该节点
                    distance.insert(neighbor, current_dist + 1);
                    parents.entry(neighbor).or_default().push(current);
                    queue.push_back(neighbor);
                }
                Some(&dist) if dist == current_dist + 1 => {
                    // 已经访问过，且距离相同，添加另一个父节点
                    parents.entry(neighbor).or_default().push(current);
                }
                _ => {
                    // 距离更长，忽略
                }
            }
        }
    }

    // 如果终点不可达，返回空列表
    if !distance.contains_key(&end) {
        return Vec::new();
    }

    // 第二步：从终点回溯，构建所有最短路径
    let mut paths = Vec::new();
    let mut current_path = Vec::new();
    build_all_paths(&parents, end, start, &mut current_path, &mut paths);

    paths
}

/// 获取所有最短路径的数量
///
/// # 示例
///
/// ```
/// use rs_graphdb::algorithms::count_all_shortest_paths;
/// use rs_graphdb::graph::db::GraphDatabase;
/// use rs_graphdb::storage::mem_store::MemStore;
/// use rs_graphdb::values::Properties;
///
/// let mut db = GraphDatabase::<MemStore>::new_in_memory();
///
/// let a = db.create_node(vec!["Node"], Properties::new());
/// let b = db.create_node(vec!["Node"], Properties::new());
/// let c = db.create_node(vec!["Node"], Properties::new());
///
/// db.create_rel(a, b, "EDGE", Properties::new());
/// db.create_rel(b, c, "EDGE", Properties::new());
/// db.create_rel(a, c, "EDGE", Properties::new());
///
/// // 两条路径：A->B->C 和 A->C
/// let count = count_all_shortest_paths(&db, a, c);
/// assert_eq!(count, 2);
/// ```
pub fn count_all_shortest_paths<E: StorageEngine>(
    db: &GraphDatabase<E>,
    start: NodeId,
    end: NodeId,
) -> usize {
    all_shortest_paths(db, start, end).len()
}

/// 检查两个节点之间是否有路径
///
/// 返回 true 如果存在至少一条路径从 start 到 end
///
/// # 示例
///
/// ```
/// use rs_graphdb::algorithms::has_path;
/// use rs_graphdb::graph::db::GraphDatabase;
/// use rs_graphdb::storage::mem_store::MemStore;
/// use rs_graphdb::values::Properties;
///
/// let mut db = GraphDatabase::<MemStore>::new_in_memory();
///
/// let a = db.create_node(vec!["Node"], Properties::new());
/// let b = db.create_node(vec!["Node"], Properties::new());
/// let c = db.create_node(vec!["Node"], Properties::new());
///
/// db.create_rel(a, b, "EDGE", Properties::new());
/// // b 和 c 之间没有连接
///
/// assert!(has_path(&db, a, b));
/// assert!(!has_path(&db, a, c));
/// ```
pub fn has_path<E: StorageEngine>(
    db: &GraphDatabase<E>,
    start: NodeId,
    end: NodeId,
) -> bool {
    if start == end {
        return true;
    }
    bfs_shortest_path(db, start, end).is_some()
}

/// BFS 最短路径（无权图）
pub fn bfs_shortest_path<E: StorageEngine>(
    db: &GraphDatabase<E>,
    start: NodeId,
    end: NodeId,
) -> Option<Vec<NodeId>> {
    bfs_shortest_path_by_rel_type(db, start, end, None)
}

/// 带关系类型过滤的 BFS 最短路径
pub fn bfs_shortest_path_by_rel_type<E: StorageEngine>(
    db: &GraphDatabase<E>,
    start: NodeId,
    end: NodeId,
    rel_types: Option<&[&str]>,
) -> Option<Vec<NodeId>> {
    let rel_types_set: Option<HashSet<&str>> = rel_types.map(|types| types.iter().cloned().collect());

    let mut queue = VecDeque::new();
    let mut visited = HashSet::new();
    let mut parent: HashMap<NodeId, NodeId> = HashMap::new();

    queue.push_back(start);
    visited.insert(start);

    while let Some(current) = queue.pop_front() {
        if current == end {
            // 重建路径
            let mut path = vec![end];
            let mut node = end;
            while let Some(&p) = parent.get(&node) {
                path.push(p);
                node = p;
                if node == start {
                    break;
                }
            }
            path.reverse();
            return Some(path);
        }

        for rel in db.neighbors_out(current) {
            // 关系类型过滤
            if let Some(ref types) = rel_types_set {
                if !types.contains(rel.typ.as_str()) {
                    continue;
                }
            }

            let neighbor = rel.end;
            if !visited.contains(&neighbor) {
                visited.insert(neighbor);
                parent.insert(neighbor, current);
                queue.push_back(neighbor);
            }
        }
    }

    None
}

#[derive(Copy, Clone, Eq, PartialEq)]
struct State {
    cost: usize,
    node: NodeId,
}

impl Ord for State {
    fn cmp(&self, other: &Self) -> Ordering {
        other.cost.cmp(&self.cost)
    }
}

impl PartialOrd for State {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Dijkstra 最短路径（加权图，暂时假设所有边权重为 1）
pub fn dijkstra<E: StorageEngine>(
    db: &GraphDatabase<E>,
    start: NodeId,
    end: NodeId,
) -> Option<(Vec<NodeId>, usize)> {
    let mut heap = BinaryHeap::new();
    let mut dist: HashMap<NodeId, usize> = HashMap::new();
    let mut parent: HashMap<NodeId, NodeId> = HashMap::new();

    dist.insert(start, 0);
    heap.push(State { cost: 0, node: start });

    while let Some(State { cost, node }) = heap.pop() {
        if node == end {
            let mut path = vec![end];
            let mut current = end;
            while let Some(&p) = parent.get(&current) {
                path.push(p);
                current = p;
                if current == start {
                    break;
                }
            }
            path.reverse();
            return Some((path, cost));
        }

        if cost > *dist.get(&node).unwrap_or(&usize::MAX) {
            continue;
        }

        for rel in db.neighbors_out(node) {
            let neighbor = rel.end;
            let next_cost = cost + 1; // 暂时假设所有边权重为 1

            if next_cost < *dist.get(&neighbor).unwrap_or(&usize::MAX) {
                dist.insert(neighbor, next_cost);
                parent.insert(neighbor, node);
                heap.push(State {
                    cost: next_cost,
                    node: neighbor,
                });
            }
        }
    }

    None
}
