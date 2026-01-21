//! 图遍历算法模块
//!
//! 提供多种图遍历算法：
//! - BFS (广度优先搜索)
//! - DFS (深度优先搜索)
//! - 可变长路径遍历
//! - 全路径探索
//! - 无向图遍历

use crate::graph::db::GraphDatabase;
use crate::storage::{NodeId, RelId, StorageEngine};
use std::collections::{HashMap, HashSet, VecDeque};

/// 路径结构，包含节点和关系的完整序列
#[derive(Debug, Clone, PartialEq)]
pub struct Path {
    pub nodes: Vec<NodeId>,
    pub rels: Vec<RelId>,
}

impl Path {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            rels: Vec::new(),
        }
    }

    pub fn with_start(start: NodeId) -> Self {
        Self {
            nodes: vec![start],
            rels: Vec::new(),
        }
    }

    pub fn extend(&mut self, node: NodeId, rel: RelId) {
        self.nodes.push(node);
        self.rels.push(rel);
    }

    pub fn length(&self) -> usize {
        self.rels.len()
    }

    pub fn start_node(&self) -> Option<NodeId> {
        self.nodes.first().copied()
    }

    pub fn end_node(&self) -> Option<NodeId> {
        self.nodes.last().copied()
    }

    pub fn contains_node(&self, node: NodeId) -> bool {
        self.nodes.contains(&node)
    }
}

/// 广度优先搜索 (BFS)
///
/// 从起始节点开始，按层次遍历图中的所有可达节点
pub fn bfs<E: StorageEngine>(
    db: &GraphDatabase<E>,
    start: NodeId,
    max_depth: Option<usize>,
) -> Vec<NodeId> {
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    let mut result = Vec::new();

    queue.push_back((start, 0));
    visited.insert(start);

    while let Some((node, depth)) = queue.pop_front() {
        result.push(node);

        // 检查深度限制
        if let Some(max) = max_depth {
            if depth >= max {
                continue;
            }
        }

        // 遍历所有出边
        for rel in db.neighbors_out(node) {
            let neighbor = rel.end;
            if !visited.contains(&neighbor) {
                visited.insert(neighbor);
                queue.push_back((neighbor, depth + 1));
            }
        }
    }

    result
}

/// 深度优先搜索 (DFS)
///
/// 从起始节点开始，沿一条路径尽可能深地遍历
pub fn dfs<E: StorageEngine>(
    db: &GraphDatabase<E>,
    start: NodeId,
    max_depth: Option<usize>,
) -> Vec<NodeId> {
    let mut visited = HashSet::new();
    let mut result = Vec::new();
    dfs_recursive(db, start, max_depth, 0, &mut visited, &mut result);
    result
}

fn dfs_recursive<E: StorageEngine>(
    db: &GraphDatabase<E>,
    node: NodeId,
    max_depth: Option<usize>,
    depth: usize,
    visited: &mut HashSet<NodeId>,
    result: &mut Vec<NodeId>,
) {
    visited.insert(node);
    result.push(node);

    // 检查深度限制
    if let Some(max) = max_depth {
        if depth >= max {
            return;
        }
    }

    // 递归遍历所有出边
    for rel in db.neighbors_out(node) {
        let neighbor = rel.end;
        if !visited.contains(&neighbor) {
            dfs_recursive(db, neighbor, max_depth, depth + 1, visited, result);
        }
    }
}

/// 带关系类型过滤的 BFS
///
/// 只遍历指定类型的关系
pub fn bfs_by_rel_type<E: StorageEngine>(
    db: &GraphDatabase<E>,
    start: NodeId,
    rel_types: &[&str],
    max_depth: Option<usize>,
) -> Vec<NodeId> {
    let rel_types_set: HashSet<&str> = rel_types.iter().cloned().collect();
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    let mut result = Vec::new();

    queue.push_back((start, 0));
    visited.insert(start);

    while let Some((node, depth)) = queue.pop_front() {
        result.push(node);

        if let Some(max) = max_depth {
            if depth >= max {
                continue;
            }
        }

        for rel in db.neighbors_out(node) {
            // 只处理指定类型的关系
            if !rel_types_set.contains(rel.typ.as_str()) {
                continue;
            }

            let neighbor = rel.end;
            if !visited.contains(&neighbor) {
                visited.insert(neighbor);
                queue.push_back((neighbor, depth + 1));
            }
        }
    }

    result
}

/// 可变长路径遍历
///
/// 查找从 start 到 end 的所有路径，路径长度在 min_hops 到 max_hops 之间
pub fn variable_length_path<E: StorageEngine>(
    db: &GraphDatabase<E>,
    start: NodeId,
    end: NodeId,
    min_hops: usize,
    max_hops: usize,
) -> Vec<Vec<NodeId>> {
    let mut paths = Vec::new();
    let mut current_path = vec![start];
    variable_length_path_recursive(db, start, end, min_hops, max_hops, &mut current_path, &mut paths);
    paths
}

fn variable_length_path_recursive<E: StorageEngine>(
    db: &GraphDatabase<E>,
    current: NodeId,
    target: NodeId,
    min_hops: usize,
    max_hops: usize,
    current_path: &mut Vec<NodeId>,
    all_paths: &mut Vec<Vec<NodeId>>,
) {
    let current_depth = current_path.len() - 1;

    // 检查是否达到目标节点且满足最小跳数
    if current == target && current_depth >= min_hops {
        all_paths.push(current_path.clone());
        // 如果达到最大跳数，不再继续探索
        if current_depth >= max_hops {
            return;
        }
    }

    // 超过最大跳数，停止探索
    if current_depth >= max_hops {
        return;
    }

    // 遍历所有出边
    for rel in db.neighbors_out(current) {
        let neighbor = rel.end;

        // 避免循环（简单检查：节点不重复）
        if current_path.contains(&neighbor) {
            continue;
        }

        current_path.push(neighbor);
        variable_length_path_recursive(db, neighbor, target, min_hops, max_hops, current_path, all_paths);
        current_path.pop();
    }
}

/// 全路径探索
///
/// 查找从 start 到 end 的所有简单路径（无重复节点）
pub fn all_simple_paths<E: StorageEngine>(
    db: &GraphDatabase<E>,
    start: NodeId,
    end: NodeId,
    max_depth: Option<usize>,
) -> Vec<Vec<NodeId>> {
    let mut paths = Vec::new();
    let mut current_path = vec![start];
    let mut visited = HashSet::new();
    visited.insert(start);

    all_simple_paths_recursive(db, start, end, 0, max_depth, &mut current_path, &mut visited, &mut paths);

    paths
}

fn all_simple_paths_recursive<E: StorageEngine>(
    db: &GraphDatabase<E>,
    current: NodeId,
    target: NodeId,
    depth: usize,
    max_depth: Option<usize>,
    current_path: &mut Vec<NodeId>,
    visited: &mut HashSet<NodeId>,
    all_paths: &mut Vec<Vec<NodeId>>,
) {
    // 达到目标节点
    if current == target && current_path.len() > 1 {
        all_paths.push(current_path.clone());
        // 如果有深度限制且已达到，不再继续
        if max_depth.map_or(false, |max| depth >= max) {
            return;
        }
    }

    // 检查深度限制
    if max_depth.map_or(false, |max| depth >= max) {
        return;
    }

    // 遍历所有出边
    for rel in db.neighbors_out(current) {
        let neighbor = rel.end;

        // 跳过已访问的节点
        if visited.contains(&neighbor) {
            continue;
        }

        // 标记访问
        visited.insert(neighbor);
        current_path.push(neighbor);

        // 递归探索
        all_simple_paths_recursive(db, neighbor, target, depth + 1, max_depth, current_path, visited, all_paths);

        // 回溯
        current_path.pop();
        visited.remove(&neighbor);
    }
}

/// 无向图遍历（同时考虑出边和入边）
///
/// 将有向图视为无向图进行遍历
pub fn undirected_bfs<E: StorageEngine>(
    db: &GraphDatabase<E>,
    start: NodeId,
    max_depth: Option<usize>,
) -> Vec<NodeId> {
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    let mut result = Vec::new();

    queue.push_back((start, 0));
    visited.insert(start);

    while let Some((node, depth)) = queue.pop_front() {
        result.push(node);

        if let Some(max) = max_depth {
            if depth >= max {
                continue;
            }
        }

        // 遍历出边
        for rel in db.neighbors_out(node) {
            let neighbor = rel.end;
            if !visited.contains(&neighbor) {
                visited.insert(neighbor);
                queue.push_back((neighbor, depth + 1));
            }
        }

        // 遍历入边（无向图）
        for rel in db.neighbors_in(node) {
            let neighbor = rel.start;
            if !visited.contains(&neighbor) {
                visited.insert(neighbor);
                queue.push_back((neighbor, depth + 1));
            }
        }
    }

    result
}

/// 带关系类型的可变长路径遍历
///
/// 类似 Neo4j 的 (a)-[:REL*min..max]->(b) 语法
pub fn variable_length_path_by_rel_type<E: StorageEngine>(
    db: &GraphDatabase<E>,
    start: NodeId,
    rel_type: &str,
    min_hops: usize,
    max_hops: usize,
) -> Vec<Vec<NodeId>> {
    let rel_type = rel_type.to_string();
    let mut paths = Vec::new();

    // BFS 按层次遍历，收集所有满足条件的路径
    let mut queue: Vec<(NodeId, Vec<NodeId>, usize)> = vec![(start, vec![start], 0)];
    let mut all_end_nodes = Vec::new();

    while let Some((current, path, hops)) = queue.pop() {
        // 如果达到最小跳数，记录这条路径
        if hops >= min_hops {
            all_end_nodes.push((current, path.clone()));
        }

        // 如果达到最大跳数，停止扩展
        if hops >= max_hops {
            continue;
        }

        // 扩展路径
        for rel in db.neighbors_out(current) {
            if rel.typ == rel_type {
                let neighbor = rel.end;
                if !path.contains(&neighbor) {
                    let mut new_path = path.clone();
                    new_path.push(neighbor);
                    queue.push((neighbor, new_path, hops + 1));
                }
            }
        }
    }

    // 去重并返回所有满足条件的路径
    let mut seen = HashSet::new();
    for (_, path) in all_end_nodes {
        let path_key = path.clone();
        if !seen.contains(&path_key) {
            seen.insert(path_key);
            paths.push(path);
        }
    }

    paths
}

/// 获取从 start 可达的所有节点（带关系类型过滤）
pub fn reachable_nodes<E: StorageEngine>(
    db: &GraphDatabase<E>,
    start: NodeId,
    rel_types: Option<&[&str]>,
) -> HashSet<NodeId> {
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();

    queue.push_back(start);
    visited.insert(start);

    let rel_types_set: Option<HashSet<&str>> = rel_types.map(|types| types.iter().cloned().collect());

    while let Some(node) = queue.pop_front() {
        for rel in db.neighbors_out(node) {
            // 如果指定了关系类型，进行过滤
            if let Some(ref types) = rel_types_set {
                if !types.contains(rel.typ.as_str()) {
                    continue;
                }
            }

            let neighbor = rel.end;
            if !visited.contains(&neighbor) {
                visited.insert(neighbor);
                queue.push_back(neighbor);
            }
        }
    }

    visited
}

/// 查找两点之间最短路径（返回完整路径，包括关系）
pub fn shortest_path_with_rels<E: StorageEngine>(
    db: &GraphDatabase<E>,
    start: NodeId,
    end: NodeId,
) -> Option<Path> {
    let mut queue = VecDeque::new();
    let mut visited = HashSet::new();
    let mut parent_node: HashMap<NodeId, NodeId> = HashMap::new();
    let mut parent_rel: HashMap<NodeId, RelId> = HashMap::new();

    queue.push_back(start);
    visited.insert(start);

    while let Some(current) = queue.pop_front() {
        if current == end {
            // 重建路径
            let mut nodes = vec![end];
            let mut rels = Vec::new();
            let mut node = end;

            while let Some(&p) = parent_node.get(&node) {
                nodes.push(p);
                if let Some(&rel_id) = parent_rel.get(&node) {
                    rels.push(rel_id);
                }
                node = p;
                if node == start {
                    break;
                }
            }

            nodes.reverse();
            rels.reverse();

            return Some(Path { nodes, rels });
        }

        for rel in db.neighbors_out(current) {
            let neighbor = rel.end;
            if !visited.contains(&neighbor) {
                visited.insert(neighbor);
                parent_node.insert(neighbor, current);
                parent_rel.insert(neighbor, rel.id);
                queue.push_back(neighbor);
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mem_store::MemStore;
    use crate::values::Properties;

    fn create_test_graph() -> GraphDatabase<MemStore> {
        let mut db = GraphDatabase::new_in_memory();

        // 创建一个简单的测试图:
        //     1
        //    / \
        //   2   3
        //  / \   \
        // 4   5   6
        //      \
        //       7

        let n1 = db.create_node(vec!["Node"], Properties::new());
        let n2 = db.create_node(vec!["Node"], Properties::new());
        let n3 = db.create_node(vec!["Node"], Properties::new());
        let n4 = db.create_node(vec!["Node"], Properties::new());
        let n5 = db.create_node(vec!["Node"], Properties::new());
        let n6 = db.create_node(vec!["Node"], Properties::new());
        let n7 = db.create_node(vec!["Node"], Properties::new());

        db.create_rel(n1, n2, "EDGE", Properties::new());
        db.create_rel(n1, n3, "EDGE", Properties::new());
        db.create_rel(n2, n4, "EDGE", Properties::new());
        db.create_rel(n2, n5, "EDGE", Properties::new());
        db.create_rel(n3, n6, "EDGE", Properties::new());
        db.create_rel(n5, n7, "EDGE", Properties::new());

        db
    }

    #[test]
    fn test_bfs_traversal() {
        let db = create_test_graph();
        let n1 = 0; // 第一个创建的节点

        let result = bfs(&db, n1, None);

        // BFS 应该按层次顺序访问所有节点
        assert_eq!(result.len(), 7);
        assert_eq!(result[0], n1);
    }

    #[test]
    fn test_bfs_with_depth_limit() {
        let db = create_test_graph();
        let n1 = 0;

        let result = bfs(&db, n1, Some(2));

        // 深度为 2，应该只能访问到节点 1, 2, 3, 4, 5, 6（节点 7 在深度 3）
        assert_eq!(result.len(), 6);
    }

    #[test]
    fn test_dfs_traversal() {
        let db = create_test_graph();
        let n1 = 0;

        let result = dfs(&db, n1, None);

        // DFS 应该访问所有节点
        assert_eq!(result.len(), 7);
    }

    #[test]
    fn test_variable_length_path() {
        let mut db = GraphDatabase::new_in_memory();

        // 创建线性图: 0 -> 1 -> 2 -> 3 -> 4
        let nodes: Vec<NodeId> = (0..5)
            .map(|_| db.create_node(vec!["Node"], Properties::new()))
            .collect();

        for i in 0..4 {
            db.create_rel(nodes[i], nodes[i + 1], "EDGE", Properties::new());
        }

        // 查找长度为 2-3 的路径
        let paths = variable_length_path(&db, nodes[0], nodes[3], 2, 3);

        // 应该找到一条路径: [0, 1, 2, 3]
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].len(), 4);
    }

    #[test]
    fn test_all_simple_paths() {
        let mut db = GraphDatabase::new_in_memory();

        // 创建菱形图:
        //     0
        //    / \
        //   1   2
        //    \ /
        //     3
        let n0 = db.create_node(vec!["Node"], Properties::new());
        let n1 = db.create_node(vec!["Node"], Properties::new());
        let n2 = db.create_node(vec!["Node"], Properties::new());
        let n3 = db.create_node(vec!["Node"], Properties::new());

        db.create_rel(n0, n1, "EDGE", Properties::new());
        db.create_rel(n0, n2, "EDGE", Properties::new());
        db.create_rel(n1, n3, "EDGE", Properties::new());
        db.create_rel(n2, n3, "EDGE", Properties::new());

        // 查找所有从 0 到 3 的路径
        let paths = all_simple_paths(&db, n0, n3, None);

        // 应该找到两条路径: [0,1,3] 和 [0,2,3]
        assert_eq!(paths.len(), 2);
    }

    #[test]
    fn test_undirected_bfs() {
        let mut db = GraphDatabase::new_in_memory();

        // 创建有向图: 0 -> 1 <- 2
        let n0 = db.create_node(vec!["Node"], Properties::new());
        let n1 = db.create_node(vec!["Node"], Properties::new());
        let n2 = db.create_node(vec!["Node"], Properties::new());

        db.create_rel(n0, n1, "EDGE", Properties::new());
        db.create_rel(n2, n1, "EDGE", Properties::new());

        // 有向 BFS 从 0 开始只能到达 0, 1
        let directed_result = bfs(&db, n0, None);
        assert_eq!(directed_result.len(), 2);

        // 无向 BFS 从 0 可以到达 0, 1, 2
        let undirected_result = undirected_bfs(&db, n0, None);
        assert_eq!(undirected_result.len(), 3);
    }

    #[test]
    fn test_bfs_by_rel_type() {
        let mut db = GraphDatabase::new_in_memory();

        let n0 = db.create_node(vec!["Node"], Properties::new());
        let n1 = db.create_node(vec!["Node"], Properties::new());
        let n2 = db.create_node(vec!["Node"], Properties::new());

        db.create_rel(n0, n1, "FRIEND", Properties::new());
        db.create_rel(n0, n2, "ENEMY", Properties::new());

        // 只遍历 FRIEND 关系
        let result = bfs_by_rel_type(&db, n0, &["FRIEND"], None);

        assert_eq!(result.len(), 2); // n0 和 n1
        assert!(result.contains(&n0));
        assert!(result.contains(&n1));
        assert!(!result.contains(&n2));
    }

    #[test]
    fn test_reachable_nodes() {
        let db = create_test_graph();
        let n1 = 0;

        let reachable = reachable_nodes(&db, n1, None);

        // 从 n1 可以到达所有节点
        assert_eq!(reachable.len(), 7);
    }

    #[test]
    fn test_shortest_path_with_rels() {
        let mut db = GraphDatabase::new_in_memory();

        // 创建简单路径: 0 -> 1 -> 2
        let n0 = db.create_node(vec!["Node"], Properties::new());
        let n1 = db.create_node(vec!["Node"], Properties::new());
        let n2 = db.create_node(vec!["Node"], Properties::new());

        db.create_rel(n0, n1, "EDGE", Properties::new());
        db.create_rel(n1, n2, "EDGE", Properties::new());

        let path = shortest_path_with_rels(&db, n0, n2);

        assert!(path.is_some());
        let path = path.unwrap();
        assert_eq!(path.nodes, vec![n0, n1, n2]);
        assert_eq!(path.rels.len(), 2);
    }
}
