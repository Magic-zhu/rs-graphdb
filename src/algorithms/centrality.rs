use crate::graph::db::GraphDatabase;
use crate::storage::{NodeId, StorageEngine};
use std::collections::{HashMap, HashSet, VecDeque};

/// 度中心性（Degree Centrality）
pub fn degree_centrality<E: StorageEngine>(
    db: &GraphDatabase<E>,
) -> HashMap<NodeId, f64> {
    let mut centrality = HashMap::new();
    let mut node_count = 0;

    for node in db.all_stored_nodes() {
        node_count += 1;
        let out_degree = db.neighbors_out(node.id).count();
        let in_degree = db.neighbors_in(node.id).count();
        let total_degree = (out_degree + in_degree) as f64;

        centrality.insert(node.id, total_degree);
    }

    // 归一化
    if node_count > 1 {
        let max_possible = (node_count - 1) as f64;
        for val in centrality.values_mut() {
            *val /= max_possible;
        }
    }

    centrality
}

/// 介数中心性（Betweenness Centrality）- 简化版
pub fn betweenness_centrality<E: StorageEngine>(
    db: &GraphDatabase<E>,
) -> HashMap<NodeId, f64> {
    let mut centrality: HashMap<NodeId, f64> = HashMap::new();
    let nodes: Vec<NodeId> = db.all_stored_nodes().map(|n| n.id).collect();

    for node in &nodes {
        centrality.insert(*node, 0.0);
    }

    // 对每对节点计算最短路径，统计每个节点被经过的次数
    for &source in &nodes {
        let paths = compute_shortest_paths(db, source, &nodes);

        for (target, path_nodes) in paths {
            if source != target {
                for &node in &path_nodes {
                    if node != source && node != target {
                        *centrality.get_mut(&node).unwrap() += 1.0;
                    }
                }
            }
        }
    }

    // 归一化
    let n = nodes.len();
    if n > 2 {
        let normalizer = ((n - 1) * (n - 2)) as f64;
        for val in centrality.values_mut() {
            *val /= normalizer;
        }
    }

    centrality
}

fn compute_shortest_paths<E: StorageEngine>(
    db: &GraphDatabase<E>,
    source: NodeId,
    all_nodes: &[NodeId],
) -> HashMap<NodeId, Vec<NodeId>> {
    let mut paths = HashMap::new();
    let mut queue = VecDeque::new();
    let mut visited = HashSet::new();
    let mut parent: HashMap<NodeId, NodeId> = HashMap::new();

    queue.push_back(source);
    visited.insert(source);

    while let Some(current) = queue.pop_front() {
        for rel in db.neighbors_out(current) {
            let neighbor = rel.end;
            if !visited.contains(&neighbor) {
                visited.insert(neighbor);
                parent.insert(neighbor, current);
                queue.push_back(neighbor);
            }
        }
    }

    // 重建所有路径
    for &target in all_nodes {
        if target == source || !parent.contains_key(&target) {
            continue;
        }

        let mut path = vec![];
        let mut node = target;
        while let Some(&p) = parent.get(&node) {
            path.push(node);
            node = p;
            if node == source {
                break;
            }
        }
        path.reverse();
        paths.insert(target, path);
    }

    paths
}
