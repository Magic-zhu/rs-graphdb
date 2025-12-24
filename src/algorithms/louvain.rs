use crate::graph::db::GraphDatabase;
use crate::storage::{NodeId, StorageEngine};
use std::collections::{HashMap, HashSet};

/// Louvain 社区检测算法（简化版）
pub fn louvain<E: StorageEngine>(
    db: &GraphDatabase<E>,
    max_iterations: usize,
) -> HashMap<NodeId, usize> {
    let nodes: Vec<NodeId> = db.all_stored_nodes().map(|n| n.id).collect();

    // 初始化：每个节点是自己的社区
    let mut communities: HashMap<NodeId, usize> = nodes
        .iter()
        .enumerate()
        .map(|(i, &node)| (node, i))
        .collect();

    let m = count_edges(db);
    if m == 0 {
        return communities;
    }

    let mut improved = true;
    let mut iteration = 0;

    while improved && iteration < max_iterations {
        improved = false;
        iteration += 1;

        for &node in &nodes {
            let current_comm = communities[&node];

            // 计算移动到每个邻居社区的模块度增益
            let mut comm_gains: HashMap<usize, f64> = HashMap::new();

            for rel in db.neighbors_out(node) {
                let neighbor = rel.end;
                let neighbor_comm = communities[&neighbor];
                *comm_gains.entry(neighbor_comm).or_insert(0.0) += 1.0;
            }

            for rel in db.neighbors_in(node) {
                let neighbor = rel.start;
                let neighbor_comm = communities[&neighbor];
                *comm_gains.entry(neighbor_comm).or_insert(0.0) += 1.0;
            }

            // 找到最佳社区
            let best_comm = comm_gains
                .iter()
                .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
                .map(|(comm, _)| *comm);

            if let Some(best) = best_comm {
                if best != current_comm && comm_gains.get(&best).unwrap_or(&0.0) > &0.0 {
                    communities.insert(node, best);
                    improved = true;
                }
            }
        }
    }

    // 重新编号社区（确保连续）
    renumber_communities(communities)
}

fn count_edges<E: StorageEngine>(db: &GraphDatabase<E>) -> usize {
    db.all_stored_nodes()
        .map(|n| db.neighbors_out(n.id).count())
        .sum()
}

fn renumber_communities(communities: HashMap<NodeId, usize>) -> HashMap<NodeId, usize> {
    let unique_comms: HashSet<usize> = communities.values().copied().collect();
    let comm_map: HashMap<usize, usize> = unique_comms
        .into_iter()
        .enumerate()
        .map(|(i, old_id)| (old_id, i))
        .collect();

    communities
        .into_iter()
        .map(|(node, old_comm)| (node, comm_map[&old_comm]))
        .collect()
}
