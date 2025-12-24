use crate::graph::db::GraphDatabase;
use crate::storage::{NodeId, StorageEngine};
use std::collections::HashMap;

/// PageRank 算法
/// 参数:
/// - damping: 阻尼系数 (通常为 0.85)
/// - iterations: 迭代次数
pub fn pagerank<E: StorageEngine>(
    db: &GraphDatabase<E>,
    damping: f64,
    iterations: usize,
) -> HashMap<NodeId, f64> {
    let nodes: Vec<NodeId> = db.all_stored_nodes().map(|n| n.id).collect();
    let n = nodes.len();

    if n == 0 {
        return HashMap::new();
    }

    let initial_rank = 1.0 / n as f64;
    let mut ranks: HashMap<NodeId, f64> = nodes.iter().map(|&id| (id, initial_rank)).collect();

    // 计算每个节点的出度
    let out_degree: HashMap<NodeId, usize> = nodes
        .iter()
        .map(|&id| {
            let degree = db.neighbors_out(id).count();
            (id, degree)
        })
        .collect();

    for _ in 0..iterations {
        let mut new_ranks: HashMap<NodeId, f64> = HashMap::new();

        for &node in &nodes {
            let mut rank = (1.0 - damping) / n as f64;

            // 遍历所有指向当前节点的节点
            for rel in db.neighbors_in(node) {
                let from_node = rel.start;
                let from_rank = ranks.get(&from_node).copied().unwrap_or(0.0);
                let from_out_degree = out_degree.get(&from_node).copied().unwrap_or(1);

                if from_out_degree > 0 {
                    rank += damping * (from_rank / from_out_degree as f64);
                }
            }

            new_ranks.insert(node, rank);
        }

        ranks = new_ranks;
    }

    // 归一化
    let sum: f64 = ranks.values().sum();
    if sum > 0.0 {
        for val in ranks.values_mut() {
            *val /= sum;
        }
    }

    ranks
}
