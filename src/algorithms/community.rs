use crate::graph::db::GraphDatabase;
use crate::storage::{NodeId, StorageEngine};
use std::collections::{HashMap, HashSet, VecDeque};

/// 连通分量检测（Connected Components）
pub fn connected_components<E: StorageEngine>(
    db: &GraphDatabase<E>,
) -> HashMap<NodeId, usize> {
    let mut component_map: HashMap<NodeId, usize> = HashMap::new();
    let mut visited: HashSet<NodeId> = HashSet::new();
    let mut component_id = 0;

    let nodes: Vec<NodeId> = db.all_stored_nodes().map(|n| n.id).collect();

    for &node in &nodes {
        if visited.contains(&node) {
            continue;
        }

        // BFS 遍历整个连通分量
        let mut queue = VecDeque::new();
        queue.push_back(node);
        visited.insert(node);

        while let Some(current) = queue.pop_front() {
            component_map.insert(current, component_id);

            // 遍历出边
            for rel in db.neighbors_out(current) {
                let neighbor = rel.end;
                if !visited.contains(&neighbor) {
                    visited.insert(neighbor);
                    queue.push_back(neighbor);
                }
            }

            // 遍历入边（无向图处理）
            for rel in db.neighbors_in(current) {
                let neighbor = rel.start;
                if !visited.contains(&neighbor) {
                    visited.insert(neighbor);
                    queue.push_back(neighbor);
                }
            }
        }

        component_id += 1;
    }

    component_map
}

/// 获取每个连通分量的节点列表
pub fn get_components<E: StorageEngine>(
    db: &GraphDatabase<E>,
) -> Vec<Vec<NodeId>> {
    let component_map = connected_components(db);
    let mut components: HashMap<usize, Vec<NodeId>> = HashMap::new();

    for (node, comp_id) in component_map {
        components.entry(comp_id).or_default().push(node);
    }

    components.into_values().collect()
}
