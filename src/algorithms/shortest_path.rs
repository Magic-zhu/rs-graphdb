use crate::graph::db::GraphDatabase;
use crate::storage::{NodeId, StorageEngine};
use std::collections::{HashMap, HashSet, VecDeque, BinaryHeap};
use std::cmp::Ordering;

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

/// BFS 最短路径（无权图）
pub fn bfs_shortest_path<E: StorageEngine>(
    db: &GraphDatabase<E>,
    start: NodeId,
    end: NodeId,
) -> Option<Vec<NodeId>> {
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
