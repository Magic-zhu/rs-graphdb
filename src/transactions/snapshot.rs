// 快照管理模块
//
// 提供图数据库状态的快照功能，用于事务回滚

use crate::storage::{NodeId, RelId};
use crate::values::Properties;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// 图状态快照
///
/// 记录某一时刻的完整图状态，用于回滚
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// 快照ID
    pub id: u64,
    /// 快照时间戳
    pub timestamp: u64,
    /// 节点数据
    pub nodes: HashMap<NodeId, SnapshotNode>,
    /// 关系数据
    pub rels: HashMap<RelId, SnapshotRel>,
    /// 出边索引
    pub outgoing: HashMap<NodeId, Vec<RelId>>,
    /// 入边索引
    pub incoming: HashMap<NodeId, Vec<RelId>>,
}

/// 快照中的节点数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotNode {
    pub id: NodeId,
    pub labels: Vec<String>,
    pub properties: Properties,
}

/// 快照中的关系数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotRel {
    pub id: RelId,
    pub start: NodeId,
    pub end: NodeId,
    pub typ: String,
    pub properties: Properties,
}

impl Snapshot {
    /// 创建新的空快照
    pub fn new(id: u64) -> Self {
        Self {
            id,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            nodes: HashMap::new(),
            rels: HashMap::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
        }
    }

    /// 添加节点到快照
    pub fn add_node(&mut self, node: SnapshotNode) {
        self.nodes.insert(node.id, node);
    }

    /// 添加关系到快照
    pub fn add_rel(&mut self, mut rel: SnapshotRel) {
        self.outgoing.entry(rel.start).or_default().push(rel.id);
        self.incoming.entry(rel.end).or_default().push(rel.id);
        self.rels.insert(rel.id, rel);
    }

    /// 获取节点数量
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// 获取关系数量
    pub fn rel_count(&self) -> usize {
        self.rels.len()
    }

    /// 清空快照
    pub fn clear(&mut self) {
        self.nodes.clear();
        self.rels.clear();
        self.outgoing.clear();
        self.incoming.clear();
    }
}

/// 快照管理器
///
/// 管理多个快照，提供快照的创建、恢复、清理功能
#[derive(Debug)]
pub struct SnapshotManager {
    /// 快照列表
    snapshots: Vec<Snapshot>,
    /// 下一个快照ID
    next_id: u64,
    /// 最大快照数量
    max_snapshots: usize,
}

impl SnapshotManager {
    /// 创建新的快照管理器
    pub fn new(max_snapshots: usize) -> Self {
        Self {
            snapshots: Vec::new(),
            next_id: 0,
            max_snapshots,
        }
    }

    /// 创建默认的快照管理器（最多保留10个快照）
    pub fn default() -> Self {
        Self::new(10)
    }

    /// 创建快照
    pub fn create_snapshot(&mut self) -> Snapshot {
        let id = self.next_id;
        self.next_id += 1;

        let snapshot = Snapshot::new(id);
        self.snapshots.push(snapshot);

        // 如果超过最大数量，删除最旧的快照
        if self.snapshots.len() > self.max_snapshots {
            self.snapshots.remove(0);
        }

        self.snapshots.last().unwrap().clone()
    }

    /// 获取最新快照
    pub fn latest(&self) -> Option<&Snapshot> {
        self.snapshots.last()
    }

    /// 获取指定ID的快照
    pub fn get(&self, id: u64) -> Option<&Snapshot> {
        self.snapshots.iter().find(|s| s.id == id)
    }

    /// 移除指定ID的快照
    pub fn remove(&mut self, id: u64) -> bool {
        if let Some(pos) = self.snapshots.iter().position(|s| s.id == id) {
            self.snapshots.remove(pos);
            true
        } else {
            false
        }
    }

    /// 清空所有快照
    pub fn clear(&mut self) {
        self.snapshots.clear();
    }

    /// 获取快照数量
    pub fn count(&self) -> usize {
        self.snapshots.len()
    }

    /// 获取所有快照ID
    pub fn snapshot_ids(&self) -> Vec<u64> {
        self.snapshots.iter().map(|s| s.id).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_creation() {
        let snapshot = Snapshot::new(1);
        assert_eq!(snapshot.id, 1);
        assert_eq!(snapshot.node_count(), 0);
        assert_eq!(snapshot.rel_count(), 0);
    }

    #[test]
    fn test_snapshot_add_node() {
        let mut snapshot = Snapshot::new(1);
        let node = SnapshotNode {
            id: 0,
            labels: vec!["Test".to_string()],
            properties: Properties::new(),
        };
        snapshot.add_node(node);
        assert_eq!(snapshot.node_count(), 1);
    }

    #[test]
    fn test_snapshot_manager() {
        let mut manager = SnapshotManager::new(3);

        let s1 = manager.create_snapshot();
        assert_eq!(s1.id, 0);
        assert_eq!(manager.count(), 1);

        let s2 = manager.create_snapshot();
        assert_eq!(s2.id, 1);
        assert_eq!(manager.count(), 2);

        assert!(manager.get(0).is_some());
        assert!(manager.get(1).is_some());
        assert!(manager.get(99).is_none());
    }

    #[test]
    fn test_snapshot_manager_max_limit() {
        let mut manager = SnapshotManager::new(2);

        manager.create_snapshot();
        manager.create_snapshot();
        manager.create_snapshot();

        // 应该只保留最新的2个快照
        assert_eq!(manager.count(), 2);
        assert!(manager.get(0).is_none());
        assert!(manager.get(1).is_some());
        assert!(manager.get(2).is_some());
    }

    #[test]
    fn test_snapshot_remove() {
        let mut manager = SnapshotManager::new(10);

        manager.create_snapshot();
        manager.create_snapshot();

        assert!(manager.remove(0));
        assert_eq!(manager.count(), 1);
        assert!(!manager.remove(0));
        assert!(manager.remove(1));
    }
}
