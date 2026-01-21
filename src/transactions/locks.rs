// 锁管理模块
//
// 提供悲观锁机制，用于控制并发访问

use crate::storage::{NodeId, RelId};
use std::collections::{HashMap, HashSet};
use std::fmt;

/// 锁类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockType {
    /// 读锁（共享锁）
    Read,
    /// 写锁（排他锁）
    Write,
}

impl fmt::Display for LockType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LockType::Read => write!(f, "READ"),
            LockType::Write => write!(f, "WRITE"),
        }
    }
}

/// 锁请求
#[derive(Debug, Clone)]
pub struct LockRequest {
    /// 事务ID
    pub tx_id: u64,
    /// 锁类型
    pub lock_type: LockType,
    /// 请求时间
    pub request_time: u64,
}

impl LockRequest {
    pub fn new(tx_id: u64, lock_type: LockType) -> Self {
        Self {
            tx_id,
            lock_type,
            request_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
}

/// 锁条目
#[derive(Debug, Clone)]
pub struct LockEntry {
    /// 节点锁
    pub node_locks: HashMap<NodeId, Vec<LockRequest>>,
    /// 关系锁
    pub rel_locks: HashMap<RelId, Vec<LockRequest>>,
}

impl LockEntry {
    pub fn new() -> Self {
        Self {
            node_locks: HashMap::new(),
            rel_locks: HashMap::new(),
        }
    }

    /// 检查是否可以获取锁
    pub fn can_acquire(&self, tx_id: u64, lock_type: LockType) -> bool {
        // 写锁需要排他访问
        if lock_type == LockType::Write {
            // 检查是否有其他事务持有任何锁
            for locks in self.node_locks.values() {
                for req in locks {
                    if req.tx_id != tx_id {
                        return false;
                    }
                }
            }
            for locks in self.rel_locks.values() {
                for req in locks {
                    if req.tx_id != tx_id {
                        return false;
                    }
                }
            }
        } else {
            // 读锁：检查是否有写锁
            for locks in self.node_locks.values() {
                for req in locks {
                    if req.tx_id != tx_id && req.lock_type == LockType::Write {
                        return false;
                    }
                }
            }
            for locks in self.rel_locks.values() {
                for req in locks {
                    if req.tx_id != tx_id && req.lock_type == LockType::Write {
                        return false;
                    }
                }
            }
        }

        true
    }
}

impl Default for LockEntry {
    fn default() -> Self {
        Self::new()
    }
}

/// 锁管理器
///
/// 管理所有锁的获取和释放
#[derive(Debug)]
pub struct LockManager {
    /// 节点锁
    node_locks: HashMap<NodeId, LockEntry>,
    /// 关系锁
    rel_locks: HashMap<RelId, LockEntry>,
    /// 等待队列（用于死锁检测）
    wait_queue: HashMap<u64, HashSet<(NodeId, RelId)>>,
    /// 死锁检测超时（秒）
    deadlock_timeout: u64,
}

impl LockManager {
    /// 创建新的锁管理器
    pub fn new() -> Self {
        Self {
            node_locks: HashMap::new(),
            rel_locks: HashMap::new(),
            wait_queue: HashMap::new(),
            deadlock_timeout: 30, // 默认30秒超时
        }
    }

    /// 尝试获取节点锁
    pub fn acquire_node_lock(
        &mut self,
        tx_id: u64,
        node_id: NodeId,
        lock_type: LockType,
    ) -> bool {
        let entry = self.node_locks.entry(node_id).or_insert_with(LockEntry::new);

        // 检查是否可以获取锁
        if !entry.can_acquire(tx_id, lock_type) {
            return false;
        }

        // 添加锁请求
        let req = LockRequest::new(tx_id, lock_type);
        entry.node_locks.entry(node_id).or_insert_with(Vec::new).push(req);
        true
    }

    /// 尝试获取关系锁
    pub fn acquire_rel_lock(
        &mut self,
        tx_id: u64,
        rel_id: RelId,
        lock_type: LockType,
    ) -> bool {
        let entry = self.rel_locks.entry(rel_id).or_insert_with(LockEntry::new);

        // 检查是否可以获取锁
        if !entry.can_acquire(tx_id, lock_type) {
            return false;
        }

        // 添加锁请求
        let req = LockRequest::new(tx_id, lock_type);
        entry.rel_locks.entry(rel_id).or_insert_with(Vec::new).push(req);
        true
    }

    /// 释放事务的所有锁
    pub fn release_all(&mut self, tx_id: u64) {
        // 释放节点锁
        for entry in self.node_locks.values_mut() {
            entry.node_locks.values_mut().for_each(|locks| {
                locks.retain(|req| req.tx_id != tx_id);
            });
            // 清理空的锁列表
            entry.node_locks.retain(|_, locks| !locks.is_empty());
        }

        // 释放关系锁
        for entry in self.rel_locks.values_mut() {
            entry.rel_locks.values_mut().for_each(|locks| {
                locks.retain(|req| req.tx_id != tx_id);
            });
            // 清理空的锁列表
            entry.rel_locks.retain(|_, locks| !locks.is_empty());
        }

        // 从等待队列中移除
        self.wait_queue.remove(&tx_id);
    }

    /// 检查是否存在死锁
    pub fn detect_deadlock(&self) -> Option<Vec<u64>> {
        // 简化的死锁检测：检查是否有循环等待
        let mut graph: HashMap<u64, Vec<u64>> = HashMap::new();

        // 构建等待图
        for (&tx_id, waiting_for) in &self.wait_queue {
            let mut blockers = Vec::new();
            for &(node_id, rel_id) in waiting_for {
                // 查找持有该锁的事务
                if let Some(entry) = self.node_locks.get(&node_id) {
                    for locks in entry.node_locks.values() {
                        for req in locks {
                            if req.tx_id != tx_id {
                                blockers.push(req.tx_id);
                            }
                        }
                    }
                }
                if let Some(entry) = self.rel_locks.get(&rel_id) {
                    for locks in entry.rel_locks.values() {
                        for req in locks {
                            if req.tx_id != tx_id {
                                blockers.push(req.tx_id);
                            }
                        }
                    }
                }
            }
            if !blockers.is_empty() {
                graph.insert(tx_id, blockers);
            }
        }

        // 检测环（简单的 DFS）
        for start_tx in graph.keys() {
            if let Some(cycle) = self.find_cycle(&graph, *start_tx, *start_tx, &mut vec![]) {
                return Some(cycle);
            }
        }

        None
    }

    /// 查找环（辅助函数）
    fn find_cycle(
        &self,
        graph: &HashMap<u64, Vec<u64>>,
        current: u64,
        start: u64,
        path: &mut Vec<u64>,
    ) -> Option<Vec<u64>> {
        if current == start && !path.is_empty() {
            return Some(path.clone());
        }

        if path.contains(&current) {
            return None;
        }

        path.push(current);

        if let Some(neighbors) = graph.get(&current) {
            for &next in neighbors {
                if let Some(cycle) = self.find_cycle(graph, next, start, path) {
                    return Some(cycle);
                }
            }
        }

        path.pop();
        None
    }

    /// 获取事务持有的锁数量
    pub fn get_lock_count(&self, tx_id: u64) -> usize {
        let mut count = 0;

        for entry in self.node_locks.values() {
            for locks in entry.node_locks.values() {
                count += locks.iter().filter(|req| req.tx_id == tx_id).count();
            }
        }

        for entry in self.rel_locks.values() {
            for locks in entry.rel_locks.values() {
                count += locks.iter().filter(|req| req.tx_id == tx_id).count();
            }
        }

        count
    }

    /// 检查节点是否被锁定
    pub fn is_node_locked(&self, node_id: NodeId) -> bool {
        if let Some(entry) = self.node_locks.get(&node_id) {
            for locks in entry.node_locks.values() {
                if !locks.is_empty() {
                    return true;
                }
            }
        }
        false
    }

    /// 检查关系是否被锁定
    pub fn is_rel_locked(&self, rel_id: RelId) -> bool {
        if let Some(entry) = self.rel_locks.get(&rel_id) {
            for locks in entry.rel_locks.values() {
                if !locks.is_empty() {
                    return true;
                }
            }
        }
        false
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}
