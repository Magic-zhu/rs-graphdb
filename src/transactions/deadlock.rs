// 增强的死锁检测模块
//
// 实现高级死锁检测和恢复机制：
// - 等待图（Wait-for Graph）构建
// - 深度优先搜索（DFS）环检测
// - 死锁受害者选择策略
// - 超时检测
// - 预防性死锁检测

use crate::storage::{NodeId, RelId};
use crate::transactions::{LockType, TransactionError, TransactionResult};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

/// 死锁检测结果
#[derive(Debug, Clone, PartialEq)]
pub struct DeadlockInfo {
    /// 涉及的事务ID列表（环中的事务）
    pub involved_transactions: Vec<u64>,
    /// 检测时间戳
    pub detected_at: u64,
    /// 环长度
    pub cycle_length: usize,
}

impl DeadlockInfo {
    /// 创建新的死锁信息
    pub fn new(involved_transactions: Vec<u64>) -> Self {
        let cycle_length = involved_transactions.len();
        Self {
            involved_transactions,
            detected_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            cycle_length,
        }
    }

    /// 获取受害者事务（建议回滚的事务）
    pub fn select_victim(&self) -> u64 {
        // 策略1: 选择ID最大的事务（最年轻的事务）
        // 策略2: 可以根据事务优先级、已执行时间等来选择
        *self.involved_transactions.iter().max().unwrap()
    }
}

/// 锁请求信息
#[derive(Debug, Clone, PartialEq)]
pub struct LockHolder {
    /// 事务ID
    pub tx_id: u64,
    /// 锁类型
    pub lock_type: LockType,
    /// 获取锁的时间
    pub acquired_at: u64,
}

impl LockHolder {
    pub fn new(tx_id: u64, lock_type: LockType) -> Self {
        Self {
            tx_id,
            lock_type,
            acquired_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
}

/// 资源类型（用于统一处理节点和关系）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Resource {
    Node(NodeId),
    Rel(RelId),
}

impl Resource {
    /// 获取资源标识符
    pub fn id(&self) -> u64 {
        match self {
            Resource::Node(id) => *id,
            Resource::Rel(id) => *id,
        }
    }
}

/// 等待图（Wait-for Graph）
///
/// 表示事务之间的等待关系
#[derive(Debug, Clone)]
pub struct WaitGraph {
    /// 邻接表：tx_id -> 等待的tx_id列表
    adjacency: HashMap<u64, Vec<u64>>,
    /// 资源持有者：resource -> 持有者事务
    resource_holders: HashMap<Resource, Vec<LockHolder>>,
    /// 事务等待的资源：tx_id -> 等待的资源列表
    waiting_resources: HashMap<u64, Vec<Resource>>,
}

impl WaitGraph {
    /// 创建新的等待图
    pub fn new() -> Self {
        Self {
            adjacency: HashMap::new(),
            resource_holders: HashMap::new(),
            waiting_resources: HashMap::new(),
        }
    }

    /// 添加事务等待关系（tx1 等待 tx2）
    pub fn add_wait_edge(&mut self, tx1: u64, tx2: u64) {
        self.adjacency.entry(tx1).or_insert_with(Vec::new).push(tx2);
    }

    /// 设置资源持有者
    pub fn set_resource_holder(&mut self, resource: Resource, holder: LockHolder) {
        self.resource_holders
            .entry(resource)
            .or_insert_with(Vec::new)
            .push(holder);
    }

    /// 移除资源持有者
    pub fn remove_resource_holder(&mut self, resource: Resource, tx_id: u64) {
        if let Some(holders) = self.resource_holders.get_mut(&resource) {
            holders.retain(|h| h.tx_id != tx_id);
            if holders.is_empty() {
                self.resource_holders.remove(&resource);
            }
        }
    }

    /// 添加事务等待资源
    pub fn add_waiting_resource(&mut self, tx_id: u64, resource: Resource) {
        self.waiting_resources
            .entry(tx_id)
            .or_insert_with(Vec::new)
            .push(resource);
    }

    /// 移除事务的所有等待
    pub fn remove_transaction(&mut self, tx_id: u64) {
        self.adjacency.remove(&tx_id);
        self.waiting_resources.remove(&tx_id);

        // 从其他事务的等待边中移除
        for wait_list in self.adjacency.values_mut() {
            wait_list.retain(|&waiting_tx| waiting_tx != tx_id);
        }

        // 从资源持有者中移除
        for holders in self.resource_holders.values_mut() {
            holders.retain(|h| h.tx_id != tx_id);
        }
    }

    /// 检测死环（使用DFS）
    pub fn detect_cycle(&self) -> Option<Vec<u64>> {
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();
        let mut path = Vec::new();

        for &start_tx in self.adjacency.keys().collect::<Vec<_>>() {
            if !visited.contains(&start_tx) {
                if let Some(cycle) = self.dfs_find_cycle(start_tx, &mut visited, &mut rec_stack, &mut path) {
                    return Some(cycle);
                }
            }
        }

        None
    }

    /// 深度优先搜索检测环
    fn dfs_find_cycle(
        &self,
        tx: u64,
        visited: &mut HashSet<u64>,
        rec_stack: &mut HashSet<u64>,
        path: &mut Vec<u64>,
    ) -> Option<Vec<u64>> {
        visited.insert(tx);
        rec_stack.insert(tx);
        path.push(tx);

        if let Some(neighbors) = self.adjacency.get(&tx) {
            for &neighbor in neighbors {
                if !visited.contains(&neighbor) {
                    if let Some(cycle) = self.dfs_find_cycle(neighbor, visited, rec_stack, path) {
                        return Some(cycle);
                    }
                } else if rec_stack.contains(&neighbor) {
                    // 找到环：从 path 中找到 neighbor 开始的环
                    if let Some(pos) = path.iter().position(|&x| x == neighbor) {
                        let cycle = path[pos..].to_vec();
                        return Some(cycle);
                    }
                }
            }
        }

        path.pop();
        rec_stack.remove(&tx);
        None
    }

    /// 获取统计信息
    pub fn stats(&self) -> WaitGraphStats {
        WaitGraphStats {
            transaction_count: self.adjacency.len(),
            edge_count: self.adjacency.values().map(|v| v.len()).sum(),
            resource_count: self.resource_holders.len(),
            waiting_transactions: self.waiting_resources.len(),
        }
    }
}

impl Default for WaitGraph {
    fn default() -> Self {
        Self::new()
    }
}

/// 等待图统计信息
#[derive(Debug, Clone, PartialEq)]
pub struct WaitGraphStats {
    /// 涉及的事务数
    pub transaction_count: usize,
    /// 等待边数量
    pub edge_count: usize,
    /// 涉及的资源数
    pub resource_count: usize,
    /// 等待中的事务数
    pub waiting_transactions: usize,
}

/// 增强的死锁检测器
pub struct DeadlockDetector {
    /// 等待图
    wait_graph: WaitGraph,
    /// 死锁历史记录
    deadlock_history: VecDeque<DeadlockInfo>,
    /// 最大历史记录数
    max_history_size: usize,
    /// 检测次数
    detection_count: u64,
}

impl DeadlockDetector {
    /// 创建新的死锁检测器
    pub fn new() -> Self {
        Self {
            wait_graph: WaitGraph::new(),
            deadlock_history: VecDeque::with_capacity(100),
            max_history_size: 100,
            detection_count: 0,
        }
    }

    /// 设置最大历史记录数
    pub fn with_max_history_size(mut self, size: usize) -> Self {
        self.max_history_size = size;
        self.deadlock_history = VecDeque::with_capacity(size);
        self
    }

    /// 记录锁获取
    pub fn on_lock_acquired(&mut self, tx_id: u64, resource: Resource, lock_type: LockType) {
        self.wait_graph.set_resource_holder(resource, LockHolder::new(tx_id, lock_type));
        // 移除该事务对这个资源的等待
        self.wait_graph.waiting_resources.entry(tx_id).or_insert_with(Vec::new).retain(|r| r != &resource);
    }

    /// 记录锁请求（可能导致等待）
    pub fn on_lock_requested(&mut self, tx_id: u64, resource: Resource, lock_type: LockType) {
        // 先收集冲突的事务ID，避免借用检查器错误
        let conflicts: Vec<_> = if let Some(holders) = self.wait_graph.resource_holders.get(&resource) {
            holders.iter()
                .filter(|holder| {
                    let conflict = match (lock_type, holder.lock_type) {
                        (LockType::Write, _) => true,  // 写锁与任何锁冲突
                        (_, LockType::Write) => true,  // 任何锁与写锁冲突
                        (LockType::Read, LockType::Read) => false,  // 读锁与读锁兼容
                    };
                    conflict && holder.tx_id != tx_id
                })
                .map(|holder| holder.tx_id)
                .collect()
        } else {
            Vec::new()
        };

        // 然后添加等待关系
        for holder_tx_id in conflicts {
            self.wait_graph.add_wait_edge(tx_id, holder_tx_id);
            self.wait_graph.add_waiting_resource(tx_id, resource);
        }
    }

    /// 记录锁释放
    pub fn on_lock_released(&mut self, tx_id: u64, resource: Resource) {
        self.wait_graph.remove_resource_holder(resource, tx_id);
        self.wait_graph.remove_transaction(tx_id);
    }

    /// 释放事务的所有锁
    pub fn release_all_locks(&mut self, tx_id: u64) {
        self.wait_graph.remove_transaction(tx_id);
    }

    /// 检测死锁
    pub fn detect_deadlock(&mut self) -> Option<DeadlockInfo> {
        self.detection_count += 1;

        if let Some(cycle) = self.wait_graph.detect_cycle() {
            let info = DeadlockInfo::new(cycle.clone());

            // 添加到历史记录
            if self.deadlock_history.len() >= self.max_history_size {
                self.deadlock_history.pop_front();
            }
            self.deadlock_history.push_back(info.clone());

            Some(info)
        } else {
            None
        }
    }

    /// 解除死锁（选择受害者并回滚）
    pub fn resolve_deadlock(&mut self, deadlock: &DeadlockInfo) -> u64 {
        let victim = deadlock.select_victim();
        // 移除受害者相关的等待边
        self.wait_graph.remove_transaction(victim);
        victim
    }

    /// 获取死锁统计
    pub fn stats(&self) -> DeadlockStats {
        DeadlockStats {
            detection_count: self.detection_count,
            deadlocks_detected: self.deadlock_history.len(),
            current_waiting_transactions: self.wait_graph.waiting_resources.len(),
            graph_stats: self.wait_graph.stats(),
        }
    }

    /// 获取死锁历史
    pub fn get_deadlock_history(&self) -> Vec<DeadlockInfo> {
        self.deadlock_history.iter().cloned().collect()
    }

    /// 清除历史记录
    pub fn clear_history(&mut self) {
        self.deadlock_history.clear();
    }
}

impl Default for DeadlockDetector {
    fn default() -> Self {
        Self::new()
    }
}

/// 死锁统计信息
#[derive(Debug, Clone, PartialEq)]
pub struct DeadlockStats {
    /// 总检测次数
    pub detection_count: u64,
    /// 检测到的死锁数
    pub deadlocks_detected: usize,
    /// 当前等待的事务数
    pub current_waiting_transactions: usize,
    /// 等待图统计
    pub graph_stats: WaitGraphStats,
}

/// 超时检测器
///
/// 检测长时间等待的事务，可能发生死锁
pub struct TimeoutDetector {
    /// 事务开始等待时间
    waiting_since: HashMap<u64, u64>,
    /// 超时阈值（秒）
    timeout_threshold: u64,
}

impl TimeoutDetector {
    /// 创建新的超时检测器
    pub fn new(timeout_secs: u64) -> Self {
        Self {
            waiting_since: HashMap::new(),
            timeout_threshold: timeout_secs,
        }
    }

    /// 记录事务开始等待
    pub fn on_wait_start(&mut self, tx_id: u64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.waiting_since.insert(tx_id, now);
    }

    /// 记录事务停止等待（获得锁或事务结束）
    pub fn on_wait_end(&mut self, tx_id: u64) {
        self.waiting_since.remove(&tx_id);
    }

    /// 检测超时事务
    pub fn detect_timeouts(&self) -> Vec<u64> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        self.waiting_since
            .iter()
            .filter(|(_, &start)| now - start >= self.timeout_threshold)
            .map(|(&tx_id, _)| tx_id)
            .collect()
    }

    /// 获取等待时长
    pub fn get_wait_duration(&self, tx_id: u64) -> Option<u64> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.waiting_since.get(&tx_id).map(|&start| now - start)
    }

    /// 清理已完成的事务
    pub fn cleanup(&mut self, tx_ids: &[u64]) {
        for tx_id in tx_ids {
            self.waiting_since.remove(tx_id);
        }
    }

    /// 获取统计信息
    pub fn stats(&self) -> TimeoutStats {
        TimeoutStats {
            waiting_transactions: self.waiting_since.len(),
            timeout_threshold: self.timeout_threshold,
        }
    }
}

/// 超时统计信息
#[derive(Debug, Clone, PartialEq)]
pub struct TimeoutStats {
    /// 等待中的事务数
    pub waiting_transactions: usize,
    /// 超时阈值（秒）
    pub timeout_threshold: u64,
}

/// 预防性死锁检测
///
/// 在分配锁之前预测并避免可能的死锁
pub struct PreventiveDeadlockDetector {
    /// 事务持有的锁集合
    tx_locks: HashMap<u64, HashSet<Resource>>,
    /// 事务等待的锁集合
    tx_waits: HashMap<u64, HashSet<Resource>>,
}

impl PreventiveDeadlockDetector {
    /// 创建新的预防性死锁检测器
    pub fn new() -> Self {
        Self {
            tx_locks: HashMap::new(),
            tx_waits: HashMap::new(),
        }
    }

    /// 检查是否可以安全地授予锁（避免死锁）
    pub fn is_safe_to_grant(
        &self,
        tx_id: u64,
        resource: Resource,
        lock_type: LockType,
    ) -> bool {
        // 简化的预防策略：检查是否会导致环

        // 如果没有其他事务在等待，总是安全的
        if self.tx_waits.is_empty() {
            return true;
        }

        // 检查授予锁是否会形成等待环
        // 场景：TX1 持有 R1，等待 R2；TX2 持有 R2，想要获取 R1
        // 这会形成 TX1 -> TX2 -> TX1 的环

        // 对于写锁，检查是否有其他事务持有目标资源且在等待当前事务持有的资源
        if lock_type == LockType::Write {
            // 检查是否有其他事务持有我们想要获取的资源
            for (other_tx, held_resources) in &self.tx_locks {
                if *other_tx != tx_id && held_resources.contains(&resource) {
                    // 其他事务持有我们想要获取的资源
                    // 检查这个事务是否在等待任何资源
                    if let Some(waiting_for) = self.tx_waits.get(other_tx) {
                        for waiting_resource in waiting_for {
                            // 检查它等待的资源是否是我们持有的
                            if let Some(our_resources) = self.tx_locks.get(&tx_id) {
                                if our_resources.contains(waiting_resource) {
                                    // 会形成环：TX1 持有 R1，等待 R2；TX2 持有 R2，想要获取 R1
                                    return false;
                                }
                            }
                        }
                    }
                }
            }
        }

        true
    }

    /// 检查两个资源是否相关（可能导致死锁）
    fn resources_related(&self, r1: Resource, r2: Resource) -> bool {
        // 简化版本：相同资源就是相关的
        // 实际应用中可以根据资源类型、层次关系等来判断
        r1 == r2
    }

    /// 记录锁获取
    pub fn on_lock_acquired(&mut self, tx_id: u64, resource: Resource) {
        self.tx_locks.entry(tx_id).or_insert_with(HashSet::new).insert(resource);
        // 从等待集合中移除
        if let Some(waits) = self.tx_waits.get_mut(&tx_id) {
            waits.remove(&resource);
        }
    }

    /// 记录锁等待
    pub fn on_lock_wait(&mut self, tx_id: u64, resource: Resource) {
        self.tx_waits.entry(tx_id).or_insert_with(HashSet::new).insert(resource);
    }

    /// 记录锁释放
    pub fn on_lock_released(&mut self, tx_id: u64, resource: Resource) {
        self.tx_locks.entry(tx_id).or_insert_with(HashSet::new).remove(&resource);
        self.tx_waits.entry(tx_id).or_insert_with(HashSet::new).remove(&resource);

        // 清理空集合
        if self.tx_locks.get(&tx_id).map_or(false, |s| s.is_empty()) {
            self.tx_locks.remove(&tx_id);
        }
        if self.tx_waits.get(&tx_id).map_or(false, |s| s.is_empty()) {
            self.tx_waits.remove(&tx_id);
        }
    }

    /// 移除事务
    pub fn remove_transaction(&mut self, tx_id: u64) {
        self.tx_locks.remove(&tx_id);
        self.tx_waits.remove(&tx_id);
    }

    /// 获取统计信息
    pub fn stats(&self) -> PreventiveStats {
        PreventiveStats {
            active_transactions: self.tx_locks.len(),
            waiting_transactions: self.tx_waits.len(),
        }
    }
}

impl Default for PreventiveDeadlockDetector {
    fn default() -> Self {
        Self::new()
    }
}

/// 预防性检测统计
#[derive(Debug, Clone, PartialEq)]
pub struct PreventiveStats {
    /// 活动事务数
    pub active_transactions: usize,
    /// 等待中的事务数
    pub waiting_transactions: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wait_graph_cycle_detection() {
        let mut graph = WaitGraph::new();

        // 创建一个环: T1 -> T2 -> T3 -> T1
        graph.add_wait_edge(1, 2);
        graph.add_wait_edge(2, 3);
        graph.add_wait_edge(3, 1);

        let cycle = graph.detect_cycle();
        assert!(cycle.is_some());
        assert_eq!(cycle.unwrap().len(), 3);
    }

    #[test]
    fn test_wait_graph_no_cycle() {
        let mut graph = WaitGraph::new();

        // 无环图: T1 -> T2, T2 -> T3
        graph.add_wait_edge(1, 2);
        graph.add_wait_edge(2, 3);

        let cycle = graph.detect_cycle();
        assert!(cycle.is_none());
    }

    #[test]
    fn test_deadlock_detector() {
        let mut detector = DeadlockDetector::new();

        // 模拟死锁情况
        let r1 = Resource::Node(1);
        let r2 = Resource::Node(2);

        // TX1 持有 R1
        detector.on_lock_acquired(1, r1, LockType::Write);

        // TX2 持有 R2
        detector.on_lock_acquired(2, r2, LockType::Write);

        // TX2 等待 R1
        detector.on_lock_requested(2, r1, LockType::Write);

        // TX1 等待 R2
        detector.on_lock_requested(1, r2, LockType::Write);

        // 应该检测到死锁
        let deadlock = detector.detect_deadlock();
        assert!(deadlock.is_some());
        assert_eq!(deadlock.as_ref().unwrap().cycle_length, 2);
    }

    #[test]
    fn test_deadlock_info_victim_selection() {
        let info = DeadlockInfo::new(vec![1, 5, 3, 2]);

        // 应该选择ID最大的事务作为受害者
        let victim = info.select_victim();
        assert_eq!(victim, 5);
    }

    #[test]
    fn test_timeout_detector() {
        let mut detector = TimeoutDetector::new(10); // 10秒超时

        let tx_id: u64 = 1;

        // 记录等待开始
        detector.on_wait_start(tx_id);

        // 短时间内不应该超时
        assert!(detector.detect_timeouts().is_empty());

        // 结束等待
        detector.on_wait_end(tx_id);

        // 现在应该没有等待的事务
        let stats = detector.stats();
        assert_eq!(stats.waiting_transactions, 0);
    }

    #[test]
    fn test_preventive_detector() {
        let mut detector = PreventiveDeadlockDetector::new();

        let r1 = Resource::Node(1);
        let r2 = Resource::Node(2);

        // TX1 持有 R1
        detector.on_lock_acquired(1, r1);

        // TX2 持有 R2
        detector.on_lock_acquired(2, r2);

        // TX1 等待 R2
        detector.on_lock_wait(1, r2);

        // TX2 尝试获取 R1（写锁）
        // 这可能导致死锁，预防性检测应该拒绝
        let safe = detector.is_safe_to_grant(2, r1, LockType::Write);
        assert!(!safe);
    }

    #[test]
    fn test_wait_graph_stats() {
        let mut graph = WaitGraph::new();

        graph.add_wait_edge(1, 2);
        graph.add_wait_edge(2, 3);

        let stats = graph.stats();
        assert_eq!(stats.transaction_count, 2);
        assert_eq!(stats.edge_count, 2);
    }
}
