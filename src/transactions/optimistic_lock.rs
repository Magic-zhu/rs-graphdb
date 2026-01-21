// 乐观锁机制
//
// 实现基于版本号的乐观并发控制
// 适用于读多写少的场景，避免长时间持有锁

use std::sync::atomic::{AtomicU64, Ordering};
use crate::storage::{NodeId, RelId};
use crate::transactions::{TransactionError, TransactionResult};

/// 乐观锁版本号
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Version {
    value: u64,
}

impl Version {
    /// 创建新版本
    pub fn new(value: u64) -> Self {
        Self { value }
    }

    /// 初始版本
    pub fn initial() -> Self {
        Self { value: 0 }
    }

    /// 下一个版本
    pub fn next(&self) -> Self {
        Self { value: self.value + 1 }
    }

    /// 获取版本号值
    pub fn value(&self) -> u64 {
        self.value
    }

    /// 检查版本是否匹配
    pub fn matches(&self, expected: Version) -> bool {
        self.value == expected.value
    }
}

/// 乐观锁条目
#[derive(Debug, Clone)]
pub struct OptimisticLock {
    version: Version,
}

impl OptimisticLock {
    /// 创建新的乐观锁
    pub fn new() -> Self {
        Self {
            version: Version::initial(),
        }
    }

    /// 获取当前版本
    pub fn version(&self) -> Version {
        self.version
    }

    /// 验证版本（读取时调用）
    pub fn verify(&self, expected: Version) -> TransactionResult<()> {
        if self.version.matches(expected) {
            Ok(())
        } else {
            Err(TransactionError::VersionConflict {
                expected: expected.value(),
                actual: self.version.value(),
            })
        }
    }

    /// 递增版本（写入时调用）
    pub fn increment(&mut self) -> Version {
        let old = self.version;
        self.version = self.version.next();
        old
    }

    /// 条件递增：仅在版本匹配时递增
    pub fn conditional_increment(&mut self, expected: Version) -> TransactionResult<Version> {
        if self.version.matches(expected) {
            Ok(self.increment())
        } else {
            Err(TransactionError::VersionConflict {
                expected: expected.value(),
                actual: self.version.value(),
            })
        }
    }
}

impl Default for OptimisticLock {
    fn default() -> Self {
        Self::new()
    }
}

/// 乐观锁管理器
///
/// 管理所有节点和关系的乐观锁
pub struct OptimisticLockManager {
    /// 节点版本锁
    node_versions: Vec<AtomicU64>,
    /// 关系版本锁
    rel_versions: Vec<AtomicU64>,
}

impl OptimisticLockManager {
    /// 创建新的乐观锁管理器
    pub fn new() -> Self {
        Self {
            node_versions: Vec::new(),
            rel_versions: Vec::new(),
        }
    }

    /// 确保节点版本数组足够大
    fn ensure_node_capacity(&mut self, node_id: NodeId) {
        let idx = node_id as usize;
        while self.node_versions.len() <= idx {
            self.node_versions.push(AtomicU64::new(0));
        }
    }

    /// 确保关系版本数组足够大
    fn ensure_rel_capacity(&mut self, rel_id: RelId) {
        let idx = rel_id as usize;
        while self.rel_versions.len() <= idx {
            self.rel_versions.push(AtomicU64::new(0));
        }
    }

    /// 读取节点版本
    pub fn read_node_version(&self, node_id: NodeId) -> Version {
        let idx = node_id as usize;
        if idx < self.node_versions.len() {
            Version::new(self.node_versions[idx].load(Ordering::Acquire))
        } else {
            Version::initial()
        }
    }

    /// 读取关系版本
    pub fn read_rel_version(&self, rel_id: RelId) -> Version {
        let idx = rel_id as usize;
        if idx < self.rel_versions.len() {
            Version::new(self.rel_versions[idx].load(Ordering::Acquire))
        } else {
            Version::initial()
        }
    }

    /// 验证节点版本
    pub fn verify_node_version(&self, node_id: NodeId, expected: Version) -> TransactionResult<()> {
        let current = self.read_node_version(node_id);
        if current.matches(expected) {
            Ok(())
        } else {
            Err(TransactionError::VersionConflict {
                expected: expected.value(),
                actual: current.value(),
            })
        }
    }

    /// 验证关系版本
    pub fn verify_rel_version(&self, rel_id: RelId, expected: Version) -> TransactionResult<()> {
        let current = self.read_rel_version(rel_id);
        if current.matches(expected) {
            Ok(())
        } else {
            Err(TransactionError::VersionConflict {
                expected: expected.value(),
                actual: current.value(),
            })
        }
    }

    /// 验证多个节点版本
    pub fn verify_node_versions(&self, ids: &[(NodeId, Version)]) -> TransactionResult<()> {
        for &(node_id, expected) in ids {
            self.verify_node_version(node_id, expected)?;
        }
        Ok(())
    }

    /// 验证多个关系版本
    pub fn verify_rel_versions(&self, ids: &[(RelId, Version)]) -> TransactionResult<()> {
        for &(rel_id, expected) in ids {
            self.verify_rel_version(rel_id, expected)?;
        }
        Ok(())
    }

    /// 递增节点版本（创建新节点）
    pub fn increment_node_version(&mut self, node_id: NodeId) -> Version {
        self.ensure_node_capacity(node_id);
        let idx = node_id as usize;
        let old = self.node_versions[idx].fetch_add(1, Ordering::AcqRel);
        Version::new(old)
    }

    /// 递增关系版本（创建新关系）
    pub fn increment_rel_version(&mut self, rel_id: RelId) -> Version {
        self.ensure_rel_capacity(rel_id);
        let idx = rel_id as usize;
        let old = self.rel_versions[idx].fetch_add(1, Ordering::AcqRel);
        Version::new(old)
    }

    /// 节点写入：读取-验证-写入
    pub fn write_node(
        &mut self,
        node_id: NodeId,
        expected_version: Version,
    ) -> TransactionResult<Version> {
        self.ensure_node_capacity(node_id);
        let idx = node_id as usize;
        let current = self.node_versions[idx].load(Ordering::Acquire);

        if current == expected_version.value() {
            // CAS 操作：仅在版本匹配时更新
            let new_version = current + 1;
            match self.node_versions[idx].compare_exchange_weak(
                current,
                new_version,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => Ok(Version::new(current)),
                Err(actual) => Err(TransactionError::VersionConflict {
                    expected: expected_version.value(),
                    actual: actual,
                }),
            }
        } else {
            Err(TransactionError::VersionConflict {
                expected: expected_version.value(),
                actual: current,
            })
        }
    }

    /// 关系写入：读取-验证-写入
    pub fn write_rel(
        &mut self,
        rel_id: RelId,
        expected_version: Version,
    ) -> TransactionResult<Version> {
        self.ensure_rel_capacity(rel_id);
        let idx = rel_id as usize;
        let current = self.rel_versions[idx].load(Ordering::Acquire);

        if current == expected_version.value() {
            // CAS 操作：仅在版本匹配时更新
            let new_version = current + 1;
            match self.rel_versions[idx].compare_exchange_weak(
                current,
                new_version,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => Ok(Version::new(current)),
                Err(actual) => Err(TransactionError::VersionConflict {
                    expected: expected_version.value(),
                    actual: actual,
                }),
            }
        } else {
            Err(TransactionError::VersionConflict {
                expected: expected_version.value(),
                actual: current,
            })
        }
    }

    /// 批量写入节点（事务提交时调用）
    pub fn write_nodes_batch(
        &mut self,
        writes: &[(NodeId, Version)],
    ) -> TransactionResult<()> {
        for &(node_id, expected_version) in writes {
            self.write_node(node_id, expected_version)?;
        }
        Ok(())
    }

    /// 批量写入关系（事务提交时调用）
    pub fn write_rels_batch(
        &mut self,
        writes: &[(RelId, Version)],
    ) -> TransactionResult<()> {
        for &(rel_id, expected_version) in writes {
            self.write_rel(rel_id, expected_version)?;
        }
        Ok(())
    }

    /// 清理无效的版本条目（用于维护）
    pub fn cleanup(&mut self, max_node_id: Option<NodeId>, max_rel_id: Option<RelId>) {
        if let Some(max_id) = max_node_id {
            let max_idx = max_id as usize + 1;
            if self.node_versions.len() > max_idx {
                self.node_versions.truncate(max_idx);
            }
        }

        if let Some(max_id) = max_rel_id {
            let max_idx = max_id as usize + 1;
            if self.rel_versions.len() > max_idx {
                self.rel_versions.truncate(max_idx);
            }
        }
    }

    /// 获取统计信息
    pub fn stats(&self) -> OptimisticLockStats {
        OptimisticLockStats {
            node_version_count: self.node_versions.len(),
            rel_version_count: self.rel_versions.len(),
            total_version_updates: self
                .node_versions
                .iter()
                .map(|v| v.load(Ordering::Relaxed))
                .sum::<u64>()
                + self
                    .rel_versions
                    .iter()
                    .map(|v| v.load(Ordering::Relaxed))
                    .sum::<u64>(),
        }
    }
}

impl Default for OptimisticLockManager {
    fn default() -> Self {
        Self::new()
    }
}

/// 乐观锁统计信息
#[derive(Debug, Clone, PartialEq)]
pub struct OptimisticLockStats {
    /// 节点版本条目数
    pub node_version_count: usize,
    /// 关系版本条目数
    pub rel_version_count: usize,
    /// 总版本更新次数
    pub total_version_updates: u64,
}

/// 乐观锁读取上下文
///
/// 在读取时记录版本号，用于后续的写入验证
#[derive(Debug, Clone)]
pub struct OptimisticReadContext {
    /// 读取的节点及其版本
    pub nodes: Vec<(NodeId, Version)>,
    /// 读取的关系及其版本
    pub rels: Vec<(RelId, Version)>,
}

impl OptimisticReadContext {
    /// 创建新的读取上下文
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            rels: Vec::new(),
        }
    }

    /// 记录读取的节点
    pub fn record_node(&mut self, node_id: NodeId, version: Version) {
        // 避免重复记录
        if !self.nodes.iter().any(|(id, _)| *id == node_id) {
            self.nodes.push((node_id, version));
        }
    }

    /// 记录读取的关系
    pub fn record_rel(&mut self, rel_id: RelId, version: Version) {
        // 避免重复记录
        if !self.rels.iter().any(|(id, _)| *id == rel_id) {
            self.rels.push((rel_id, version));
        }
    }

    /// 合并另一个上下文
    pub fn merge(&mut self, other: OptimisticReadContext) {
        for (node_id, version) in other.nodes {
            self.record_node(node_id, version);
        }
        for (rel_id, version) in other.rels {
            self.record_rel(rel_id, version);
        }
    }

    /// 验证所有读取的数据是否未被修改
    pub fn verify(&self, lock_manager: &OptimisticLockManager) -> TransactionResult<()> {
        lock_manager.verify_node_versions(&self.nodes)?;
        lock_manager.verify_rel_versions(&self.rels)?;
        Ok(())
    }
}

impl Default for OptimisticReadContext {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{NodeId, RelId};

    #[test]
    fn test_version_creation() {
        let v1 = Version::initial();
        assert_eq!(v1.value(), 0);

        let v2 = v1.next();
        assert_eq!(v2.value(), 1);

        let v3 = Version::new(5);
        assert_eq!(v3.value(), 5);
    }

    #[test]
    fn test_version_matches() {
        let v1 = Version::new(5);
        let v2 = Version::new(5);
        let v3 = Version::new(6);

        assert!(v1.matches(v2));
        assert!(!v1.matches(v3));
    }

    #[test]
    fn test_optimistic_lock() {
        let mut lock = OptimisticLock::new();
        assert_eq!(lock.version(), Version::initial());

        let old = lock.increment();
        assert_eq!(old, Version::initial());
        assert_eq!(lock.version(), Version::new(1));

        let v2 = lock.increment();
        assert_eq!(v2, Version::new(1));
        assert_eq!(lock.version(), Version::new(2));
    }

    #[test]
    fn test_optimistic_lock_verify() {
        let mut lock = OptimisticLock::new();
        let v1 = lock.version();

        assert!(lock.verify(v1).is_ok());

        lock.increment();
        assert!(lock.verify(v1).is_err());
    }

    #[test]
    fn test_optimistic_lock_conditional_increment() {
        let mut lock = OptimisticLock::new();
        let v1 = lock.version();

        assert!(lock.conditional_increment(v1).is_ok());
        assert!(lock.conditional_increment(v1).is_err());
    }

    #[test]
    fn test_lock_manager_read_write() {
        let mut manager = OptimisticLockManager::new();
        let node_id: NodeId = 1;

        // 初始读取
        let v1 = manager.read_node_version(node_id);
        assert_eq!(v1, Version::initial());

        // 创建节点
        let old = manager.increment_node_version(node_id);
        assert_eq!(old, Version::initial());

        // 读取新版本
        let v2 = manager.read_node_version(node_id);
        assert_eq!(v2, Version::new(1));

        // 验证版本
        assert!(manager.verify_node_version(node_id, v2).is_ok());
        assert!(manager.verify_node_version(node_id, v1).is_err());
    }

    #[test]
    fn test_lock_manager_write() {
        let mut manager = OptimisticLockManager::new();
        let node_id: NodeId = 1;

        // 初始版本
        let v1 = manager.read_node_version(node_id);

        // 写入成功
        assert!(manager.write_node(node_id, v1).is_ok());

        // 再次写入需要新版本
        let v2 = manager.read_node_version(node_id);
        assert!(manager.write_node(node_id, v2).is_ok());

        // 使用旧版本写入失败
        assert!(manager.write_node(node_id, v1).is_err());
    }

    #[test]
    fn test_read_context() {
        let mut ctx = OptimisticReadContext::new();
        let manager = OptimisticLockManager::new();
        let node_id: NodeId = 1;

        let v1 = manager.read_node_version(node_id);
        ctx.record_node(node_id, v1);

        // 验证通过
        assert!(ctx.verify(&manager).is_ok());

        // 修改版本
        let mut manager2 = manager;
        manager2.increment_node_version(node_id);

        // 验证失败
        assert!(ctx.verify(&manager2).is_err());
    }

    #[test]
    fn test_read_context_merge() {
        let mut ctx1 = OptimisticReadContext::new();
        let mut ctx2 = OptimisticReadContext::new();

        let node_id1: NodeId = 1;
        let node_id2: NodeId = 2;

        ctx1.record_node(node_id1, Version::new(1));
        ctx2.record_node(node_id2, Version::new(2));

        ctx1.merge(ctx2);

        assert_eq!(ctx1.nodes.len(), 2);
    }
}
