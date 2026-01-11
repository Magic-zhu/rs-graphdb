//! 缓存统计和监控模块
//!
//! 提供缓存命中率、延迟等统计信息

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// 缓存统计信息
#[derive(Debug, Clone)]
pub struct CacheStats {
    hits: Arc<AtomicU64>,
    misses: Arc<AtomicU64>,
    evictions: Arc<AtomicU64>,
    current_entries: Arc<AtomicUsize>,
    current_size_bytes: Arc<AtomicUsize>,
    total_latency_ns: Arc<AtomicU64>,
    last_update: Arc<AtomicU64>,
}

impl Default for CacheStats {
    fn default() -> Self {
        Self::new()
    }
}

impl CacheStats {
    /// 创建新的统计对象
    pub fn new() -> Self {
        Self {
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
            evictions: Arc::new(AtomicU64::new(0)),
            current_entries: Arc::new(AtomicUsize::new(0)),
            current_size_bytes: Arc::new(AtomicUsize::new(0)),
            total_latency_ns: Arc::new(AtomicU64::new(0)),
            last_update: Arc::new(AtomicU64::new(0)),
        }
    }

    /// 记录缓存命中
    pub fn record_hit(&self, latency_ns: u64) {
        self.hits.fetch_add(1, Ordering::Relaxed);
        self.total_latency_ns.fetch_add(latency_ns, Ordering::Relaxed);
        self.last_update.store(
            Instant::now().duration_since(Instant::now()).as_nanos() as u64,
            Ordering::Relaxed,
        );
    }

    /// 记录缓存未命中
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录淘汰
    pub fn record_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    /// 更新条目数
    pub fn update_entries(&self, count: usize) {
        self.current_entries.store(count, Ordering::Relaxed);
    }

    /// 更新内存使用
    pub fn update_size(&self, size_bytes: usize) {
        self.current_size_bytes.store(size_bytes, Ordering::Relaxed);
    }

    /// 获取命中次数
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// 获取未命中次数
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// 获取淘汰次数
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    /// 获取当前条目数
    pub fn current_entries(&self) -> usize {
        self.current_entries.load(Ordering::Relaxed)
    }

    /// 获取当前内存使用（字节）
    pub fn current_size_bytes(&self) -> usize {
        self.current_size_bytes.load(Ordering::Relaxed)
    }

    /// 获取总请求数
    pub fn total_requests(&self) -> u64 {
        self.hits() + self.misses()
    }

    /// 获取命中率
    pub fn hit_rate(&self) -> f64 {
        let total = self.total_requests();
        if total == 0 {
            0.0
        } else {
            self.hits() as f64 / total as f64
        }
    }

    /// 获取平均命中延迟（纳秒）
    pub fn avg_hit_latency_ns(&self) -> u64 {
        let hits = self.hits();
        if hits == 0 {
            0
        } else {
            self.total_latency_ns.load(Ordering::Relaxed) / hits
        }
    }

    /// 重置统计信息
    pub fn reset(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
        self.total_latency_ns.store(0, Ordering::Relaxed);
    }

    /// 生成报告
    pub fn report(&self) -> CacheReport {
        CacheReport {
            hit_rate: self.hit_rate(),
            total_requests: self.total_requests(),
            hits: self.hits(),
            misses: self.misses(),
            evictions: self.evictions(),
            current_entries: self.current_entries(),
            memory_usage_mb: self.current_size_bytes() / 1024 / 1024,
            avg_hit_latency_us: self.avg_hit_latency_ns() / 1000,
        }
    }
}

/// 缓存报告
#[derive(Debug, Clone, serde::Serialize)]
pub struct CacheReport {
    pub hit_rate: f64,
    pub total_requests: u64,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub current_entries: usize,
    pub memory_usage_mb: usize,
    pub avg_hit_latency_us: u64,
}

/// 整体缓存报告
#[derive(Debug, Clone, serde::Serialize)]
pub struct OverallCacheReport {
    pub node: CacheReport,
    pub adjacency: CacheReport,
    pub query: CacheReport,
    pub index: CacheReport,
    pub total_hit_rate: f64,
    pub total_memory_mb: usize,
}

impl OverallCacheReport {
    /// 从各个缓存统计生成整体报告
    pub fn from_stats(
        node: &CacheStats,
        adjacency: &CacheStats,
        query: &CacheStats,
        index: &CacheStats,
    ) -> Self {
        let total_requests = node.total_requests()
            + adjacency.total_requests()
            + query.total_requests()
            + index.total_requests();

        let total_hits = node.hits() + adjacency.hits() + query.hits() + index.hits();

        let total_hit_rate = if total_requests == 0 {
            0.0
        } else {
            total_hits as f64 / total_requests as f64
        };

        let total_memory_mb = (node.current_size_bytes()
            + adjacency.current_size_bytes()
            + query.current_size_bytes()
            + index.current_size_bytes())
            / 1024
            / 1024;

        Self {
            node: node.report(),
            adjacency: adjacency.report(),
            query: query.report(),
            index: index.report(),
            total_hit_rate,
            total_memory_mb,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_basic() {
        let stats = CacheStats::new();

        stats.record_hit(100);
        stats.record_hit(200);
        stats.record_miss();

        assert_eq!(stats.hits(), 2);
        assert_eq!(stats.misses(), 1);
        assert!((stats.hit_rate() - 0.666).abs() < 0.01);
    }

    #[test]
    fn test_avg_latency() {
        let stats = CacheStats::new();

        stats.record_hit(100);
        stats.record_hit(200);
        stats.record_hit(300);

        assert_eq!(stats.avg_hit_latency_ns(), 200);
    }

    #[test]
    fn test_reset() {
        let stats = CacheStats::new();

        stats.record_hit(100);
        stats.record_miss();
        stats.record_eviction();

        stats.reset();

        assert_eq!(stats.hits(), 0);
        assert_eq!(stats.misses(), 0);
        assert_eq!(stats.evictions(), 0);
    }

    #[test]
    fn test_zero_hit_rate() {
        let stats = CacheStats::new();
        assert_eq!(stats.hit_rate(), 0.0);
    }
}
