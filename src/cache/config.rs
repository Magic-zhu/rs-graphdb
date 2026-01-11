//! 缓存配置模块
//!
//! 定义缓存系统的各种配置选项

use std::time::Duration;

/// 缓存配置
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// 总缓存占可用内存的比例 (0.0 - 1.0)
    pub total_cache_ratio: f64,

    /// 节点缓存占总缓存的比例
    pub node_cache_ratio: f64,

    /// 邻接表缓存占总缓存的比例
    pub adjacency_ratio: f64,

    /// 查询缓存占总缓存的比例
    pub query_ratio: f64,

    /// 索引缓存占总缓存的比例
    pub index_ratio: f64,

    /// 节点缓存 TTL
    pub node_ttl: Duration,

    /// 邻接表缓存 TTL
    pub adjacency_ttl: Duration,

    /// 查询缓存 TTL
    pub query_ttl: Duration,

    /// 索引缓存 TTL
    pub index_ttl: Duration,

    /// 是否启用缓存
    pub enabled: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            total_cache_ratio: 0.3,  // 30% of available memory
            node_cache_ratio: 0.4,   // 40% of total cache
            adjacency_ratio: 0.3,    // 30% of total cache
            query_ratio: 0.2,        // 20% of total cache
            index_ratio: 0.1,        // 10% of total cache
            node_ttl: Duration::from_secs(300),    // 5 minutes
            adjacency_ttl: Duration::from_secs(120), // 2 minutes
            query_ttl: Duration::from_secs(60),     // 1 minute
            index_ttl: Duration::from_secs(600),    // 10 minutes
            enabled: true,
        }
    }
}

impl CacheConfig {
    /// 创建低内存配置
    pub fn low_memory() -> Self {
        Self {
            total_cache_ratio: 0.1,  // 10% of available memory
            ..Default::default()
        }
    }

    /// 创建高性能配置
    pub fn high_performance() -> Self {
        Self {
            total_cache_ratio: 0.5,  // 50% of available memory
            node_ttl: Duration::from_secs(600),     // 10 minutes
            adjacency_ttl: Duration::from_secs(300), // 5 minutes
            query_ttl: Duration::from_secs(120),     // 2 minutes
            ..Default::default()
        }
    }

    /// 计算各缓存的内存分配
    pub fn allocate(&self, available_bytes: usize) -> CacheSizes {
        let total = (available_bytes as f64 * self.total_cache_ratio) as usize;
        CacheSizes {
            node: (total as f64 * self.node_cache_ratio) as usize,
            adjacency: (total as f64 * self.adjacency_ratio) as usize,
            query: (total as f64 * self.query_ratio) as usize,
            index: (total as f64 * self.index_ratio) as usize,
        }
    }

    /// 获取系统可用内存（估算）
    pub fn get_available_memory() -> usize {
        // 简单估算：假设系统有 8GB 可用内存
        // 实际应用中可以使用 sysinfo crate 获取真实值
        8usize * 1024 * 1024 * 1024 // 8GB
    }
}

/// 缓存大小分配
#[derive(Debug, Clone, Copy)]
pub struct CacheSizes {
    pub node: usize,
    pub adjacency: usize,
    pub query: usize,
    pub index: usize,
}

impl CacheSizes {
    /// 总缓存大小
    pub fn total(&self) -> usize {
        self.node + self.adjacency + self.query + self.index
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = CacheConfig::default();
        assert_eq!(config.total_cache_ratio, 0.3);
        assert_eq!(config.node_cache_ratio, 0.4);
        assert_eq!(config.adjacency_ratio, 0.3);
        assert_eq!(config.query_ratio, 0.2);
        assert_eq!(config.index_ratio, 0.1);
    }

    #[test]
    fn test_memory_allocation() {
        let config = CacheConfig::default();
        let available = 1_000_000_000usize; // 1GB
        let sizes = config.allocate(available);

        // 总缓存应该是 300MB (30% of 1GB)
        assert_eq!(sizes.total(), 300_000_000);

        // 各缓存比例
        assert_eq!(sizes.node, 120_000_000);      // 40% of 300MB
        assert_eq!(sizes.adjacency, 90_000_000);  // 30% of 300MB
        assert_eq!(sizes.query, 60_000_000);      // 20% of 300MB
        assert_eq!(sizes.index, 30_000_000);      // 10% of 300MB
    }

    #[test]
    fn test_low_memory_config() {
        let config = CacheConfig::low_memory();
        assert_eq!(config.total_cache_ratio, 0.1);
    }

    #[test]
    fn test_high_performance_config() {
        let config = CacheConfig::high_performance();
        assert_eq!(config.total_cache_ratio, 0.5);
    }
}
