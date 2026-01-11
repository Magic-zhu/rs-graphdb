//! 应用层缓存模块
//!
//! 提供多种缓存类型以优化图数据库的查询性能

pub mod config;
pub mod lru;
pub mod stats;
pub mod node_cache;
pub mod adjacency_cache;
pub mod query_cache;
pub mod index_cache;
pub mod manager;

pub use config::{CacheConfig, CacheSizes};
pub use stats::{CacheStats, CacheReport, OverallCacheReport};
pub use node_cache::NodeCache;
pub use adjacency_cache::AdjacencyCache;
pub use query_cache::QueryCache;
pub use index_cache::IndexCache;
pub use manager::CacheManager;
