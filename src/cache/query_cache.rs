//! 查询结果缓存模块
//!
//! 缓存完整查询结果，避免重复的遍历和计算

use super::lru::LruCache;
use super::stats::CacheStats;
use crate::storage::NodeId;
use crate::values::Value;
use std::time::{Duration, Instant};

/// 查询指纹
#[derive(Debug, Clone)]
pub struct QueryFingerprint {
    /// 查询类型
    pub query_type: QueryType,
    /// 查询参数
    pub params: Vec<(String, Value)>,
    /// 遍历深度
    pub traversal_depth: usize,
}

// 手动实现 PartialEq 和 Eq
impl PartialEq for QueryFingerprint {
    fn eq(&self, other: &Self) -> bool {
        self.query_type == other.query_type
            && self.params.len() == other.params.len()
            && self.traversal_depth == other.traversal_depth
            && self.params.iter().zip(other.params.iter()).all(|(a, b)| {
                a.0 == b.0 && self.value_eq(&a.1, &b.1)
            })
    }
}

impl Eq for QueryFingerprint {}

// 手动实现 Hash
impl std::hash::Hash for QueryFingerprint {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.query_type.hash(state);
        self.traversal_depth.hash(state);
        // 使用参数长度和键的哈希，避免值哈希问题
        self.params.len().hash(state);
        for (key, _) in &self.params {
            key.hash(state);
        }
    }
}

impl QueryFingerprint {
    /// 比较两个 Value 是否相等
    fn value_eq(&self, a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => {
                // 浮点数使用近似比较
                (a - b).abs() < 1e-9
            }
            _ => false,
        }
    }
}

/// 查询类型
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum QueryType {
    /// 标签查询
    Label(String),
    /// 属性查询
    Property { label: String, prop: String },
    /// 遍历查询
    Traversal { pattern: String },
    /// 聚合查询
    Aggregation { func: String },
}

/// 缓存的查询结果
#[derive(Debug, Clone)]
pub struct CachedResult {
    /// 节点ID列表
    pub node_ids: Vec<NodeId>,
    /// 缓存时间戳
    pub timestamp: Instant,
    /// 命中次数
    pub hit_count: u64,
    /// 估算大小（字节）
    pub size_bytes: usize,
}

/// 查询缓存
pub struct QueryCache {
    cache: LruCache<QueryFingerprint, CachedResult>,
    stats: CacheStats,
    default_ttl: Duration,
    max_result_size: usize,
}

impl QueryCache {
    /// 创建新的查询缓存
    pub fn new(max_entries: usize, default_ttl: Duration, max_result_size: usize) -> Self {
        Self {
            cache: LruCache::new(max_entries),
            stats: CacheStats::new(),
            default_ttl,
            max_result_size,
        }
    }

    /// 获取查询结果
    pub fn get(&mut self, fingerprint: &QueryFingerprint) -> Option<Vec<NodeId>> {
        let start = Instant::now();

        // 检查是否过期
        if let Some(cached) = self.cache.get(fingerprint) {
            if cached.timestamp.elapsed() > self.default_ttl {
                // 过期，移除并返回 None
                self.cache.remove(fingerprint);
                self.stats.record_miss();
                return None;
            }

            let latency = start.elapsed().as_nanos() as u64;
            self.stats.record_hit(latency);
            Some(cached.node_ids.clone())
        } else {
            self.stats.record_miss();
            None
        }
    }

    /// 插入查询结果
    pub fn put(&mut self, fingerprint: QueryFingerprint, node_ids: Vec<NodeId>) -> bool {
        // 检查结果大小限制
        let size_bytes = node_ids.len() * std::mem::size_of::<NodeId>();
        if size_bytes > self.max_result_size {
            return false; // 结果太大，不缓存
        }

        let cached = CachedResult {
            node_ids,
            timestamp: Instant::now(),
            hit_count: 0,
            size_bytes,
        };

        self.cache.put(fingerprint, cached, size_bytes);
        self.stats.update_entries(self.cache.len());
        self.stats.update_size(self.cache.current_bytes());
        true
    }

    /// 使查询失效
    pub fn invalidate(&mut self, fingerprint: &QueryFingerprint) {
        self.cache.remove(fingerprint);
        self.update_stats();
    }

    /// 使所有查询失效（在图变更时调用）
    pub fn invalidate_all(&mut self) {
        self.cache.clear();
        self.update_stats();
    }

    /// 清空缓存
    pub fn clear(&mut self) {
        self.cache.clear();
        self.stats.update_entries(0);
        self.stats.update_size(0);
    }

    /// 获取统计信息
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// 更新统计信息
    fn update_stats(&self) {
        self.stats.update_entries(self.cache.len());
        self.stats.update_size(self.cache.current_bytes());
    }

    /// 清理过期条目
    pub fn cleanup_expired(&mut self) {
        let keys_to_remove: Vec<_> = self
            .cache
            .keys()
            .iter()
            .filter(|key| {
                if let Some(cached) = self.cache.get(key) {
                    cached.timestamp.elapsed() > self.default_ttl
                } else {
                    false
                }
            })
            .cloned()
            .collect();

        for key in keys_to_remove {
            self.cache.remove(&key);
        }

        self.update_stats();
    }
}

impl QueryFingerprint {
    /// 为标签查询创建指纹
    pub fn label_query(label: &str) -> Self {
        Self {
            query_type: QueryType::Label(label.to_string()),
            params: vec![],
            traversal_depth: 0,
        }
    }

    /// 为属性查询创建指纹
    pub fn property_query(label: &str, prop: &str, value: &Value) -> Self {
        Self {
            query_type: QueryType::Property {
                label: label.to_string(),
                prop: prop.to_string(),
            },
            params: vec![("value".to_string(), value.clone())],
            traversal_depth: 0,
        }
    }

    /// 为遍历查询创建指纹
    pub fn traversal_query(pattern: &str, depth: usize) -> Self {
        Self {
            query_type: QueryType::Traversal {
                pattern: pattern.to_string(),
            },
            params: vec![],
            traversal_depth: depth,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut cache = QueryCache::new(
            10,
            Duration::from_secs(60),
            1000,
        );

        let fingerprint = QueryFingerprint::label_query("User");
        let node_ids = vec![1, 2, 3];

        cache.put(fingerprint.clone(), node_ids.clone());

        assert_eq!(cache.get(&fingerprint), Some(node_ids));
    }

    #[test]
    fn test_expiration() {
        let mut cache = QueryCache::new(
            10,
            Duration::from_millis(100), // 短 TTL
            1000,
        );

        let fingerprint = QueryFingerprint::label_query("User");
        let node_ids = vec![1, 2, 3];

        cache.put(fingerprint.clone(), node_ids);

        // 立即获取应该命中
        assert!(cache.get(&fingerprint).is_some());

        // 等待过期
        std::thread::sleep(Duration::from_millis(150));

        // 应该过期
        assert!(cache.get(&fingerprint).is_none());
    }

    #[test]
    fn test_result_size_limit() {
        let mut cache = QueryCache::new(
            10,
            Duration::from_secs(60),
            10, // 只允许 10 字节
        );

        let fingerprint = QueryFingerprint::label_query("User");
        let node_ids = vec![1, 2, 3, 4, 5]; // 40 字节，超过限制

        let result = cache.put(fingerprint, node_ids);

        assert!(!result); // 应该返回 false，表示未缓存
    }

    #[test]
    fn test_invalidation() {
        let mut cache = QueryCache::new(
            10,
            Duration::from_secs(60),
            1000,
        );

        let fingerprint = QueryFingerprint::label_query("User");
        let node_ids = vec![1, 2, 3];

        cache.put(fingerprint.clone(), node_ids);

        cache.invalidate(&fingerprint);

        assert_eq!(cache.get(&fingerprint), None);
    }

    #[test]
    fn test_fingerprint_creation() {
        let label_fp = QueryFingerprint::label_query("User");
        assert_eq!(label_fp.params.len(), 0);

        let prop_fp = QueryFingerprint::property_query("User", "name", &Value::Text("Alice".to_string()));
        assert_eq!(prop_fp.params.len(), 1);

        let trav_fp = QueryFingerprint::traversal_query("FRIEND", 2);
        assert_eq!(trav_fp.traversal_depth, 2);
    }

    #[test]
    fn test_stats() {
        let mut cache = QueryCache::new(
            10,
            Duration::from_secs(60),
            1000,
        );

        let fingerprint = QueryFingerprint::label_query("User");
        let node_ids = vec![1, 2, 3];

        cache.put(fingerprint.clone(), node_ids);

        // 命中
        cache.get(&fingerprint);
        // 未命中
        cache.get(&QueryFingerprint::label_query("Admin"));

        assert_eq!(cache.stats().hits(), 1);
        assert_eq!(cache.stats().misses(), 1);
    }

    #[test]
    fn test_cleanup_expired() {
        let mut cache = QueryCache::new(
            10,
            Duration::from_millis(100),
            1000,
        );

        cache.put(QueryFingerprint::label_query("User"), vec![1, 2]);
        cache.put(QueryFingerprint::label_query("Admin"), vec![3, 4]);

        // 等待过期
        std::thread::sleep(Duration::from_millis(150));

        cache.cleanup_expired();

        assert_eq!(cache.cache.len(), 0);
    }
}
