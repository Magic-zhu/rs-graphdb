//! 索引缓存模块
//!
//! 缓存索引查询结果，加速属性查找

use super::lru::LruCache;
use super::stats::CacheStats;
use crate::storage::NodeId;
use crate::values::Value;
use std::collections::HashMap;
use std::time::Duration;

/// 索引键
pub type IndexKey = (String, String, ValueKey);

/// 值键（用于索引查找）
#[derive(Debug, Clone)]
pub enum ValueKey {
    Text(String),
    Int(i64),
    Bool(bool),
    Float(f64),
}

// 手动实现 PartialEq
impl PartialEq for ValueKey {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ValueKey::Text(a), ValueKey::Text(b)) => a == b,
            (ValueKey::Int(a), ValueKey::Int(b)) => a == b,
            (ValueKey::Bool(a), ValueKey::Bool(b)) => a == b,
            (ValueKey::Float(a), ValueKey::Float(b)) => {
                // 浮点数使用近似比较
                (a - b).abs() < f64::EPSILON
            }
            _ => false,
        }
    }
}

impl Eq for ValueKey {}

// 手动实现 Hash，因为 f64 不实现 Hash
impl std::hash::Hash for ValueKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            ValueKey::Text(s) => s.hash(state),
            ValueKey::Int(i) => i.hash(state),
            ValueKey::Bool(b) => b.hash(state),
            ValueKey::Float(f) => {
                // 将 f64 转为 u64 的位表示进行哈希
                let bits = f.to_bits();
                bits.hash(state)
            }
        }
    }
}

impl ValueKey {
    /// 从 Value 创建 ValueKey
    pub fn from_value(value: &Value) -> Self {
        match value {
            Value::Int(i) => ValueKey::Int(*i),
            Value::Bool(b) => ValueKey::Bool(*b),
            Value::Text(s) => ValueKey::Text(s.clone()),
            Value::Float(f) => ValueKey::Float(*f),
        }
    }
}

/// 索引缓存
pub struct IndexCache {
    /// 主索引缓存：(label, prop_name, value) -> Vec<NodeId>
    primary: LruCache<IndexKey, Vec<NodeId>>,
    /// 值域缓存：(label, prop_name) -> Vec<ValueKey>
    value_domains: LruCache<(String, String), Vec<ValueKey>>,
    stats: CacheStats,
    ttl: Option<Duration>,
}

impl IndexCache {
    /// 创建新的索引缓存
    pub fn new(max_size: usize, ttl: Option<Duration>) -> Self {
        Self {
            primary: LruCache::new(max_size),
            value_domains: LruCache::new(max_size / 10), // 值域缓存较小
            stats: CacheStats::new(),
            ttl,
        }
    }

    /// 获取索引结果（根据标签和属性查找）
    pub fn get(&mut self, label: &str, prop_name: &str, value: &Value) -> Option<Vec<NodeId>> {
        let start = std::time::Instant::now();

        let key = (label.to_string(), prop_name.to_string(), ValueKey::from_value(value));

        if let Some(node_ids) = self.primary.get(&key) {
            let latency = start.elapsed().as_nanos() as u64;
            self.stats.record_hit(latency);
            Some(node_ids.clone())
        } else {
            self.stats.record_miss();
            None
        }
    }

    /// 插入索引结果
    pub fn put(&mut self, label: &str, prop_name: &str, value: &Value, node_ids: Vec<NodeId>) {
        let key = (label.to_string(), prop_name.to_string(), ValueKey::from_value(value));
        let size_bytes = node_ids.len() * std::mem::size_of::<NodeId>();
        self.primary.put(key, node_ids, size_bytes);
        self.update_stats();
    }

    /// 获取属性的所有可能值
    pub fn get_value_domain(&mut self, label: &str, prop_name: &str) -> Option<Vec<ValueKey>> {
        let start = std::time::Instant::now();

        let key = (label.to_string(), prop_name.to_string());

        if let Some(values) = self.value_domains.get(&key) {
            let latency = start.elapsed().as_nanos() as u64;
            self.stats.record_hit(latency);
            Some(values.clone())
        } else {
            self.stats.record_miss();
            None
        }
    }

    /// 插入值域
    pub fn put_value_domain(&mut self, label: &str, prop_name: &str, values: Vec<ValueKey>) {
        let key = (label.to_string(), prop_name.to_string());
        let size_bytes = values.len() * std::mem::size_of::<ValueKey>();
        self.value_domains.put(key, values, size_bytes);
        self.update_stats();
    }

    /// 使索引失效
    pub fn invalidate(&mut self, label: &str, prop_name: &str, value: &Value) {
        let key = (label.to_string(), prop_name.to_string(), ValueKey::from_value(value));
        self.primary.remove(&key);
        self.update_stats();
    }

    /// 使整个标签的索引失效
    pub fn invalidate_label(&mut self, label: &str) {
        // 移除所有匹配的索引条目
        let keys_to_remove: Vec<_> = self
            .primary
            .keys()
            .iter()
            .filter(|(l, _, _)| l == label)
            .cloned()
            .collect();

        for key in keys_to_remove {
            self.primary.remove(&key);
        }

        // 移除值域缓存
        let domain_keys_to_remove: Vec<_> = self
            .value_domains
            .keys()
            .iter()
            .filter(|(l, _)| l == label)
            .cloned()
            .collect();

        for key in domain_keys_to_remove {
            self.value_domains.remove(&key);
        }

        self.update_stats();
    }

    /// 使节点相关的索引失效（当节点属性变更时）
    pub fn invalidate_node(&mut self, label: &str, props: &HashMap<String, Value>) {
        for (prop_name, value) in props {
            self.invalidate(label, prop_name, value);
        }
    }

    /// 清空缓存
    pub fn clear(&mut self) {
        self.primary.clear();
        self.value_domains.clear();
        self.update_stats();
    }

    /// 获取统计信息
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// 更新统计信息
    fn update_stats(&self) {
        let total_entries = self.primary.len() + self.value_domains.len();
        self.stats.update_entries(total_entries);

        let total_bytes = self.primary.current_bytes() + self.value_domains.current_bytes();
        self.stats.update_size(total_bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::values::Properties;
    use std::collections::HashMap;

    #[test]
    fn test_basic_operations() {
        let mut cache = IndexCache::new(10, Some(Duration::from_secs(600)));

        let node_ids = vec![1, 2, 3];
        let value = Value::Text("Alice".to_string());

        cache.put("User", "name", &value, node_ids.clone());

        assert_eq!(cache.get("User", "name", &value), Some(node_ids));
        assert_eq!(cache.get("User", "age", &Value::Int(25)), None);
    }

    #[test]
    fn test_value_domain() {
        let mut cache = IndexCache::new(10, Some(Duration::from_secs(600)));

        let values = vec![
            ValueKey::Text("Alice".to_string()),
            ValueKey::Text("Bob".to_string()),
            ValueKey::Text("Charlie".to_string()),
        ];

        let value_keys: Vec<ValueKey> = values.iter().map(|v| {
            match v {
                ValueKey::Text(s) => ValueKey::Text(s.clone()),
                _ => unreachable!(),
            }
        }).collect();

        cache.put_value_domain("User", "name", value_keys);

        let result = cache.get_value_domain("User", "name");
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 3);
    }

    #[test]
    fn test_invalidation() {
        let mut cache = IndexCache::new(10, Some(Duration::from_secs(600)));

        let node_ids = vec![1, 2, 3];
        let value = Value::Text("Alice".to_string());

        cache.put("User", "name", &value, node_ids);

        cache.invalidate("User", "name", &value);

        assert_eq!(cache.get("User", "name", &value), None);
    }

    #[test]
    fn test_label_invalidation() {
        let mut cache = IndexCache::new(10, Some(Duration::from_secs(600)));

        cache.put("User", "name", &Value::Text("Alice".to_string()), vec![1]);
        cache.put("User", "age", &Value::Int(25), vec![1, 2]);
        cache.put("Admin", "name", &Value::Text("Bob".to_string()), vec![3]);

        cache.invalidate_label("User");

        assert_eq!(cache.get("User", "name", &Value::Text("Alice".to_string())), None);
        assert_eq!(cache.get("User", "age", &Value::Int(25)), None);
        assert_eq!(cache.get("Admin", "name", &Value::Text("Bob".to_string())), Some(vec![3]));
    }

    #[test]
    fn test_node_invalidation() {
        let mut cache = IndexCache::new(10, Some(Duration::from_secs(600)));

        cache.put("User", "name", &Value::Text("Alice".to_string()), vec![1]);
        cache.put("User", "age", &Value::Int(25), vec![1, 2]);

        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::Text("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(25));

        cache.invalidate_node("User", &props);

        assert_eq!(cache.get("User", "name", &Value::Text("Alice".to_string())), None);
        assert_eq!(cache.get("User", "age", &Value::Int(25)), None);
    }

    #[test]
    fn test_clear() {
        let mut cache = IndexCache::new(10, Some(Duration::from_secs(600)));

        cache.put("User", "name", &Value::Text("Alice".to_string()), vec![1]);
        cache.put_value_domain("User", "name", vec![ValueKey::Text("Alice".to_string())]);

        cache.clear();

        assert_eq!(cache.get("User", "name", &Value::Text("Alice".to_string())), None);
        assert_eq!(cache.get_value_domain("User", "name"), None);
    }

    #[test]
    fn test_stats() {
        let mut cache = IndexCache::new(10, Some(Duration::from_secs(600)));

        cache.put("User", "name", &Value::Text("Alice".to_string()), vec![1, 2]);

        // 命中
        cache.get("User", "name", &Value::Text("Alice".to_string()));
        // 未命中
        cache.get("User", "name", &Value::Text("Bob".to_string()));

        assert_eq!(cache.stats().hits(), 1);
        assert_eq!(cache.stats().misses(), 1);
    }
}
