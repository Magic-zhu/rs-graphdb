// 增强的复合索引模块
//
// 实现高级复合索引功能：
// - 多属性复合索引
// - 前缀查询（使用部分属性）
// - 索引统计信息
// - 自动索引选择
// - 索引覆盖扫描

use crate::storage::NodeId;
use crate::values::Value;
use std::collections::{HashMap, BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

/// 复合索引键
///
/// 支持多种值类型的组合键
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CompositeIndexValue {
    Int(i64),
    Float(u64), // 使用 f64::to_bits() 进行可哈希和可比较
    Bool(bool),
    String(String),
    Null,
}

impl CompositeIndexValue {
    /// 从 Value 创建复合索引值
    pub fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Int(i) => Some(CompositeIndexValue::Int(*i)),
            Value::Float(f) => Some(CompositeIndexValue::Float(f.to_bits())),
            Value::Bool(b) => Some(CompositeIndexValue::Bool(*b)),
            Value::Text(s) => Some(CompositeIndexValue::String(s.clone())),
            Value::Null => Some(CompositeIndexValue::Null),
            _ => None, // List 和 Map 不支持索引
        }
    }

    /// 获取值的字节表示（用于统计）
    pub fn size_bytes(&self) -> usize {
        match self {
            CompositeIndexValue::Int(_) => 8,
            CompositeIndexValue::Float(_) => 8,
            CompositeIndexValue::Bool(_) => 1,
            CompositeIndexValue::String(s) => s.len(),
            CompositeIndexValue::Null => 0,
        }
    }
}

/// 复合索引定义
///
/// 定义一个复合索引的结构
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CompositeIndexDef {
    /// 标签
    pub label: String,
    /// 属性名列表（按索引顺序）
    pub properties: Vec<String>,
    /// 是否唯一索引
    pub unique: bool,
    /// 索引ID
    pub id: usize,
}

impl CompositeIndexDef {
    /// 创建新的复合索引定义
    pub fn new(id: usize, label: String, properties: Vec<String>, unique: bool) -> Self {
        Self {
            label,
            properties,
            unique,
            id,
        }
    }

    /// 生成前缀键
    ///
    /// 用于前缀查询，只使用前 n 个属性
    pub fn prefix_key(&self, values: &[CompositeIndexValue], prefix_len: usize) -> Vec<CompositeIndexValue> {
        if prefix_len >= values.len() {
            values.to_vec()
        } else {
            values[..prefix_len].to_vec()
        }
    }

    /// 检查是否可以使用此索引进行查询
    ///
    /// 如果查询的属性是索引属性的前缀，则可以使用
    pub fn can_satisfy(&self, query_props: &[String]) -> bool {
        if query_props.len() > self.properties.len() {
            return false;
        }

        // 检查查询属性是否匹配索引属性的前缀
        for (i, query_prop) in query_props.iter().enumerate() {
            if &self.properties[i] != query_prop {
                return false;
            }
        }

        true
    }
}

/// 复合索引
///
/// 使用 BTreeMap 支持范围查询和前缀查询
#[derive(Debug, Clone)]
pub struct CompositeIndex {
    /// 索引定义
    def: CompositeIndexDef,
    /// 索引数据: Vec<value> -> BTreeSet<node_id>
    /// 使用 BTreeSet 支持范围查询和自动排序
    data: BTreeMap<Vec<CompositeIndexValue>, BTreeSet<NodeId>>,
    /// 统计信息
    stats: CompositeIndexStats,
}

impl CompositeIndex {
    /// 创建新的复合索引
    pub fn new(def: CompositeIndexDef) -> Self {
        Self {
            def,
            data: BTreeMap::new(),
            stats: CompositeIndexStats::default(),
        }
    }

    /// 获取索引定义
    pub fn def(&self) -> &CompositeIndexDef {
        &self.def
    }

    /// 添加条目到索引
    ///
    /// # 参数
    /// - `values`: 属性值列表（与索引定义中的属性列表对应）
    /// - `node_id`: 节点ID
    pub fn insert(&mut self, values: Vec<CompositeIndexValue>, node_id: NodeId) {
        if values.len() != self.def.properties.len() {
            return;
        }

        let entry = self.data.entry(values).or_insert_with(BTreeSet::new);
        let is_new = entry.insert(node_id);

        // 更新统计
        self.stats.insert_count += 1;
        if is_new {
            self.stats.unique_keys += 1;
        }
        self.stats.total_entries = self.data.iter().map(|(_, ids)| ids.len()).sum();
    }

    /// 精确匹配查询
    ///
    /// # 参数
    /// - `values`: 完整的属性值列表
    ///
    /// # 返回
    /// 匹配的节点ID列表
    pub fn find(&self, values: &[CompositeIndexValue]) -> Vec<NodeId> {
        self.data
            .get(values)
            .map(|ids| ids.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// 前缀查询
    ///
    /// 使用前 n 个属性进行查询
    ///
    /// # 参数
    /// - `prefix_values`: 前缀属性值
    ///
    /// # 返回
    /// 匹配的节点ID列表
    pub fn find_prefix(&self, prefix_values: &[CompositeIndexValue]) -> Vec<NodeId> {
        let mut result = Vec::new();

        // 空前缀匹配所有
        if prefix_values.is_empty() {
            return self.data.values().flat_map(|ids| ids.iter().cloned()).collect();
        }

        // BTreeMap 是有序的，可以执行范围查询
        // 我们需要找到所有以 prefix_values 开头的键
        //
        // 策略：
        // 1. start = prefix_values
        // 2. end = prefix_values + [最小可能值]，然后找到第一个不匹配的键
        //
        // 更简单的方法：直接迭代所有键，检查是否以前缀开头

        for (key, ids) in &self.data {
            // 检查键是否以前缀开头
            if key.len() >= prefix_values.len() {
                let mut matches = true;
                for (i, prefix_val) in prefix_values.iter().enumerate() {
                    if &key[i] != prefix_val {
                        matches = false;
                        break;
                    }
                }
                if matches {
                    result.extend(ids.iter().cloned());
                }
            }
        }

        result
    }

    /// 范围查询
    ///
    /// 查询指定范围内的值
    ///
    /// # 参数
    /// - `min_values`: 最小值（包含）
    /// - `max_values`: 最大值（包含）
    ///
    /// # 返回
    /// 范围内的节点ID列表
    pub fn find_range(
        &self,
        min_values: &[CompositeIndexValue],
        max_values: &[CompositeIndexValue],
    ) -> Vec<NodeId> {
        let mut result = Vec::new();

        for (_, ids) in self.data.range(min_values.to_vec()..=max_values.to_vec()) {
            result.extend(ids.iter().cloned());
        }

        result
    }

    /// 删除节点
    pub fn remove(&mut self, values: &[CompositeIndexValue], node_id: NodeId) -> bool {
        if let Some(entry) = self.data.get_mut(values) {
            let existed = entry.remove(&node_id);
            if entry.is_empty() {
                self.data.remove(values);
            }

            // 更新统计
            if existed {
                self.stats.delete_count += 1;
                self.stats.total_entries = self.data.iter().map(|(_, ids)| ids.len()).sum();
            }

            existed
        } else {
            false
        }
    }

    /// 清空索引
    pub fn clear(&mut self) {
        self.data.clear();
        self.stats = CompositeIndexStats::default();
    }

    /// 获取统计信息
    pub fn stats(&self) -> &CompositeIndexStats {
        &self.stats
    }

    /// 获取索引大小（键数量）
    pub fn size(&self) -> usize {
        self.data.len()
    }

    /// 获取索引中的节点总数
    pub fn total_entries(&self) -> usize {
        self.stats.total_entries
    }

    /// 更新统计信息
    pub fn update_stats(&mut self) {
        self.stats.unique_keys = self.data.len();
        self.stats.total_entries = self.data.iter().map(|(_, ids)| ids.len()).sum();
    }
}

/// 复合索引统计信息
#[derive(Debug, Clone, Default)]
pub struct CompositeIndexStats {
    /// 插入次数
    pub insert_count: u64,
    /// 删除次数
    pub delete_count: u64,
    /// 查询次数
    pub query_count: u64,
    /// 唯一键数量
    pub unique_keys: usize,
    /// 总条目数（节点总数）
    pub total_entries: usize,
}

impl CompositeIndexStats {
    /// 计算索引的选择性
    ///
    /// 选择性 = 唯一键数量 / 总条目数
    /// 值越接近1，选择性越高，索引效果越好
    pub fn selectivity(&self) -> f64 {
        if self.total_entries == 0 {
            0.0
        } else {
            self.unique_keys as f64 / self.total_entries as f64
        }
    }

    /// 计算平均每个键的节点数
    pub fn avg_nodes_per_key(&self) -> f64 {
        if self.unique_keys == 0 {
            0.0
        } else {
            self.total_entries as f64 / self.unique_keys as f64
        }
    }
}

/// 复合索引管理器
///
/// 管理多个复合索引
#[derive(Debug, Clone)]
pub struct CompositeIndexManager {
    /// 索引列表（按ID索引）
    indexes: HashMap<usize, CompositeIndex>,
    /// 下一个索引ID
    next_id: usize,
}

impl CompositeIndexManager {
    /// 创建新的复合索引管理器
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
            next_id: 0,
        }
    }

    /// 创建复合索引
    ///
    /// # 参数
    /// - `label`: 节点标签
    /// - `properties`: 属性名列表
    /// - `unique`: 是否唯一索引
    ///
    /// # 返回
    /// 索引ID
    pub fn create_index(
        &mut self,
        label: String,
        properties: Vec<String>,
        unique: bool,
    ) -> usize {
        let id = self.next_id;
        self.next_id += 1;

        let def = CompositeIndexDef::new(id, label, properties, unique);
        let index = CompositeIndex::new(def);

        self.indexes.insert(id, index);
        id
    }

    /// 删除索引
    pub fn drop_index(&mut self, index_id: usize) -> bool {
        self.indexes.remove(&index_id).is_some()
    }

    /// 获取索引
    pub fn get_index(&self, index_id: usize) -> Option<&CompositeIndex> {
        self.indexes.get(&index_id)
    }

    /// 获取可变索引
    pub fn get_index_mut(&mut self, index_id: usize) -> Option<&mut CompositeIndex> {
        self.indexes.get_mut(&index_id)
    }

    /// 查找可以用于给定查询的索引
    ///
    /// # 参数
    /// - `label`: 节点标签
    /// - `query_props`: 查询中使用的属性列表
    ///
    /// # 返回
    /// 匹配的索引ID列表（按属性数量排序，优先返回最匹配的）
    pub fn find_usable_indexes(
        &self,
        label: &str,
        query_props: &[String],
    ) -> Vec<usize> {
        let mut matches: Vec<_> = self
            .indexes
            .iter()
            .filter(|(_, index)| {
                index.def().label == label && index.def().can_satisfy(query_props)
            })
            .map(|(id, index)| (*id, index.def().properties.len()))
            .collect();

        // 按属性数量排序（属性越多，索引越精确）
        matches.sort_by(|a, b| b.1.cmp(&a.1));

        matches.into_iter().map(|(id, _)| id).collect()
    }

    /// 为所有相关索引添加节点
    ///
    /// # 参数
    /// - `label`: 节点标签
    /// - `properties`: 属性名列表
    /// - `values`: 属性值列表
    /// - `node_id`: 节点ID
    pub fn insert_node(
        &mut self,
        label: &str,
        properties: &[String],
        values: &[Value],
        node_id: NodeId,
    ) {
        for index in self.indexes.values_mut() {
            if index.def().label != label {
                continue;
            }

            // 检查属性是否匹配
            if index.def().properties.len() > properties.len() {
                continue;
            }

            // 构建索引值
            let mut index_values = Vec::new();
            let mut matches = true;

            for prop in &index.def().properties {
                if let Some(pos) = properties.iter().position(|p| p == prop) {
                    if let Some(idx_value) = CompositeIndexValue::from_value(&values[pos]) {
                        index_values.push(idx_value);
                    } else {
                        matches = false;
                        break;
                    }
                } else {
                    matches = false;
                    break;
                }
            }

            if matches {
                index.insert(index_values, node_id);
            }
        }
    }

    /// 删除节点（从所有索引中）
    pub fn remove_node(&mut self, label: &str, properties: &[String], values: &[Value], node_id: NodeId) {
        for index in self.indexes.values_mut() {
            if index.def().label != label {
                continue;
            }

            // 构建索引值
            let mut index_values = Vec::new();

            for prop in &index.def().properties {
                if let Some(pos) = properties.iter().position(|p| p == prop) {
                    if let Some(idx_value) = CompositeIndexValue::from_value(&values[pos]) {
                        index_values.push(idx_value);
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }

            if index_values.len() == index.def().properties.len() {
                index.remove(&index_values, node_id);
            }
        }
    }

    /// 获取所有索引的统计信息
    pub fn get_all_stats(&self) -> Vec<(usize, CompositeIndexStats)> {
        self.indexes
            .iter()
            .map(|(id, index)| (*id, index.stats().clone()))
            .collect()
    }

    /// 获取索引数量
    pub fn count(&self) -> usize {
        self.indexes.len()
    }

    /// 清空所有索引
    pub fn clear_all(&mut self) {
        for index in self.indexes.values_mut() {
            index.clear();
        }
    }
}

impl Default for CompositeIndexManager {
    fn default() -> Self {
        Self::new()
    }
}

/// 线程安全的复合索引管理器
#[derive(Debug, Clone)]
pub struct ThreadSafeCompositeIndexManager {
    inner: Arc<RwLock<CompositeIndexManager>>,
}

impl ThreadSafeCompositeIndexManager {
    /// 创建新的线程安全复合索引管理器
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(CompositeIndexManager::new())),
        }
    }

    /// 创建复合索引
    pub fn create_index(
        &self,
        label: String,
        properties: Vec<String>,
        unique: bool,
    ) -> usize {
        let mut manager = self.inner.write().unwrap();
        manager.create_index(label, properties, unique)
    }

    /// 删除索引
    pub fn drop_index(&self, index_id: usize) -> bool {
        let mut manager = self.inner.write().unwrap();
        manager.drop_index(index_id)
    }

    /// 为所有相关索引添加节点
    pub fn insert_node(
        &self,
        label: &str,
        properties: &[String],
        values: &[Value],
        node_id: NodeId,
    ) {
        let mut manager = self.inner.write().unwrap();
        manager.insert_node(label, properties, values, node_id);
    }

    /// 删除节点
    pub fn remove_node(
        &self,
        label: &str,
        properties: &[String],
        values: &[Value],
        node_id: NodeId,
    ) {
        let mut manager = self.inner.write().unwrap();
        manager.remove_node(label, properties, values, node_id);
    }

    /// 查找可用索引
    pub fn find_usable_indexes(&self, label: &str, query_props: &[String]) -> Vec<usize> {
        let manager = self.inner.read().unwrap();
        manager.find_usable_indexes(label, query_props)
    }

    /// 获取所有统计信息
    pub fn get_all_stats(&self) -> Vec<(usize, CompositeIndexStats)> {
        let manager = self.inner.read().unwrap();
        manager.get_all_stats()
    }
}

impl Default for ThreadSafeCompositeIndexManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== CompositeIndexValue 测试 ==========

    #[test]
    fn test_composite_index_value_from_int() {
        let value = Value::Int(42);
        let idx_value = CompositeIndexValue::from_value(&value);
        assert!(idx_value.is_some());
        assert_eq!(idx_value.unwrap(), CompositeIndexValue::Int(42));
    }

    #[test]
    fn test_composite_index_value_from_float() {
        let value = Value::Float(3.14);
        let idx_value = CompositeIndexValue::from_value(&value);
        assert!(idx_value.is_some());
        // Float 转换为 u64 bits
        assert!(matches!(idx_value.unwrap(), CompositeIndexValue::Float(_)));
    }

    #[test]
    fn test_composite_index_value_from_text() {
        let value = Value::Text("hello".to_string());
        let idx_value = CompositeIndexValue::from_value(&value);
        assert!(idx_value.is_some());
        assert_eq!(idx_value.unwrap(), CompositeIndexValue::String("hello".to_string()));
    }

    #[test]
    fn test_composite_index_value_size() {
        let int_val = CompositeIndexValue::Int(42);
        assert_eq!(int_val.size_bytes(), 8);

        let str_val = CompositeIndexValue::String("hello".to_string());
        assert_eq!(str_val.size_bytes(), 5);
    }

    // ========== CompositeIndexDef 测试 ==========

    #[test]
    fn test_composite_index_def_creation() {
        let def = CompositeIndexDef::new(
            1,
            "User".to_string(),
            vec!["name".to_string(), "age".to_string()],
            false,
        );

        assert_eq!(def.id, 1);
        assert_eq!(def.label, "User");
        assert_eq!(def.properties.len(), 2);
        assert!(!def.unique);
    }

    #[test]
    fn test_can_satisfy_exact_match() {
        let def = CompositeIndexDef::new(
            1,
            "User".to_string(),
            vec!["name".to_string(), "age".to_string()],
            false,
        );

        // 完全匹配
        assert!(def.can_satisfy(&["name".to_string(), "age".to_string()]));
    }

    #[test]
    fn test_can_satisfy_prefix_match() {
        let def = CompositeIndexDef::new(
            1,
            "User".to_string(),
            vec!["name".to_string(), "age".to_string()],
            false,
        );

        // 前缀匹配
        assert!(def.can_satisfy(&["name".to_string()]));
    }

    #[test]
    fn test_can_satisfy_no_match() {
        let def = CompositeIndexDef::new(
            1,
            "User".to_string(),
            vec!["name".to_string(), "age".to_string()],
            false,
        );

        // 属性不匹配
        assert!(!def.can_satisfy(&["email".to_string()]));
        assert!(!def.can_satisfy(&["age".to_string()])); // 顺序错误
    }

    #[test]
    fn test_prefix_key() {
        let def = CompositeIndexDef::new(
            1,
            "User".to_string(),
            vec!["name".to_string(), "age".to_string()],
            false,
        );

        let values = vec![
            CompositeIndexValue::String("Alice".to_string()),
            CompositeIndexValue::Int(30),
        ];

        // 前缀长度为1
        let prefix = def.prefix_key(&values, 1);
        assert_eq!(prefix.len(), 1);
        assert_eq!(prefix[0], CompositeIndexValue::String("Alice".to_string()));

        // 前缀长度超过实际长度
        let prefix = def.prefix_key(&values, 5);
        assert_eq!(prefix.len(), 2);
    }

    // ========== CompositeIndex 测试 ==========

    #[test]
    fn test_composite_index_insert_and_find() {
        let def = CompositeIndexDef::new(
            1,
            "User".to_string(),
            vec!["name".to_string(), "age".to_string()],
            false,
        );
        let mut index = CompositeIndex::new(def);

        let values = vec![
            CompositeIndexValue::String("Alice".to_string()),
            CompositeIndexValue::Int(30),
        ];

        index.insert(values.clone(), 1);
        index.insert(values.clone(), 2);

        let result = index.find(&values);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&1));
        assert!(result.contains(&2));
    }

    #[test]
    fn test_composite_index_prefix_query() {
        let def = CompositeIndexDef::new(
            1,
            "User".to_string(),
            vec!["name".to_string(), "age".to_string()],
            false,
        );
        let mut index = CompositeIndex::new(def);

        // 插入多个节点，name 相同但 age 不同
        index.insert(
            vec![
                CompositeIndexValue::String("Alice".to_string()),
                CompositeIndexValue::Int(30),
            ],
            1,
        );
        index.insert(
            vec![
                CompositeIndexValue::String("Alice".to_string()),
                CompositeIndexValue::Int(25),
            ],
            2,
        );
        index.insert(
            vec![
                CompositeIndexValue::String("Bob".to_string()),
                CompositeIndexValue::Int(35),
            ],
            3,
        );

        // 使用 name 前缀查询
        let prefix = vec![CompositeIndexValue::String("Alice".to_string())];
        let result = index.find_prefix(&prefix);

        assert_eq!(result.len(), 2);
        assert!(result.contains(&1));
        assert!(result.contains(&2));
        assert!(!result.contains(&3));
    }

    #[test]
    fn test_composite_index_range_query() {
        let def = CompositeIndexDef::new(
            1,
            "User".to_string(),
            vec!["age".to_string()],
            false,
        );
        let mut index = CompositeIndex::new(def);

        index.insert(vec![CompositeIndexValue::Int(20)], 1);
        index.insert(vec![CompositeIndexValue::Int(25)], 2);
        index.insert(vec![CompositeIndexValue::Int(30)], 3);
        index.insert(vec![CompositeIndexValue::Int(35)], 4);

        // 查询年龄在 25-30 之间
        let min = vec![CompositeIndexValue::Int(25)];
        let max = vec![CompositeIndexValue::Int(30)];
        let result = index.find_range(&min, &max);

        assert_eq!(result.len(), 2);
        assert!(result.contains(&2));
        assert!(result.contains(&3));
    }

    #[test]
    fn test_composite_index_remove() {
        let def = CompositeIndexDef::new(
            1,
            "User".to_string(),
            vec!["name".to_string()],
            false,
        );
        let mut index = CompositeIndex::new(def);

        let values = vec![CompositeIndexValue::String("Alice".to_string())];
        index.insert(values.clone(), 1);
        index.insert(values.clone(), 2);

        assert_eq!(index.find(&values).len(), 2);

        // 删除节点 1
        assert!(index.remove(&values, 1));
        assert_eq!(index.find(&values), vec![2]);

        // 再次删除应该返回 false
        assert!(!index.remove(&values, 1));
    }

    #[test]
    fn test_composite_index_stats() {
        let def = CompositeIndexDef::new(
            1,
            "User".to_string(),
            vec!["age".to_string()],
            false,
        );
        let mut index = CompositeIndex::new(def);

        index.insert(vec![CompositeIndexValue::Int(20)], 1);
        index.insert(vec![CompositeIndexValue::Int(20)], 2);
        index.insert(vec![CompositeIndexValue::Int(30)], 3);

        index.update_stats();

        let stats = index.stats();
        assert_eq!(stats.unique_keys, 2);
        assert_eq!(stats.total_entries, 3);
        assert_eq!(stats.insert_count, 3);

        // 选择性应该约为 0.67 (2/3)
        let selectivity = stats.selectivity();
        assert!(selectivity > 0.6 && selectivity < 0.7);
    }

    // ========== CompositeIndexManager 测试 ==========

    #[test]
    fn test_composite_index_manager_create() {
        let mut manager = CompositeIndexManager::new();

        let id1 = manager.create_index(
            "User".to_string(),
            vec!["name".to_string()],
            false,
        );

        let id2 = manager.create_index(
            "User".to_string(),
            vec!["name".to_string(), "age".to_string()],
            false,
        );

        assert_ne!(id1, id2);
        assert_eq!(manager.count(), 2);
    }

    #[test]
    fn test_composite_index_manager_find_usable() {
        let mut manager = CompositeIndexManager::new();

        manager.create_index(
            "User".to_string(),
            vec!["name".to_string(), "age".to_string()],
            false,
        );

        manager.create_index(
            "User".to_string(),
            vec!["name".to_string()],
            false,
        );

        // 查询 name 属性，两个索引都可以用
        let indexes = manager.find_usable_indexes("User", &["name".to_string()]);
        assert_eq!(indexes.len(), 2);

        // 第一个应该是双属性索引（更精确）
        let index1 = manager.get_index(indexes[0]).unwrap();
        assert_eq!(index1.def().properties.len(), 2);
    }

    #[test]
    fn test_composite_index_manager_insert_node() {
        let mut manager = CompositeIndexManager::new();

        let id = manager.create_index(
            "User".to_string(),
            vec!["name".to_string()],
            false,
        );

        let properties = vec!["name".to_string(), "age".to_string()];
        let values = vec![
            Value::Text("Alice".to_string()),
            Value::Int(30),
        ];

        manager.insert_node("User", &properties, &values, 1);

        let index = manager.get_index(id).unwrap();
        let result = index.find(&[CompositeIndexValue::String("Alice".to_string())]);
        assert_eq!(result, vec![1]);
    }

    #[test]
    fn test_composite_index_manager_remove_node() {
        let mut manager = CompositeIndexManager::new();

        let id = manager.create_index(
            "User".to_string(),
            vec!["name".to_string()],
            false,
        );

        let properties = vec!["name".to_string()];
        let values = vec![Value::Text("Alice".to_string())];

        manager.insert_node("User", &properties, &values, 1);
        manager.insert_node("User", &properties, &values, 2);

        manager.remove_node("User", &properties, &values, 1);

        let index = manager.get_index(id).unwrap();
        let result = index.find(&[CompositeIndexValue::String("Alice".to_string())]);
        assert_eq!(result, vec![2]);
    }

    #[test]
    fn test_composite_index_manager_drop_index() {
        let mut manager = CompositeIndexManager::new();

        let id = manager.create_index(
            "User".to_string(),
            vec!["name".to_string()],
            false,
        );

        assert_eq!(manager.count(), 1);
        assert!(manager.drop_index(id));
        assert_eq!(manager.count(), 0);
        assert!(!manager.drop_index(id)); // 再次删除应该返回 false
    }

    #[test]
    fn test_composite_index_manager_get_all_stats() {
        let mut manager = CompositeIndexManager::new();

        let id1 = manager.create_index(
            "User".to_string(),
            vec!["name".to_string()],
            false,
        );

        manager.insert_node(
            "User",
            &["name".to_string()],
            &[Value::Text("Alice".to_string())],
            1,
        );

        let stats = manager.get_all_stats();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].0, id1);
        assert_eq!(stats[0].1.insert_count, 1);
    }

    // ========== ThreadSafeCompositeIndexManager 测试 ==========

    #[test]
    fn test_thread_safe_composite_index_manager() {
        let manager = ThreadSafeCompositeIndexManager::new();

        let id = manager.create_index(
            "User".to_string(),
            vec!["name".to_string()],
            false,
        );

        manager.insert_node(
            "User",
            &["name".to_string()],
            &[Value::Text("Alice".to_string())],
            1,
        );

        let indexes = manager.find_usable_indexes("User", &["name".to_string()]);
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0], id);

        let stats = manager.get_all_stats();
        assert_eq!(stats.len(), 1);
    }
}
