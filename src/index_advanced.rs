// 高级索引模块
//
// 提供全文索引和范围索引功能

use crate::storage::NodeId;
use crate::values::Value;
use std::collections::{HashMap, BTreeMap, BTreeSet, HashSet};
use std::hash::{Hash, Hasher};

/// 浮点数包装器，用于实现 Hash 和 Eq
///
/// f64 本身不实现 Hash，所以需要包装器来处理
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrderedFloat(f64);

impl OrderedFloat {
    pub fn new(value: f64) -> Self {
        Self(value)
    }

    pub fn value(&self) -> f64 {
        self.0
    }
}

impl Eq for OrderedFloat {}

impl Hash for OrderedFloat {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // 使用浮点数的位表示来计算 hash
        state.write_u64(self.0.to_bits());
    }
}

impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// 全文索引
///
/// 用于文本搜索，支持分词和包含查询
/// 例如：WHERE n.name CONTAINS "keyword"
#[derive(Debug)]
pub struct FullTextIndex {
    /// 词项索引: (label, property_name, word) -> [node_id]
    /// 使用倒排索引结构，每个词指向包含该词的节点ID列表
    inverted_index: HashMap<(String, String, String), Vec<NodeId>>,
    /// 文档长度: node_id -> word_count
    /// 用于计算相关性和评分
    doc_lengths: HashMap<NodeId, usize>,
}

impl FullTextIndex {
    /// 创建新的全文索引
    pub fn new() -> Self {
        Self {
            inverted_index: HashMap::new(),
            doc_lengths: HashMap::new(),
        }
    }

    /// 分词器：将文本分解为词项
    ///
    /// 支持中文和英文分词
    fn tokenize(text: &str) -> Vec<String> {
        let mut tokens = Vec::new();

        // 按空格分割的英文单词
        for word in text.split_whitespace() {
            let cleaned: String = word
                .chars()
                .filter(|c| c.is_alphanumeric())
                .collect();
            if !cleaned.is_empty() {
                tokens.push(cleaned.to_lowercase());
            }
        }

        tokens
    }

    /// 添加文档到全文索引
    ///
    /// # 参数
    /// - `label`: 节点标签
    /// - `property_name`: 属性名
    /// - `text`: 文本内容
    /// - `node_id`: 节点ID
    pub fn add(
        &mut self,
        label: &str,
        property_name: &str,
        text: &str,
        node_id: NodeId,
    ) {
        // 分词
        let tokens = Self::tokenize(text);

        // 记录文档长度
        self.doc_lengths.insert(node_id, tokens.len());

        // 为每个词项添加到倒排索引
        for token in tokens {
            let key = (label.to_string(), property_name.to_string(), token);
            let entry = self.inverted_index.entry(key).or_default();
            if !entry.contains(&node_id) {
                entry.push(node_id);
            }
        }
    }

    /// 全文搜索：包含指定词项的文档
    ///
    /// # 参数
    /// - `label`: 节点标签
    /// - `property_name`: 属性名
    /// - `query`: 搜索词
    ///
    /// # 返回
    /// 包含搜索词的节点ID列表
    pub fn search(
        &self,
        label: &str,
        property_name: &str,
        query: &str,
    ) -> Vec<NodeId> {
        // 对查询进行分词
        let query_tokens = Self::tokenize(query);

        if query_tokens.is_empty() {
            return Vec::new();
        }

        // 收集所有匹配的节点ID
        let mut result_set = Vec::new();

        for token in query_tokens {
            let key = (label.to_string(), property_name.to_string(), token);
            if let Some(node_ids) = self.inverted_index.get(&key) {
                for &node_id in node_ids {
                    if !result_set.contains(&node_id) {
                        result_set.push(node_id);
                    }
                }
            }
        }

        result_set
    }

    /// 全文搜索：包含所有指定词项的文档（AND 查询）
    ///
    /// # 参数
    /// - `label`: 节点标签
    /// - `property_name`: 属性名
    /// - `query`: 搜索词（多个词）
    ///
    /// # 返回
    /// 同时包含所有搜索词的节点ID列表
    pub fn search_and(
        &self,
        label: &str,
        property_name: &str,
        query: &str,
    ) -> Vec<NodeId> {
        let query_tokens = Self::tokenize(query);

        if query_tokens.is_empty() {
            return Vec::new();
        }

        // 获取每个词项的节点ID集合
        let mut node_sets: Vec<BTreeSet<NodeId>> = Vec::new();

        for token in query_tokens {
            let key = (label.to_string(), property_name.to_string(), token);
            if let Some(node_ids) = self.inverted_index.get(&key) {
                node_sets.push(node_ids.iter().cloned().collect());
            } else {
                // 如果有一个词项不存在，则直接返回空结果
                return Vec::new();
            }
        }

        // 计算交集（AND 操作）
        if node_sets.is_empty() {
            return Vec::new();
        }

        let mut result = node_sets[0].clone();
        for set in &node_sets[1..] {
            result = result.intersection(set).cloned().collect();
        }

        result.into_iter().collect()
    }

    /// 删除节点的索引
    pub fn remove(&mut self, node_id: NodeId) {
        // 从倒排索引中删除
        for entry in self.inverted_index.values_mut() {
            entry.retain(|&id| id != node_id);
        }

        // 从文档长度中删除
        self.doc_lengths.remove(&node_id);
    }

    /// 清空所有索引
    pub fn clear(&mut self) {
        self.inverted_index.clear();
        self.doc_lengths.clear();
    }

    /// 获取索引中的词项数量
    pub fn term_count(&self) -> usize {
        self.inverted_index.len()
    }

    /// 获取索引中的文档数量
    pub fn doc_count(&self) -> usize {
        self.doc_lengths.len()
    }
}

impl Default for FullTextIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// 范围索引
///
/// 用于范围查询优化，例如：WHERE n.age > 25 AND n.age < 50
/// 使用 BTreeMap 实现高效的范围查询
#[derive(Debug)]
pub struct RangeIndex {
    /// 整数范围索引: (label, property_name) -> BTreeMap<value, [node_id]>
    int_index: HashMap<(String, String), BTreeMap<i64, Vec<NodeId>>>,
    /// 浮点数范围索引: (label, property_name) -> BTreeMap<OrderedFloat, [node_id]>
    float_index: HashMap<(String, String), BTreeMap<OrderedFloat, Vec<NodeId>>>,
}

impl RangeIndex {
    /// 创建新的范围索引
    pub fn new() -> Self {
        Self {
            int_index: HashMap::new(),
            float_index: HashMap::new(),
        }
    }

    /// 添加整数值到范围索引
    fn add_int(
        &mut self,
        label: &str,
        property_name: &str,
        value: i64,
        node_id: NodeId,
    ) {
        let key = (label.to_string(), property_name.to_string());
        let tree = self.int_index.entry(key).or_default();
        let entry = tree.entry(value).or_default();
        if !entry.contains(&node_id) {
            entry.push(node_id);
        }
    }

    /// 添加浮点数值到范围索引
    fn add_float(
        &mut self,
        label: &str,
        property_name: &str,
        value: f64,
        node_id: NodeId,
    ) {
        let key = (label.to_string(), property_name.to_string());
        let tree = self.float_index.entry(key).or_default();
        let ordered_value = OrderedFloat::new(value);
        let entry = tree.entry(ordered_value).or_default();
        if !entry.contains(&node_id) {
            entry.push(node_id);
        }
    }

    /// 添加值到范围索引
    ///
    /// # 参数
    /// - `label`: 节点标签
    /// - `property_name`: 属性名
    /// - `value`: 属性值
    /// - `node_id`: 节点ID
    pub fn add(
        &mut self,
        label: &str,
        property_name: &str,
        value: &Value,
        node_id: NodeId,
    ) {
        match value {
            Value::Int(i) => {
                self.add_int(label, property_name, *i, node_id);
            }
            Value::Float(f) => {
                self.add_float(label, property_name, *f, node_id);
            }
            _ => {
                // 其他类型不支持范围索引
            }
        }
    }

    /// 范围查询：大于指定值
    ///
    /// # 参数
    /// - `label`: 节点标签
    /// - `property_name`: 属性名
    /// - `value`: 比较值
    ///
    /// # 返回
    /// 大于指定值的节点ID列表
    pub fn greater_than(
        &self,
        label: &str,
        property_name: &str,
        value: &Value,
    ) -> Vec<NodeId> {
        let key = (label.to_string(), property_name.to_string());

        match value {
            Value::Int(v) => {
                if let Some(tree) = self.int_index.get(&key) {
                    return tree
                        .range(v..)
                        .flat_map(|(_, ids)| ids.iter().copied())
                        .collect();
                }
            }
            Value::Float(v) => {
                if let Some(tree) = self.float_index.get(&key) {
                    return tree
                        .range(OrderedFloat::new(*v)..)
                        .flat_map(|(_, ids)| ids.iter().copied())
                        .collect();
                }
            }
            _ => {}
        }

        Vec::new()
    }

    /// 范围查询：大于等于指定值
    pub fn greater_or_equal(
        &self,
        label: &str,
        property_name: &str,
        value: &Value,
    ) -> Vec<NodeId> {
        let key = (label.to_string(), property_name.to_string());

        match value {
            Value::Int(v) => {
                if let Some(tree) = self.int_index.get(&key) {
                    return tree
                        .range(v..)
                        .flat_map(|(_, ids)| ids.iter().copied())
                        .collect();
                }
            }
            Value::Float(v) => {
                if let Some(tree) = self.float_index.get(&key) {
                    return tree
                        .iter()
                        .filter(|(val, _)| val.value() >= *v)
                        .flat_map(|(_, ids)| ids.iter().copied())
                        .collect();
                }
            }
            _ => {}
        }

        Vec::new()
    }

    /// 范围查询：小于指定值
    pub fn less_than(
        &self,
        label: &str,
        property_name: &str,
        value: &Value,
    ) -> Vec<NodeId> {
        let key = (label.to_string(), property_name.to_string());

        match value {
            Value::Int(v) => {
                if let Some(tree) = self.int_index.get(&key) {
                    return tree
                        .range(..v)
                        .flat_map(|(_, ids)| ids.iter().copied())
                        .collect();
                }
            }
            Value::Float(v) => {
                if let Some(tree) = self.float_index.get(&key) {
                    return tree
                        .iter()
                        .filter(|(val, _)| val.value() < *v)
                        .flat_map(|(_, ids)| ids.iter().copied())
                        .collect();
                }
            }
            _ => {}
        }

        Vec::new()
    }

    /// 范围查询：小于等于指定值
    pub fn less_or_equal(
        &self,
        label: &str,
        property_name: &str,
        value: &Value,
    ) -> Vec<NodeId> {
        let key = (label.to_string(), property_name.to_string());

        match value {
            Value::Int(v) => {
                if let Some(tree) = self.int_index.get(&key) {
                    return tree
                        .range(..=v)
                        .flat_map(|(_, ids)| ids.iter().copied())
                        .collect();
                }
            }
            Value::Float(v) => {
                if let Some(tree) = self.float_index.get(&key) {
                    return tree
                        .iter()
                        .filter(|(val, _)| val.value() <= *v)
                        .flat_map(|(_, ids)| ids.iter().copied())
                        .collect();
                }
            }
            _ => {}
        }

        Vec::new()
    }

    /// 范围查询：在指定范围内（包含边界）
    ///
    /// # 参数
    /// - `label`: 节点标签
    /// - `property_name`: 属性名
    /// - `min_value`: 最小值
    /// - `max_value`: 最大值
    ///
    /// # 返回
    /// 在指定范围内的节点ID列表
    pub fn range(
        &self,
        label: &str,
        property_name: &str,
        min_value: &Value,
        max_value: &Value,
    ) -> Vec<NodeId> {
        let key = (label.to_string(), property_name.to_string());

        match (min_value, max_value) {
            (Value::Int(min_v), Value::Int(max_v)) => {
                if let Some(tree) = self.int_index.get(&key) {
                    return tree
                        .range(min_v..=max_v)
                        .flat_map(|(_, ids)| ids.iter().copied())
                        .collect();
                }
            }
            (Value::Float(min_v), Value::Float(max_v)) => {
                if let Some(tree) = self.float_index.get(&key) {
                    return tree
                        .iter()
                        .filter(|(val, _)| val.value() >= *min_v && val.value() <= *max_v)
                        .flat_map(|(_, ids)| ids.iter().copied())
                        .collect();
                }
            }
            _ => {}
        }

        Vec::new()
    }

    /// 删除节点的索引
    pub fn remove(&mut self, node_id: NodeId) {
        // 从整数索引中删除
        for tree in self.int_index.values_mut() {
            for entry in tree.values_mut() {
                entry.retain(|&id| id != node_id);
            }
        }

        // 从浮点数索引中删除
        for tree in self.float_index.values_mut() {
            for entry in tree.values_mut() {
                entry.retain(|&id| id != node_id);
            }
        }
    }

    /// 清空所有索引
    pub fn clear(&mut self) {
        self.int_index.clear();
        self.float_index.clear();
    }

    /// 获取整数字段的索引数量
    pub fn int_field_count(&self) -> usize {
        self.int_index.len()
    }

    /// 获取浮点数字段的索引数量
    pub fn float_field_count(&self) -> usize {
        self.float_index.len()
    }
}

impl Default for RangeIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== 全文索引测试 ==========

    #[test]
    fn test_fulltext_tokenize() {
        let tokens = FullTextIndex::tokenize("Hello World");
        assert!(tokens.contains(&"hello".to_string()));
        assert!(tokens.contains(&"world".to_string()));
    }

    #[test]
    fn test_fulltext_add_and_search() {
        let mut index = FullTextIndex::new();

        index.add("User", "name", "Alice Smith", 1);
        index.add("User", "name", "Bob Johnson", 2);
        index.add("User", "name", "Charlie Brown", 3);

        // 搜索 "Alice"
        let result = index.search("User", "name", "Alice");
        assert_eq!(result, vec![1]);

        // 搜索 "Smith"
        let result = index.search("User", "name", "Smith");
        assert_eq!(result, vec![1]);
    }

    #[test]
    fn test_fulltext_search_multiple() {
        let mut index = FullTextIndex::new();

        index.add("User", "bio", "software engineer", 1);
        index.add("User", "bio", "software developer", 2);
        index.add("User", "bio", "data scientist", 3);

        // 搜索 "software" - 应该返回 1 和 2
        let result = index.search("User", "bio", "software");
        assert_eq!(result.len(), 2);
        assert!(result.contains(&1));
        assert!(result.contains(&2));
    }

    #[test]
    fn test_fulltext_search_and() {
        let mut index = FullTextIndex::new();

        index.add("User", "bio", "machine learning engineer", 1);
        index.add("User", "bio", "machine learning", 2);
        index.add("User", "bio", "deep learning", 3);

        // 搜索 "machine learning" - AND 查询
        let result = index.search_and("User", "bio", "machine learning");
        assert_eq!(result.len(), 2);
        assert!(result.contains(&1));
        assert!(result.contains(&2));
    }

    #[test]
    fn test_fulltext_remove() {
        let mut index = FullTextIndex::new();

        index.add("User", "name", "Alice", 1);
        index.add("User", "name", "Alice", 2);

        assert_eq!(index.search("User", "name", "Alice").len(), 2);

        index.remove(1);
        assert_eq!(index.search("User", "name", "Alice"), vec![2]);
    }

    // ========== 范围索引测试 ==========

    #[test]
    fn test_range_add_int() {
        let mut index = RangeIndex::new();

        index.add("User", "age", &Value::Int(25), 1);
        index.add("User", "age", &Value::Int(30), 2);
        index.add("User", "age", &Value::Int(35), 3);
    }

    #[test]
    fn test_range_greater_than() {
        let mut index = RangeIndex::new();

        index.add("User", "age", &Value::Int(20), 1);
        index.add("User", "age", &Value::Int(25), 2);
        index.add("User", "age", &Value::Int(30), 3);

        let result = index.greater_than("User", "age", &Value::Int(24));
        assert_eq!(result.len(), 2);
        assert!(result.contains(&2));
        assert!(result.contains(&3));
    }

    #[test]
    fn test_range_less_than() {
        let mut index = RangeIndex::new();

        index.add("User", "age", &Value::Int(20), 1);
        index.add("User", "age", &Value::Int(25), 2);
        index.add("User", "age", &Value::Int(30), 3);

        let result = index.less_than("User", "age", &Value::Int(26));
        assert_eq!(result.len(), 2);
        assert!(result.contains(&1));
        assert!(result.contains(&2));
    }

    #[test]
    fn test_range_between() {
        let mut index = RangeIndex::new();

        index.add("User", "age", &Value::Int(20), 1);
        index.add("User", "age", &Value::Int(25), 2);
        index.add("User", "age", &Value::Int(30), 3);
        index.add("User", "age", &Value::Int(35), 4);

        let result = index.range("User", "age", &Value::Int(22), &Value::Int(32));
        assert_eq!(result.len(), 2);
        assert!(result.contains(&2));
        assert!(result.contains(&3));
    }

    #[test]
    fn test_range_float() {
        let mut index = RangeIndex::new();

        index.add("Product", "price", &Value::Float(10.5), 1);
        index.add("Product", "price", &Value::Float(20.0), 2);
        index.add("Product", "price", &Value::Float(30.5), 3);

        let result = index.range("Product", "price", &Value::Float(15.0), &Value::Float(25.0));
        assert_eq!(result, vec![2]);
    }
}
