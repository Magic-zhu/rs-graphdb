use crate::storage::NodeId;
use crate::values::Value;
use std::collections::HashMap;

// 导入高级索引
use crate::index_advanced::{FullTextIndex, RangeIndex};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ValueKey {
    Int(i64),
    Bool(bool),
    Text(String),
}

impl TryFrom<&Value> for ValueKey {
    type Error = ();

    fn try_from(v: &Value) -> Result<Self, Self::Error> {
        match v {
            Value::Int(i) => Ok(ValueKey::Int(*i)),
            Value::Bool(b) => Ok(ValueKey::Bool(*b)),
            Value::Text(s) => Ok(ValueKey::Text(s.clone())),
            _ => Err(()),
        }
    }
}

/// 复合索引键
///
/// 用于多属性索引，例如 (name, age) 的复合索引
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CompositeKey {
    /// 标签
    pub label: String,
    /// 属性名列表（按索引顺序）
    pub properties: Vec<String>,
    /// 值列表（与属性名一一对应）
    pub values: Vec<ValueKey>,
}

impl CompositeKey {
    /// 创建一个新的复合键
    pub fn new(label: String, properties: Vec<String>, values: Vec<ValueKey>) -> Self {
        assert_eq!(properties.len(), values.len(),
            "Properties and values must have the same length");
        Self { label, properties, values }
    }

    /// 从切片创建复合键
    pub fn from_slices(
        label: &str,
        properties: &[&str],
        values: &[ValueKey],
    ) -> Self {
        Self {
            label: label.to_string(),
            properties: properties.iter().map(|s| s.to_string()).collect(),
            values: values.to_vec(),
        }
    }
}

/// 一个非常简单的属性索引：
/// (label, property_name, value) -> [node_id]
#[derive(Default)]
pub struct PropertyIndex {
    /// 单属性索引: (label, property_name, value) -> [node_id]
    map: HashMap<(String, String, ValueKey), Vec<NodeId>>,
    /// 复合索引: composite_key -> [node_id]
    composite_map: HashMap<CompositeKey, Vec<NodeId>>,
    /// 全文索引
    fulltext_index: FullTextIndex,
    /// 范围索引
    range_index: RangeIndex,
}

impl PropertyIndex {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            composite_map: HashMap::new(),
            fulltext_index: FullTextIndex::new(),
            range_index: RangeIndex::new(),
        }
    }

    /// 添加单属性索引
    pub fn add(
        &mut self,
        label: &str,
        prop_name: &str,
        value: &Value,
        node_id: NodeId,
    ) {
        if let Ok(key) = ValueKey::try_from(value) {
            let k = (label.to_string(), prop_name.to_string(), key);
            let entry = self.map.entry(k).or_default();
            if !entry.contains(&node_id) {
                entry.push(node_id);
            }
        }
    }

    /// 查询单属性索引
    pub fn find(
        &self,
        label: &str,
        prop_name: &str,
        value: &Value,
    ) -> Vec<NodeId> {
        if let Ok(key) = ValueKey::try_from(value) {
            let k = (label.to_string(), prop_name.to_string(), key);
            self.map.get(&k).cloned().unwrap_or_default()
        } else {
            Vec::new()
        }
    }

    /// 添加复合索引
    ///
    /// # 参数
    /// - `label`: 节点标签
    /// - `properties`: 属性名列表（按索引顺序）
    /// - `values`: 属性值列表（与属性名一一对应）
    /// - `node_id`: 要索引的节点ID
    pub fn add_composite(
        &mut self,
        label: &str,
        properties: &[&str],
        values: &[Value],
        node_id: NodeId,
    ) {
        // 将所有值转换为 ValueKey
        let value_keys: Vec<ValueKey> = values
            .iter()
            .filter_map(|v| ValueKey::try_from(v).ok())
            .collect();

        // 如果所有值都能转换，则创建复合索引
        if value_keys.len() == values.len() {
            let key = CompositeKey::from_slices(label, properties, &value_keys);
            let entry = self.composite_map.entry(key).or_default();
            if !entry.contains(&node_id) {
                entry.push(node_id);
            }
        }
    }

    /// 查询复合索引
    ///
    /// # 参数
    /// - `label`: 节点标签
    /// - `properties`: 属性名列表
    /// - `values`: 属性值列表
    ///
    /// # 返回
    /// 匹配的节点ID列表
    pub fn find_composite(
        &self,
        label: &str,
        properties: &[&str],
        values: &[Value],
    ) -> Vec<NodeId> {
        // 将所有值转换为 ValueKey
        let value_keys: Vec<ValueKey> = values
            .iter()
            .filter_map(|v| ValueKey::try_from(v).ok())
            .collect();

        if value_keys.len() != values.len() {
            return Vec::new();
        }

        let key = CompositeKey::from_slices(label, properties, &value_keys);
        self.composite_map.get(&key).cloned().unwrap_or_default()
    }

    /// 删除节点的索引（用于删除节点时清理索引）
    pub fn remove(&mut self, node_id: NodeId) {
        // 从单属性索引中删除
        for entry in self.map.values_mut() {
            entry.retain(|&id| id != node_id);
        }

        // 从复合索引中删除
        for entry in self.composite_map.values_mut() {
            entry.retain(|&id| id != node_id);
        }

        // 从高级索引中删除
        self.fulltext_index.remove(node_id);
        self.range_index.remove(node_id);
    }

    /// 清空所有索引
    pub fn clear(&mut self) {
        self.map.clear();
        self.composite_map.clear();
        self.fulltext_index.clear();
        self.range_index.clear();
    }

    /// 获取单属性索引的数量
    pub fn single_index_count(&self) -> usize {
        self.map.len()
    }

    /// 获取复合索引的数量
    pub fn composite_index_count(&self) -> usize {
        self.composite_map.len()
    }

    // ========== 全文索引 API ==========

    /// 添加全文索引
    ///
    /// # 参数
    /// - `label`: 节点标签
    /// - `property_name`: 属性名
    /// - `text`: 文本内容
    /// - `node_id`: 节点ID
    pub fn add_fulltext(
        &mut self,
        label: &str,
        property_name: &str,
        text: &str,
        node_id: NodeId,
    ) {
        self.fulltext_index.add(label, property_name, text, node_id);
    }

    /// 全文搜索（OR 查询）
    ///
    /// 返回包含任意搜索词的节点
    pub fn search_fulltext(
        &self,
        label: &str,
        property_name: &str,
        query: &str,
    ) -> Vec<NodeId> {
        self.fulltext_index.search(label, property_name, query)
    }

    /// 全文搜索（AND 查询）
    ///
    /// 返回同时包含所有搜索词的节点
    pub fn search_fulltext_and(
        &self,
        label: &str,
        property_name: &str,
        query: &str,
    ) -> Vec<NodeId> {
        self.fulltext_index.search_and(label, property_name, query)
    }

    // ========== 范围索引 API ==========

    /// 添加范围索引（自动处理）
    ///
    /// 如果值是数值类型（Int/Float），自动添加到范围索引
    pub fn add_range(
        &mut self,
        label: &str,
        property_name: &str,
        value: &Value,
        node_id: NodeId,
    ) {
        self.range_index.add(label, property_name, value, node_id);
    }

    /// 范围查询：大于
    pub fn range_greater_than(
        &self,
        label: &str,
        property_name: &str,
        value: &Value,
    ) -> Vec<NodeId> {
        self.range_index.greater_than(label, property_name, value)
    }

    /// 范围查询：小于
    pub fn range_less_than(
        &self,
        label: &str,
        property_name: &str,
        value: &Value,
    ) -> Vec<NodeId> {
        self.range_index.less_than(label, property_name, value)
    }

    /// 范围查询：范围之间
    pub fn range_between(
        &self,
        label: &str,
        property_name: &str,
        min_value: &Value,
        max_value: &Value,
    ) -> Vec<NodeId> {
        self.range_index.range(label, property_name, min_value, max_value)
    }
}
