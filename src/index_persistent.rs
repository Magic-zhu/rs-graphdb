//! 持久化属性索引模块
//!
//! 将索引持久化到 sled 数据库，支持启动时重建索引

use crate::storage::{NodeId, StoredNode};
use crate::values::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 索引键，用于持久化存储
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexKey {
    pub label: String,
    pub property: String,
    pub value: IndexValue,
}

/// 可索引的值类型
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum IndexValue {
    Int(i64),
    Bool(bool),
    Text(String),
}

impl From<&Value> for Option<IndexValue> {
    fn from(v: &Value) -> Self {
        match v {
            Value::Int(i) => Some(IndexValue::Int(*i)),
            Value::Bool(b) => Some(IndexValue::Bool(*b)),
            Value::Text(s) => Some(IndexValue::Text(s.clone())),
            Value::Float(_) => None, // Float 不支持精确索引
        }
    }
}

/// 持久化属性索引
pub struct PersistentPropertyIndex {
    tree: sled::Tree,
}

impl PersistentPropertyIndex {
    /// 创建新的持久化索引
    pub fn new(tree: sled::Tree) -> Self {
        Self { tree }
    }

    /// 生成索引键
    fn index_key(&self, label: &str, property: &str, value: &IndexValue) -> Vec<u8> {
        let key = IndexKey {
            label: label.to_string(),
            property: property.to_string(),
            value: value.clone(),
        };
        bincode::serialize(&key).unwrap()
    }

    /// 添加节点到索引
    pub fn add(&self, label: &str, property: &str, value: &Value, node_id: NodeId) -> Result<(), Box<dyn std::error::Error>> {
        let idx_value: IndexValue = match value {
            Value::Int(i) => IndexValue::Int(*i),
            Value::Bool(b) => IndexValue::Bool(*b),
            Value::Text(s) => IndexValue::Text(s.clone()),
            Value::Float(_) => return Ok(()), // Float 不支持索引
        };

        let key = self.index_key(label, property, &idx_value);

        // 读取现有的节点 ID 列表
        let mut node_ids: Vec<NodeId> = self
            .tree
            .get(&key)?
            .and_then(|v| bincode::deserialize(&v).ok())
            .unwrap_or_default();

        // 避免重复添加
        if !node_ids.contains(&node_id) {
            node_ids.push(node_id);
            node_ids.sort(); // 保持排序以便高效查询
            node_ids.dedup();

            // 写回
            self.tree.insert(key, bincode::serialize(&node_ids)?)?;
        }

        Ok(())
    }

    /// 从索引中移除节点
    pub fn remove(&self, label: &str, property: &str, value: &Value, node_id: NodeId) -> Result<(), Box<dyn std::error::Error>> {
        let idx_value: IndexValue = match value {
            Value::Int(i) => IndexValue::Int(*i),
            Value::Bool(b) => IndexValue::Bool(*b),
            Value::Text(s) => IndexValue::Text(s.clone()),
            Value::Float(_) => return Ok(()), // Float 不支持索引
        };

        let key = self.index_key(label, property, &idx_value);

        // 读取现有的节点 ID 列表
        if let Some(data) = self.tree.get(&key)? {
            let mut node_ids: Vec<NodeId> = bincode::deserialize(&data)?;
            node_ids.retain(|&id| id != node_id);

            if node_ids.is_empty() {
                // 如果列表为空，删除索引条目
                self.tree.remove(&key)?;
            } else {
                // 否则写回更新后的列表
                self.tree.insert(key, bincode::serialize(&node_ids)?)?;
            }
        }

        Ok(())
    }

    /// 查找匹配的节点 ID 列表
    pub fn find(&self, label: &str, property: &str, value: &Value) -> Result<Vec<NodeId>, Box<dyn std::error::Error>> {
        let idx_value: IndexValue = match value {
            Value::Int(i) => IndexValue::Int(*i),
            Value::Bool(b) => IndexValue::Bool(*b),
            Value::Text(s) => IndexValue::Text(s.clone()),
            Value::Float(_) => return Ok(Vec::new()),
        };

        let key = self.index_key(label, property, &idx_value);

        if let Some(data) = self.tree.get(&key)? {
            Ok(bincode::deserialize(&data)?)
        } else {
            Ok(Vec::new())
        }
    }

    /// 重建索引（从现有节点数据）
    pub fn rebuild(&self, nodes: &[StoredNode], indexed_properties: &[(String, String)]) -> Result<(), Box<dyn std::error::Error>> {
        // 清空现有索引
        for key in self.tree.iter().keys() {
            if let Ok(key) = key {
                self.tree.remove(key)?;
            }
        }

        // 遍历所有节点，重建索引
        for node in nodes {
            for label in &node.labels {
                for (indexed_label, indexed_prop) in indexed_properties {
                    if label == indexed_label {
                        if let Some(value) = node.props.get(indexed_prop) {
                            self.add(label, indexed_prop, value, node.id)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// 获取所有索引条目的数量
    pub fn count(&self) -> usize {
        self.tree.iter().count()
    }

    /// 清空所有索引
    pub fn clear(&self) -> Result<(), Box<dyn std::error::Error>> {
        for key in self.tree.iter().keys() {
            if let Ok(key) = key {
                self.tree.remove(&key)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::values::Properties;
    use std::collections::HashMap;
    use tempfile::TempDir;

    #[test]
    fn test_persistent_index_basic() {
        let temp_dir = TempDir::new().unwrap();
        let db = sled::open(temp_dir.path()).unwrap();
        let tree = db.open_tree("index").unwrap();
        let index = PersistentPropertyIndex::new(tree);

        // 添加索引
        let value = Value::Text("Alice".to_string());
        index.add("User", "name", &value, 1).unwrap();
        index.add("User", "name", &value, 2).unwrap();

        // 查询索引
        let result = index.find("User", "name", &value).unwrap();
        assert_eq!(result, vec![1, 2]);

        // 移除一个节点
        index.remove("User", "name", &value, 1).unwrap();
        let result = index.find("User", "name", &value).unwrap();
        assert_eq!(result, vec![2]);
    }

    #[test]
    fn test_index_rebuild() {
        let temp_dir = TempDir::new().unwrap();
        let db = sled::open(temp_dir.path()).unwrap();
        let tree = db.open_tree("index").unwrap();
        let index = PersistentPropertyIndex::new(tree);

        // 创建测试节点
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));

        let nodes = vec![
            StoredNode {
                id: 1,
                labels: vec!["User".to_string()],
                props: props.clone(),
            },
            StoredNode {
                id: 2,
                labels: vec!["User".to_string()],
                props,
            },
        ];

        // 重建索引
        let indexed = vec![("User".to_string(), "name".to_string())];
        index.rebuild(&nodes, &indexed).unwrap();

        // 验证索引
        let result = index.find("User", "name", &Value::Text("Alice".to_string())).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_index_value_types() {
        let temp_dir = TempDir::new().unwrap();
        let db = sled::open(temp_dir.path()).unwrap();
        let tree = db.open_tree("index").unwrap();
        let index = PersistentPropertyIndex::new(tree);

        // 测试不同类型的值
        index.add("User", "age", &Value::Int(30), 1).unwrap();
        index.add("User", "active", &Value::Bool(true), 2).unwrap();
        index.add("User", "name", &Value::Text("Alice".to_string()), 3).unwrap();

        // Float 不应被索引
        index.add("User", "score", &Value::Float(0.5), 4).unwrap();

        // 验证
        assert_eq!(index.find("User", "age", &Value::Int(30)).unwrap(), vec![1]);
        assert_eq!(index.find("User", "active", &Value::Bool(true)).unwrap(), vec![2]);
        assert_eq!(index.find("User", "name", &Value::Text("Alice".to_string())).unwrap(), vec![3]);
        assert_eq!(index.find("User", "score", &Value::Float(0.5)).unwrap(), vec![] as Vec<NodeId>); // Float 未索引
    }
}
