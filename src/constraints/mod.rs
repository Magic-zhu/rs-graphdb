//! 图约束模块
//!
//! 支持两种类型的约束：
//! - 唯一性约束 (Uniqueness Constraint): 确保节点的某个属性值在标签内唯一
//! - 存在性约束 (Existence Constraint): 确保节点的某个属性必须存在

use crate::storage::{NodeId, StorageEngine};
use crate::values::Value;
use std::collections::HashMap;
use std::sync::{RwLock, Arc};

/// 约束类型
#[derive(Debug, Clone, PartialEq)]
pub enum ConstraintType {
    /// 唯一性约束：确保属性值在标签内唯一
    Uniqueness,
    /// 存在性约束：确保属性必须存在
    Existence,
}

/// 约束定义
#[derive(Debug, Clone)]
pub struct Constraint {
    /// 约束类型
    pub constraint_type: ConstraintType,
    /// 标签
    pub label: String,
    /// 属性名
    pub property: String,
}

impl Constraint {
    /// 创建新的唯一性约束
    pub fn uniqueness(label: &str, property: &str) -> Self {
        Constraint {
            constraint_type: ConstraintType::Uniqueness,
            label: label.to_string(),
            property: property.to_string(),
        }
    }

    /// 创建新的存在性约束
    pub fn existence(label: &str, property: &str) -> Self {
        Constraint {
            constraint_type: ConstraintType::Existence,
            label: label.to_string(),
            property: property.to_string(),
        }
    }

    /// 获取约束的唯一标识
    pub fn key(&self) -> String {
        format!(
            "{}:{}:{}",
            match self.constraint_type {
                ConstraintType::Uniqueness => "unique",
                ConstraintType::Existence => "exists",
            },
            self.label,
            self.property
        )
    }
}

/// 约束验证结果
#[derive(Debug, Clone, PartialEq)]
pub enum ConstraintValidation {
    /// 验证通过
    Valid,
    /// 验证失败
    Violated { message: String },
}

/// 约束管理器
///
/// 存储和管理所有图约束
pub struct ConstraintManager {
    /// 所有约束的集合
    constraints: RwLock<HashMap<String, Constraint>>,
}

impl ConstraintManager {
    /// 创建新的约束管理器
    pub fn new() -> Self {
        ConstraintManager {
            constraints: RwLock::new(HashMap::new()),
        }
    }

    /// 添加约束
    pub fn add_constraint(&self, constraint: Constraint) -> Result<(), String> {
        let key = constraint.key();
        let mut constraints = self.constraints.write()
            .map_err(|e| format!("Failed to acquire write lock: {}", e))?;

        if constraints.contains_key(&key) {
            return Err(format!("Constraint already exists: {}", key));
        }

        constraints.insert(key, constraint);
        Ok(())
    }

    /// 移除约束
    pub fn drop_constraint(&self, label: &str, property: &str, constraint_type: &ConstraintType) -> Result<bool, String> {
        let key = format!(
            "{}:{}:{}",
            match constraint_type {
                ConstraintType::Uniqueness => "unique",
                ConstraintType::Existence => "exists",
            },
            label,
            property
        );

        let mut constraints = self.constraints.write()
            .map_err(|e| format!("Failed to acquire write lock: {}", e))?;

        Ok(constraints.remove(&key).is_some())
    }

    /// 获取所有约束
    pub fn get_all_constraints(&self) -> Vec<Constraint> {
        self.constraints.read()
            .map(|constraints| constraints.values().cloned().collect())
            .unwrap_or_default()
    }

    /// 获取特定标签的所有约束
    pub fn get_constraints_for_label(&self, label: &str) -> Vec<Constraint> {
        self.constraints.read()
            .map(|constraints| {
                constraints
                    .values()
                    .filter(|c| c.label == label)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// 验证节点是否满足约束
    pub fn validate_node<E: StorageEngine>(
        &self,
        db: &crate::graph::db::GraphDatabase<E>,
        node_id: NodeId,
    ) -> Result<ConstraintValidation, String> {
        let node = db.get_node(node_id)
            .ok_or("Node not found")?;

        let constraints = self.constraints.read()
            .map_err(|e| format!("Failed to acquire read lock: {}", e))?;

        // 只检查适用于该节点标签的约束
        let applicable_constraints: Vec<_> = constraints
            .values()
            .filter(|c| node.has_label(&c.label))
            .collect();

        for constraint in applicable_constraints {
            match &constraint.constraint_type {
                ConstraintType::Existence => {
                    // 检查属性是否存在
                    if !node.props.contains_key(&constraint.property) {
                        return Ok(ConstraintValidation::Violated {
                            message: format!(
                                "Existence constraint violated: node {:?} (label: {}) missing required property '{}'",
                                node_id, constraint.label, constraint.property
                            ),
                        });
                    }
                }
                ConstraintType::Uniqueness => {
                    // 检查属性值是否唯一
                    if let Some(value) = node.props.get(&constraint.property) {
                        // 查询具有相同标签和属性值的其他节点
                        let mut duplicates = Vec::new();
                        for stored_node in db.all_stored_nodes() {
                            if stored_node.id == node_id {
                                continue;
                            }

                            let other_node = crate::graph::model::Node {
                                id: stored_node.id,
                                labels: stored_node.labels.clone(),
                                props: stored_node.props.clone(),
                            };

                            if other_node.has_label(&constraint.label) {
                                if let Some(other_value) = other_node.get(&constraint.property) {
                                    if other_value == value {
                                        duplicates.push(stored_node.id);
                                    }
                                }
                            }
                        }

                        if !duplicates.is_empty() {
                            return Ok(ConstraintValidation::Violated {
                                message: format!(
                                    "Uniqueness constraint violated: node {:?} (label: {}) has duplicate value {:?} for property '{}'. Existing nodes: {:?}",
                                    node_id, constraint.label, value, constraint.property, duplicates
                                ),
                            });
                        }
                    }
                }
            }
        }

        Ok(ConstraintValidation::Valid)
    }

    /// 获取约束数量
    pub fn count(&self) -> usize {
        self.constraints.read()
            .map(|c| c.len())
            .unwrap_or(0)
    }
}

impl Default for ConstraintManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::values::Properties;
    use crate::storage::mem_store::MemStore;

    fn create_test_db() -> crate::graph::db::GraphDatabase<MemStore> {
        crate::graph::db::GraphDatabase::new_in_memory()
    }

    #[test]
    fn test_constraint_key() {
        let c1 = Constraint::uniqueness("User", "email");
        assert_eq!(c1.key(), "unique:User:email");

        let c2 = Constraint::existence("User", "name");
        assert_eq!(c2.key(), "exists:User:name");
    }

    #[test]
    fn test_add_constraint() {
        let manager = ConstraintManager::new();
        let constraint = Constraint::uniqueness("User", "email");

        assert!(manager.add_constraint(constraint.clone()).is_ok());
        assert_eq!(manager.count(), 1);

        // 重复添加应该失败
        assert!(manager.add_constraint(constraint).is_err());
        assert_eq!(manager.count(), 1);
    }

    #[test]
    fn test_drop_constraint() {
        let manager = ConstraintManager::new();
        let constraint = Constraint::uniqueness("User", "email");

        manager.add_constraint(constraint).unwrap();
        assert_eq!(manager.count(), 1);

        // 删除约束
        let result = manager.drop_constraint("User", "email", &ConstraintType::Uniqueness);
        assert!(result.unwrap());
        assert_eq!(manager.count(), 0);

        // 再次删除应该返回 false
        let result = manager.drop_constraint("User", "email", &ConstraintType::Uniqueness);
        assert!(!result.unwrap());
    }

    #[test]
    fn test_get_constraints_for_label() {
        let manager = ConstraintManager::new();

        manager.add_constraint(Constraint::uniqueness("User", "email")).unwrap();
        manager.add_constraint(Constraint::existence("User", "name")).unwrap();
        manager.add_constraint(Constraint::uniqueness("Product", "sku")).unwrap();

        let user_constraints = manager.get_constraints_for_label("User");
        assert_eq!(user_constraints.len(), 2);

        let product_constraints = manager.get_constraints_for_label("Product");
        assert_eq!(product_constraints.len(), 1);
    }

    #[test]
    fn test_validate_existence_constraint() {
        let mut db = create_test_db();
        let manager = ConstraintManager::new();

        // 添加存在性约束
        manager.add_constraint(Constraint::existence("User", "name")).unwrap();

        // 创建带有 name 属性的节点
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Alice".to_string()));
        let node_id = db.create_node(vec!["User"], props);

        // 验证应该通过
        let result = manager.validate_node(&db, node_id).unwrap();
        assert_eq!(result, ConstraintValidation::Valid);

        // 创建不带 name 属性的节点
        let node_id2 = db.create_node(vec!["User"], Properties::new());

        // 验证应该失败
        let result = manager.validate_node(&db, node_id2).unwrap();
        match result {
            ConstraintValidation::Violated { message } => {
                assert!(message.contains("missing required property"));
                assert!(message.contains("name"));
            }
            _ => panic!("Expected constraint violation"),
        }
    }

    #[test]
    fn test_validate_uniqueness_constraint() {
        let mut db = create_test_db();
        let manager = ConstraintManager::new();

        // 添加唯一性约束
        manager.add_constraint(Constraint::uniqueness("User", "email")).unwrap();

        // 创建第一个节点
        let mut props1 = Properties::new();
        props1.insert("email".to_string(), Value::Text("alice@example.com".to_string()));
        let node_id1 = db.create_node(vec!["User"], props1);

        // 验证应该通过
        let result = manager.validate_node(&db, node_id1).unwrap();
        assert_eq!(result, ConstraintValidation::Valid);

        // 创建第二个具有相同 email 的节点
        let mut props2 = Properties::new();
        props2.insert("email".to_string(), Value::Text("alice@example.com".to_string()));
        let node_id2 = db.create_node(vec!["User"], props2);

        // 验证应该失败
        let result = manager.validate_node(&db, node_id2).unwrap();
        match result {
            ConstraintValidation::Violated { message } => {
                assert!(message.contains("Uniqueness constraint violated"));
                assert!(message.contains("email"));
            }
            _ => panic!("Expected constraint violation"),
        }

        // 创建具有不同 email 的节点
        let mut props3 = Properties::new();
        props3.insert("email".to_string(), Value::Text("bob@example.com".to_string()));
        let node_id3 = db.create_node(vec!["User"], props3);

        // 验证应该通过
        let result = manager.validate_node(&db, node_id3).unwrap();
        assert_eq!(result, ConstraintValidation::Valid);
    }

    #[test]
    fn test_validate_multiple_constraints() {
        let mut db = create_test_db();
        let manager = ConstraintManager::new();

        // 添加多个约束
        manager.add_constraint(Constraint::existence("User", "name")).unwrap();
        manager.add_constraint(Constraint::uniqueness("User", "email")).unwrap();

        // 创建满足所有约束的节点
        let mut props = Properties::new();
        props.insert("name".to_string(), Value::Text("Alice".to_string()));
        props.insert("email".to_string(), Value::Text("alice@example.com".to_string()));
        let node_id = db.create_node(vec!["User"], props);

        let result = manager.validate_node(&db, node_id).unwrap();
        assert_eq!(result, ConstraintValidation::Valid);

        // 创建缺少 name 的节点
        let mut props2 = Properties::new();
        props2.insert("email".to_string(), Value::Text("bob@example.com".to_string()));
        let node_id2 = db.create_node(vec!["User"], props2);

        let result = manager.validate_node(&db, node_id2).unwrap();
        match result {
            ConstraintValidation::Violated { .. } => {
                // 应该是存在性约束失败
            }
            _ => panic!("Expected constraint violation"),
        }
    }
}
