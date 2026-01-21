use std::collections::{HashMap, HashSet};

/// 索引配置：定义哪些 (label, property) 需要被索引
#[derive(Debug, Clone)]
pub struct IndexSchema {
    /// (label, property_name) 的集合 - 单属性索引
    indexed: HashSet<(String, String)>,
    /// 复合索引配置
    /// key: 索引名称 (如 "user_name_email")
    /// value: (label, [properties]) - 标签和属性列表
    composite_indexes: HashMap<String, (String, Vec<String>)>,
}

impl IndexSchema {
    pub fn new() -> Self {
        Self {
            indexed: HashSet::new(),
            composite_indexes: HashMap::new(),
        }
    }

    /// 添加一个 (label, property) 到索引配置
    pub fn add_index(&mut self, label: &str, property: &str) {
        self.indexed
            .insert((label.to_string(), property.to_string()));
    }

    /// 添加复合索引
    ///
    /// # 参数
    /// - `name`: 索引名称（用于标识和删除索引）
    /// - `label`: 节点标签
    /// - `properties`: 属性名列表（按索引顺序）
    pub fn add_composite_index(&mut self, name: &str, label: &str, properties: &[&str]) {
        self.composite_indexes.insert(
            name.to_string(),
            (label.to_string(), properties.iter().map(|s| s.to_string()).collect()),
        );
    }

    /// 删除复合索引
    pub fn remove_composite_index(&mut self, name: &str) -> bool {
        self.composite_indexes.remove(name).is_some()
    }

    /// 检查某个 (label, property) 是否需要被索引
    pub fn should_index(&self, label: &str, property: &str) -> bool {
        self.indexed.contains(&(label.to_string(), property.to_string()))
    }

    /// 检查是否存在某个复合索引
    ///
    /// # 参数
    /// - `label`: 节点标签
    /// - `properties`: 属性名列表
    ///
    /// # 返回
    /// 如果存在匹配的复合索引，返回索引名称和属性列表
    pub fn get_composite_index(&self, label: &str, properties: &[&str]) -> Option<(String, Vec<String>)> {
        let props_vec: Vec<String> = properties.iter().map(|s| s.to_string()).collect();
        self.composite_indexes
            .iter()
            .find(|(_, (l, p))| l == label && p == &props_vec)
            .map(|(name, (_, p))| (name.clone(), p.clone()))
    }

    /// 获取所有复合索引
    pub fn get_all_composite_indexes(&self) -> &HashMap<String, (String, Vec<String>)> {
        &self.composite_indexes
    }

    /// 预定义一个默认 schema（User.name, User.age, User.id）
    pub fn default() -> Self {
        let mut schema = Self::new();
        schema.add_index("User", "name");
        schema.add_index("User", "age");
        schema.add_index("User", "id");
        schema
    }
}
