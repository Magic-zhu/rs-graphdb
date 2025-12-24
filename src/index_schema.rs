use std::collections::HashSet;

/// 索引配置：定义哪些 (label, property) 需要被索引
#[derive(Debug, Clone)]
pub struct IndexSchema {
    /// (label, property_name) 的集合
    indexed: HashSet<(String, String)>,
}

impl IndexSchema {
    pub fn new() -> Self {
        Self {
            indexed: HashSet::new(),
        }
    }

    /// 添加一个 (label, property) 到索引配置
    pub fn add_index(&mut self, label: &str, property: &str) {
        self.indexed
            .insert((label.to_string(), property.to_string()));
    }

    /// 检查某个 (label, property) 是否需要被索引
    pub fn should_index(&self, label: &str, property: &str) -> bool {
        self.indexed.contains(&(label.to_string(), property.to_string()))
    }

    /// 预定义一个默认 schema（User.name 和 User.age）
    pub fn default() -> Self {
        let mut schema = Self::new();
        schema.add_index("User", "name");
        schema.add_index("User", "age");
        schema
    }
}
