use crate::storage::NodeId;
use crate::values::Value;
use std::collections::HashMap;

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

/// 一个非常简单的属性索引：
/// (label, property_name, value) -> [node_id]
#[derive(Default)]
pub struct PropertyIndex {
    map: HashMap<(String, String, ValueKey), Vec<NodeId>>,
}

impl PropertyIndex {
    pub fn new() -> Self {
        Self { map: HashMap::new() }
    }

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
}
