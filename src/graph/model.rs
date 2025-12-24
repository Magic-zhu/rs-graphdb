use crate::storage::{NodeId, RelId};
use crate::values::{Properties, Value};

#[derive(Debug, Clone)]
pub struct Node {
    pub id: NodeId,
    pub labels: Vec<String>,
    pub props: Properties,
}

impl Node {
    pub fn has_label(&self, label: &str) -> bool {
        self.labels.iter().any(|l| l == label)
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.props.get(key)
    }
}

#[derive(Debug, Clone)]
pub struct Relationship {
    pub id: RelId,
    pub start: NodeId,
    pub end: NodeId,
    pub typ: String,
    pub props: Properties,
}
