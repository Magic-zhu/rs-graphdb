use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Int(i64),
    Bool(bool),
    Text(String),
    Float(f64),
}

pub type Properties = HashMap<String, Value>;
