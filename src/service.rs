use crate::graph::db::GraphDatabase;
use crate::graph::model::{Node, Relationship};
use crate::storage::{NodeId, RelId, StorageEngine};
use crate::values::Properties;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub enum ServiceError {
    Internal(String),
    NotFound,
}

impl From<ServiceError> for (axum::http::StatusCode, String) {
    fn from(err: ServiceError) -> Self {
        use axum::http::StatusCode;
        match err {
            ServiceError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            ServiceError::NotFound => (StatusCode::NOT_FOUND, "Not found".to_string()),
        }
    }
}

pub struct GraphService<E: StorageEngine> {
    db: Arc<Mutex<GraphDatabase<E>>>,
}

impl<E: StorageEngine> GraphService<E> {
    pub fn new(db: Arc<Mutex<GraphDatabase<E>>>) -> Self {
        Self { db }
    }

    pub fn db(&self) -> &Arc<Mutex<GraphDatabase<E>>> {
        &self.db
    }

    pub async fn create_node(
        &self,
        labels: Vec<&str>,
        props: Properties,
    ) -> Result<NodeId, ServiceError> {
        let mut guard = self
            .db
            .lock()
            .map_err(|_| ServiceError::Internal("DB lock poisoned".into()))?;
        let id = guard.create_node(labels, props);
        Ok(id)
    }

    pub async fn create_rel(
        &self,
        start: NodeId,
        end: NodeId,
        typ: &str,
        props: Properties,
    ) -> Result<RelId, ServiceError> {
        let mut guard = self
            .db
            .lock()
            .map_err(|_| ServiceError::Internal("DB lock poisoned".into()))?;
        let id = guard.create_rel(start, end, typ, props);
        Ok(id)
    }

    pub async fn get_node(&self, id: NodeId) -> Result<Node, ServiceError> {
        let guard = self
            .db
            .lock()
            .map_err(|_| ServiceError::Internal("DB lock poisoned".into()))?;
        guard.get_node(id).ok_or(ServiceError::NotFound)
    }

    pub async fn get_rel(&self, id: RelId) -> Result<Relationship, ServiceError> {
        let guard = self
            .db
            .lock()
            .map_err(|_| ServiceError::Internal("DB lock poisoned".into()))?;
        guard.get_rel(id).ok_or(ServiceError::NotFound)
    }
}
