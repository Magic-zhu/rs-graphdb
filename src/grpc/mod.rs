pub mod proto {
    tonic::include_proto!("rsgraphdb");
}

use crate::service::{GraphService, ServiceError};
use crate::storage::StorageEngine;
use crate::values::{Properties, Value as RustValue};
use proto::graph_db_service_server::{GraphDbService, GraphDbServiceServer};
use proto::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::{Request, Response, Status};

// Rust Value <-> Proto Value 转换
fn rust_value_to_proto(v: &RustValue) -> Value {
    let value = match v {
        RustValue::Int(i) => value::Value::IntValue(*i),
        RustValue::Bool(b) => value::Value::BoolValue(*b),
        RustValue::Text(s) => value::Value::TextValue(s.clone()),
        RustValue::Float(f) => value::Value::FloatValue(*f),
    };
    Value { value: Some(value) }
}

fn proto_value_to_rust(v: &Value) -> Option<RustValue> {
    v.value.as_ref().and_then(|val| match val {
        value::Value::IntValue(i) => Some(RustValue::Int(*i)),
        value::Value::BoolValue(b) => Some(RustValue::Bool(*b)),
        value::Value::TextValue(s) => Some(RustValue::Text(s.clone())),
        value::Value::FloatValue(f) => Some(RustValue::Float(*f)),
    })
}

fn rust_props_to_proto(props: &Properties) -> HashMap<String, Value> {
    props
        .iter()
        .map(|(k, v)| (k.clone(), rust_value_to_proto(v)))
        .collect()
}

fn proto_props_to_rust(props: &HashMap<String, Value>) -> Properties {
    let mut result = Properties::new();
    for (k, v) in props {
        if let Some(rv) = proto_value_to_rust(v) {
            result.insert(k.clone(), rv);
        }
    }
    result
}

impl From<ServiceError> for Status {
    fn from(err: ServiceError) -> Self {
        match err {
            ServiceError::Internal(msg) => Status::internal(msg),
            ServiceError::NotFound => Status::not_found("Not found"),
        }
    }
}

pub struct GrpcGraphService<E: StorageEngine> {
    service: Arc<GraphService<E>>,
}

impl<E: StorageEngine> GrpcGraphService<E> {
    pub fn new(service: Arc<GraphService<E>>) -> Self {
        Self { service }
    }
}

#[tonic::async_trait]
impl<E: StorageEngine + Send + Sync + 'static> GraphDbService for GrpcGraphService<E> {
    async fn create_node(
        &self,
        request: Request<CreateNodeRequest>,
    ) -> Result<Response<Node>, Status> {
        let req = request.into_inner();
        let labels: Vec<&str> = req.labels.iter().map(|s| s.as_str()).collect();
        let props = proto_props_to_rust(&req.properties);

        let id = self.service.create_node(labels, props).await?;

        let node = self.service.get_node(id).await?;

        let proto_node = Node {
            id,
            labels: node.labels,
            properties: rust_props_to_proto(&node.props),
        };

        Ok(Response::new(proto_node))
    }

    async fn create_relationship(
        &self,
        request: Request<CreateRelationshipRequest>,
    ) -> Result<Response<Relationship>, Status> {
        let req = request.into_inner();
        let props = proto_props_to_rust(&req.properties);

        let id = self
            .service
            .create_rel(req.start, req.end, &req.rel_type, props)
            .await?;

        let rel = self.service.get_rel(id).await?;

        let proto_rel = Relationship {
            id,
            start: rel.start,
            end: rel.end,
            rel_type: rel.typ,
            properties: rust_props_to_proto(&rel.props),
        };

        Ok(Response::new(proto_rel))
    }

    async fn execute_cypher(
        &self,
        request: Request<ExecuteCypherRequest>,
    ) -> Result<Response<ExecuteCypherResponse>, Status> {
        let _req = request.into_inner();

        // TODO: 实现 Cypher 查询支持
        // 目前返回空结果
        let response = ExecuteCypherResponse { rows: vec![] };
        Ok(Response::new(response))
    }
}

pub async fn run_grpc_server<E: StorageEngine + Send + Sync + 'static>(
    service: Arc<GraphService<E>>,
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let grpc_service = GrpcGraphService::new(service);
    let svc = GraphDbServiceServer::new(grpc_service);

    println!("gRPC server running on {}", addr);

    tonic::transport::Server::builder()
        .add_service(svc)
        .serve(addr)
        .await?;

    Ok(())
}
