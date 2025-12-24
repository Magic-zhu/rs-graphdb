use axum::{
    extract::State,
    http::{header, StatusCode},
    response::Html,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

use crate::graph::db::GraphDatabase;
use crate::query::Query;
use crate::storage::mem_store::MemStore;
use crate::values::{Properties, Value};

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<Mutex<GraphDatabase<MemStore>>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateNodeRequest {
    pub labels: Vec<String>,
    pub properties: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct CreateNodeResponse {
    pub id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateRelRequest {
    pub start: u64,
    pub end: u64,
    pub rel_type: String,
    pub properties: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct CreateRelResponse {
    pub id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRequest {
    pub label: String,
    pub property: Option<String>,
    pub value: Option<String>,
    pub out_rel: Option<String>,
    pub in_rel: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct NodeResponse {
    pub id: u64,
    pub labels: Vec<String>,
    pub properties: serde_json::Map<String, serde_json::Value>,
}

pub fn create_router(state: AppState) -> Router {
    use tower_http::cors::{CorsLayer, Any};

    Router::new()
        .route("/", get(root))
        .route("/ui", get(ui_handler))
        .route("/nodes", post(create_node))
        .route("/rels", post(create_rel))
        .route("/query", post(query))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any)
        )
        .with_state(state)
}

async fn root() -> &'static str {
    "Rust Graph Database API - Visit /ui for web interface"
}

async fn ui_handler() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn create_node(
    State(state): State<AppState>,
    Json(payload): Json<CreateNodeRequest>,
) -> Result<Json<CreateNodeResponse>, StatusCode> {
    let mut db = state.db.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let props = convert_json_map_to_properties(&payload.properties);
    let labels: Vec<&str> = payload.labels.iter().map(|s| s.as_str()).collect();

    let id = db.create_node(labels, props);

    Ok(Json(CreateNodeResponse { id }))
}

async fn create_rel(
    State(state): State<AppState>,
    Json(payload): Json<CreateRelRequest>,
) -> Result<Json<CreateRelResponse>, StatusCode> {
    let mut db = state.db.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let props = convert_json_map_to_properties(&payload.properties);

    let id = db.create_rel(payload.start, payload.end, &payload.rel_type, props);

    Ok(Json(CreateRelResponse { id }))
}

async fn query(
    State(state): State<AppState>,
    Json(payload): Json<QueryRequest>,
) -> Result<Json<Vec<NodeResponse>>, StatusCode> {
    let db = state.db.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut q = Query::new(&*db);

    // 如果提供了 property 和 value，走索引查询
    if let (Some(prop), Some(val)) = (&payload.property, &payload.value) {
        q = q.from_label_and_prop_eq(&payload.label, prop, val);
    } else {
        q = q.from_label(&payload.label);
    }

    if let Some(rel) = &payload.out_rel {
        q = q.out(rel);
    }

    if let Some(rel) = &payload.in_rel {
        q = q.in_(rel);
    }

    let nodes = q.collect_nodes();

    let result: Vec<NodeResponse> = nodes
        .into_iter()
        .map(|n| NodeResponse {
            id: n.id,
            labels: n.labels,
            properties: convert_properties_to_json_map(&n.props),
        })
        .collect();

    Ok(Json(result))
}

fn convert_json_map_to_properties(map: &serde_json::Map<String, serde_json::Value>) -> Properties {
    let mut props = Properties::new();
    for (k, v) in map {
        if let Some(val) = json_value_to_value(v) {
            props.insert(k.clone(), val);
        }
    }
    props
}

fn json_value_to_value(v: &serde_json::Value) -> Option<Value> {
    match v {
        serde_json::Value::Number(n) => n.as_i64().map(Value::Int),
        serde_json::Value::Bool(b) => Some(Value::Bool(*b)),
        serde_json::Value::String(s) => Some(Value::Text(s.clone())),
        _ => None,
    }
}

fn convert_properties_to_json_map(props: &Properties) -> serde_json::Map<String, serde_json::Value> {
    let mut map = serde_json::Map::new();
    for (k, v) in props {
        if let Some(jv) = value_to_json_value(v) {
            map.insert(k.clone(), jv);
        }
    }
    map
}

fn value_to_json_value(v: &Value) -> Option<serde_json::Value> {
    match v {
        Value::Int(i) => Some(serde_json::Value::Number((*i).into())),
        Value::Bool(b) => Some(serde_json::Value::Bool(*b)),
        Value::Text(s) => Some(serde_json::Value::String(s.clone())),
        Value::Float(f) => serde_json::Number::from_f64(*f).map(serde_json::Value::Number),
    }
}

pub async fn run_server(state: AppState, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let app = create_router(state);
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));

    println!("Server running on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
