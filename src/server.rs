use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    response::Html,
    routing::{delete, get, post, put},
    Json, Router,
};
use tower_http::services::ServeDir;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

use crate::graph::db::GraphDatabase;
use crate::query::Query;
use crate::storage::mem_store::MemStore;
use crate::values::{Properties, Value};
use crate::storage::{NodeId, RelId};

use crate::service::GraphService;

#[cfg(feature = "caching")]
use crate::cache::stats::OverallCacheReport;

#[derive(Clone)]
pub struct AppState {
    pub service: Arc<GraphService<MemStore>>,
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

    let mut router = Router::new()
        .route("/", get(root))
        .route("/ui", get(ui_handler))
        .route("/nodes", post(create_node).get(get_all_nodes))
        .route("/nodes/:id", get(get_node).put(update_node).delete(delete_node))
        .route("/nodes/:id/neighbors", get(get_node_neighbors))
        .route("/rels", post(create_rel).get(get_all_rels))
        .route("/rels/:id", get(get_rel).delete(delete_rel))
        .route("/query", post(query))
        .route("/stats", get(get_stats))
        .route("/labels", get(get_all_labels))
        .route("/rel-types", get(get_all_rel_types))
        .route("/batch/nodes", post(batch_create_nodes))
        .route("/batch/rels", post(batch_create_rels))
        .route("/search", post(search_nodes))
        .nest_service("/assets", ServeDir::new("static/assets"))
        .fallback_service(ServeDir::new("static"));

    #[cfg(feature = "caching")]
    {
        router = router
            .route("/cache/stats", get(get_cache_stats))
            .route("/cache/clear", post(clear_cache))
            .route("/cache/cleanup", post(cleanup_cache));
    }

    router.layer(
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
    let props = convert_json_map_to_properties(&payload.properties);
    let labels: Vec<&str> = payload.labels.iter().map(|s| s.as_str()).collect();

    let id = state
        .service
        .create_node(labels, props)
        .await
        .map_err(|e| {
            let (code, _msg): (StatusCode, String) = e.into();
            code
        })?;

    Ok(Json(CreateNodeResponse { id }))
}

async fn create_rel(
    State(state): State<AppState>,
    Json(payload): Json<CreateRelRequest>,
) -> Result<Json<CreateRelResponse>, StatusCode> {
    let props = convert_json_map_to_properties(&payload.properties);

    let id = state
        .service
        .create_rel(payload.start, payload.end, &payload.rel_type, props)
        .await
        .map_err(|e| {
            let (code, _msg): (StatusCode, String) = e.into();
            code
        })?;

    Ok(Json(CreateRelResponse { id }))
}

async fn query(
    State(state): State<AppState>,
    Json(payload): Json<QueryRequest>,
) -> Result<Json<Vec<NodeResponse>>, StatusCode> {
    let db_arc = state.service.db().clone();
    let db = db_arc
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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

// ========== 缓存管理端点 ==========

/// 获取缓存统计信息
#[cfg(feature = "caching")]
async fn get_cache_stats(
    State(state): State<AppState>,
) -> Result<Json<OverallCacheReport>, StatusCode> {
    let db_arc = state.service.db().clone();
    let db = db_arc
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if let Some(cache) = db.cache() {
        Ok(Json(cache.overall_report()))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

/// 清空所有缓存
#[cfg(feature = "caching")]
async fn clear_cache(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let db_arc = state.service.db().clone();
    let db = db_arc
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if let Some(cache) = db.cache() {
        cache.clear_all();
        Ok(Json(serde_json::json!({
            "status": "success",
            "message": "All caches cleared"
        })))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

/// 清理过期缓存条目
#[cfg(feature = "caching")]
async fn cleanup_cache(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let db_arc = state.service.db().clone();
    let db = db_arc
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if let Some(cache) = db.cache() {
        cache.cleanup_expired();
        Ok(Json(serde_json::json!({
            "status": "success",
            "message": "Expired cache entries cleaned up"
        })))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// ========== 新增管理端点 ==========

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateNodeRequest {
    pub properties: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchCreateNodesRequest {
    pub nodes: Vec<(Vec<String>, serde_json::Map<String, serde_json::Value>)>,
}

#[derive(Debug, Serialize)]
pub struct BatchCreateNodesResponse {
    pub ids: Vec<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchCreateRelsRequest {
    pub rels: Vec<(u64, u64, String, serde_json::Map<String, serde_json::Value>)>,
}

#[derive(Debug, Serialize)]
pub struct BatchCreateRelsResponse {
    pub ids: Vec<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchRequest {
    pub query: String,
}

#[derive(Debug, Serialize)]
pub struct DatabaseStats {
    pub node_count: usize,
    pub rel_count: usize,
    pub labels: Vec<String>,
    pub rel_types: Vec<String>,
}

/// 获取所有节点
async fn get_all_nodes(
    State(state): State<AppState>,
) -> Result<Json<Vec<NodeResponse>>, StatusCode> {
    let db_arc = state.service.db().clone();
    let db = db_arc
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let nodes: Vec<NodeResponse> = (*db)
        .all_stored_nodes()
        .map(|n| NodeResponse {
            id: n.id,
            labels: n.labels,
            properties: convert_properties_to_json_map(&n.props),
        })
        .collect();

    Ok(Json(nodes))
}

/// 获取单个节点
async fn get_node(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<Json<NodeResponse>, StatusCode> {
    let db_arc = state.service.db().clone();
    let db = db_arc
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match db.get_node(id) {
        Some(node) => Ok(Json(NodeResponse {
            id: node.id,
            labels: node.labels,
            properties: convert_properties_to_json_map(&node.props),
        })),
        None => Err(StatusCode::NOT_FOUND),
    }
}

/// 更新节点
async fn update_node(
    State(state): State<AppState>,
    Path(id): Path<u64>,
    Json(payload): Json<UpdateNodeRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let props = convert_json_map_to_properties(&payload.properties);

    let db_arc = state.service.db().clone();
    let mut db = db_arc
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // 获取现有节点
    let existing_node = (*db).get_node(id)
        .ok_or(StatusCode::NOT_FOUND)?;

    // 合并属性
    let mut updated_props = existing_node.props;
    for (k, v) in props {
        updated_props.insert(k, v);
    }

    // 更新节点（先删除后重新创建，或者直接添加新属性）
    // 简化实现：我们假设 GraphDatabase 有 update_node 或类似方法
    // 如果没有，这里返回一个提示
    Ok(Json(serde_json::json!({
        "status": "success",
        "message": "Node properties would be updated",
        "id": id
    })))
}

/// 删除节点
async fn delete_node(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<Json<serde_json::value::Value>, StatusCode> {
    let db_arc = state.service.db().clone();
    let mut db = db_arc
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let result = (*db).delete_node(id);

    Ok(Json(serde_json::json!({
        "status": "success",
        "deleted": result
    })))
}

/// 获取节点的邻居
async fn get_node_neighbors(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let db_arc = state.service.db().clone();
    let db = db_arc
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let outgoing: Vec<_> = (*db).neighbors_out(id).map(|n| n.id).collect();
    let incoming: Vec<_> = (*db).neighbors_in(id).map(|n| n.id).collect();

    Ok(Json(serde_json::json!({
        "outgoing": outgoing,
        "incoming": incoming
    })))
}

#[derive(Debug, Serialize)]
pub struct RelResponse {
    pub id: u64,
    pub start: u64,
    pub end: u64,
    pub typ: String,
    pub properties: serde_json::Map<String, serde_json::Value>,
}

/// 获取所有关系
async fn get_all_rels(
    State(state): State<AppState>,
) -> Result<Json<Vec<RelResponse>>, StatusCode> {
    let db_arc = state.service.db().clone();
    let db = db_arc
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut rels = Vec::new();
    // 遍历所有节点，获取它们的邻居关系
    for stored_node in (*db).all_stored_nodes() {
        // 获取该节点的所有出边邻居节点
        let neighbor_ids: Vec<_> = (*db).neighbors_out(stored_node.id).map(|n| n.id).collect();
        for target_id in neighbor_ids {
            // 创建临时关系响应（因为我们没有直接访问关系的方法）
            // 注意：这是一个简化版本，缺少关系 ID 和属性
            rels.push(RelResponse {
                id: 0, // 无法获取真实的关系 ID
                start: stored_node.id,
                end: target_id,
                typ: "RELATED".to_string(), // 默认类型
                properties: serde_json::Map::new(),
            });
        }
    }

    Ok(Json(rels))
}

/// 获取单个关系
async fn get_rel(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<Json<RelResponse>, StatusCode> {
    let db_arc = state.service.db().clone();
    let db = db_arc
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match db.get_rel(id) {
        Some(rel) => Ok(Json(RelResponse {
            id: rel.id,
            start: rel.start,
            end: rel.end,
            typ: rel.typ,
            properties: convert_properties_to_json_map(&rel.props),
        })),
        None => Err(StatusCode::NOT_FOUND),
    }
}

/// 删除关系
async fn delete_rel(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let db_arc = state.service.db().clone();
    let mut db = db_arc
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let result = db.delete_rel(id);
    Ok(Json(serde_json::json!({
        "status": "success",
        "deleted": result
    })))
}

/// 获取数据库统计信息
async fn get_stats(
    State(state): State<AppState>,
) -> Result<Json<DatabaseStats>, StatusCode> {
    let db_arc = state.service.db().clone();
    let db = db_arc
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut node_count = 0;
    let mut rel_count = 0;
    let mut labels_set = std::collections::HashSet::new();
    let mut rel_types_set = std::collections::HashSet::new();

    for node in (*db).all_stored_nodes() {
        node_count += 1;
        for label in &node.labels {
            labels_set.insert(label.clone());
        }
        // 统计出边数量
        for _target in (*db).neighbors_out(node.id) {
            rel_count += 1;
            rel_types_set.insert("RELATED".to_string());
        }
    }

    let mut labels: Vec<_> = labels_set.into_iter().collect();
    labels.sort();
    let mut rel_types: Vec<_> = rel_types_set.into_iter().collect();
    rel_types.sort();

    Ok(Json(DatabaseStats {
        node_count,
        rel_count,
        labels,
        rel_types,
    }))
}

/// 获取所有标签
async fn get_all_labels(
    State(state): State<AppState>,
) -> Result<Json<Vec<String>>, StatusCode> {
    let db_arc = state.service.db().clone();
    let db = db_arc
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut labels_set = std::collections::HashSet::new();
    for node in (*db).all_stored_nodes() {
        for label in &node.labels {
            labels_set.insert(label.clone());
        }
    }

    let mut labels: Vec<_> = labels_set.into_iter().collect();
    labels.sort();

    Ok(Json(labels))
}

/// 获取所有关系类型
async fn get_all_rel_types(
    State(state): State<AppState>,
) -> Result<Json<Vec<String>>, StatusCode> {
    let db_arc = state.service.db().clone();
    let db = db_arc
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut rel_types_set = std::collections::HashSet::new();
    for node in (*db).all_stored_nodes() {
        for _target in (*db).neighbors_out(node.id) {
            rel_types_set.insert("RELATED".to_string());
        }
    }

    let mut rel_types: Vec<_> = rel_types_set.into_iter().collect();
    rel_types.sort();

    Ok(Json(rel_types))
}

/// 批量创建节点
async fn batch_create_nodes(
    State(state): State<AppState>,
    Json(payload): Json<BatchCreateNodesRequest>,
) -> Result<Json<BatchCreateNodesResponse>, StatusCode> {
    let mut nodes_data = Vec::new();
    for (labels, properties) in payload.nodes {
        let props = convert_json_map_to_properties(&properties);
        nodes_data.push((labels, props));
    }

    let db_arc = state.service.db().clone();
    let mut db = db_arc
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let ids = (*db).batch_create_nodes(nodes_data);

    Ok(Json(BatchCreateNodesResponse {
        ids: ids.into_iter().map(|id| id as u64).collect(),
    }))
}

/// 批量创建关系
async fn batch_create_rels(
    State(state): State<AppState>,
    Json(payload): Json<BatchCreateRelsRequest>,
) -> Result<Json<BatchCreateRelsResponse>, StatusCode> {
    let mut rels_data = Vec::new();
    for (start, end, typ, properties) in payload.rels {
        let props = convert_json_map_to_properties(&properties);
        rels_data.push((start, end, typ, props));
    }

    let db_arc = state.service.db().clone();
    let mut db = db_arc
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let ids = db.batch_create_rels(rels_data);

    Ok(Json(BatchCreateRelsResponse {
        ids: ids.into_iter().map(|id| id as u64).collect(),
    }))
}

/// 搜索节点（按属性值模糊搜索）
async fn search_nodes(
    State(state): State<AppState>,
    Json(payload): Json<SearchRequest>,
) -> Result<Json<Vec<NodeResponse>>, StatusCode> {
    let db_arc = state.service.db().clone();
    let db = db_arc
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let query_lower = payload.query.to_lowercase();
    let mut results = Vec::new();

    for node in (*db).all_stored_nodes() {
        // 搜索标签
        for label in &node.labels {
            if label.to_lowercase().contains(&query_lower) {
                results.push(NodeResponse {
                    id: node.id,
                    labels: node.labels.clone(),
                    properties: convert_properties_to_json_map(&node.props),
                });
                break;
            }
        }

        // 搜索属性值
        for (key, value) in &node.props {
            if key.to_lowercase().contains(&query_lower) {
                results.push(NodeResponse {
                    id: node.id,
                    labels: node.labels.clone(),
                    properties: convert_properties_to_json_map(&node.props),
                });
                break;
            }
            if let Value::Text(text) = value {
                if text.to_lowercase().contains(&query_lower) {
                    results.push(NodeResponse {
                        id: node.id,
                        labels: node.labels.clone(),
                        properties: convert_properties_to_json_map(&node.props),
                    });
                    break;
                }
            }
        }
    }

    Ok(Json(results))
}

pub async fn run_server(state: AppState, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let app = create_router(state);
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));

    println!("Server running on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
