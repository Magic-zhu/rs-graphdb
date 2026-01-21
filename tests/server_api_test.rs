// 集成测试：服务器 API 端点
use http_body_util::BodyExt;
use serde::de::DeserializeOwned;
use std::sync::{Arc, Mutex};
use tower::ServiceExt;

use rs_graphdb::graph::db::GraphDatabase;
use rs_graphdb::server::{create_router, AppState};
use rs_graphdb::service::GraphService;
use rs_graphdb::storage::mem_store::MemStore;
use rs_graphdb::values::{Properties, Value};

/// 辅助函数：创建测试用应用状态
fn create_test_state() -> AppState {
    let db = GraphDatabase::<MemStore>::new_in_memory();
    let db = Arc::new(Mutex::new(db));

    // 添加一些测试数据
    {
        let mut guard = db.lock().unwrap();
        let alice = guard.create_node(vec!["User"], {
            let mut props = Properties::new();
            props.insert("name".to_string(), Value::Text("Alice".to_string()));
            props.insert("age".to_string(), Value::Int(30));
            props
        });

        let bob = guard.create_node(vec!["User"], {
            let mut props = Properties::new();
            props.insert("name".to_string(), Value::Text("Bob".to_string()));
            props.insert("age".to_string(), Value::Int(25));
            props
        });

        guard.create_rel(alice, bob, "FRIEND", {
            let mut props = Properties::new();
            props.insert("since".to_string(), Value::Text("2020".to_string()));
            props
        });
    }

    let service = Arc::new(GraphService::new(db));
    AppState::new(service)
}

/// 辅助函数：发送 GET 请求并解析响应
async fn get_json<T: DeserializeOwned>(app: &axum::Router, path: &str) -> T {
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri(path)
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body = response.into_body();
    let bytes = body.collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

/// 辅助函数：发送 POST 请求并解析响应
async fn post_json<T: DeserializeOwned>(
    app: &axum::Router,
    path: &str,
    body: serde_json::Value,
) -> T {
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri(path)
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body = response.into_body();
    let bytes = body.collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

// ========== 基础端点测试 ==========

#[tokio::test]
async fn test_root_endpoint() {
    let state = create_test_state();
    let app = create_router(state);

    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body = response.into_body();
    let bytes = body.collect().await.unwrap().to_bytes();
    assert_eq!(
        std::str::from_utf8(&bytes).unwrap(),
        "Rust Graph Database API - Visit /ui for web interface"
    );
}

#[tokio::test]
async fn test_ui_endpoint() {
    let state = create_test_state();
    let app = create_router(state);

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri("/ui")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body = response.into_body();
    let bytes = body.collect().await.unwrap().to_bytes();
    let html = std::str::from_utf8(&bytes).unwrap();
    assert!(html.contains("<!DOCTYPE html>") || html.contains("<html"));
}

// ========== 节点操作测试 ==========

#[tokio::test]
async fn test_create_node() {
    let state = create_test_state();
    let app = create_router(state);

    let response: serde_json::Value = post_json(
        &app,
        "/nodes",
        serde_json::json!({
            "labels": ["Person"],
            "properties": {
                "name": "Charlie",
                "age": 35
            }
        }),
    )
    .await;

    // 新节点应该有 ID（具体值取决于数据库）
    assert!(response["id"].is_number());
}

#[tokio::test]
async fn test_get_all_nodes() {
    let state = create_test_state();
    let app = create_router(state);

    let nodes: Vec<serde_json::Value> = get_json(&app, "/nodes").await;

    assert_eq!(nodes.len(), 2);

    // 验证有两个 User 节点
    let alice = nodes.iter().find(|n| n["properties"]["name"] == "Alice");
    let bob = nodes.iter().find(|n| n["properties"]["name"] == "Bob");

    assert!(alice.is_some());
    assert!(bob.is_some());
    assert_eq!(alice.unwrap()["labels"][0], "User");
    assert_eq!(alice.unwrap()["properties"]["age"], 30);
}

#[tokio::test]
async fn test_get_node_by_id() {
    let state = create_test_state();
    let app = create_router(state);

    // 先获取所有节点找到 Alice 的 ID
    let nodes: Vec<serde_json::Value> = get_json(&app, "/nodes").await;
    let alice = nodes.iter().find(|n| n["properties"]["name"] == "Alice").unwrap();
    let alice_id = alice["id"].as_u64().unwrap();

    // 通过 ID 获取节点
    let node: serde_json::Value = get_json(&app, &format!("/nodes/{}", alice_id)).await;

    assert_eq!(node["id"], alice_id);
    assert_eq!(node["labels"][0], "User");
    assert_eq!(node["properties"]["name"], "Alice");
}

#[tokio::test]
async fn test_get_node_neighbors() {
    let state = create_test_state();
    let app = create_router(state);

    // 先获取所有节点找到 Alice 的 ID
    let nodes: Vec<serde_json::Value> = get_json(&app, "/nodes").await;
    let alice = nodes.iter().find(|n| n["properties"]["name"] == "Alice").unwrap();
    let bob = nodes.iter().find(|n| n["properties"]["name"] == "Bob").unwrap();
    let alice_id = alice["id"].as_u64().unwrap();
    let bob_id = bob["id"].as_u64().unwrap();

    let neighbors: serde_json::Value = get_json(&app, &format!("/nodes/{}/neighbors", alice_id)).await;

    assert_eq!(neighbors["outgoing"].as_array().unwrap().len(), 1);
    assert_eq!(neighbors["outgoing"][0], bob_id);
    assert_eq!(neighbors["incoming"].as_array().unwrap().len(), 0);
}

// ========== 关系操作测试 ==========

#[tokio::test]
async fn test_create_rel() {
    let state = create_test_state();
    let app = create_router(state);

    // 先获取节点找到 Alice 和 Bob 的 ID
    let nodes: Vec<serde_json::Value> = get_json(&app, "/nodes").await;
    let alice = nodes.iter().find(|n| n["properties"]["name"] == "Alice").unwrap();
    let bob = nodes.iter().find(|n| n["properties"]["name"] == "Bob").unwrap();
    let alice_id = alice["id"].as_u64().unwrap();
    let bob_id = bob["id"].as_u64().unwrap();

    let response: serde_json::Value = post_json(
        &app,
        "/rels",
        serde_json::json!({
            "start": bob_id,
            "end": alice_id,
            "rel_type": "FRIEND",
            "properties": {
                "since": "2021"
            }
        }),
    )
    .await;

    // 新关系应该有 ID
    assert!(response["id"].is_number());
}

#[tokio::test]
async fn test_get_all_rels() {
    let state = create_test_state();
    let app = create_router(state);

    let rels: Vec<serde_json::Value> = get_json(&app, "/rels").await;

    assert_eq!(rels.len(), 1);
    assert_eq!(rels[0]["typ"], "FRIEND");
}

// ========== 查询操作测试 ==========

#[tokio::test]
async fn test_query_by_label() {
    let state = create_test_state();
    let app = create_router(state);

    let results: Vec<serde_json::Value> = post_json(
        &app,
        "/query",
        serde_json::json!({
            "label": "User"
        }),
    )
    .await;

    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn test_query_by_label_and_property() {
    let state = create_test_state();
    let app = create_router(state);

    let results: Vec<serde_json::Value> = post_json(
        &app,
        "/query",
        serde_json::json!({
            "label": "User",
            "property": "name",
            "value": "Alice"
        }),
    )
    .await;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["properties"]["name"], "Alice");
}

#[tokio::test]
async fn test_search_nodes() {
    let state = create_test_state();
    let app = create_router(state);

    let results: Vec<serde_json::Value> = post_json(
        &app,
        "/search",
        serde_json::json!({
            "query": "Alice"
        }),
    )
    .await;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["properties"]["name"], "Alice");
}

// ========== 统计信息测试 ==========

#[tokio::test]
async fn test_get_stats() {
    let state = create_test_state();
    let app = create_router(state);

    let stats: serde_json::Value = get_json(&app, "/stats").await;

    assert_eq!(stats["node_count"], 2);
    assert_eq!(stats["rel_count"], 1);
    assert!(stats["labels"].as_array().unwrap().contains(&serde_json::json!("User")));
}

#[tokio::test]
async fn test_get_labels() {
    let state = create_test_state();
    let app = create_router(state);

    let labels: Vec<String> = get_json(&app, "/labels").await;

    assert_eq!(labels, vec!["User"]);
}

#[tokio::test]
async fn test_get_rel_types() {
    let state = create_test_state();
    let app = create_router(state);

    let rel_types: Vec<String> = get_json(&app, "/rel-types").await;

    assert_eq!(rel_types, vec!["FRIEND"]);
}

// ========== 批量操作测试 ==========

#[tokio::test]
async fn test_batch_create_nodes() {
    let state = create_test_state();
    let app = create_router(state);

    let response: serde_json::Value = post_json(
        &app,
        "/batch/nodes",
        serde_json::json!({
            "nodes": [
                [["Person"], {"name": "Dave"}],
                [["Person"], {"name": "Eve"}]
            ]
        }),
    )
    .await;

    assert_eq!(response["ids"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn test_batch_create_rels() {
    let state = create_test_state();
    let app = create_router(state);

    let response: serde_json::Value = post_json(
        &app,
        "/batch/rels",
        serde_json::json!({
            "rels": [
                [1, 2, "KNOWS", {}],
                [2, 1, "KNOWS", {}]
            ]
        }),
    )
    .await;

    assert_eq!(response["ids"].as_array().unwrap().len(), 2);
}

// ========== 系统信息端点测试 ==========

#[tokio::test]
async fn test_get_sysinfo() {
    let state = create_test_state();
    let app = create_router(state);

    let sysinfo: serde_json::Value = get_json(&app, "/sysinfo").await;

    // 验证系统信息结构
    assert!(sysinfo["kernel_version"].is_string());
    assert!(sysinfo["store_size"].is_number());
    assert!(sysinfo["node_id_count"].is_number());
    assert!(sysinfo["rel_id_count"].is_number());
    assert!(sysinfo["uptime"].is_string());
    assert!(sysinfo["databases"].is_array());

    // 验证运行时间格式
    let uptime = sysinfo["uptime"].as_str().unwrap();
    assert!(uptime.contains("h") || uptime.contains("m"));

    // 验证数据库列表
    let databases = sysinfo["databases"].as_array().unwrap();
    assert_eq!(databases.len(), 1);
    assert_eq!(databases[0]["name"], "default");
    assert_eq!(databases[0]["node_count"], 2);
    assert_eq!(databases[0]["rel_count"], 1);
}

#[tokio::test]
async fn test_get_queries() {
    let state = create_test_state();
    let app = create_router(state);

    let queries: Vec<serde_json::Value> = get_json(&app, "/queries").await;

    // 当前实现返回空列表，因为没有查询追踪机制
    assert!(queries.is_empty());
}

#[tokio::test]
async fn test_get_databases() {
    let state = create_test_state();
    let app = create_router(state);

    let databases: Vec<serde_json::Value> = get_json(&app, "/dbs").await;

    assert_eq!(databases.len(), 1);
    assert_eq!(databases[0]["name"], "default");
}

// ========== 删除操作测试 ==========

#[tokio::test]
async fn test_delete_node() {
    let state = create_test_state();
    let app = create_router(state);

    // 先获取所有节点找到一个要删除的节点 ID
    let nodes: Vec<serde_json::Value> = get_json(&app, "/nodes").await;
    let alice_id = nodes[0]["id"].as_u64().unwrap();

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("DELETE")
                .uri(&format!("/nodes/{}", alice_id))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(result["status"], "success");
    // deleted 字段应该是布尔值
    assert!(result["deleted"].is_boolean());
}

#[tokio::test]
async fn test_delete_rel() {
    let state = create_test_state();
    let app = create_router(state);

    // 先获取所有关系找到一个要删除的关系 ID
    let rels: Vec<serde_json::Value> = get_json(&app, "/rels").await;
    let rel_id = rels[0]["id"].as_u64().unwrap();

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("DELETE")
                .uri(&format!("/rels/{}", rel_id))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(result["status"], "success");
    // deleted 字段应该是布尔值
    assert!(result["deleted"].is_boolean());
}

// ========== 错误处理测试 ==========

#[tokio::test]
async fn test_get_nonexistent_node() {
    let state = create_test_state();
    let app = create_router(state);

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri("/nodes/999")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 404);
}

#[tokio::test]
async fn test_get_nonexistent_rel() {
    let state = create_test_state();
    let app = create_router(state);

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri("/rels/999")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 404);
}

#[tokio::test]
async fn test_invalid_json() {
    let state = create_test_state();
    let app = create_router(state);

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/nodes")
                .header("content-type", "application/json")
                .body(axum::body::Body::from("{invalid json}"))
                .unwrap(),
        )
        .await
        .unwrap();

    // 应该返回错误状态码
    assert!(response.status().is_client_error() || response.status().is_server_error());
}

// ========== CORS 测试 ==========

#[tokio::test]
async fn test_cors_headers() {
    let state = create_test_state();
    let app = create_router(state);

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("OPTIONS")
                .uri("/nodes")
                .header("origin", "http://example.com")
                .header("access-control-request-method", "GET")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // CORS 预检请求应该成功
    assert!(response.status().is_success() || response.status().as_u16() == 204);
}

// ========== 更新操作测试 ==========

#[tokio::test]
async fn test_update_node() {
    let state = create_test_state();
    let app = create_router(state);

    // 先获取所有节点找到一个要更新的节点 ID
    let nodes: Vec<serde_json::Value> = get_json(&app, "/nodes").await;
    let alice_id = nodes[0]["id"].as_u64().unwrap();

    // 更新节点属性
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("PUT")
                .uri(&format!("/nodes/{}", alice_id))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::json!({"properties": {"age": 31}}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(result["status"], "success");
}

#[tokio::test]
async fn test_update_rel() {
    let state = create_test_state();
    let app = create_router(state);

    // 先获取所有关系找到一个要更新的关系 ID
    let rels: Vec<serde_json::Value> = get_json(&app, "/rels").await;
    let rel_id = rels[0]["id"].as_u64().unwrap();

    // 更新关系属性
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("PUT")
                .uri(&format!("/rels/{}", rel_id))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::json!({"properties": {"since": "2022"}}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(result["status"], "success");
}

#[tokio::test]
async fn test_update_nonexistent_node() {
    let state = create_test_state();
    let app = create_router(state);

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("PUT")
                .uri("/nodes/999")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::json!({"properties": {"age": 31}}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 404);
}

// ========== Cypher 查询执行测试 ==========

#[tokio::test]
async fn test_cypher_match_query() {
    let state = create_test_state();
    let app = create_router(state);

    let response: serde_json::Value = post_json(
        &app,
        "/cypher",
        serde_json::json!({
            "query": "MATCH (n:User) RETURN n"
        }),
    )
    .await;

    assert_eq!(response["result_type"], "nodes");
    assert!(response["data"]["nodes"].is_array());
    assert_eq!(response["data"]["nodes"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn test_cypher_match_with_label_prop() {
    let state = create_test_state();
    let app = create_router(state);

    let response: serde_json::Value = post_json(
        &app,
        "/cypher",
        serde_json::json!({
            "query": "MATCH (n:User {name: \"Alice\"}) RETURN n"
        }),
    )
    .await;

    assert_eq!(response["result_type"], "nodes");
    assert_eq!(response["data"]["nodes"].as_array().unwrap().len(), 1);
    assert_eq!(response["data"]["nodes"][0]["properties"]["name"], "Alice");
}

#[tokio::test]
async fn test_cypher_create() {
    let state = create_test_state();
    let app = create_router(state);

    let response: serde_json::Value = post_json(
        &app,
        "/cypher",
        serde_json::json!({
            "query": "CREATE (p:Person {name: \"Charlie\", age: 35})"
        }),
    )
    .await;

    assert_eq!(response["result_type"], "created");
    assert!(response["data"]["node_ids"].is_array());
    assert_eq!(response["data"]["node_ids"].as_array().unwrap().len(), 1);
    assert_eq!(response["stats"]["nodes_created"], 1);
}

#[tokio::test]
async fn test_cypher_create_with_relationship() {
    let state = create_test_state();
    let app = create_router(state);

    let response: serde_json::Value = post_json(
        &app,
        "/cypher",
        serde_json::json!({
            "query": "CREATE (p1:Person {name: \"Dave\"})-[:KNOWS]->(p2:Person {name: \"Eve\"})"
        }),
    )
    .await;

    assert_eq!(response["result_type"], "created");
    assert_eq!(response["stats"]["nodes_created"], 2);
    assert_eq!(response["stats"]["rels_created"], 1);
}

#[tokio::test]
async fn test_cypher_invalid_query() {
    let state = create_test_state();
    let app = create_router(state);

    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/cypher")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::json!({"query": "INVALID QUERY"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    // 应该返回错误状态码
    assert!(response.status().is_client_error() || response.status().is_server_error());
}

#[tokio::test]
async fn test_cypher_delete() {
    let state = create_test_state();
    let app = create_router(state);

    // 首先创建一个测试节点
    let _create_response: serde_json::Value = post_json(
        &app,
        "/cypher",
        serde_json::json!({
            "query": "CREATE (p:TempNode {name: \"ToDelete\"})"
        }),
    )
    .await;

    // 然后删除它
    let delete_response: serde_json::Value = post_json(
        &app,
        "/cypher",
        serde_json::json!({
            "query": "MATCH (p:TempNode) DELETE p"
        }),
    )
    .await;

    assert_eq!(delete_response["result_type"], "deleted");
    assert_eq!(delete_response["stats"]["nodes_deleted"], 1);
}

#[tokio::test]
async fn test_cypher_return_order_limit() {
    let state = create_test_state();
    let app = create_router(state);

    let response: serde_json::Value = post_json(
        &app,
        "/cypher",
        serde_json::json!({
            "query": "MATCH (n:User) RETURN n ORDER BY n.name DESC LIMIT 1"
        }),
    )
    .await;

    assert_eq!(response["result_type"], "nodes");
    // 应该返回 1 个结果（Bob 在字母顺序上排在 Alice 后面）
    assert_eq!(response["data"]["nodes"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn test_cypher_count_aggregation() {
    let state = create_test_state();
    let app = create_router(state);

    let response: serde_json::Value = post_json(
        &app,
        "/cypher",
        serde_json::json!({
            "query": "MATCH (n:User) RETURN COUNT(*)"
        }),
    )
    .await;

    assert_eq!(response["result_type"], "nodes");
    // COUNT(*) 返回的结果
    assert!(response["data"]["nodes"].is_array());
}

#[tokio::test]
async fn test_cypher_traversal() {
    let state = create_test_state();
    let app = create_router(state);

    let response: serde_json::Value = post_json(
        &app,
        "/cypher",
        serde_json::json!({
            "query": "MATCH (a:User)-[:FRIEND]->(b:User) RETURN b"
        }),
    )
    .await;

    assert_eq!(response["result_type"], "nodes");
    // 应该返回 Bob (Alice 的 FRIEND)
    let nodes = response["data"]["nodes"].as_array().unwrap();
    assert!(nodes.len() >= 1);
}
