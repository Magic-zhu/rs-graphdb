use rs_graphdb::grpc::proto::graph_db_service_client::GraphDbServiceClient;
use rs_graphdb::grpc::proto::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = GraphDbServiceClient::connect("http://127.0.0.1:50051").await?;

    println!("已连接到 gRPC server");

    // 1. 创建节点 A
    let create_node_req = CreateNodeRequest {
        labels: vec!["Person".to_string()],
        properties: std::collections::HashMap::from([
            (
                "name".to_string(),
                Value {
                    value: Some(value::Value::TextValue("Alice".to_string())),
                },
            ),
            (
                "age".to_string(),
                Value {
                    value: Some(value::Value::IntValue(30)),
                },
            ),
        ]),
    };

    let response = client.create_node(create_node_req).await?;
    let node_a = response.into_inner();
    println!("创建节点 A, id = {}", node_a.id);

    // 2. 创建节点 B
    let create_node_req_b = CreateNodeRequest {
        labels: vec!["Person".to_string()],
        properties: std::collections::HashMap::from([
            (
                "name".to_string(),
                Value {
                    value: Some(value::Value::TextValue("Bob".to_string())),
                },
            ),
            (
                "age".to_string(),
                Value {
                    value: Some(value::Value::IntValue(25)),
                },
            ),
        ]),
    };

    let response_b = client.create_node(create_node_req_b).await?;
    let node_b = response_b.into_inner();
    println!("创建节点 B, id = {}", node_b.id);

    // 3. 创建关系
    let create_rel_req = CreateRelationshipRequest {
        start: node_a.id,
        end: node_b.id,
        rel_type: "KNOWS".to_string(),
        properties: std::collections::HashMap::from([(
            "since".to_string(),
            Value {
                value: Some(value::Value::IntValue(2020)),
            },
        )]),
    };

    let response_rel = client.create_relationship(create_rel_req).await?;
    let rel = response_rel.into_inner();
    println!("创建关系, id = {}, {} -[:{}]-> {}", rel.id, rel.start, rel.rel_type, rel.end);

    println!("\n✅ gRPC 客户端测试完成！");
    Ok(())
}
