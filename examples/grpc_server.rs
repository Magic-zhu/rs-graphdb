use rs_graphdb::graph::db::GraphDatabase;
use rs_graphdb::grpc::run_grpc_server;
use rs_graphdb::service::GraphService;
use rs_graphdb::storage::mem_store::MemStore;
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = GraphDatabase::<MemStore>::new_in_memory();
    let db = Arc::new(Mutex::new(db));
    let service = Arc::new(GraphService::new(db));

    let addr: std::net::SocketAddr = "127.0.0.1:50051".parse()?;

    run_grpc_server(service, addr).await
}
