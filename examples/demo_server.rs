use rs_graphdb::graph::db::GraphDatabase;
use rs_graphdb::server::{run_server, AppState};
use rs_graphdb::service::GraphService;
use rs_graphdb::storage::mem_store::MemStore;
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = GraphDatabase::<MemStore>::new_in_memory();
    let db = Arc::new(Mutex::new(db));
    let service = Arc::new(GraphService::new(db));
    let state = AppState::new(service);

    run_server(state, 3000).await?;
    Ok(())
}
