use rust_graphdb::server::{run_server, AppState};
use rust_graphdb::GraphDatabase;
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = GraphDatabase::new_in_memory();
    let state = AppState {
        db: Arc::new(Mutex::new(db)),
    };

    run_server(state, 3000).await?;
    Ok(())
}
