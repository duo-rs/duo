use std::sync::Arc;

use jage::TraceBundle;
use parking_lot::RwLock;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello Jage!");
    let bundle = Arc::new(RwLock::new(TraceBundle::new()));
    jage::spawn_grpc_server(Arc::clone(&bundle));
    jage::run_web_server(bundle).await;

    Ok(())
}
