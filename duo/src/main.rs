use std::sync::Arc;

use anyhow::Result;
use duo::Warehouse;
use parking_lot::RwLock;
use tracing::Level;
use tracing_subscriber::{filter::Targets, fmt, layer::SubscriberExt, util::SubscriberInitExt};

// ASCII Art generated from https://patorjk.com/software/taag/#p=display&h=0&v=0&f=ANSI%20Regular&t=Duo
const JAGE_BANNER: &str = r"
                                  
██████  ██    ██  ██████  
██   ██ ██    ██ ██    ██ 
██   ██ ██    ██ ██    ██ 
██   ██ ██    ██ ██    ██ 
██████   ██████   ██████  
                                  
";

#[tokio::main]
async fn main() -> Result<()> {
    println!("{}", JAGE_BANNER);
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(Targets::new().with_target("duo", Level::DEBUG))
        .init();
    let warehouse = Arc::new(RwLock::new(Warehouse::new()));
    duo::spawn_grpc_server(Arc::clone(&warehouse));
    duo::run_web_server(warehouse).await?;

    Ok(())
}
