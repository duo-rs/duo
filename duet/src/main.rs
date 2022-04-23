use std::sync::Arc;

use anyhow::Result;
use duet::Warehouse;
use parking_lot::RwLock;
use tracing::Level;
use tracing_subscriber::{filter::Targets, fmt, layer::SubscriberExt, util::SubscriberInitExt};

// ASCII Art generated from https://patorjk.com/software/taag/#p=display&h=0&v=0&f=ANSI%20Shadow&t=Duet
const JAGE_BANNER: &str = r"

     ██╗ █████╗  ██████╗ ███████╗
     ██║██╔══██╗██╔════╝ ██╔════╝
     ██║███████║██║  ███╗█████╗  
██   ██║██╔══██║██║   ██║██╔══╝  
╚█████╔╝██║  ██║╚██████╔╝███████╗
 ╚════╝ ╚═╝  ╚═╝ ╚═════╝ ╚══════╝
                                 
";

#[tokio::main]
async fn main() -> Result<()> {
    println!("{}", JAGE_BANNER);
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(Targets::new().with_target("duet", Level::DEBUG))
        .init();
    let warehouse = Arc::new(RwLock::new(Warehouse::new()));
    duet::spawn_grpc_server(Arc::clone(&warehouse));
    duet::run_web_server(warehouse).await?;

    Ok(())
}
