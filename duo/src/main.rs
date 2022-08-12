use std::sync::Arc;

use anyhow::Result;
use clap::StructOpt;
use duo::{PersistConfig, Warehouse};
use parking_lot::RwLock;
use tracing::Level;
use tracing_subscriber::{filter::Targets, fmt, layer::SubscriberExt, util::SubscriberInitExt};

// ASCII Art generated from https://patorjk.com/software/taag/#p=display&h=0&v=0&f=ANSI%20Regular&t=Duo
static DUO_BANNER: &str = r"
                                  
██████  ██    ██  ██████  
██   ██ ██    ██ ██    ██ 
██   ██ ██    ██ ██    ██ 
██   ██ ██    ██ ██    ██ 
██████   ██████   ██████  
                                  
";

#[derive(Debug, clap::Parser)]
#[clap(name = "duo")]
#[clap(about = "Observability duo: Logging and Tracing.", long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, clap::Subcommand)]
enum Commands {
    /// Start the duo server.
    Start {
        /// The web server listening port.
        #[clap(short, default_value_t = 3000)]
        web_port: u16,
        /// The gRPC server listening port.
        #[clap(short, default_value_t = 6000)]
        grpc_port: u16,
        /// How many days of observation data will be load to duo server, default 2 weeks.
        /// 0 means not perform load operation (including today's data), all data collect will be append to today's log.
        #[clap(short, default_value_t = 14)]
        persist_data_load_time: u32,
        /// where the observation data is stored.
        #[clap(short, default_value = "./")]
        persist_data_path: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("{}", DUO_BANNER);
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(Targets::new().with_target("duo", Level::DEBUG))
        .init();
    let warehouse: Arc<RwLock<Warehouse>> = Arc::new(RwLock::new(Warehouse::new()));

    match Cli::parse().command {
        Commands::Start {
            web_port,
            grpc_port,
            persist_data_load_time,
            persist_data_path
        } => {
            let persist_config = PersistConfig {
                path: persist_data_path,
                log_load_time: persist_data_load_time,
            };
            warehouse.write().replay(persist_config.clone()).await?;
            duo::spawn_grpc_server(Arc::clone(&warehouse), grpc_port, persist_config);
            duo::run_web_server(warehouse, web_port).await?;
        }
    }

    Ok(())
}
