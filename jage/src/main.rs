#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello Jage!");

    jage::spawn_grpc_server();
    jage::run_web_server().await;

    Ok(())
}
