use std::time::Duration;

use duo_api::instrument::instrument_client::InstrumentClient;
use tonic::transport::Uri;

use crate::client::DuoClient;

pub struct Connection;

impl Connection {
    const BACKOFF: Duration = Duration::from_millis(500);
    const MAX_BACKOFF: Duration = Duration::from_secs(5);

    pub async fn connect(name: &'static str, uri: Uri) -> DuoClient {
        let mut backoff = Duration::from_secs(0);
        loop {
            if backoff == Duration::from_secs(0) {
                tracing::debug!(to = %uri, "connecting");
            } else {
                tracing::debug!(reconnect_in = ?backoff, "reconnecting");
                tokio::time::sleep(backoff).await;
            }

            let try_connect = async {
                let client = InstrumentClient::connect(uri.clone())
                    .await
                    .map_err(|err| format!("InstrumentClient connect error: {}", err))?;
                Ok::<InstrumentClient<_>, String>(client)
            };

            match try_connect.await {
                Ok(connected_client) => {
                    tracing::debug!("connected successfully!");
                    let mut client = DuoClient::new(name, connected_client);
                    client.registry_process().await;
                    return client;
                }
                Err(error) => {
                    tracing::warn!(%error, "error connecting");
                    backoff = std::cmp::max(backoff + Self::BACKOFF, Self::MAX_BACKOFF);
                }
            };
        }
    }
}
