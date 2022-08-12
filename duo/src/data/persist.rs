use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing::debug;

use crate::{Process};
use crate::aggregator::AggregatedData;
use crate::data::serialize::{LogPersist, ProcessPersist, TracePersist};
use crate::data::writer::PersistWriter;

#[derive(Clone)]
pub struct PersistConfig {
    /// current path of writer write. for example `./log/`
    pub path: String,
    pub log_load_time: u32,
}

pub struct Persist {
    persist_sender: Sender<PersistMessage>,
    persist_receiver: Arc<RwLock<Receiver<PersistMessage>>>,
}

#[derive(Debug)]
enum PersistMessage {
    Process(Process),
    Data(AggregatedData),
}

impl Persist {
    pub fn new() -> Self {
        let (persist_sender, persist_receiver) = channel::<PersistMessage>(4096);
        Self {
            persist_sender,
            persist_receiver: Arc::new(RwLock::new(persist_receiver)),
        }
    }

    pub fn bootstrap(& self, mut config: PersistConfig) {
        let persist_receiver = Arc::clone(&self.persist_receiver);
        tokio::spawn(async move {
            let base_path = config.path;
            config.path = format!("{}{}", base_path, "process");
            let mut process_writer = PersistWriter::new(config.clone()).await.unwrap();
            config.path = format!("{}{}", base_path, "trace");
            let mut trace_writer = PersistWriter::new(config.clone()).await.unwrap();
            config.path = format!("{}{}", base_path, "log");
            let mut log_writer = PersistWriter::new(config).await.unwrap();
            loop {
                let mut persist_receiver = persist_receiver.write();
                match persist_receiver.recv().await {
                    Some(PersistMessage::Process(process)) => {
                        if (!process.id.is_empty()) && (!process.service_name.is_empty()) {
                            if let Err(e) = process_writer.write(ProcessPersist::from(process)).await {
                                debug!("persist process info error: {:?}", e);
                            }
                        }
                    }
                    Some(PersistMessage::Data(data)) => {
                        if !data.logs.is_empty() {
                            for log in data.logs {
                                if let Err(e) = log_writer.write(LogPersist::from(log)).await {
                                    debug!("persist log info error: {:?}", e);
                                }
                            }
                        }
                        if !data.traces.is_empty() {
                            for trace in data.traces {
                                if let Err(e) = trace_writer.write(TracePersist::from(trace.1)).await {
                                    debug!("persist trace info error: {:?}", e);
                                }
                            }
                        }
                        if let Err(e) = process_writer.flush().await {
                            debug!("persist flush process info error: {:?}", e);
                        }
                        if let Err(e) = log_writer.flush().await {
                            debug!("persist flush log info error: {:?}", e);
                        }
                        if let Err(e) = trace_writer.flush().await {
                            debug!("persist flush trace info error: {:?}", e);
                        }
                    }
                    None => {}
                }
            }
        });
    }
    pub async fn persist_process(&self, process: Process) {
        self.persist_sender.send(PersistMessage::Process(process)).await.map_err(|e| debug!("persist process info error: {:?}", e)).expect("print debug info error");
    }

    pub async fn persist_data(&self, data: AggregatedData) {
        self.persist_sender.send(PersistMessage::Data(data)).await.map_err(|e| debug!("persist data info error: {:?}", e)).expect("print debug info error");
    }
}
