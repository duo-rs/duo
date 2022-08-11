use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing::debug;

use crate::{Process};
use crate::aggregator::AggregatedData;

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
    //TODO: 如果 bootstrap 里的线程不改变 self, 可以将 bootstrap 改成不可变, 去掉 RWLock
    pub fn bootstrap(&mut self) {
        let persist_receiver = Arc::clone(&self.persist_receiver);
        tokio::spawn(async move {
            // let process_file =
            loop {
                let mut persist_receiver = persist_receiver.write();
                match persist_receiver.recv().await {
                    Some(PersistMessage::Process(process)) => {
                        //TODO: Process { id: "example:0", service_name: "example", tags: {"duo-version": Value { inner: Some(StrVal("0.1.0")) }} }
                        debug!("hhh: receive persist process info: {:?}", process);
                    }
                    Some(PersistMessage::Data(data)) => {
                        //TODO:
                        debug!("hhh: receive persist data info: {:?}", data);
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
