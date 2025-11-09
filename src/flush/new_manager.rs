use crate::flush::Task;
use std::sync::Arc;

pub struct FlushNewManager {
    sender: flume::Sender<Arc<Task>>,
    receiver: flume::Receiver<Arc<Task>>,
}

impl FlushNewManager {
    pub fn new() -> Self {
        let (tx, rx) = flume::bounded(100_000);

        Self {
            sender: tx,
            receiver: rx,
        }
    }

    pub fn enqueue(&self, task: Arc<Task>) {
        self.sender.send(task).expect("3.0.0 handle error?");
    }

    pub fn dequeue(&self) -> Arc<Task> {
        self.receiver.recv().expect("3.0.0 handle error?")
    }
}
