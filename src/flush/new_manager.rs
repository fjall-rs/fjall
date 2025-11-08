use crate::flush::Task;
use std::sync::Arc;

pub struct FlushNewManagerInner {
    sender: flume::Sender<Arc<Task>>,
    receiver: flume::Receiver<Arc<Task>>,
}

#[derive(Clone)]
pub struct FlushNewManager(Arc<FlushNewManagerInner>);

impl FlushNewManager {
    pub fn new() -> Self {
        let (tx, rx) = flume::bounded(100_000);

        Self(Arc::new(FlushNewManagerInner {
            sender: tx,
            receiver: rx,
        }))
    }

    pub fn enqueue(&self, task: Arc<Task>) {
        self.0.sender.send(task).expect("3.0.0 handle error?");
    }

    pub fn dequeue(&self) -> Arc<Task> {
        self.0.receiver.recv().expect("3.0.0 handle error?")
    }
}
