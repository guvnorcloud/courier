use crate::sources::LogEvent;
use tokio::sync::mpsc;

pub struct Buffer { rx: mpsc::Receiver<LogEvent> }

impl Buffer {
    pub fn new(rx: mpsc::Receiver<LogEvent>) -> Self { Self { rx } }
    pub async fn recv(&mut self) -> Option<LogEvent> { self.rx.recv().await }
}

pub fn create(capacity: usize) -> (mpsc::Sender<LogEvent>, Buffer) {
    let (tx, rx) = mpsc::channel(capacity);
    (tx, Buffer::new(rx))
}
