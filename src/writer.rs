use std::{
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
    thread,
    time::Duration,
};

use crate::metric::Metric;

struct Inner {
    sync: bool,
    socket: PathBuf,
    batch_size: usize,
    messages: Mutex<Vec<Metric>>,
    condvar: Condvar,
    shutdown: Mutex<bool>,
}

impl Inner {
    fn flush_locked(&self, messages: &mut Vec<Metric>, force: bool) {
        if (force && !messages.is_empty()) || messages.len() >= self.batch_size {
            let payload = match serde_json::to_string(messages.as_slice()) {
                Ok(p) => p,
                Err(e) => {
                    log::warn!("prom_multi_proc: failed to serialize batch: {e}");
                    messages.clear();
                    return;
                }
            };
            messages.clear();
            if let Err(e) = write_socket(&self.socket, &payload) {
                log::warn!("prom_multi_proc: failed to write batch to socket: {e}");
            }
        }
    }
}

fn write_socket(socket: &Path, payload: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut stream = UnixStream::connect(socket)?;
    stream.write_all(payload.as_bytes())?;
    Ok(())
}

/// Thread-safe metric writer.
///
/// In **async mode** (default): buffers metrics, flushes when `batch_size` is reached or
/// `batch_timeout` expires on a background thread.
///
/// In **sync mode**: writes directly to the socket on every call with no buffering or
/// background thread. Use `Writer::new_sync` to create a sync writer.
///
/// Never panics into the caller on socket errors — failures are logged and the batch is dropped.
#[derive(Clone)]
pub struct Writer {
    inner: Arc<Inner>,
}

impl Writer {
    /// Create a buffered async writer.
    pub fn new(socket: impl AsRef<Path>, batch_size: usize, batch_timeout: Duration) -> Self {
        let inner = Arc::new(Inner {
            sync: false,
            socket: socket.as_ref().to_path_buf(),
            batch_size,
            messages: Mutex::new(Vec::new()),
            condvar: Condvar::new(),
            shutdown: Mutex::new(false),
        });

        let bg = Arc::clone(&inner);
        thread::Builder::new()
            .name("prom_multi_proc_flush".into())
            .spawn(move || {
                loop {
                    let shutdown = bg.shutdown.lock().unwrap();
                    let (shutdown, _timeout) = bg
                        .condvar
                        .wait_timeout(shutdown, batch_timeout)
                        .unwrap();
                    let is_shutdown = *shutdown;
                    drop(shutdown);
                    let mut messages = bg.messages.lock().unwrap();
                    bg.flush_locked(&mut messages, true);
                    drop(messages);
                    if is_shutdown {
                        break;
                    }
                }
            })
            .expect("failed to spawn flush thread");

        Writer { inner }
    }

    /// Create a synchronous writer that sends each write directly to the socket.
    pub fn new_sync(socket: impl AsRef<Path>) -> Self {
        Writer {
            inner: Arc::new(Inner {
                sync: true,
                socket: socket.as_ref().to_path_buf(),
                batch_size: 1,
                messages: Mutex::new(Vec::new()),
                condvar: Condvar::new(),
                shutdown: Mutex::new(false),
            }),
        }
    }

    pub fn is_sync(&self) -> bool {
        self.inner.sync
    }

    /// Push a single metric and flush if batch is full (async) or write immediately (sync).
    pub fn write(&self, metric: Metric) {
        self.write_many(std::iter::once(metric));
    }

    /// Push multiple metrics atomically.
    /// In async mode: buffers and flushes when batch_size is reached.
    /// In sync mode: writes all metrics in a single socket call.
    pub fn write_many(&self, metrics: impl IntoIterator<Item = Metric>) {
        if self.inner.sync {
            let batch: Vec<Metric> = metrics.into_iter().collect();
            if batch.is_empty() {
                return;
            }
            match serde_json::to_string(&batch) {
                Ok(payload) => {
                    if let Err(e) = write_socket(&self.inner.socket, &payload) {
                        log::warn!("prom_multi_proc: failed to write to socket: {e}");
                    }
                }
                Err(e) => {
                    log::warn!("prom_multi_proc: failed to serialize metrics: {e}");
                }
            }
            return;
        }

        let mut messages = self.inner.messages.lock().unwrap();
        messages.extend(metrics);
        self.inner.flush_locked(&mut messages, false);
    }

    /// Force-flush any buffered metrics. No-op in sync mode.
    pub fn flush(&self) {
        if self.inner.sync {
            return;
        }
        let mut messages = self.inner.messages.lock().unwrap();
        self.inner.flush_locked(&mut messages, true);
    }

    /// Returns true if the socket path exists and appears writable.
    pub fn socket_available(&self) -> bool {
        let p = &self.inner.socket;
        p.exists() && std::fs::metadata(p).map(|m| !m.permissions().readonly()).unwrap_or(false)
    }

    /// Flush remaining metrics and stop the background thread. No-op in sync mode.
    pub fn shutdown(&self) {
        if self.inner.sync {
            return;
        }
        self.flush();
        let mut shutdown = self.inner.shutdown.lock().unwrap();
        *shutdown = true;
        self.inner.condvar.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::Read,
        os::unix::net::UnixListener,
        sync::mpsc,
        thread,
        time::Duration,
    };
    use tempfile::tempdir;

    fn start_server(listener: UnixListener, tx: mpsc::Sender<String>) {
        thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(mut s) => {
                        let mut buf = String::new();
                        s.read_to_string(&mut buf).unwrap();
                        tx.send(buf).unwrap();
                    }
                    Err(_) => break,
                }
            }
        });
    }

    #[test]
    fn test_write_single_metric() {
        let dir = tempdir().unwrap();
        let socket_path = dir.path().join("test.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let (tx, rx) = mpsc::channel();
        start_server(listener, tx);

        let writer = Writer::new(&socket_path, 1, Duration::from_secs(5));
        writer.write(Metric::new("my_counter", "inc", 1.0, vec![]));

        let received = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&received).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0]["name"], "my_counter");
        assert_eq!(parsed[0]["method"], "inc");
        assert_eq!(parsed[0]["value"], 1.0);
    }

    #[test]
    fn test_batch_flushes_at_batch_size() {
        let dir = tempdir().unwrap();
        let socket_path = dir.path().join("test.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let (tx, rx) = mpsc::channel();
        start_server(listener, tx);

        let writer = Writer::new(&socket_path, 3, Duration::from_secs(30));
        writer.write(Metric::new("counter", "inc", 1.0, vec![]));
        writer.write(Metric::new("counter", "inc", 1.0, vec![]));
        // No flush yet — batch_size is 3
        assert!(rx.try_recv().is_err());
        writer.write(Metric::new("counter", "inc", 1.0, vec![]));

        let received = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&received).unwrap();
        assert_eq!(parsed.len(), 3);
    }

    #[test]
    fn test_write_many_atomic() {
        let dir = tempdir().unwrap();
        let socket_path = dir.path().join("test.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let (tx, rx) = mpsc::channel();
        start_server(listener, tx);

        let writer = Writer::new(&socket_path, 10, Duration::from_secs(30));
        let metrics = vec![
            Metric::new("counter_a", "inc", 1.0, vec![]),
            Metric::new("gauge_b", "set", 42.0, vec!["val".into()]),
        ];
        writer.write_many(metrics);
        writer.flush();

        let received = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&received).unwrap();
        assert_eq!(parsed.len(), 2);
    }

    #[test]
    fn test_shutdown_flushes_remaining() {
        let dir = tempdir().unwrap();
        let socket_path = dir.path().join("test.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let (tx, rx) = mpsc::channel();
        start_server(listener, tx);

        let writer = Writer::new(&socket_path, 100, Duration::from_secs(30));
        writer.write(Metric::new("counter", "inc", 1.0, vec![]));
        writer.shutdown();

        let received = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&received).unwrap();
        assert_eq!(parsed.len(), 1);
    }

    #[test]
    fn test_missing_socket_does_not_panic() {
        let writer = Writer::new("/nonexistent/path/test.sock", 1, Duration::from_secs(5));
        writer.write(Metric::new("counter", "inc", 1.0, vec![]));
    }

    #[test]
    fn test_sync_writer_sends_immediately() {
        let dir = tempdir().unwrap();
        let socket_path = dir.path().join("test.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let (tx, rx) = mpsc::channel();
        start_server(listener, tx);

        let writer = Writer::new_sync(&socket_path);
        assert!(writer.is_sync());

        writer.write(Metric::new("my_counter", "inc", 1.0, vec![]));
        let received = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&received).unwrap();
        assert_eq!(parsed.len(), 1);

        // Each call is a separate socket write.
        writer.write(Metric::new("my_counter", "inc", 1.0, vec![]));
        let received2 = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let parsed2: Vec<serde_json::Value> = serde_json::from_str(&received2).unwrap();
        assert_eq!(parsed2.len(), 1);
    }

    #[test]
    fn test_sync_write_many_is_one_socket_call() {
        let dir = tempdir().unwrap();
        let socket_path = dir.path().join("test.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let (tx, rx) = mpsc::channel();
        start_server(listener, tx);

        let writer = Writer::new_sync(&socket_path);
        writer.write_many(vec![
            Metric::new("counter_a", "inc", 1.0, vec![]),
            Metric::new("gauge_b", "set", 5.0, vec![]),
        ]);

        let received = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&received).unwrap();
        assert_eq!(parsed.len(), 2);
    }

    #[test]
    fn test_sync_flush_and_shutdown_are_noops() {
        let writer = Writer::new_sync("/nonexistent.sock");
        writer.flush();
        writer.shutdown();
    }

    #[test]
    fn test_sync_missing_socket_does_not_panic() {
        let writer = Writer::new_sync("/nonexistent/path/test.sock");
        writer.write(Metric::new("counter", "inc", 1.0, vec![]));
    }
}
