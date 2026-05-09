use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crate::{
    batch::Batch,
    collector::CollectorInner,
    counter::Counter,
    error::{Error, Result},
    gauge::Gauge,
    histogram::Histogram,
    spec::{load_specs, MetricSpec},
    summary::Summary,
    validate::{apply_prefix, normalize_prefix, validate_label, validate_name, validate_prefix},
    writer::Writer,
};

/// Metric handle, one variant per type.
#[derive(Clone)]
pub enum MetricHandle {
    Counter(Counter),
    Gauge(Gauge),
    Histogram(Histogram),
    Summary(Summary),
}

/// Configuration for a `Client`.
pub struct Config {
    socket: PathBuf,
    metrics: PathBuf,
    /// `None` means "use default (1)" or "N/A in sync mode".
    batch_size: Option<usize>,
    /// `None` means "use default (3 s)" or "N/A in sync mode".
    batch_timeout: Option<Duration>,
    prefix: String,
    validate: bool,
    sync: bool,
}

impl Config {
    pub fn new(socket: impl AsRef<Path>, metrics: impl AsRef<Path>) -> Self {
        Self {
            socket: socket.as_ref().to_path_buf(),
            metrics: metrics.as_ref().to_path_buf(),
            batch_size: None,
            batch_timeout: None,
            prefix: String::new(),
            validate: false,
            sync: false,
        }
    }

    pub fn batch_size(mut self, n: usize) -> Result<Self> {
        if n == 0 {
            return Err(Error::InvalidBatchSize(n));
        }
        self.batch_size = Some(n);
        Ok(self)
    }

    pub fn batch_timeout(mut self, d: Duration) -> Result<Self> {
        if d.is_zero() {
            return Err(Error::InvalidBatchTimeout(d.as_secs()));
        }
        self.batch_timeout = Some(d);
        Ok(self)
    }

    /// Set the metric name prefix. A `_` separator is appended automatically if missing.
    pub fn prefix(mut self, p: impl AsRef<str>) -> Result<Self> {
        let normalized = normalize_prefix(p.as_ref());
        validate_prefix(&normalized)?;
        self.prefix = normalized;
        Ok(self)
    }

    /// When true, label counts are validated before buffering (returns `Err` instead of silently dropping).
    pub fn validate(mut self, v: bool) -> Self {
        self.validate = v;
        self
    }

    /// Enable synchronous mode: each write goes directly to the socket with no buffering or
    /// background thread. Mutually exclusive with `batch_size` and `batch_timeout`.
    pub fn sync(mut self, s: bool) -> Self {
        self.sync = s;
        self
    }

    pub fn build(self) -> Result<Client> {
        Client::from_config(self)
    }
}

/// Thread-safe Prometheus metrics client for multi-process applications.
///
/// Connects to a `prom_multi_proc` daemon over a Unix socket.
/// In async mode (default) buffers metrics and sends them in batches.
/// In sync mode each write is sent immediately.
pub struct Client {
    writer: Writer,
    metrics: HashMap<String, MetricHandle>,
    prefix: String,
}

impl Client {
    fn from_config(cfg: Config) -> Result<Self> {
        if cfg.sync && (cfg.batch_size.is_some() || cfg.batch_timeout.is_some()) {
            return Err(Error::SyncModeConflict);
        }

        if !cfg.socket.exists() {
            log::warn!("prom_multi_proc: socket does not exist: {}", cfg.socket.display());
        }

        let writer = if cfg.sync {
            Writer::new_sync(&cfg.socket)
        } else {
            let batch_size = cfg.batch_size.unwrap_or(1);
            let batch_timeout = cfg.batch_timeout.unwrap_or(Duration::from_secs(3));
            Writer::new(&cfg.socket, batch_size, batch_timeout)
        };

        let specs = load_specs(&cfg.metrics)?;
        let mut metrics: HashMap<String, MetricHandle> = HashMap::new();

        for spec in specs {
            let handle = build_handle(&spec, &cfg.prefix, &writer)?;
            let key = stripped_key(&spec.name, &cfg.prefix);
            if metrics.contains_key(&key) {
                return Err(Error::DuplicateMetric(key));
            }
            metrics.insert(key, handle);
        }

        Ok(Client { writer, metrics, prefix: cfg.prefix })
    }

    /// Retrieve a metric handle by its short name (without prefix).
    pub fn metric(&self, name: &str) -> Option<&MetricHandle> {
        self.metrics.get(name)
    }

    /// Returns true if a metric with the given short name exists.
    pub fn has_metric(&self, name: &str) -> bool {
        self.metrics.contains_key(name)
    }

    /// Returns all registered metric short names.
    pub fn metric_names(&self) -> impl Iterator<Item = &str> {
        self.metrics.keys().map(String::as_str)
    }

    /// Retrieve a counter by short name.
    pub fn counter(&self, name: &str) -> Option<&Counter> {
        match self.metrics.get(name) {
            Some(MetricHandle::Counter(c)) => Some(c),
            _ => None,
        }
    }

    /// Retrieve a gauge by short name.
    pub fn gauge(&self, name: &str) -> Option<&Gauge> {
        match self.metrics.get(name) {
            Some(MetricHandle::Gauge(g)) => Some(g),
            _ => None,
        }
    }

    /// Retrieve a histogram by short name.
    pub fn histogram(&self, name: &str) -> Option<&Histogram> {
        match self.metrics.get(name) {
            Some(MetricHandle::Histogram(h)) => Some(h),
            _ => None,
        }
    }

    /// Retrieve a summary by short name.
    pub fn summary(&self, name: &str) -> Option<&Summary> {
        match self.metrics.get(name) {
            Some(MetricHandle::Summary(s)) => Some(s),
            _ => None,
        }
    }

    /// Execute a closure that accumulates multiple metric writes and sends them atomically.
    pub fn multi<F>(&self, f: F)
    where
        F: FnOnce(&mut Batch),
    {
        let mut batch = Batch::new();
        f(&mut batch);
        if !batch.metrics.is_empty() {
            self.writer.write_many(batch.metrics);
        }
    }

    /// Force flush any buffered metrics. No-op in sync mode.
    pub fn flush(&self) {
        self.writer.flush();
    }

    /// Flush and stop the background thread. No-op in sync mode.
    pub fn shutdown(&self) {
        self.writer.shutdown();
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn is_sync(&self) -> bool {
        self.writer.is_sync()
    }
}

fn stripped_key(full_name: &str, prefix: &str) -> String {
    if prefix.is_empty() {
        full_name.to_string()
    } else {
        full_name.strip_prefix(prefix).unwrap_or(full_name).to_string()
    }
}

fn build_handle(spec: &MetricSpec, prefix: &str, writer: &Writer) -> Result<MetricHandle> {
    validate_name(&spec.name)?;

    for label in &spec.labels {
        validate_label(label)?;
    }

    if spec.help.trim().is_empty() {
        return Err(Error::MissingHelp(spec.name.clone()));
    }

    // Apply prefix if the metric name doesn't already carry it.
    let wire_name = apply_prefix(&spec.name, prefix);
    let label_keys: Vec<String> = spec.labels.iter().map(String::clone).collect();
    let inner = Arc::new(CollectorInner::new(wire_name, label_keys, writer.clone()));

    let handle = match spec.metric_type.as_str() {
        "counter" => MetricHandle::Counter(Counter(inner)),
        "gauge" => MetricHandle::Gauge(Gauge(inner)),
        "histogram" => MetricHandle::Histogram(Histogram(inner)),
        "summary" => MetricHandle::Summary(Summary(inner)),
        other => return Err(Error::UnknownType(other.to_string())),
    };

    Ok(handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_specs(specs: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "{}", specs).unwrap();
        f
    }

    fn basic_specs() -> NamedTempFile {
        write_specs(
            r#"[
                {"name":"myapp_requests","type":"counter","help":"Total requests","labels":["method"]},
                {"name":"myapp_memory","type":"gauge","help":"Memory usage"},
                {"name":"myapp_latency","type":"histogram","help":"Latency"},
                {"name":"myapp_response_size","type":"summary","help":"Response size"}
            ]"#,
        )
    }

    #[test]
    fn test_client_builds_metrics() {
        let specs = basic_specs();
        let client = Config::new("/nonexistent.sock", specs.path()).build().unwrap();
        // Without a prefix, keys are the full metric names from the spec file.
        assert!(client.counter("myapp_requests").is_some());
        assert!(client.gauge("myapp_memory").is_some());
        assert!(client.histogram("myapp_latency").is_some());
        assert!(client.summary("myapp_response_size").is_some());
    }

    #[test]
    fn test_client_with_prefix() {
        let specs = basic_specs();
        let client = Config::new("/nonexistent.sock", specs.path())
            .prefix("myapp")
            .unwrap()
            .build()
            .unwrap();
        // Keys are stripped names.
        assert!(client.counter("requests").is_some());
        assert!(client.gauge("memory").is_some());
        assert_eq!(client.prefix(), "myapp_");
    }

    #[test]
    fn test_prefix_applied_to_unprefixed_spec_name() {
        // Spec names without the prefix should have it applied automatically.
        let specs = write_specs(
            r#"[{"name":"requests","type":"counter","help":"Requests"}]"#,
        );
        let client = Config::new("/nonexistent.sock", specs.path())
            .prefix("myapp")
            .unwrap()
            .build()
            .unwrap();
        // Key is stripped name; wire name has the prefix.
        assert!(client.counter("requests").is_some());
        assert_eq!(client.counter("requests").unwrap().name(), "myapp_requests");
    }

    #[test]
    fn test_client_metric_not_found() {
        let specs = basic_specs();
        let client = Config::new("/nonexistent.sock", specs.path()).build().unwrap();
        assert!(client.counter("nonexistent").is_none());
    }

    #[test]
    fn test_unknown_metric_type_fails() {
        let specs = write_specs(r#"[{"name":"my_metric","type":"bogus","help":"oops"}]"#);
        let result = Config::new("/nonexistent.sock", specs.path()).build();
        assert!(matches!(result, Err(Error::UnknownType(_))));
    }

    #[test]
    fn test_missing_help_fails() {
        let specs = write_specs(r#"[{"name":"my_metric","type":"counter","help":"  "}]"#);
        let result = Config::new("/nonexistent.sock", specs.path()).build();
        assert!(matches!(result, Err(Error::MissingHelp(_))));
    }

    #[test]
    fn test_invalid_batch_size_fails() {
        let result = Config::new("/sock", "/specs").batch_size(0);
        assert!(matches!(result, Err(Error::InvalidBatchSize(0))));
    }

    #[test]
    fn test_duplicate_metric_fails() {
        let specs = write_specs(
            r#"[
                {"name":"my_counter","type":"counter","help":"A counter"},
                {"name":"my_counter","type":"counter","help":"Another counter"}
            ]"#,
        );
        let result = Config::new("/nonexistent.sock", specs.path()).build();
        assert!(matches!(result, Err(Error::DuplicateMetric(_))));
    }

    #[test]
    fn test_sync_mode_rejects_batch_size() {
        let specs = write_specs(r#"[{"name":"my_counter","type":"counter","help":"A counter"}]"#);
        let result = Config::new("/nonexistent.sock", specs.path())
            .batch_size(10)
            .unwrap()
            .sync(true)
            .build();
        assert!(matches!(result, Err(Error::SyncModeConflict)));
    }

    #[test]
    fn test_sync_mode_rejects_batch_timeout() {
        let specs = write_specs(r#"[{"name":"my_counter","type":"counter","help":"A counter"}]"#);
        let result = Config::new("/nonexistent.sock", specs.path())
            .batch_timeout(Duration::from_secs(10))
            .unwrap()
            .sync(true)
            .build();
        assert!(matches!(result, Err(Error::SyncModeConflict)));
    }

    #[test]
    fn test_sync_mode_builds_successfully() {
        let specs = write_specs(r#"[{"name":"my_counter","type":"counter","help":"A counter"}]"#);
        let client = Config::new("/nonexistent.sock", specs.path())
            .sync(true)
            .build()
            .unwrap();
        assert!(client.is_sync());
        assert!(client.counter("my_counter").is_some());
    }

    #[test]
    fn test_multi_batch() {
        let specs = write_specs(
            r#"[
                {"name":"batch_counter","type":"counter","help":"A counter"},
                {"name":"batch_gauge","type":"gauge","help":"A gauge"}
            ]"#,
        );
        let client = Config::new("/nonexistent.sock", specs.path()).build().unwrap();
        // multi should not panic even on a dead socket.
        client.multi(|batch| {
            if let Some(c) = client.counter("batch_counter") {
                let _ = batch.counter(c).inc(vec![]);
            }
            if let Some(g) = client.gauge("batch_gauge") {
                let _ = batch.gauge(g).set(10.0, vec![]);
            }
        });
    }
}
