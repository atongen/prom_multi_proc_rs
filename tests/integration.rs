use std::{
    io::Read,
    os::unix::net::UnixListener,
    sync::mpsc,
    thread,
    time::Duration,
};
use tempfile::{tempdir, NamedTempFile};
use std::io::Write;

use prom_multi_proc::{Config, Error};

fn write_specs(specs: &str) -> NamedTempFile {
    let mut f = NamedTempFile::new().unwrap();
    write!(f, "{}", specs).unwrap();
    f
}

fn start_server(listener: UnixListener, tx: mpsc::Sender<String>) {
    thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(mut s) => {
                    let mut buf = String::new();
                    s.read_to_string(&mut buf).unwrap();
                    if !buf.is_empty() {
                        tx.send(buf).ok();
                    }
                }
                Err(_) => break,
            }
        }
    });
}

#[test]
fn test_counter_sends_inc_over_socket() {
    let dir = tempdir().unwrap();
    let socket_path = dir.path().join("test.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let (tx, rx) = mpsc::channel();
    start_server(listener, tx);

    let specs = write_specs(r#"[{"name":"myapp_requests","type":"counter","help":"Requests","labels":["method"]}]"#);
    let client = Config::new(&socket_path, specs.path())
        .prefix("myapp")
        .unwrap()
        .build()
        .unwrap();

    client.counter("requests").unwrap().inc(vec!["GET".into()]).unwrap();

    let received = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&received).unwrap();

    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0]["name"], "myapp_requests");
    assert_eq!(parsed[0]["method"], "inc");
    assert_eq!(parsed[0]["value"], 1.0);
    assert_eq!(parsed[0]["label_values"][0], "GET");
}

#[test]
fn test_gauge_sends_set_over_socket() {
    let dir = tempdir().unwrap();
    let socket_path = dir.path().join("test.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let (tx, rx) = mpsc::channel();
    start_server(listener, tx);

    let specs = write_specs(r#"[{"name":"myapp_memory","type":"gauge","help":"Memory"}]"#);
    let client = Config::new(&socket_path, specs.path())
        .prefix("myapp")
        .unwrap()
        .build()
        .unwrap();

    client.gauge("memory").unwrap().set(512.0, vec![]).unwrap();

    let received = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&received).unwrap();

    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0]["name"], "myapp_memory");
    assert_eq!(parsed[0]["method"], "set");
    assert_eq!(parsed[0]["value"], 512.0);
}

#[test]
fn test_histogram_observe_over_socket() {
    let dir = tempdir().unwrap();
    let socket_path = dir.path().join("test.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let (tx, rx) = mpsc::channel();
    start_server(listener, tx);

    let specs = write_specs(r#"[{"name":"myapp_latency","type":"histogram","help":"Latency"}]"#);
    let client = Config::new(&socket_path, specs.path())
        .prefix("myapp")
        .unwrap()
        .build()
        .unwrap();

    client.histogram("latency").unwrap().observe(0.123, vec![]).unwrap();

    let received = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&received).unwrap();

    assert_eq!(parsed[0]["name"], "myapp_latency");
    assert_eq!(parsed[0]["method"], "observe");
    assert!((parsed[0]["value"].as_f64().unwrap() - 0.123).abs() < 1e-9);
}

#[test]
fn test_summary_observe_over_socket() {
    let dir = tempdir().unwrap();
    let socket_path = dir.path().join("test.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let (tx, rx) = mpsc::channel();
    start_server(listener, tx);

    let specs = write_specs(r#"[{"name":"myapp_response_size","type":"summary","help":"Response size"}]"#);
    let client = Config::new(&socket_path, specs.path())
        .prefix("myapp")
        .unwrap()
        .build()
        .unwrap();

    client.summary("response_size").unwrap().observe(1024.0, vec![]).unwrap();

    let received = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&received).unwrap();

    assert_eq!(parsed[0]["name"], "myapp_response_size");
    assert_eq!(parsed[0]["method"], "observe");
}

#[test]
fn test_multi_sends_batch_atomically() {
    let dir = tempdir().unwrap();
    let socket_path = dir.path().join("test.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let (tx, rx) = mpsc::channel();
    start_server(listener, tx);

    let specs = write_specs(
        r#"[
            {"name":"myapp_counter","type":"counter","help":"A counter"},
            {"name":"myapp_gauge","type":"gauge","help":"A gauge"}
        ]"#,
    );
    let client = Config::new(&socket_path, specs.path())
        .prefix("myapp")
        .unwrap()
        .batch_size(100)
        .unwrap()
        .build()
        .unwrap();

    client.multi(|batch| {
        if let Some(c) = client.counter("counter") {
            let _ = batch.counter(c).inc(vec![]);
        }
        if let Some(g) = client.gauge("gauge") {
            let _ = batch.gauge(g).set(99.0, vec![]);
        }
    });
    client.flush();

    let received = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&received).unwrap();

    assert_eq!(parsed.len(), 2);
    let names: Vec<&str> = parsed.iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"myapp_counter"));
    assert!(names.contains(&"myapp_gauge"));
}

#[test]
fn test_batching_accumulates_until_batch_size() {
    let dir = tempdir().unwrap();
    let socket_path = dir.path().join("test.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let (tx, rx) = mpsc::channel();
    start_server(listener, tx);

    let specs = write_specs(r#"[{"name":"myapp_events","type":"counter","help":"Events"}]"#);
    let client = Config::new(&socket_path, specs.path())
        .prefix("myapp")
        .unwrap()
        .batch_size(3)
        .unwrap()
        .build()
        .unwrap();

    let counter = client.counter("events").unwrap();
    counter.inc(vec![]).unwrap(); // 1
    counter.inc(vec![]).unwrap(); // 2
    // Should not have flushed yet.
    assert!(rx.try_recv().is_err());
    counter.inc(vec![]).unwrap(); // 3 → triggers flush

    let received = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&received).unwrap();
    assert_eq!(parsed.len(), 3);
}

#[test]
fn test_shutdown_flushes_partial_batch() {
    let dir = tempdir().unwrap();
    let socket_path = dir.path().join("test.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let (tx, rx) = mpsc::channel();
    start_server(listener, tx);

    let specs = write_specs(r#"[{"name":"myapp_events","type":"counter","help":"Events"}]"#);
    let client = Config::new(&socket_path, specs.path())
        .prefix("myapp")
        .unwrap()
        .batch_size(100)
        .unwrap()
        .build()
        .unwrap();

    client.counter("events").unwrap().inc(vec![]).unwrap();
    client.shutdown();

    let received = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&received).unwrap();
    assert_eq!(parsed.len(), 1);
}

#[test]
fn test_dead_socket_does_not_panic() {
    let specs = write_specs(r#"[{"name":"myapp_counter","type":"counter","help":"Counter"}]"#);
    let client = Config::new("/nonexistent/path/test.sock", specs.path())
        .prefix("myapp")
        .unwrap()
        .build()
        .unwrap();
    // Must not panic.
    client.counter("counter").unwrap().inc(vec![]).unwrap();
    client.flush();
}

#[test]
fn test_prefix_strips_from_key_but_sends_full_name() {
    let dir = tempdir().unwrap();
    let socket_path = dir.path().join("test.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let (tx, rx) = mpsc::channel();
    start_server(listener, tx);

    let specs = write_specs(r#"[{"name":"myapp_requests","type":"counter","help":"Requests"}]"#);
    let client = Config::new(&socket_path, specs.path())
        .prefix("myapp")
        .unwrap()
        .build()
        .unwrap();

    // Access by stripped key.
    client.counter("requests").unwrap().inc(vec![]).unwrap();

    let received = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&received).unwrap();
    // Wire format contains the full name.
    assert_eq!(parsed[0]["name"], "myapp_requests");
}

#[test]
fn test_no_prefix_uses_full_name_as_key() {
    let specs = write_specs(r#"[{"name":"myapp_requests","type":"counter","help":"Requests"}]"#);
    let client = Config::new("/nonexistent.sock", specs.path()).build().unwrap();
    // Without prefix the full spec name is the lookup key.
    assert!(client.counter("myapp_requests").is_some());
    assert!(client.counter("requests").is_none());
}

#[test]
fn test_wrong_metric_type_returns_none() {
    let specs = write_specs(r#"[{"name":"myapp_requests","type":"counter","help":"Requests"}]"#);
    let client = Config::new("/nonexistent.sock", specs.path())
        .prefix("myapp")
        .unwrap()
        .build()
        .unwrap();
    // Asking for a gauge on a counter metric returns None.
    assert!(client.gauge("requests").is_none());
    assert!(client.counter("requests").is_some());
}

#[test]
fn test_metric_names() {
    let specs = write_specs(
        r#"[
            {"name":"myapp_requests","type":"counter","help":"Requests"},
            {"name":"myapp_memory","type":"gauge","help":"Memory"}
        ]"#,
    );
    let client = Config::new("/nonexistent.sock", specs.path())
        .prefix("myapp")
        .unwrap()
        .build()
        .unwrap();

    let mut names: Vec<&str> = client.metric_names().collect();
    names.sort();
    assert_eq!(names, vec!["memory", "requests"]);
}

#[test]
fn test_label_count_mismatch_returns_error() {
    let specs = write_specs(
        r#"[{"name":"myapp_requests","type":"counter","help":"Requests","labels":["method","status"]}]"#,
    );
    let client = Config::new("/nonexistent.sock", specs.path())
        .prefix("myapp")
        .unwrap()
        .build()
        .unwrap();
    let counter = client.counter("requests").unwrap();
    // Too few labels.
    assert!(matches!(counter.inc(vec!["GET".into()]), Err(Error::LabelCountMismatch { .. })));
    // Correct count.
    assert!(counter.inc(vec!["GET".into(), "200".into()]).is_ok());
}

#[test]
fn test_prefix_applied_to_unprefixed_spec_name() {
    let dir = tempdir().unwrap();
    let socket_path = dir.path().join("test.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let (tx, rx) = mpsc::channel();
    start_server(listener, tx);

    // Spec name has no prefix — client should apply it automatically.
    let specs = write_specs(r#"[{"name":"requests","type":"counter","help":"Requests"}]"#);
    let client = Config::new(&socket_path, specs.path())
        .prefix("myapp")
        .unwrap()
        .build()
        .unwrap();

    client.counter("requests").unwrap().inc(vec![]).unwrap();

    let received = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&received).unwrap();
    // Wire format has the fully-prefixed name.
    assert_eq!(parsed[0]["name"], "myapp_requests");
}

#[test]
fn test_sync_mode_writes_immediately() {
    let dir = tempdir().unwrap();
    let socket_path = dir.path().join("test.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let (tx, rx) = mpsc::channel();
    start_server(listener, tx);

    let specs = write_specs(r#"[{"name":"myapp_events","type":"counter","help":"Events"}]"#);
    let client = Config::new(&socket_path, specs.path())
        .prefix("myapp")
        .unwrap()
        .sync(true)
        .build()
        .unwrap();

    assert!(client.is_sync());
    client.counter("events").unwrap().inc(vec![]).unwrap();

    let received = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&received).unwrap();
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0]["name"], "myapp_events");
    assert_eq!(parsed[0]["method"], "inc");
}

#[test]
fn test_sync_mode_each_write_is_separate_socket_call() {
    let dir = tempdir().unwrap();
    let socket_path = dir.path().join("test.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let (tx, rx) = mpsc::channel();
    start_server(listener, tx);

    let specs = write_specs(r#"[{"name":"myapp_events","type":"counter","help":"Events"}]"#);
    let client = Config::new(&socket_path, specs.path())
        .prefix("myapp")
        .unwrap()
        .sync(true)
        .build()
        .unwrap();

    let counter = client.counter("events").unwrap();
    counter.inc(vec![]).unwrap();
    counter.inc(vec![]).unwrap();

    // Both should arrive as separate socket connections.
    let r1 = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let r2 = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let p1: Vec<serde_json::Value> = serde_json::from_str(&r1).unwrap();
    let p2: Vec<serde_json::Value> = serde_json::from_str(&r2).unwrap();
    assert_eq!(p1.len(), 1);
    assert_eq!(p2.len(), 1);
}

#[test]
fn test_sync_mode_rejects_batch_size() {
    let specs = write_specs(r#"[{"name":"myapp_events","type":"counter","help":"Events"}]"#);
    let result = Config::new("/nonexistent.sock", specs.path())
        .batch_size(10)
        .unwrap()
        .sync(true)
        .build();
    assert!(matches!(result, Err(prom_multi_proc::Error::SyncModeConflict)));
}

#[test]
fn test_sync_mode_dead_socket_does_not_panic() {
    let specs = write_specs(r#"[{"name":"myapp_events","type":"counter","help":"Events"}]"#);
    let client = Config::new("/nonexistent/path.sock", specs.path())
        .prefix("myapp")
        .unwrap()
        .sync(true)
        .build()
        .unwrap();
    client.counter("events").unwrap().inc(vec![]).unwrap();
}

#[test]
fn test_gauge_all_methods_over_socket() {
    let dir = tempdir().unwrap();
    let socket_path = dir.path().join("test.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let (tx, rx) = mpsc::channel();
    start_server(listener, tx);

    let specs = write_specs(r#"[{"name":"myapp_workers","type":"gauge","help":"Workers"}]"#);
    let client = Config::new(&socket_path, specs.path())
        .prefix("myapp")
        .unwrap()
        .batch_size(6)
        .unwrap()
        .build()
        .unwrap();

    let g = client.gauge("workers").unwrap();
    g.set(10.0, vec![]).unwrap();
    g.inc(vec![]).unwrap();
    g.dec(vec![]).unwrap();
    g.add(2.0, vec![]).unwrap();
    g.sub(1.0, vec![]).unwrap();
    g.set_to_current_time(vec![]).unwrap(); // 6th → triggers flush

    let received = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&received).unwrap();
    assert_eq!(parsed.len(), 6);

    let methods: Vec<&str> = parsed.iter().map(|v| v["method"].as_str().unwrap()).collect();
    assert_eq!(methods, vec!["set", "inc", "dec", "add", "sub", "set_to_current_time"]);
}
