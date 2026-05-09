use prom_multi_proc::Config;
use std::time::Duration;

fn main() {
    // Point this at a running prom_multi_proc daemon socket and a metrics JSON spec file.
    let client = Config::new("/tmp/prom_multi_proc.sock", "/tmp/metrics.json")
        .batch_size(10)
        .unwrap()
        .batch_timeout(Duration::from_secs(5))
        .unwrap()
        .prefix("myapp")
        .unwrap()
        .build()
        .expect("failed to build client");

    if let Some(counter) = client.counter("requests") {
        let _ = counter.inc(vec!["GET".into(), "200".into()]);
    }

    if let Some(gauge) = client.gauge("memory_bytes") {
        let _ = gauge.set(256.0 * 1024.0 * 1024.0, vec![]);
    }

    // Send multiple metrics atomically.
    client.multi(|batch| {
        if let Some(c) = client.counter("requests") {
            let _ = batch.counter(c).add(5.0, vec!["POST".into(), "201".into()]);
        }
        if let Some(h) = client.histogram("request_duration_seconds") {
            let _ = batch.histogram(h).observe(0.042, vec![]);
        }
    });

    client.shutdown();
}
