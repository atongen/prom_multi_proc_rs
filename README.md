# prom_multi_proc

Rust client library for collecting Prometheus metrics in multi-process applications.
Designed for forking servers and multi-worker daemons. Writes metrics as JSON to a Unix
socket listened to by [prom_multi_proc](https://github.com/atongen/prom_multi_proc).

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
prom_multi_proc = "0.1.0"
```

## General Usage

### Define metrics

Create a JSON file to define the Prometheus metrics your application will track:

```json
[
    {
        "type": "counter",
        "name": "app_requests_total",
        "help": "Total HTTP requests",
        "labels": ["method", "status"]
    },
    {
        "type": "gauge",
        "name": "app_workers_active",
        "help": "Number of active workers"
    },
    {
        "type": "histogram",
        "name": "app_request_duration_seconds",
        "help": "Request duration in seconds",
        "labels": ["method"]
    },
    {
        "type": "summary",
        "name": "app_response_size_bytes",
        "help": "Response size in bytes",
        "labels": ["method"]
    }
]
```

This file is shared by both the aggregator process and the Rust client. Metric names
in the JSON file may or may not include the prefix — the client applies it automatically
if it is missing.

### Install and start the aggregator process

Download, install, and start [prom_multi_proc](https://github.com/atongen/prom_multi_proc)
using the metrics JSON definition file. Note the socket path.

The Rust client functions normally if no aggregator is listening on the socket — failed
writes are logged and silently dropped so the host application is never interrupted.

### Collect metrics

Build a `Client` with `Config` and begin recording metrics:

```rust
use prom_multi_proc::Config;
use std::time::Duration;

let client = Config::new("/var/run/prom_multi_proc/metrics.sock", "/etc/myapp/metrics.json")
    .prefix("app").unwrap()
    .batch_size(10).unwrap()
    .batch_timeout(Duration::from_secs(5)).unwrap()
    .validate(true)
    .build()
    .expect("failed to build client");

// Metrics are accessed by their short name (prefix stripped).
client.counter("requests_total").unwrap()
    .inc(vec!["GET".into(), "200".into()]).unwrap();

client.histogram("request_duration_seconds").unwrap()
    .observe(0.042, vec!["GET".into()]).unwrap();
```

### Configuration options

| Option | Default | Description |
|---|---|---|
| `prefix(p)` | `""` | Metric name prefix. A trailing `_` is appended automatically if missing. Applied to spec names that don't already carry it. |
| `batch_size(n)` | `1` | Flush after accumulating this many messages. Mutually exclusive with `sync`. |
| `batch_timeout(d)` | `3s` | Force-flush on this interval even if the batch is not full. Mutually exclusive with `sync`. |
| `validate(v)` | `false` | Return `Err` on label count mismatches instead of silently dropping |
| `sync(s)` | `false` | Write each metric directly to the socket with no buffering or background thread |

### Prefix behavior

The `prefix` option works the same way as the `-metric-prefix` flag on the aggregator server:
if a metric name in the JSON spec already starts with the prefix, it is used as-is; otherwise
the prefix is prepended. A trailing `_` separator is added automatically if missing.

```rust
// Both of these result in wire name "app_requests_total":
// spec: {"name": "app_requests_total", ...}  → already has prefix, used as-is
// spec: {"name": "requests_total", ...}       → prefix applied → "app_requests_total"

// Metrics are always accessed by the short (stripped) name:
client.counter("requests_total").unwrap().inc(vec![]).unwrap();
```

### Sync mode

Sync mode bypasses the batch buffer and background flush thread, writing directly to the
socket on every call. Useful for scripts, tests, or single-process applications where
background threads are undesirable.

```rust
let client = Config::new("/var/run/prom_multi_proc/metrics.sock", "/etc/myapp/metrics.json")
    .prefix("app").unwrap()
    .sync(true)
    .build()
    .expect("failed to build client");
```

`sync(true)` is mutually exclusive with `batch_size` and `batch_timeout`.

### Multi / batch writes

Use `Client::multi` to record multiple metrics in a single atomic socket write:

```rust
client.multi(|batch| {
    if let Some(c) = client.counter("requests_total") {
        let _ = batch.counter(c).inc(vec!["POST".into(), "201".into()]);
    }
    if let Some(g) = client.gauge("workers_active") {
        let _ = batch.gauge(g).set(8.0, vec![]);
    }
    if let Some(h) = client.histogram("request_duration_seconds") {
        let _ = batch.histogram(h).observe(0.015, vec!["POST".into()]);
    }
});
```

### Shutdown

Call `shutdown()` to flush any remaining buffered metrics and stop the background thread
before the process exits. No-op in sync mode.

```rust
client.shutdown();
```

## Wire format

Each flush sends a JSON array over the Unix socket and then closes the connection:

```json
[
  {"name":"app_requests_total","method":"inc","value":1.0,"label_values":["GET","200"]},
  {"name":"app_workers_active","method":"set","value":8.0,"label_values":[]}
]
```

## Metric types and methods

| Type | Methods |
|---|---|
| `counter` | `inc(labels)`, `add(value, labels)` |
| `gauge` | `set(value, labels)`, `inc(labels)`, `dec(labels)`, `add(value, labels)`, `sub(value, labels)`, `set_to_current_time(labels)` |
| `histogram` | `observe(value, labels)` |
| `summary` | `observe(value, labels)` |

All methods return `Result<(), Error>`. Label count mismatches return `Err(Error::LabelCountMismatch)`
regardless of the `validate` setting. Socket errors are always logged and dropped — they never
propagate to the caller.

## Error safety

The client is designed to never panic or propagate socket errors into the host application:

- Socket connection failures are logged via the `log` crate and the metric is dropped
- Serialization errors (should never occur in practice) are logged and the batch is dropped
- Label count mismatches return `Err` and are the only errors callers need to handle

## License

Available as open source under the [MIT License](http://opensource.org/licenses/MIT).
