use crate::{
    counter::{BatchCounter, Counter},
    error::Result,
    gauge::{BatchGauge, Gauge},
    histogram::{BatchHistogram, Histogram},
    metric::Metric,
    summary::{BatchSummary, Summary},
};

/// Collects metrics from multiple operations without sending them to the socket.
/// All accumulated metrics are sent in a single atomic flush when the `Batch` is consumed.
pub struct Batch {
    pub(crate) metrics: Vec<Metric>,
}

impl Batch {
    pub(crate) fn new() -> Self {
        Batch { metrics: Vec::new() }
    }

    /// Access a counter within the batch context.
    pub fn counter<'a>(&'a mut self, counter: &'a Counter) -> BatchCounter<'a> {
        BatchCounter(counter, &mut self.metrics)
    }

    /// Access a gauge within the batch context.
    pub fn gauge<'a>(&'a mut self, gauge: &'a Gauge) -> BatchGauge<'a> {
        BatchGauge(gauge, &mut self.metrics)
    }

    /// Access a histogram within the batch context.
    pub fn histogram<'a>(&'a mut self, histogram: &'a Histogram) -> BatchHistogram<'a> {
        BatchHistogram(histogram, &mut self.metrics)
    }

    /// Access a summary within the batch context.
    pub fn summary<'a>(&'a mut self, summary: &'a Summary) -> BatchSummary<'a> {
        BatchSummary(summary, &mut self.metrics)
    }

    /// Push a raw pre-built metric.
    pub fn push(&mut self, metric: Metric) {
        self.metrics.push(metric);
    }
}

/// Convenience: batch operation that returns an error if any metric call fails.
/// All metrics are still accumulated up to the point of failure.
pub fn batch_result(f: impl FnOnce(&mut Batch) -> Result<()>) -> (Vec<Metric>, Result<()>) {
    let mut b = Batch::new();
    let result = f(&mut b);
    (b.metrics, result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{collector::CollectorInner, writer::Writer};
    use std::{sync::Arc, time::Duration};

    fn make_counter(name: &str, label_keys: Vec<&str>) -> Counter {
        let writer = Writer::new("/nonexistent.sock", 100, Duration::from_secs(5));
        let inner = CollectorInner::new(name, label_keys.into_iter().map(String::from).collect(), writer);
        Counter(Arc::new(inner))
    }

    fn make_gauge(name: &str, label_keys: Vec<&str>) -> Gauge {
        let writer = Writer::new("/nonexistent.sock", 100, Duration::from_secs(5));
        let inner = CollectorInner::new(name, label_keys.into_iter().map(String::from).collect(), writer);
        Gauge(Arc::new(inner))
    }

    #[test]
    fn test_batch_accumulates_metrics() {
        let counter = make_counter("my_counter", vec![]);
        let gauge = make_gauge("my_gauge", vec![]);

        let mut batch = Batch::new();
        batch.counter(&counter).inc(vec![]).unwrap();
        batch.gauge(&gauge).set(42.0, vec![]).unwrap();

        assert_eq!(batch.metrics.len(), 2);
        assert_eq!(batch.metrics[0].name, "my_counter");
        assert_eq!(batch.metrics[0].method, "inc");
        assert_eq!(batch.metrics[1].name, "my_gauge");
        assert_eq!(batch.metrics[1].method, "set");
        assert_eq!(batch.metrics[1].value, 42.0);
    }

    #[test]
    fn test_batch_label_mismatch_returns_error() {
        let counter = make_counter("my_counter", vec!["env"]);
        let mut batch = Batch::new();
        let result = batch.counter(&counter).inc(vec![]); // missing label
        assert!(result.is_err());
        // Batch itself is empty because we got an error before pushing.
        assert!(batch.metrics.is_empty());
    }
}
