use std::sync::Arc;

use crate::{collector::CollectorInner, error::Result, metric::Metric};

/// A Prometheus counter metric. Counters only go up.
#[derive(Clone)]
pub struct Counter(pub(crate) Arc<CollectorInner>);

impl Counter {
    pub fn name(&self) -> &str {
        self.0.name()
    }

    pub fn label_keys(&self) -> &[String] {
        self.0.label_keys()
    }

    /// Increment by 1.
    pub fn inc(&self, label_values: Vec<String>) -> Result<()> {
        self.0.write("inc", 1.0, label_values)
    }

    /// Add an arbitrary non-negative value.
    pub fn add(&self, value: f64, label_values: Vec<String>) -> Result<()> {
        self.0.write("add", value, label_values)
    }

    pub(crate) fn build_metric(&self, method: &str, value: f64, label_values: Vec<String>) -> Result<Metric> {
        self.0.build_metric(method, value, label_values)
    }
}

/// Batch handle for a counter (collects metrics without writing to socket).
pub struct BatchCounter<'a>(pub(crate) &'a Counter, pub(crate) &'a mut Vec<Metric>);

impl<'a> BatchCounter<'a> {
    pub fn inc(&mut self, label_values: Vec<String>) -> Result<()> {
        let m = self.0.build_metric("inc", 1.0, label_values)?;
        self.1.push(m);
        Ok(())
    }

    pub fn add(&mut self, value: f64, label_values: Vec<String>) -> Result<()> {
        let m = self.0.build_metric("add", value, label_values)?;
        self.1.push(m);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{collector::CollectorInner, writer::Writer};
    use std::time::Duration;

    fn make_counter(label_keys: Vec<&str>) -> Counter {
        let writer = Writer::new("/nonexistent.sock", 100, Duration::from_secs(5));
        let inner = CollectorInner::new("my_counter", label_keys.into_iter().map(String::from).collect(), writer);
        Counter(Arc::new(inner))
    }

    #[test]
    fn test_inc_no_labels() {
        let c = make_counter(vec![]);
        assert!(c.inc(vec![]).is_ok());
    }

    #[test]
    fn test_inc_with_labels() {
        let c = make_counter(vec!["method", "status"]);
        assert!(c.inc(vec!["GET".into(), "200".into()]).is_ok());
    }

    #[test]
    fn test_label_mismatch_returns_error() {
        let c = make_counter(vec!["method"]);
        assert!(c.inc(vec![]).is_err());
        assert!(c.inc(vec!["a".into(), "b".into()]).is_err());
    }

    #[test]
    fn test_add() {
        let c = make_counter(vec![]);
        assert!(c.add(5.0, vec![]).is_ok());
    }
}
