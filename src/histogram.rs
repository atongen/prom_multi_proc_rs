use std::sync::Arc;

use crate::{collector::CollectorInner, error::Result, metric::Metric};

/// A Prometheus histogram metric.
#[derive(Clone)]
pub struct Histogram(pub(crate) Arc<CollectorInner>);

impl Histogram {
    pub fn name(&self) -> &str {
        self.0.name()
    }

    pub fn label_keys(&self) -> &[String] {
        self.0.label_keys()
    }

    pub fn observe(&self, value: f64, label_values: Vec<String>) -> Result<()> {
        self.0.write("observe", value, label_values)
    }

    pub(crate) fn build_metric(&self, value: f64, label_values: Vec<String>) -> Result<Metric> {
        self.0.build_metric("observe", value, label_values)
    }
}

/// Batch handle for a histogram.
pub struct BatchHistogram<'a>(pub(crate) &'a Histogram, pub(crate) &'a mut Vec<Metric>);

impl<'a> BatchHistogram<'a> {
    pub fn observe(&mut self, value: f64, label_values: Vec<String>) -> Result<()> {
        let m = self.0.build_metric(value, label_values)?;
        self.1.push(m);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{collector::CollectorInner, writer::Writer};
    use std::time::Duration;

    fn make_histogram(label_keys: Vec<&str>) -> Histogram {
        let writer = Writer::new("/nonexistent.sock", 100, Duration::from_secs(5));
        let inner = CollectorInner::new("my_histogram", label_keys.into_iter().map(String::from).collect(), writer);
        Histogram(Arc::new(inner))
    }

    #[test]
    fn test_observe_no_labels() {
        let h = make_histogram(vec![]);
        assert!(h.observe(0.5, vec![]).is_ok());
    }

    #[test]
    fn test_observe_with_labels() {
        let h = make_histogram(vec!["path"]);
        assert!(h.observe(0.123, vec!["/api".into()]).is_ok());
    }

    #[test]
    fn test_label_mismatch() {
        let h = make_histogram(vec!["path"]);
        assert!(h.observe(0.5, vec![]).is_err());
    }
}
