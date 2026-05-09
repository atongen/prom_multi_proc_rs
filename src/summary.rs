use std::sync::Arc;

use crate::{collector::CollectorInner, error::Result, metric::Metric};

/// A Prometheus summary metric.
#[derive(Clone)]
pub struct Summary(pub(crate) Arc<CollectorInner>);

impl Summary {
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

/// Batch handle for a summary.
pub struct BatchSummary<'a>(pub(crate) &'a Summary, pub(crate) &'a mut Vec<Metric>);

impl<'a> BatchSummary<'a> {
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

    fn make_summary(label_keys: Vec<&str>) -> Summary {
        let writer = Writer::new("/nonexistent.sock", 100, Duration::from_secs(5));
        let inner = CollectorInner::new("my_summary", label_keys.into_iter().map(String::from).collect(), writer);
        Summary(Arc::new(inner))
    }

    #[test]
    fn test_observe_no_labels() {
        let s = make_summary(vec![]);
        assert!(s.observe(0.5, vec![]).is_ok());
    }

    #[test]
    fn test_observe_with_labels() {
        let s = make_summary(vec!["env"]);
        assert!(s.observe(1.5, vec!["production".into()]).is_ok());
    }

    #[test]
    fn test_label_mismatch() {
        let s = make_summary(vec!["env"]);
        assert!(s.observe(0.5, vec![]).is_err());
    }
}
