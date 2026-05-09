use std::sync::Arc;

use crate::{collector::CollectorInner, error::Result, metric::Metric};

/// A Prometheus gauge metric. Can go up and down.
#[derive(Clone)]
pub struct Gauge(pub(crate) Arc<CollectorInner>);

impl Gauge {
    pub fn name(&self) -> &str {
        self.0.name()
    }

    pub fn label_keys(&self) -> &[String] {
        self.0.label_keys()
    }

    pub fn set(&self, value: f64, label_values: Vec<String>) -> Result<()> {
        self.0.write("set", value, label_values)
    }

    pub fn inc(&self, label_values: Vec<String>) -> Result<()> {
        self.0.write("inc", 1.0, label_values)
    }

    pub fn dec(&self, label_values: Vec<String>) -> Result<()> {
        self.0.write("dec", 1.0, label_values)
    }

    pub fn add(&self, value: f64, label_values: Vec<String>) -> Result<()> {
        self.0.write("add", value, label_values)
    }

    pub fn sub(&self, value: f64, label_values: Vec<String>) -> Result<()> {
        self.0.write("sub", value, label_values)
    }

    /// Set gauge to the current Unix timestamp (seconds since epoch).
    pub fn set_to_current_time(&self, label_values: Vec<String>) -> Result<()> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        self.0.write("set_to_current_time", secs, label_values)
    }

    pub(crate) fn build_metric(&self, method: &str, value: f64, label_values: Vec<String>) -> Result<Metric> {
        self.0.build_metric(method, value, label_values)
    }
}

/// Batch handle for a gauge.
pub struct BatchGauge<'a>(pub(crate) &'a Gauge, pub(crate) &'a mut Vec<Metric>);

impl<'a> BatchGauge<'a> {
    pub fn set(&mut self, value: f64, label_values: Vec<String>) -> Result<()> {
        let m = self.0.build_metric("set", value, label_values)?;
        self.1.push(m);
        Ok(())
    }

    pub fn inc(&mut self, label_values: Vec<String>) -> Result<()> {
        let m = self.0.build_metric("inc", 1.0, label_values)?;
        self.1.push(m);
        Ok(())
    }

    pub fn dec(&mut self, label_values: Vec<String>) -> Result<()> {
        let m = self.0.build_metric("dec", 1.0, label_values)?;
        self.1.push(m);
        Ok(())
    }

    pub fn add(&mut self, value: f64, label_values: Vec<String>) -> Result<()> {
        let m = self.0.build_metric("add", value, label_values)?;
        self.1.push(m);
        Ok(())
    }

    pub fn sub(&mut self, value: f64, label_values: Vec<String>) -> Result<()> {
        let m = self.0.build_metric("sub", value, label_values)?;
        self.1.push(m);
        Ok(())
    }

    pub fn set_to_current_time(&mut self, label_values: Vec<String>) -> Result<()> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        let m = self.0.build_metric("set_to_current_time", secs, label_values)?;
        self.1.push(m);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{collector::CollectorInner, writer::Writer};
    use std::time::Duration;

    fn make_gauge(label_keys: Vec<&str>) -> Gauge {
        let writer = Writer::new("/nonexistent.sock", 100, Duration::from_secs(5));
        let inner = CollectorInner::new("my_gauge", label_keys.into_iter().map(String::from).collect(), writer);
        Gauge(Arc::new(inner))
    }

    #[test]
    fn test_all_methods_no_labels() {
        let g = make_gauge(vec![]);
        assert!(g.set(42.0, vec![]).is_ok());
        assert!(g.inc(vec![]).is_ok());
        assert!(g.dec(vec![]).is_ok());
        assert!(g.add(5.0, vec![]).is_ok());
        assert!(g.sub(3.0, vec![]).is_ok());
        assert!(g.set_to_current_time(vec![]).is_ok());
    }

    #[test]
    fn test_label_mismatch() {
        let g = make_gauge(vec!["env"]);
        assert!(g.set(1.0, vec![]).is_err());
    }
}
