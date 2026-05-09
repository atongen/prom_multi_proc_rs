use crate::{
    error::{Error, Result},
    metric::Metric,
    writer::Writer,
};

/// Shared state for a single metric collector (counter/gauge/histogram/summary).
#[derive(Clone)]
pub struct CollectorInner {
    pub(crate) name: String,
    pub(crate) label_keys: Vec<String>,
    pub(crate) writer: Writer,
}

impl CollectorInner {
    pub fn new(name: impl Into<String>, label_keys: Vec<String>, writer: Writer) -> Self {
        Self { name: name.into(), label_keys, writer }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn label_keys(&self) -> &[String] {
        &self.label_keys
    }

    pub fn write(&self, method: &str, value: f64, label_values: Vec<String>) -> Result<()> {
        if label_values.len() != self.label_keys.len() {
            return Err(Error::LabelCountMismatch {
                expected: self.label_keys.len(),
                got: label_values.len(),
            });
        }
        self.writer.write(Metric::new(&self.name, method, value, label_values));
        Ok(())
    }

    pub fn build_metric(&self, method: &str, value: f64, label_values: Vec<String>) -> Result<Metric> {
        if label_values.len() != self.label_keys.len() {
            return Err(Error::LabelCountMismatch {
                expected: self.label_keys.len(),
                got: label_values.len(),
            });
        }
        Ok(Metric::new(&self.name, method, value, label_values))
    }
}
