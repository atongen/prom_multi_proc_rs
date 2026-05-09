use serde::Deserialize;
use std::path::Path;

use crate::error::{Error, Result};

#[derive(Debug, Clone, Deserialize)]
pub struct MetricSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub metric_type: String,
    pub help: String,
    #[serde(default)]
    pub labels: Vec<String>,
}

pub fn load_specs(path: impl AsRef<Path>) -> Result<Vec<MetricSpec>> {
    let path = path.as_ref();
    if !path.is_file() {
        return Err(Error::SpecFileNotFound(path.display().to_string()));
    }
    let content = std::fs::read_to_string(path).map_err(Error::Io)?;
    serde_json::from_str(&content)
        .map_err(|e| Error::SpecFileInvalidJson(format!("{}: {}", path.display(), e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_load_valid_specs() {
        let mut f = NamedTempFile::new().unwrap();
        write!(
            f,
            r#"[
                {{"name":"myapp_requests","type":"counter","help":"Total requests","labels":["method","status"]}},
                {{"name":"myapp_latency","type":"histogram","help":"Request latency"}}
            ]"#
        )
        .unwrap();
        let specs = load_specs(f.path()).unwrap();
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].name, "myapp_requests");
        assert_eq!(specs[0].metric_type, "counter");
        assert_eq!(specs[0].labels, vec!["method", "status"]);
        assert_eq!(specs[1].labels, Vec::<String>::new());
    }

    #[test]
    fn test_load_missing_file() {
        let result = load_specs("/nonexistent/path/metrics.json");
        assert!(matches!(result, Err(Error::SpecFileNotFound(_))));
    }

    #[test]
    fn test_load_invalid_json() {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "not json").unwrap();
        let result = load_specs(f.path());
        assert!(matches!(result, Err(Error::SpecFileInvalidJson(_))));
    }
}
