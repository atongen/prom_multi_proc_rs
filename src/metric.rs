use serde::{Deserialize, Serialize};

/// Wire format for a single metric observation sent over the Unix socket.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metric {
    pub name: String,
    pub method: String,
    pub value: f64,
    pub label_values: Vec<String>,
}

impl Metric {
    pub fn new(name: impl Into<String>, method: impl Into<String>, value: f64, label_values: Vec<String>) -> Self {
        Self {
            name: name.into(),
            method: method.into(),
            value,
            label_values,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_metric() {
        let m = Metric::new("my_counter", "inc", 1.0, vec!["v1".into(), "v2".into()]);
        let json = serde_json::to_string(&m).unwrap();
        assert_eq!(
            json,
            r#"{"name":"my_counter","method":"inc","value":1.0,"label_values":["v1","v2"]}"#
        );
    }

    #[test]
    fn test_deserialize_metric() {
        let json = r#"{"name":"my_counter","method":"inc","value":1.0,"label_values":["v1"]}"#;
        let m: Metric = serde_json::from_str(json).unwrap();
        assert_eq!(m.name, "my_counter");
        assert_eq!(m.method, "inc");
        assert_eq!(m.value, 1.0);
        assert_eq!(m.label_values, vec!["v1"]);
    }

    #[test]
    fn test_serialize_batch() {
        let metrics = vec![
            Metric::new("my_counter", "inc", 1.0, vec![]),
            Metric::new("my_gauge", "set", 42.0, vec!["label_val".into()]),
        ];
        let json = serde_json::to_string(&metrics).unwrap();
        assert!(json.starts_with('['));
        assert!(json.ends_with(']'));
        assert!(json.contains("my_counter"));
        assert!(json.contains("my_gauge"));
    }
}
