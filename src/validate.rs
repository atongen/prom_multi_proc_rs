use crate::error::{Error, Result};

/// Valid metric/label names: start with lowercase letter, rest are lowercase alphanumeric or underscore.
pub fn validate_name(name: &str) -> Result<()> {
    let bytes = name.as_bytes();
    if bytes.is_empty() || !bytes[0].is_ascii_lowercase() {
        return Err(Error::InvalidName(name.to_string()));
    }
    if !bytes[1..].iter().all(|&b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_') {
        return Err(Error::InvalidName(name.to_string()));
    }
    Ok(())
}

pub fn validate_label(label: &str) -> Result<()> {
    let bytes = label.as_bytes();
    if bytes.is_empty() || !bytes[0].is_ascii_lowercase() {
        return Err(Error::InvalidLabel(label.to_string()));
    }
    if !bytes[1..].iter().all(|&b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_') {
        return Err(Error::InvalidLabel(label.to_string()));
    }
    Ok(())
}

/// Normalize prefix: append `_` if non-empty and not already ending with `_`.
pub fn normalize_prefix(prefix: &str) -> String {
    if prefix.is_empty() || prefix.ends_with('_') {
        prefix.to_string()
    } else {
        format!("{prefix}_")
    }
}

/// Validate prefix (without trailing `_`).
pub fn validate_prefix(prefix: &str) -> Result<()> {
    if prefix.is_empty() {
        return Ok(());
    }
    let trimmed = prefix.trim_end_matches('_');
    if trimmed.is_empty() {
        return Err(Error::InvalidPrefix(prefix.to_string()));
    }
    validate_name(trimmed).map_err(|_| Error::InvalidPrefix(prefix.to_string()))
}

/// Apply prefix to name, skipping if name already starts with prefix.
pub fn apply_prefix(name: &str, prefix: &str) -> String {
    if prefix.is_empty() || name.starts_with(prefix) {
        name.to_string()
    } else {
        format!("{prefix}{name}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_name_valid() {
        assert!(validate_name("my_metric").is_ok());
        assert!(validate_name("abc").is_ok());
        assert!(validate_name("a1_b2").is_ok());
        assert!(validate_name("myapp_requests_total").is_ok());
    }

    #[test]
    fn test_validate_name_invalid() {
        assert!(validate_name("").is_err());
        assert!(validate_name("1abc").is_err());
        assert!(validate_name("MyMetric").is_err());
        assert!(validate_name("my-metric").is_err());
        assert!(validate_name("my metric").is_err());
    }

    #[test]
    fn test_normalize_prefix() {
        assert_eq!(normalize_prefix(""), "");
        assert_eq!(normalize_prefix("myapp"), "myapp_");
        assert_eq!(normalize_prefix("myapp_"), "myapp_");
    }

    #[test]
    fn test_validate_prefix() {
        assert!(validate_prefix("").is_ok());
        assert!(validate_prefix("myapp").is_ok());
        assert!(validate_prefix("myapp_").is_ok());
        assert!(validate_prefix("_").is_err());
        assert!(validate_prefix("MyApp").is_err());
    }

    #[test]
    fn test_apply_prefix() {
        assert_eq!(apply_prefix("requests", "myapp_"), "myapp_requests");
        assert_eq!(apply_prefix("myapp_requests", "myapp_"), "myapp_requests");
        assert_eq!(apply_prefix("requests", ""), "requests");
    }
}
