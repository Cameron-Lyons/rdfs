use anyhow::{Result, bail};

pub fn normalize_path(path: &str) -> Result<String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        bail!("path must not be empty");
    }
    if !trimmed.starts_with('/') {
        bail!("path must be absolute");
    }

    let mut parts = Vec::new();
    for part in trimmed.split('/') {
        if part.is_empty() {
            continue;
        }
        if part == "." || part == ".." {
            bail!("path contains unsupported segment: {part}");
        }
        parts.push(part);
    }

    if parts.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

pub fn parent_path(path: &str) -> Option<String> {
    if path == "/" {
        return None;
    }

    let idx = path.rfind('/')?;
    if idx == 0 {
        Some("/".to_string())
    } else {
        Some(path[..idx].to_string())
    }
}

pub fn basename(path: &str) -> &str {
    if path == "/" {
        "/"
    } else {
        path.rsplit('/').next().unwrap_or(path)
    }
}

pub fn replace_prefix(path: &str, from: &str, to: &str) -> String {
    if path == from {
        return to.to_string();
    }
    let suffix = path.strip_prefix(from).unwrap_or(path);
    format!("{to}{suffix}")
}

pub fn is_child_of(path: &str, parent: &str) -> bool {
    if parent == "/" {
        return path != "/";
    }
    path.starts_with(&format!("{parent}/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_compacts_slashes() {
        assert_eq!(normalize_path("//a///b").unwrap(), "/a/b");
    }

    #[test]
    fn normalize_rejects_relative_segments() {
        assert!(normalize_path("/a/../b").is_err());
        assert!(normalize_path("a/b").is_err());
    }
}
