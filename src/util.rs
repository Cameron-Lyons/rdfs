use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn checksum_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    digest_hex(hasher)
}

pub fn digest_hex(hasher: Sha256) -> String {
    let digest = hasher.finalize();
    let bytes: &[u8] = digest.as_ref();
    let mut checksum = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut checksum, "{byte:02x}").expect("writing to a String cannot fail");
    }
    checksum
}

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Generates a unique 128-bit hex identifier by hashing a caller-supplied
/// scope (e.g. the node id) together with the wall clock, the process id,
/// and a process-wide counter. The counter makes ids unique within a
/// process, the clock and pid across restarts, and the scope across nodes.
pub fn unique_id(scope: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut hasher = Sha256::new();
    hasher.update(scope.as_bytes());
    hasher.update([0]);
    hasher.update(nanos.to_le_bytes());
    hasher.update(std::process::id().to_le_bytes());
    hasher.update(COUNTER.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    let mut id = digest_hex(hasher);
    id.truncate(32);
    id
}

#[cfg(test)]
mod tests {
    use super::{checksum_hex, unique_id};
    use std::collections::HashSet;

    #[test]
    fn checksum_hex_formats_sha256_digest() {
        assert_eq!(
            checksum_hex(b"hello"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn unique_ids_do_not_repeat() {
        let ids = (0..10_000)
            .map(|_| unique_id("test"))
            .collect::<HashSet<_>>();
        assert_eq!(ids.len(), 10_000);
        assert!(ids.iter().all(|id| id.len() == 32));
    }
}
