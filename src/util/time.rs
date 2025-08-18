use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[inline]
pub fn now_unix() -> (u64, u32) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0));
    (now.as_secs(), now.subsec_nanos())
}
