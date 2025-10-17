// Common utilities for tests

use std::sync::atomic::{AtomicU16, Ordering};

use yep_cache_line_size::{CacheLevel, CacheType, get_cache_line_size};

static CACHE_LINE_SIZE: AtomicU16 = AtomicU16::new(0);

pub fn str_to_u8(s: &str) -> &[u8] {
    s.as_bytes()
}

pub fn str_from_u8(buf: &[u8]) -> &str {
    let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    match std::str::from_utf8(&buf[..len]) {
        Ok(s) => s,
        Err(e) => panic!(
            "couldn't parse as utf-8 string, err: {:?} buf: {:?}",
            e, buf
        ),
    }
}

pub fn copy_str_to_slice(s: &str, buf: &mut [u8]) {
    let s_bytes = str_to_u8(s);
    assert!(s_bytes.len() <= buf.len(), "dst buffer not large enough!");

    buf[..s_bytes.len()].copy_from_slice(s_bytes);
    buf[s_bytes.len()..].fill(0);
}

pub fn align_to_cache_line(size: u16) -> u16 {
    if CACHE_LINE_SIZE.load(Ordering::Relaxed) == 0 {
        CACHE_LINE_SIZE.store(
            get_cache_line_size(CacheLevel::L1, CacheType::Data).unwrap() as u16,
            Ordering::Relaxed,
        );
    }

    let cache_line_size = CACHE_LINE_SIZE.load(Ordering::Relaxed);
    size.div_ceil(cache_line_size) * cache_line_size
}
