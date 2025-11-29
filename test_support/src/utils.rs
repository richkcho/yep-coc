// Common utilities for tests

use std::sync::atomic::{AtomicU16, Ordering};

use yep_cache_line_size::{CacheLevel, CacheType, get_cache_line_size};

static CACHE_LINE_SIZE: AtomicU16 = AtomicU16::new(0);

fn ensure_cache_line_size() -> u16 {
    if CACHE_LINE_SIZE.load(Ordering::Relaxed) == 0 {
        CACHE_LINE_SIZE.store(
            get_cache_line_size(CacheLevel::L1, CacheType::Data).unwrap() as u16,
            Ordering::Relaxed,
        );
    }
    CACHE_LINE_SIZE.load(Ordering::Relaxed)
}

/// Simple exponential backoff used by examples/benchmarks to avoid hammering the scheduler.
pub fn backoff(pow: &mut u8) {
    if *pow < 6 {
        let spins = 1 << *pow;
        for _ in 0..spins {
            std::hint::spin_loop();
        }
        *pow += 1;
    } else {
        std::thread::yield_now();
    }
}

/// Return the current L1 data cache line size (cached after first lookup).
pub fn cache_line_size() -> u16 {
    ensure_cache_line_size()
}

pub fn str_to_u8(s: &str) -> &[u8] {
    s.as_bytes()
}

pub fn str_from_u8(buf: &[u8]) -> &str {
    let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    match std::str::from_utf8(&buf[..len]) {
        Ok(s) => s,
        Err(e) => panic!("couldn't parse as utf-8 string, err: {e} buf: {buf:?}"),
    }
}

pub fn copy_str_to_slice(s: &str, buf: &mut [u8]) {
    let s_bytes = str_to_u8(s);
    assert!(s_bytes.len() <= buf.len(), "dst buffer not large enough!");

    buf[..s_bytes.len()].copy_from_slice(s_bytes);
    buf[s_bytes.len()..].fill(0);
}

pub fn align_to_cache_line(size: u16) -> u16 {
    let cache_line_size = ensure_cache_line_size();
    size.div_ceil(cache_line_size) * cache_line_size
}
