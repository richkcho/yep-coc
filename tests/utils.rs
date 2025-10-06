// Common utilities for tests
pub fn str_to_u8(s: &str) -> &[u8] {
        return s.as_bytes();
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
    assert!(s_bytes.len() < buf.len(), "dst buffer not large enough!");

    buf[..s_bytes.len()].copy_from_slice(s_bytes);
    buf[s_bytes.len()..].fill(0);
}
