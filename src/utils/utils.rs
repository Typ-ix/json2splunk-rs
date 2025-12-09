use std::io::{self, Read as IoRead};
use serde_json::Value;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use std::path::Path;
use std::os::unix::ffi::OsStrExt;
use xxhash_rust::xxh3::xxh3_64;

/// Streaming lossy UTF-8 decoder:
/// - reads raw bytes from `inner`
/// - converts them with `String::from_utf8_lossy`
/// - strips BOM + NULs per chunk
/// - exposes UTF-8 bytes via `Read`
///
/// This lets us feed non-UTF-8 CSV files to `csv::Reader` without
/// loading the whole file into memory.
pub struct LossyUtf8Reader<R: IoRead> {
    inner: R,
    in_buf: Vec<u8>,
    out_buf: Vec<u8>,
    out_pos: usize,
}

impl<R: IoRead> LossyUtf8Reader<R> {

    pub fn new(inner: R) -> Self {
        Self {
            inner,
            in_buf: vec![0u8; 8192], // chunk size (8 KiB)
            out_buf: Vec::new(),
            out_pos: 0,
        }
    }
}

impl<R: IoRead> IoRead for LossyUtf8Reader<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        // Serve leftover UTF-8 bytes first
        if self.out_pos < self.out_buf.len() {
            let remaining = self.out_buf.len() - self.out_pos;
            let to_copy = remaining.min(out.len());
            out[..to_copy]
                .copy_from_slice(&self.out_buf[self.out_pos..self.out_pos + to_copy]);
            self.out_pos += to_copy;
            return Ok(to_copy);
        }

        // Read new raw bytes from inner
        let n = self.inner.read(&mut self.in_buf)?;
        if n == 0 {
            return Ok(0); // EOF
        }

        // Lossy UTF-8 decode
        let mut s = String::from_utf8_lossy(&self.in_buf[..n]).into_owned();

        // Strip BOM + NULs per chunk (streaming)
        s.retain(|c| c != '\u{feff}' && c != '\u{0}');

        self.out_buf.clear();
        self.out_buf.extend_from_slice(s.as_bytes());
        self.out_pos = 0;

        let to_copy = self.out_buf.len().min(out.len());
        out[..to_copy].copy_from_slice(&self.out_buf[..to_copy]);
        self.out_pos = to_copy;
        Ok(to_copy)
    }
}

/// Nested JSON lookup using dotted path.
pub fn get_nested_value<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;

    for part in path.split('.') {
        match current {
            Value::Object(map) => {
                current = map.get(part)?;
            }
            Value::Array(arr) => {
                let idx: usize = part.parse().ok()?;
                current = arr.get(idx)?;
            }
            _ => {
                return None;
            }
        }
    }

    Some(current)
}

pub fn parse_timestamp_string(s: &str, fmt_opt: Option<&str>) -> Option<f64> {
    if let Some(fmt) = fmt_opt {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s, fmt) {
            let dt: DateTime<Utc> = Utc.from_utc_datetime(&ndt);
            return Some(
                dt.timestamp() as f64 + f64::from(dt.timestamp_subsec_micros()) / 1e6,
            );
        }
    }

    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        let dt_utc = dt.with_timezone(&Utc);
        return Some(
            dt_utc.timestamp() as f64
                + f64::from(dt_utc.timestamp_subsec_micros()) / 1e6,
        );
    }

    if let Ok(num) = s.parse::<f64>() {
        return Some(epoch_from_number(num));
    }

    None
}

/// Extract host from a record using a dotted path.
pub fn extract_host_from_record(event: &Value, host_path: &str) -> Option<String> {
    let v = get_nested_value(event, host_path)?;
    v.as_str().map(|s| s.to_string())
}

/// Heuristic to convert numeric epoch values.
pub fn epoch_from_number(num: f64) -> f64 {
    let abs = num.abs();

    if abs > 1.0e18 {
        // nanoseconds
        num / 1.0e9
    } else if abs > 1.0e15 {
        // microseconds
        num / 1.0e6
    } else if abs > 1.0e12 {
        // milliseconds
        num / 1.0e3
    } else {
        // assume seconds
        num
    }
}

/// Extract timestamp from a record using the list of paths + optional format.
pub fn extract_timestamp_from_record(
    event: &Value,
    paths: &[String],
    fmt_opt: Option<&str>,
) -> Option<f64> {
    for path_str in paths {
        let Some(v) = get_nested_value(event, path_str) else {
            // Path not present in this record, try the next one
            continue;
        };
        if v.is_null() {
            continue;
        }

        if let Some(num) = v.as_f64() {
            return Some(epoch_from_number(num));
        }

        if let Some(s) = v.as_str() {
            if s.is_empty() {
                continue;
            }
            if let Some(epoch) = parse_timestamp_string(s, fmt_opt) {
                return Some(epoch);
            }
        }
    }

    None
}

pub fn normalize_host(base: &str) -> String {
    let lower = base.to_lowercase();
    match lower.split('.').next() {
        Some(first) if !first.is_empty() => first.to_string(),
        _ => lower,
    }
}

pub fn hash_path(path: &Path) -> u64 {
    let bytes = path.as_os_str().as_bytes();
    xxh3_64(bytes)
}

pub fn is_valid_hec_time(ts: f64) -> bool {
    // Splunk will reject events containing time like 1960 etc.

    // Must be a real number
    if !ts.is_finite() {
        return false;
    }

    // Reject pre-epoch times
    if ts < 0.0 {
        return false;
    }

    // Upper bound: now + 10 years
    // (roughly 10 * 365 * 86400 seconds; leap years don't matter here)
    let now = chrono::Utc::now().timestamp() as f64;
    let max_future = now + 10.0 * 365.0 * 24.0 * 3600.0;

    if ts > max_future {
        return false;
    }

    true
}