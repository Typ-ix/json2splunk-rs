use std::time::Duration;

use log::{debug, error};
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue};
use serde_json::Value;
use uuid::Uuid;

/// HTTP Event Collector client for sending events to Splunk.
#[derive(Clone)]
pub struct HttpEventCollector {
    pub token: String,
    pub http_event_server: String,
    pub input_type: String,
    pub host: String,
    pub http_event_port: String,
    pub http_event_server_ssl: bool,
    pub ssl_verify: bool,

    // The client is actually changed by Json2Splunk
    pub client: Client, 

    pub index: Option<String>,
    pub sourcetype: Option<String>,
    pub pop_null_fields: bool,

    batch_events: Vec<String>,
    current_byte_length: usize,
    pub max_byte_length: usize,
}
impl HttpEventCollector {
    pub fn new(token: &str, http_event_server: &str, input_type: &str, client: Client) -> Self {
        
        let host = hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "localhost".to_string());

        HttpEventCollector {
            token: token.to_string(),
            http_event_server: http_event_server.to_string(),
            input_type: input_type.to_string(),
            host,
            http_event_port: "8088".to_string(),
            http_event_server_ssl: true,
            ssl_verify: false,
            client,
            index: None,
            sourcetype: None,
            pop_null_fields: false,
            batch_events: Vec::new(),
            current_byte_length: 0,
            max_byte_length: 100_000,
        }
    }

    /// Build server URI like the Python version.
    pub fn server_uri(&self) -> String {
        let protocol = if self.http_event_server_ssl { "https" } else { "http" };

        let mut input_url = if self.input_type == "raw" {
            format!("/raw?channel={}", Uuid::new_v4())
        } else {
            "/event".to_string()
        };

        if self.sourcetype.is_some() || self.index.is_some() {
            if !input_url.contains('?') {
                input_url.push('?');
            }
        }

        if let Some(ref st) = self.sourcetype {
            if !input_url.ends_with('?') {
                input_url.push('&');
            }
            input_url.push_str(&format!("sourcetype={}", st));
        }

        if let Some(ref idx) = self.index {
            if !input_url.ends_with('?') && !input_url.ends_with('&') {
                input_url.push('&');
            }
            input_url.push_str(&format!("index={}", idx));
        }

        format!(
            "{}://{}:{}/services/collector{}",
            protocol, self.http_event_server, self.http_event_port, input_url
        )
    }

    fn headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        let token_value = format!("Splunk {}", self.token);

        headers.insert(
            "Authorization",
            HeaderValue::from_str(&token_value).unwrap(),
        );
        headers.insert(
            "X-Splunk-Request-Channel",
            HeaderValue::from_str(&Uuid::new_v4().to_string()).unwrap(),
        );
        headers
    }

    /// Post a batch payload to HEC, retrying on 503 "Server is busy".
    fn post_payload(&self, payload: &str) -> Result<(), reqwest::Error> {
        let uri = self.server_uri();
        let max_attempts = 5;

        for attempt in 0..max_attempts {
            debug!(
                "Posting to HEC URI: {} (attempt {}/{})",
                uri,
                attempt + 1,
                max_attempts
            );

            let resp = self
                .client
                .post(&uri)
                .headers(self.headers())
                .body(payload.to_string())
                .send()?;

            let status = resp.status();
            let body_text = resp.text().unwrap_or_default();

            if status.is_success() {
                debug!("HEC status={} body={}", status, body_text);
                return Ok(());
            }

            // Splunk is overloaded: "Server is busy"
            if status.as_u16() == 503 && attempt + 1 < max_attempts {
                error!(
                    "HEC busy (503). body={}; will retry after backoff (attempt {}/{})",
                    body_text,
                    attempt + 1,
                    max_attempts
                );
                // simple linear backoff: 500ms, 1s, 1.5s, 2s, ...
                let backoff_ms = 500 * (attempt + 1) as u64;
                std::thread::sleep(Duration::from_millis(backoff_ms));
                continue;
            }

            // Any other HTTP error or final 503 attempt: log and give up on this batch
            error!(
                "HEC error status={} body={}; giving up on this batch",
                status, body_text
            );

            // Attempt to parse Splunk error message and extract invalid event
            if let Ok(err_json) = serde_json::from_str::<serde_json::Value>(&body_text) {
                if let Some(code) = err_json.get("code").and_then(|v| v.as_i64()) {
                    if code == 15 {
                        if let Some(invalid_idx) = err_json
                            .get("invalid-event-number")
                            .and_then(|v| v.as_u64())
                        {
                            let idx = invalid_idx as usize;

                            if idx < self.batch_events.len() {
                                let bad_event = &self.batch_events[idx];
                                
                                debug!(
                                    "\n==================== BAD SPLUNK EVENT (index {}) ====================\n{}\n====================================================================",
                                    idx, bad_event
                                );

                                // Optional: Also pretty-print JSON if valid
                                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(bad_event) {
                                    debug!(
                                        "\nPretty JSON:\n{}\n",
                                        serde_json::to_string_pretty(&parsed).unwrap()
                                    );
                                }
                            } else {
                                debug!("Splunk reported invalid event {}, but batch has only {} events!", idx, self.batch_events.len());
                            }
                        }
                    }
                }
            }

            return Ok(());

        }

        Ok(())
    }

    /// Queue an event in the batch buffer (auto-flush on size).
    pub fn batch_event(&mut self, mut payload: Value) {
        if self.input_type == "json" {
            if !payload.get("host").is_some() {
                payload["host"] = Value::String(self.host.clone());
            }
            if !payload.get("time").is_some() {
                let now = chrono::Utc::now().timestamp_millis() as f64 / 1000.0;
                payload["time"] = Value::from(now);
            }

            if self.pop_null_fields {
                if let Some(event_obj) = payload.get_mut("event") {
                    if let Some(map) = event_obj.as_object_mut() {
                        map.retain(|_, v| !v.is_null());
                    }
                }
            }
        }

        let payload_str = if self.input_type == "json" {
            serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_string())
        } else {
            let mut s = format!("{}", payload);
            if !s.ends_with('\n') {
                s.push('\n');
            }
            s
        };

        let len = payload_str.len();

        if self.current_byte_length + len > self.max_byte_length {
            debug!("Auto flush: existing batch too large, flushing now.");
            self.flush_batch();
        }

        self.current_byte_length += len;
        self.batch_events.push(payload_str);
    }

    /// Flush buffered batch events to Splunk.
    pub fn flush_batch(&mut self) {
        if self.batch_events.is_empty() {
            return;
        }

        let payload = self.batch_events.join("");
        debug!("Flushing {} bytes to Splunk HEC", payload.len());

        if let Err(e) = self.post_payload(&payload) {
            // Network / client errors (DNS, TLS, timeout, etc.)
            error!("Error sending batch to HEC (network error): {}", e);
        }

        self.batch_events.clear();
        self.current_byte_length = 0;
    }
}
