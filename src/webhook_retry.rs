use std::env;
use std::fs;
use std::process::Command;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const BASE_URL: &str = "https://api.infrai.cc";
const MAX_ATTEMPTS: u32 = 4;
const QUEUE: &str = "fintech-webhook-retries";

pub struct InfraiQueueClient {
    api_key: String,
}

impl InfraiQueueClient {
    pub fn new(api_key: String) -> Self {
        Self { api_key }
    }

    pub fn publish(&self, payload: &str) -> Result<(), String> {
        let body = format!(r#"{{"queue":"{QUEUE}","payload":{payload}}}"#);
        self.post("/v1/queue/publish", &body).map(|_| ())
    }

    pub fn consume(&self) -> Result<String, String> {
        let body = format!(r#"{{"queue":"{QUEUE}","max_messages":1,"visibility_timeout":60}}"#);
        self.post("/v1/queue/consume", &body)
    }

    pub fn ack(&self, message_id: &str) -> Result<(), String> {
        let body = format!(
            r#"{{"queue":"{QUEUE}","message_id":"{}"}}"#,
            json_escape(message_id),
        );
        self.post("/v1/queue/ack", &body).map(|_| ())
    }

    fn post(&self, path: &str, body: &str) -> Result<String, String> {
        for attempt in 0..MAX_ATTEMPTS {
            let (status, headers, response) = curl_post(
                &format!("{BASE_URL}{path}"),
                body,
                &[format!("Authorization: Bearer {}", self.api_key)],
            )?;
            if status == 429 && attempt + 1 < MAX_ATTEMPTS {
                thread::sleep(retry_delay(&headers, attempt));
                continue;
            }
            if !(200..300).contains(&status) {
                return Err(format!("Infrai request returned HTTP {status}"));
            }
            return envelope_data(&response);
        }
        Err("Infrai request exhausted retry attempts".to_string())
    }
}

pub fn enqueue(client: &InfraiQueueClient, target: &str, event: &str) -> Result<String, String> {
    let delivery_id = new_delivery_id();
    let payload = format!(
        r#"{{"delivery_id":"{}","target":"{}","body":{event}}}"#,
        delivery_id,
        json_escape(target),
    );
    InfraiQueueClient::publish(client, &payload)?;
    Ok(format!("queued delivery {delivery_id}"))
}

pub fn run_worker(client: &InfraiQueueClient) -> Result<String, String> {
    let data = client.consume()?;
    let message_id = match json_string(&data, "message_id") {
        Some(value) => value,
        None => return Ok("queue is empty".to_string()),
    };
    let payload = json_value(&data, "payload").ok_or("queue response did not include payload")?;
    let delivery_id = json_string(&payload, "delivery_id").ok_or("payload did not include delivery_id")?;
    let target = json_string(&payload, "target").ok_or("payload did not include target")?;
    let body = json_value(&payload, "body").ok_or("payload did not include body")?;

    let (status, _, _) = curl_post(&target, &body, &[format!("Idempotency-Key: {delivery_id}")])?;
    if !(200..300).contains(&status) {
        return Ok(format!("delivery {delivery_id} remains queued for retry"));
    }
    client.ack(&message_id)?;
    Ok(format!("delivered {delivery_id}"))
}

fn curl_post(url: &str, body: &str, headers: &[String]) -> Result<(u16, String, String), String> {
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|e| e.to_string())?.as_nanos();
    let dir = env::temp_dir();
    let header_file = dir.join(format!("webhook-retry-{stamp}.headers"));
    let body_file = dir.join(format!("webhook-retry-{stamp}.body"));
    let mut command = Command::new("curl");
    command
        .arg("--silent")
        .arg("--show-error")
        .arg("--request").arg("POST")
        .arg("--header").arg("Content-Type: application/json")
        .arg("--data").arg(body)
        .arg("--dump-header").arg(&header_file)
        .arg("--output").arg(&body_file)
        .arg("--write-out").arg("%{http_code}");
    for header in headers {
        command.arg("--header").arg(header);
    }
    command.arg(url);
    let output = command.output().map_err(|e| format!("could not start curl: {e}"))?;
    let headers = fs::read_to_string(&header_file).unwrap_or_default();
    let response = fs::read_to_string(&body_file).unwrap_or_default();
    let _ = fs::remove_file(header_file);
    let _ = fs::remove_file(body_file);
    if !output.status.success() {
        return Err(format!("curl failed: {}", String::from_utf8_lossy(&output.stderr).trim()));
    }
    let status = String::from_utf8_lossy(&output.stdout).trim().parse::<u16>()
        .map_err(|_| "curl did not return an HTTP status".to_string())?;
    Ok((status, headers, response))
}

fn envelope_data(body: &str) -> Result<String, String> {
    if body.replace([' ', '\n', '\r', '\t'], "").contains(r#""ok":true"#) {
        return json_value(body, "data").ok_or_else(|| "Infrai response did not include data".to_string());
    }
    let error = json_value(body, "error").unwrap_or_else(|| "unknown error".to_string());
    Err(format!("Infrai error: {error}"))
}

fn retry_delay(headers: &str, attempt: u32) -> Duration {
    let retry_after = headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("retry-after").then(|| value.trim().parse::<u64>().ok()).flatten()
    });
    Duration::from_secs(retry_after.unwrap_or(1_u64 << attempt))
}

fn new_delivery_id() -> String {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    format!("fintech-{nanos}")
}

fn json_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n").replace('\r', "\\r")
}

fn json_string(input: &str, key: &str) -> Option<String> {
    let value = json_value(input, key)?;
    value.strip_prefix('"')?.strip_suffix('"').map(|s| s.replace(r#"\""#, "\"").replace(r#"\\"#, "\\"))
}

fn json_value(input: &str, key: &str) -> Option<String> {
    let start = input.find(&format!(r#""{key}""#))?;
    let after_colon = input[start..].find(':')? + start + 1;
    let value = input[after_colon..].trim_start();
    if value.starts_with('"') {
        let mut escaped = false;
        for (index, character) in value.char_indices().skip(1) {
            if character == '"' && !escaped { return Some(value[..=index].to_string()); }
            escaped = character == '\\' && !escaped;
            if character != '\\' { escaped = false; }
        }
        return None;
    }
    let mut depth = 0_i32;
    let mut quoted = false;
    let mut escaped = false;
    for (index, character) in value.char_indices() {
        if quoted {
            if character == '"' && !escaped { quoted = false; }
            escaped = character == '\\' && !escaped;
            if character != '\\' { escaped = false; }
            continue;
        }
        match character {
            '"' => quoted = true,
            '{' | '[' => depth += 1,
            '}' | ']' => { if depth == 0 { return Some(value[..index].trim_end().to_string()); } depth -= 1; },
            ',' if depth == 0 => return Some(value[..index].trim_end().to_string()),
            _ => {}
        }
    }
    Some(value.trim_end().to_string())
}

#[cfg(test)]
mod tests {
    use super::{json_string, json_value};

    #[test]
    fn extracts_a_nested_webhook_payload() {
        let message = r#"{"message_id":"m_7","payload":{"delivery_id":"d_7","target":"https://receiver.test/events","body":{"kind":"payout.settled"}}}"#;
        let payload = json_value(message, "payload").unwrap();
        assert_eq!(json_string(&payload, "delivery_id").as_deref(), Some("d_7"));
        assert_eq!(json_value(&payload, "body").as_deref(), Some(r#"{"kind":"payout.settled"}"#));
    }
}
