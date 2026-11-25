//! The admin socket a validator opens in its ledger before its RPC search. Test branch only: the
//! events dashboard reads the start phase and process id there until the validator's start
//! progress events arrive.

use {
    solana_core::validator::ValidatorStartProgress,
    std::{
        io::{Read, Write},
        os::unix::net::UnixStream,
        path::Path,
        time::Duration,
    },
};

const TIMEOUT: Duration = Duration::from_millis(500);
/// Replies here are a few hundred bytes.
const MOST_BYTES: usize = 65_536;

pub fn start_progress(ledger: &Path) -> Option<ValidatorStartProgress> {
    serde_json::from_value(call(ledger, "startProgress")?).ok()
}

pub fn pid(ledger: &Path) -> Option<u32> {
    serde_json::from_value(call(ledger, "pid")?).ok()
}

/// One JSON-RPC call's `result`, `None` where the socket is absent or does not answer.
fn call(ledger: &Path, method: &str) -> Option<serde_json::Value> {
    let mut stream = UnixStream::connect(ledger.join("admin.rpc")).ok()?;
    stream.set_read_timeout(Some(TIMEOUT)).ok()?;
    stream.set_write_timeout(Some(TIMEOUT)).ok()?;
    let request = serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": []});
    stream.write_all(request.to_string().as_bytes()).ok()?;
    let mut reply = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        let read = stream.read(&mut buffer).ok()?;
        if read == 0 {
            return None;
        }
        reply.extend_from_slice(buffer.get(..read)?);
        if let Some(result) = result_of(&reply) {
            return Some(result);
        }
        if reply.len() > MOST_BYTES {
            return None;
        }
    }
}

/// The reply's `result` once the bytes hold a whole JSON object.
fn result_of(reply: &[u8]) -> Option<serde_json::Value> {
    let mut parsed = serde_json::Deserializer::from_slice(reply).into_iter::<serde_json::Value>();
    parsed
        .next()?
        .ok()?
        .get_mut("result")
        .map(serde_json::Value::take)
}

#[cfg(test)]
mod tests {
    use {super::*, std::os::unix::net::UnixListener};

    #[test]
    fn test_a_reply_is_read_once_whole() {
        assert_eq!(result_of(br#"{"jsonrpc":"2.0","res"#), None);
        assert_eq!(
            result_of(b"{\"jsonrpc\":\"2.0\",\"result\":42,\"id\":1}\n"),
            Some(serde_json::json!(42))
        );
    }

    #[test]
    fn test_the_phase_is_read_from_the_socket() {
        let ledger = tempfile::tempdir().unwrap();
        let listener = UnixListener::bind(ledger.path().join("admin.rpc")).unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 256];
            let read = stream.read(&mut request).unwrap();
            assert!(String::from_utf8_lossy(&request[..read]).contains("startProgress"));
            let reply =
                r#"{"jsonrpc":"2.0","result":{"ProcessingLedger":{"slot":5,"max_slot":9}},"id":1}"#;
            stream.write_all(reply.as_bytes()).unwrap();
        });
        assert_eq!(
            start_progress(ledger.path()),
            Some(ValidatorStartProgress::ProcessingLedger {
                slot: 5,
                max_slot: 9
            })
        );
        server.join().unwrap();
        assert_eq!(start_progress(ledger.path()), None);
    }
}
