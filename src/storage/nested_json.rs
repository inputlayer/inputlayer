//! Decoding persisted JSON that nests deeper than `serde_json`'s default
//! limit of 128.
//!
//! A stored rule nests a few JSON levels per term level, so a rule the
//! parser accepts can exceed that limit. Refusing it on reload would make the
//! write-ahead log or rule catalog unreadable and the engine unable to start.

use serde::de::DeserializeOwned;

/// Deepest persisted JSON accepted: a term at the parser's highest nesting
/// limit, at up to four JSON levels per term level, inside its record.
const MAX_DEPTH: usize = 4 * crate::parser::MAX_NESTING_DEPTH_CEILING + 64;

/// `serde_json`'s own recursion limit.
const SERDE_JSON_DEPTH: usize = 128;

/// Deserialize `json`, accepting nesting up to [`MAX_DEPTH`]. Input within
/// `serde_json`'s limit takes the ordinary path; deeper input is decoded on
/// a thread with an engine-sized stack.
pub fn from_slice<T: DeserializeOwned + Send>(json: &[u8]) -> Result<T, String> {
    let depth = nesting_depth(json);
    if depth < SERDE_JSON_DEPTH {
        return serde_json::from_slice(json).map_err(|e| e.to_string());
    }
    if depth > MAX_DEPTH {
        return Err(format!(
            "JSON nests {depth} levels deep, more than the {MAX_DEPTH} allowed"
        ));
    }
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("deep-json-decode".to_string())
            .stack_size(crate::ENGINE_THREAD_STACK_BYTES)
            .spawn_scoped(scope, || {
                let mut de = serde_json::Deserializer::from_slice(json);
                de.disable_recursion_limit();
                let value = T::deserialize(&mut de).map_err(|e| e.to_string())?;
                de.end().map_err(|e| e.to_string())?;
                Ok(value)
            })
            .map_err(|e| format!("Failed to spawn JSON decode thread: {e}"))?
            .join()
            .map_err(|_| "JSON decode thread panicked".to_string())?
    })
}

/// Deepest `[`/`{` nesting in `json`, ignoring brackets inside strings.
fn nesting_depth(json: &[u8]) -> usize {
    let (mut depth, mut max) = (0usize, 0usize);
    let (mut in_string, mut escaped) = (false, false);
    for &byte in json {
        if in_string {
            match byte {
                _ if escaped => escaped = false,
                b'\\' => escaped = true,
                b'"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'[' | b'{' => {
                depth += 1;
                max = max.max(depth);
            }
            b']' | b'}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    max
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn nested(depth: usize) -> String {
        format!("{}{}", "[".repeat(depth), "]".repeat(depth))
    }

    #[test]
    fn depth_ignores_brackets_in_strings() {
        assert_eq!(nesting_depth(br#"{"a": "[[{\"]", "b": [1, [2]]}"#), 3);
        assert_eq!(nesting_depth(b"7"), 0);
    }

    #[test]
    fn decodes_past_serde_json_limit_up_to_max_depth() {
        let shallow: serde_json::Value = from_slice(nested(3).as_bytes()).unwrap();
        assert_eq!(shallow, serde_json::json!([[[]]]));
        assert!(serde_json::from_str::<serde_json::Value>(&nested(MAX_DEPTH)).is_err());
        let deep: serde_json::Value = from_slice(nested(MAX_DEPTH).as_bytes()).unwrap();
        // Dropping deep values recurses too: do it on an engine-sized stack.
        std::thread::Builder::new()
            .stack_size(crate::ENGINE_THREAD_STACK_BYTES)
            .spawn(move || drop(deep))
            .unwrap()
            .join()
            .unwrap();
        let err = from_slice::<serde_json::Value>(nested(MAX_DEPTH + 1).as_bytes()).unwrap_err();
        assert!(err.contains("nests"), "{err}");
    }
}
