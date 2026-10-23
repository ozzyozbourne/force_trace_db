//! Embedded model pricing ($ per 1M tokens) + model-name normalization.
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelPrice {
    /// $ / 1M uncached input tokens.
    pub input: f64,
    /// $ / 1M output tokens (embeddings: 0).
    pub output: f64,
    /// $ / 1M cache-read input tokens (defaults to `input`).
    #[serde(default)]
    pub cached_input: Option<f64>,
    /// $ / 1M cache-write input tokens (defaults to `input`).
    #[serde(default)]
    pub cache_write: Option<f64>,
}

/// Token usage as normalized; `prompt` includes cached tokens (GenAI semconv).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Usage {
    pub prompt: Option<i64>,
    pub completion: Option<i64>,
    pub cache_read: Option<i64>,
    pub cache_write: Option<i64>,
}

#[derive(Deserialize)]
struct PricingFile {
    models: std::collections::BTreeMap<String, ModelPrice>,
}
const PRICING_TOML: &str = include_str!("pricing.toml");

#[derive(Debug, Clone)]
pub struct Pricing {
    models: HashMap<String, ModelPrice>,
}

impl Default for Pricing {
    fn default() -> Self {
        Self::builtin()
    }
}

impl Pricing {
    /// The embedded table. Its validity is a unit test, so the expect cannot fire in a released binary.
    pub fn builtin() -> Self {
        let f: PricingFile = toml::from_str(PRICING_TOML).expect("embedded pricing.toml is valid");
        Self {
            models: f.models.into_iter().map(|(k, v)| (canonical_model(&k), v)).collect(),
        }
    }

    /// Exact canonical name, else the longest table key that is a prefix ending at a `-` boundary
    /// (`gpt-3.5-turbo-0125` -> `gpt-3.5-turbo`, `claude-sonnet-4-5` -> `claude-sonnet-4`).
    pub fn lookup(&self, model: &str) -> Option<(&str, &ModelPrice)> {
        let c = canonical_model(model);
        if let Some((k, v)) = self.models.get_key_value(c.as_str()) {
            return Some((k.as_str(), v));
        }
        self.models
            .iter()
            .filter(|(k, _)| c.len() > k.len() && c.starts_with(k.as_str()) && c.as_bytes()[k.len()] == b'-')
            .max_by_key(|(k, _)| k.len())
            .map(|(k, v)| (k.as_str(), v))
    }

    /// None for an unknown model or when no token counts are present.
    pub fn cost(&self, model: &str, u: &Usage) -> Option<f64> {
        if u.prompt.is_none() && u.completion.is_none() {
            return None;
        }
        let (_, pr) = self.lookup(model)?;
        let prompt = u.prompt.unwrap_or(0) as f64;
        let cr = u.cache_read.unwrap_or(0) as f64;
        let cw = u.cache_write.unwrap_or(0) as f64;
        let uncached = (prompt - cr - cw).max(0.0);
        let micro = uncached * pr.input
            + cr * pr.cached_input.unwrap_or(pr.input)
            + cw * pr.cache_write.unwrap_or(pr.input)
            + u.completion.unwrap_or(0) as f64 * pr.output;
        Some(micro / 1e6)
    }
}

/// lowercase -> last `/` segment (`openai/gpt-4o`) -> drop Bedrock vendor prefix (`us.anthropic.`) ->
/// drop `:N` and `-vN` suffixes -> drop `-latest` -> drop a date snapshot (`-2024-08-06`, `-20250514`).
pub fn canonical_model(m: &str) -> String {
    let mut s = m.trim().to_ascii_lowercase();
    if let Some(i) = s.rfind('/') {
        s = s[i + 1..].to_string();
    }
    for vendor in ["anthropic.", "meta.", "mistral.", "cohere.", "amazon."] {
        if let Some(i) = s.find(vendor) {
            s = s[i + vendor.len()..].to_string();
        }
    }
    if let Some(i) = s.rfind(':') {
        if s[i + 1..].bytes().all(|b| b.is_ascii_digit()) {
            s.truncate(i);
        }
    }
    if let Some(i) = s.rfind("-v") {
        if i + 2 < s.len() && s[i + 2..].bytes().all(|b| b.is_ascii_digit()) {
            s.truncate(i);
        }
    }
    if let Some(x) = s.strip_suffix("-latest") {
        s = x.to_string();
    }
    let b = s.as_bytes();
    let digits = |r: std::ops::Range<usize>| r.clone().all(|i| b[i].is_ascii_digit());
    let n = b.len();
    if n > 11
        && b[n - 11] == b'-'
        && b[n - 6] == b'-'
        && b[n - 3] == b'-'
        && digits(n - 10..n - 6)
        && digits(n - 5..n - 3)
        && digits(n - 2..n)
    {
        s.truncate(n - 11); // -YYYY-MM-DD
    } else if n > 9 && b[n - 9] == b'-' && digits(n - 8..n) && (b[n - 8] == b'2') {
        s.truncate(n - 9); // -YYYYMMDD
    }
    s
}

/// Lowercase + alias map; `None` for blanks.
pub fn normalize_provider(p: &str) -> Option<String> {
    let s = p.trim().to_ascii_lowercase();
    let s = match s.as_str() {
        "" => return None,
        "az.ai.openai" | "azure_openai" | "azure" => "azure.ai.openai",
        "az.ai.inference" => "azure.ai.inference",
        "mistral_ai" | "mistralai" => "mistral",
        "google" | "vertexai" | "vertex_ai" => "gcp.gen_ai",
        "bedrock" | "aws_bedrock" => "aws.bedrock",
        "xai" => "x_ai",
        other => other,
    };
    Some(s.to_string())
}

/// `openai/gpt-4o` -> `openai`.
pub fn provider_from_model(model: &str) -> Option<String> {
    model.split_once('/').and_then(|(p, _)| normalize_provider(p))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn canonical() {
        for (raw, want) in [
            ("gpt-4o-2024-08-06", "gpt-4o"),
            ("OpenAI/GPT-4o-mini-2024-07-18", "gpt-4o-mini"),
            ("claude-sonnet-4-20250514", "claude-sonnet-4"),
            ("us.anthropic.claude-3-5-sonnet-20241022-v2:0", "claude-3-5-sonnet"),
            ("claude-3-5-haiku-latest", "claude-3-5-haiku"),
            ("gpt-3.5-turbo-0125", "gpt-3.5-turbo-0125"),
        ] {
            assert_eq!(canonical_model(raw), want, "{raw}");
        }
        let pr = Pricing::builtin();
        assert_eq!(pr.lookup("gpt-3.5-turbo-0125").unwrap().0, "gpt-3.5-turbo");
        assert_eq!(pr.lookup("o3-mini-2025-01-31").unwrap().0, "o3-mini");
        assert_eq!(pr.lookup("gpt-4-0613").unwrap().0, "gpt-4");
        assert_eq!(pr.lookup("gpt-4.1-2025-04-14").unwrap().0, "gpt-4.1");
        assert!(pr.lookup("llama-3-70b").is_none());
        for k in [
            "gpt-4o",
            "gpt-4o-mini",
            "gpt-4.1",
            "gpt-4.1-mini",
            "gpt-4.1-nano",
            "gpt-4-turbo",
            "gpt-4",
            "gpt-3.5-turbo",
            "o1",
            "o1-mini",
            "o3",
            "o3-mini",
            "o4-mini",
            "text-embedding-3-small",
            "text-embedding-3-large",
            "text-embedding-ada-002",
            "claude-opus-4",
            "claude-sonnet-4",
            "claude-3-7-sonnet",
            "claude-3-5-sonnet",
            "claude-3-5-haiku",
            "claude-3-haiku",
            "gemini-2.5-pro",
            "gemini-2.5-flash",
        ] {
            assert_eq!(pr.lookup(k).unwrap().0, k);
        }
        assert_eq!(pr.models.len(), 24);
    }
}
