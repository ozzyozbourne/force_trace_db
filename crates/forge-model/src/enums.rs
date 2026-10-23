use crate::error::ModelError;
use std::{fmt, str::FromStr};

macro_rules! str_enum {
    ($name:ident, $what:literal, { $($v:ident => $s:literal),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum $name { $($v),+ }
        impl $name {
            pub const ALL: &'static [$name] = &[$($name::$v),+];
            pub const fn as_str(self) -> &'static str { match self { $($name::$v => $s),+ } }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(self.as_str()) }
        }
        /// Strict parse of the stored (canonical, lowercase) form.
        impl FromStr for $name {
            type Err = ModelError;
            fn from_str(s: &str) -> Result<Self, ModelError> {
                match s { $($s => Ok($name::$v),)+ _ => Err(ModelError::UnknownEnum { what: $what, value: s.to_string() }) }
            }
        }
    };
}
str_enum!(RunType, "run_type", {
    Llm => "llm", ChatModel => "chat_model", Tool => "tool", Chain => "chain", Agent => "agent",
    Retriever => "retriever", Embedding => "embedding", Other => "other",
});
str_enum!(SpanStatus, "status", { Unset => "unset", Ok => "ok", Error => "error" });

impl RunType {
    /// Only these get cost_usd (master decision 14).
    pub fn is_costed(self) -> bool {
        matches!(self, RunType::Llm | RunType::ChatModel | RunType::Embedding)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        for &r in RunType::ALL {
            assert_eq!(r.as_str().parse::<RunType>().unwrap(), r);
            assert_eq!(serde_json::to_string(&r).unwrap(), format!("\"{}\"", r.as_str()));
            assert_eq!(serde_json::from_str::<RunType>(&format!("\"{r}\"")).unwrap(), r);
        }
        for &s in SpanStatus::ALL {
            assert_eq!(s.as_str().parse::<SpanStatus>().unwrap(), s);
            assert_eq!(serde_json::to_string(&s).unwrap(), format!("\"{}\"", s.as_str()));
        }
        assert!(matches!(
            "LLM".parse::<RunType>(),
            Err(ModelError::UnknownEnum { what: "run_type", .. })
        ));
        assert!(matches!(
            "bad".parse::<SpanStatus>(),
            Err(ModelError::UnknownEnum { what: "status", .. })
        ));
    }

    #[test]
    fn costed_run_types() {
        let costed: Vec<_> = RunType::ALL.iter().filter(|r| r.is_costed()).collect();
        assert_eq!(costed, [&RunType::Llm, &RunType::ChatModel, &RunType::Embedding]);
    }
}
