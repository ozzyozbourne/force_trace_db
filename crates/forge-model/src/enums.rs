/// A Copy enum stored as fixed strings: `as_str`, `ALL`, Display, strict FromStr and serde, all from one table.
macro_rules! str_enum {
    ($(#[$m:meta])* $name:ident, $what:literal, { $($v:ident => $s:literal),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
        $(#[$m])*
        pub enum $name { $(#[serde(rename = $s)] $v),+ }
        impl $name {
            pub const ALL: &'static [$name] = &[$($name::$v),+];
            pub const fn as_str(self) -> &'static str { match self { $($name::$v => $s),+ } }
        }
        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(self.as_str()) }
        }
        /// Strict parse of the stored (canonical) form.
        impl std::str::FromStr for $name {
            type Err = $crate::error::ModelError;
            fn from_str(s: &str) -> Result<Self, $crate::error::ModelError> {
                match s { $($s => Ok($name::$v),)+ _ => Err($crate::error::ModelError::UnknownEnum { what: $what, value: s.to_string() }) }
            }
        }
    };
}
pub(crate) use str_enum;

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
    use crate::error::ModelError;

    fn round_trip<T>(all: &[T])
    where
        T: Copy + PartialEq + std::fmt::Debug + std::fmt::Display + std::str::FromStr<Err = ModelError>,
        T: serde::Serialize + serde::de::DeserializeOwned,
    {
        for &v in all {
            assert_eq!(v.to_string().parse::<T>().unwrap(), v);
            assert_eq!(serde_json::to_string(&v).unwrap(), format!("\"{v}\""));
            assert_eq!(serde_json::from_str::<T>(&format!("\"{v}\"")).unwrap(), v);
        }
    }

    #[test]
    fn round_trips() {
        round_trip(RunType::ALL);
        round_trip(SpanStatus::ALL);
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
