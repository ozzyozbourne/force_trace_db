use crate::error::{ModelError, Result};
use std::{fmt, str::FromStr, sync::Arc};

macro_rules! byte_id {
    ($name:ident, $n:literal, $what:literal) => {
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        pub struct $name(pub [u8; $n]);
        impl $name {
            pub const LEN: usize = $n;
            pub fn from_slice(b: &[u8]) -> Result<Self> {
                let a: [u8; $n] = b.try_into().map_err(|_| ModelError::InvalidIdLength {
                    what: $what,
                    expected: $n,
                    got: b.len(),
                })?;
                Ok(Self(a))
            }
            pub fn as_bytes(&self) -> &[u8; $n] {
                &self.0
            }
            /// OTLP treats all-zero ids as invalid.
            pub fn is_zero(&self) -> bool {
                self.0 == [0u8; $n]
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&hex::encode(self.0))
            }
        }
        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), hex::encode(self.0))
            }
        }
        impl FromStr for $name {
            type Err = ModelError;
            fn from_str(s: &str) -> Result<Self> {
                let mut a = [0u8; $n];
                hex::decode_to_slice(s, &mut a).map_err(|source| ModelError::InvalidIdHex { what: $what, source })?;
                Ok(Self(a))
            }
        }
        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
                s.serialize_str(&hex::encode(self.0))
            }
        }
        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
                let s = <std::borrow::Cow<'de, str>>::deserialize(d)?;
                s.parse().map_err(serde::de::Error::custom)
            }
        }
    };
}
byte_id!(TraceId, 16, "trace_id");
byte_id!(SpanId, 8, "span_id");

/// Tenant key. Also an object-path component (`segments/<project_id>/...`), hence the charset rule.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ProjectId(Arc<str>);
impl ProjectId {
    pub fn new(s: impl Into<String>) -> Result<Self> {
        let s = s.into();
        let ok = (1..=128).contains(&s.len())
            && !s.starts_with('.')
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'));
        if ok {
            Ok(Self(s.into()))
        } else {
            Err(ModelError::InvalidProjectId(s))
        }
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for ProjectId {
    type Error = ModelError;
    fn try_from(s: String) -> Result<Self> {
        Self::new(s)
    }
}
impl From<ProjectId> for String {
    fn from(p: ProjectId) -> String {
        p.0.to_string()
    }
}
impl fmt::Display for ProjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl fmt::Debug for ProjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ProjectId({})", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_ids_hex_round_trip() {
        let t: TraceId = "0102030405060708090a0b0c0d0e0f10".parse().unwrap();
        assert_eq!(t.to_string(), "0102030405060708090a0b0c0d0e0f10");
        assert_eq!(TraceId::from_slice(t.as_bytes()).unwrap(), t);
        assert!(!t.is_zero());
        assert!(TraceId::default().is_zero());
        assert!(matches!(
            SpanId::from_slice(&[1; 7]),
            Err(ModelError::InvalidIdLength {
                what: "span_id",
                expected: 8,
                got: 7
            })
        ));
        assert!(matches!("0102".parse::<SpanId>(), Err(ModelError::InvalidIdHex { .. })));
        let s = SpanId([0xab; 8]);
        assert_eq!(serde_json::to_string(&s).unwrap(), "\"abababababababab\"");
        assert_eq!(serde_json::from_str::<SpanId>("\"abababababababab\"").unwrap(), s);
    }

    #[test]
    fn project_id_charset() {
        for ok in ["default".to_string(), "a.b-c_1".into(), "x".repeat(128)] {
            assert_eq!(ProjectId::new(ok.clone()).unwrap().as_str(), ok);
        }
        for bad in [String::new(), ".x".into(), "a/b".into(), "a b".into(), "x".repeat(129)] {
            assert!(matches!(ProjectId::new(bad), Err(ModelError::InvalidProjectId(_))));
        }
        assert_eq!(format!("{:?}", ProjectId::new("p").unwrap()), "ProjectId(p)");
        assert!(serde_json::from_str::<ProjectId>("\"a/b\"").is_err());
    }
}
