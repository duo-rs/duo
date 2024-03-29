use std::fmt::Display;

use serde_json::{Number, Value as JsonValue};

use crate::ValueEnum;

tonic::include_proto!("rs.duo.common");

impl Value {
    pub fn type_name(&self) -> &str {
        if let Some(inner) = &self.inner {
            match inner {
                ValueEnum::StrVal(_) => "str",
                ValueEnum::U64Val(_) => "u64",
                ValueEnum::I64Val(_) => "i64",
                ValueEnum::BoolVal(_) => "bool",
            }
        } else {
            ""
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(inner) = &self.inner {
            match inner {
                ValueEnum::StrVal(v) => write!(f, "{v}"),
                ValueEnum::U64Val(v) => write!(f, "{v}"),
                ValueEnum::I64Val(v) => write!(f, "{v}"),
                ValueEnum::BoolVal(v) => write!(f, "{v}"),
            }
        } else {
            write!(f, "")
        }
    }
}

impl From<tracing_core::Level> for Level {
    fn from(level: tracing_core::Level) -> Self {
        match level {
            tracing_core::Level::ERROR => Level::Error,
            tracing_core::Level::WARN => Level::Warn,
            tracing_core::Level::INFO => Level::Info,
            tracing_core::Level::DEBUG => Level::Debug,
            tracing_core::Level::TRACE => Level::Trace,
        }
    }
}
impl From<Level> for tracing_core::Level {
    fn from(level: Level) -> Self {
        match level {
            Level::Error => tracing_core::Level::ERROR,
            Level::Warn => tracing_core::Level::WARN,
            Level::Info => tracing_core::Level::INFO,
            Level::Debug => tracing_core::Level::DEBUG,
            Level::Trace => tracing_core::Level::TRACE,
        }
    }
}

impl From<value::Inner> for Value {
    fn from(inner: value::Inner) -> Self {
        Value { inner: Some(inner) }
    }
}

impl From<i32> for Value {
    fn from(val: i32) -> Self {
        value::Inner::I64Val(val as i64).into()
    }
}

impl From<u32> for Value {
    fn from(val: u32) -> Self {
        value::Inner::U64Val(val as u64).into()
    }
}

impl From<i64> for Value {
    fn from(val: i64) -> Self {
        value::Inner::I64Val(val).into()
    }
}

impl From<u64> for Value {
    fn from(val: u64) -> Self {
        value::Inner::U64Val(val).into()
    }
}

impl From<bool> for Value {
    fn from(val: bool) -> Self {
        value::Inner::BoolVal(val).into()
    }
}

impl From<&str> for Value {
    fn from(val: &str) -> Self {
        value::Inner::StrVal(val.into()).into()
    }
}

impl From<String> for Value {
    fn from(val: String) -> Self {
        value::Inner::StrVal(val).into()
    }
}

impl From<&dyn std::fmt::Debug> for Value {
    fn from(val: &dyn std::fmt::Debug) -> Self {
        value::Inner::StrVal(format!("{:?}", val)).into()
    }
}

impl From<Value> for JsonValue {
    fn from(val: Value) -> Self {
        if let Some(inner) = val.inner {
            match inner {
                ValueEnum::StrVal(v) => JsonValue::String(v),
                ValueEnum::U64Val(v) => JsonValue::Number(Number::from(v)),
                ValueEnum::I64Val(v) => JsonValue::Number(Number::from(v)),
                ValueEnum::BoolVal(v) => JsonValue::Bool(v),
            }
        } else {
            JsonValue::Null
        }
    }
}
