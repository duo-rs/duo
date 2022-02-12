tonic::include_proto!("rs.jage.common");

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
        value::Inner::DebugVal(format!("{:?}", val)).into()
    }
}
