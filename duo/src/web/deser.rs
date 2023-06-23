use std::collections::HashMap;

use serde::de;
use serde_json::Value;
use time::{Duration, OffsetDateTime};

pub(super) fn option_ignore_error<'de, T, D>(d: D) -> Result<Option<T>, D::Error>
where
    T: de::Deserialize<'de>,
    D: de::Deserializer<'de>,
{
    Ok(T::deserialize(d).ok())
}

pub fn option_miscrosecond<'de, D>(d: D) -> Result<Option<OffsetDateTime>, D::Error>
where
    D: de::Deserializer<'de>,
{
    d.deserialize_option(OptionMicroSecondsTimestampVisitor)
}

pub fn map_list<'de, D>(d: D) -> Result<HashMap<String, Value>, D::Error>
where
    D: de::Deserializer<'de>,
{
    d.deserialize_any(ListValueVisitor)
}

pub(super) fn option_duration<'de, D>(d: D) -> Result<Option<Duration>, D::Error>
where
    D: de::Deserializer<'de>,
{
    d.deserialize_option(OptionDurationVisitor)
}

pub mod miscrosecond {
    use serde::{Deserializer, Serializer};
    use time::OffsetDateTime;

    use super::MicroSecondsTimestampVisitor;

    pub fn serialize<S>(time: &OffsetDateTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64((time.unix_timestamp_nanos() / 1000) as i64)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(MicroSecondsTimestampVisitor)
    }
}

pub mod level {
    use std::str::FromStr;

    use serde::{Deserialize, Deserializer, Serializer};
    use tracing::Level;

    #[allow(unused)]
    pub fn serialize<S>(level: &Level, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(level.as_str())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Level, D::Error>
    where
        D: Deserializer<'de>,
    {
        let level = String::deserialize(deserializer)?;
        Ok(Level::from_str(&level).expect("invalid level"))
    }
}

struct ListValueVisitor;

impl<'de> de::Visitor<'de> for ListValueVisitor {
    type Value = HashMap<String, Value>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "an array string")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let list = serde_json::from_str(v).unwrap();
        Ok(list)
    }
}

struct OptionMicroSecondsTimestampVisitor;

impl<'de> de::Visitor<'de> for OptionMicroSecondsTimestampVisitor {
    type Value = Option<OffsetDateTime>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a unix timestamp in microseconds or none")
    }

    fn visit_some<D>(self, d: D) -> Result<Self::Value, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        d.deserialize_i64(MicroSecondsTimestampVisitor).map(Some)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(None)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(None)
    }
}

struct MicroSecondsTimestampVisitor;

impl<'de> de::Visitor<'de> for MicroSecondsTimestampVisitor {
    type Value = OffsetDateTime;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a unix timestamp in microseconds")
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(
            OffsetDateTime::from_unix_timestamp_nanos((v * 1000) as i128)
                .expect("invalid timestamp format"),
        )
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(
            OffsetDateTime::from_unix_timestamp_nanos((v * 1000) as i128)
                .expect("invalid timestamp format"),
        )
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let timestamp = v.parse::<i64>().expect("invalid timestamp format");
        self.visit_i64(timestamp)
    }
}

struct OptionDurationVisitor;

impl<'de> de::Visitor<'de> for OptionDurationVisitor {
    type Value = Option<Duration>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a duration or none")
    }

    fn visit_some<D>(self, d: D) -> Result<Self::Value, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        Ok(d.deserialize_str(DurationVisitor).ok())
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(None)
    }
}

struct DurationVisitor;

impl<'de> de::Visitor<'de> for DurationVisitor {
    type Value = Duration;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a duration")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        parse_duration(v)
            .map_err(de::Error::custom)
            .map(Duration::microseconds)
    }
}

fn parse_duration(duration: &str) -> anyhow::Result<i64> {
    let duration = duration.to_lowercase();
    if let Some(d) = duration.strip_suffix("us") {
        Ok(d.parse()?)
    } else if let Some(d) = duration.strip_suffix("ms") {
        Ok(d.parse::<i64>()? * 1000)
    } else if let Some(d) = duration.strip_suffix('s') {
        Ok(d.parse::<i64>()? * 1_000_000)
    } else {
        anyhow::bail!("Invalid duration {}", duration)
    }
}
