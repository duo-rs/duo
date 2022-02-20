use serde::de;
use time::OffsetDateTime;

pub(super) fn option_ignore_error<'de, T, D>(d: D) -> Result<Option<T>, D::Error>
where
    T: de::Deserialize<'de>,
    D: de::Deserializer<'de>,
{
    Ok(T::deserialize(d).ok())
}

pub(super) fn option_miscrosecond<'de, D>(d: D) -> Result<Option<OffsetDateTime>, D::Error>
where
    D: de::Deserializer<'de>,
{
    d.deserialize_option(OptionMicroSecondsTimestampVisitor)
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

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let timestamp = v.parse::<i64>().expect("invalid timestamp format");
        self.visit_i64(timestamp)
    }
}
