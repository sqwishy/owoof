use std::{
    borrow::{Borrow, Cow},
    collections::HashMap,
    convert::TryFrom,
    ops::Deref,
};

use anyhow::Context;

use crate::{sql, T_ATTRIBUTE, T_ENTITY};

pub static ENTITY_UUID: AttributeName = AttributeName(Cow::Borrowed(":entity/uuid"));

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
pub struct EntityId(#[serde(with = "crate::types::_serde::entity")] uuid::Uuid);

impl Deref for EntityId {
    type Target = uuid::Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<I> From<I> for EntityId
where
    I: Into<uuid::Uuid>,
{
    fn from(i: I) -> Self {
        EntityId(i.into())
    }
}

impl rusqlite::ToSql for EntityId {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput> {
        self.0.to_sql()
    }
}

#[derive(Clone, Eq, PartialEq, Hash, Debug, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct AttributeName<'a>(
    #[serde(deserialize_with = "deserialize_cow_with_attribute_prefix")] Cow<'a, str>,
);

impl<'a> AttributeName<'a> {
    pub fn from_static(s: &'static str) -> Self {
        AttributeName::try_from(Cow::Borrowed(s)).unwrap()
    }
}

impl<'a> Deref for AttributeName<'a> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0.borrow()
    }
}

impl From<&'static str> for AttributeName<'static> {
    fn from(s: &'static str) -> Self {
        AttributeName::from_static(s)
    }
}

impl<'a> From<Attribute<'a>> for AttributeName<'a> {
    fn from(attr: Attribute<'a>) -> Self {
        attr.ident
    }
}

impl<'a> TryFrom<Cow<'a, str>> for AttributeName<'a> {
    type Error = String;

    fn try_from(s: Cow<'a, str>) -> Result<Self, Self::Error> {
        if let Some(idx) = s.find(|s: char| s.is_ascii_whitespace()) {
            return Err(format!("unexpected whitespace at {}", idx));
        }

        if !s.starts_with(':') {
            if let Some(c) = s.get(0..1) {
                return Err(format!("expected leading ':' found '{}'", c));
            } else {
                return Err("expected leading ':' found nothing".to_owned());
            }
        }

        Ok(AttributeName(s))
    }
}

impl<'a> rusqlite::ToSql for AttributeName<'a> {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput> {
        let (colon, name) = self.0.split_at(1);
        assert_eq!(colon, ":");
        name.to_sql()
    }
}

impl<'a> rusqlite::types::FromSql for AttributeName<'a> {
    fn column_result(value: rusqlite::types::ValueRef) -> rusqlite::types::FromSqlResult<Self> {
        value.as_str().map(|s| {
            let mut n = String::with_capacity(1 + s.len());
            n.push(':');
            n.push_str(s);
            AttributeName(Cow::from(n))
        })
    }
}

pub trait RowIdOr<T> {
    fn row_id_or(&self) -> either::Either<i64, &T>;
}

/// TODO
#[derive(Debug, Copy, Clone)]
pub(crate) struct RowId(pub i64);

impl<T> RowIdOr<T> for RowId {
    fn row_id_or(&self) -> either::Either<i64, &T> {
        either::Left(self.0)
    }
}

impl RowIdOr<EntityId> for EntityId {
    fn row_id_or(&self) -> either::Either<i64, &EntityId> {
        either::Right(self)
    }
}

impl RowIdOr<EntityId> for Attribute<'_> {
    fn row_id_or(&self) -> either::Either<i64, &EntityId> {
        either::Right(&self.entity.id)
    }
}

impl<'a> RowIdOr<AttributeName<'a>> for AttributeName<'a> {
    fn row_id_or(&self) -> either::Either<i64, &AttributeName<'a>> {
        either::Right(self)
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Entity {
    pub(crate) rowid: i64,
    pub id: EntityId,
}

/// Not implemented for all T, because Entity and Attribute rowids are not the same!
impl RowIdOr<EntityId> for Entity {
    fn row_id_or(&self) -> either::Either<i64, &EntityId> {
        either::Left(self.rowid)
    }
}

impl Deref for Entity {
    type Target = EntityId; //uuid::Uuid;

    fn deref(&self) -> &Self::Target {
        &self.id
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Attribute<'a> {
    pub entity: Entity,
    pub ident: AttributeName<'a>,
}

/// Not implemented for all T, because Entity and Attribute rowids are not the same!
impl<'a> RowIdOr<AttributeName<'a>> for Attribute<'a> {
    fn row_id_or<'s>(&'s self) -> either::Either<i64, &'s AttributeName<'a>> {
        either::Left(self.entity.rowid)
    }
}

impl<'a> Deref for Attribute<'a> {
    type Target = AttributeName<'a>;

    fn deref(&self) -> &Self::Target {
        &self.ident
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum Affinity {
    Entity,
    Attribute,
    Other(i64),
}

impl Affinity {
    pub(crate) fn t_and_bind(&self) -> (&i64, &'static str) {
        match self {
            Affinity::Entity => (&T_ENTITY, sql::bind_entity()),
            Affinity::Attribute => (&T_ATTRIBUTE, "?"),
            Affinity::Other(t) => (t, "?"),
        }
    }
}

impl rusqlite::types::FromSql for Affinity {
    fn column_result(v: rusqlite::types::ValueRef) -> rusqlite::types::FromSqlResult<Self> {
        Affinity::try_from(v.as_i64()?)
            .map_err(|e| anyhow::format_err!("affinity out of range: {}", e).into())
            .map_err(|e| rusqlite::types::FromSqlError::Other(e))
    }
}

impl TryFrom<i64> for Affinity {
    type Error = i64;

    fn try_from(t: i64) -> Result<Self, Self::Error> {
        match t {
            T_ENTITY => Ok(Affinity::Entity),
            T_ATTRIBUTE => Ok(Affinity::Attribute),
            _ if t >= 0 => Ok(Affinity::Other(t)),
            _ => Err(t),
        }
    }
}

/// Has an affinity ... TODO rename this
pub trait Assertable {
    fn affinity(&self) -> Affinity;
}

impl Assertable for EntityId {
    fn affinity(&self) -> Affinity {
        Affinity::Entity
    }
}

impl<'a> Assertable for AttributeName<'a> {
    fn affinity(&self) -> Affinity {
        Affinity::Attribute
    }
}

impl<'a, I> Assertable for I
where
    I: Into<rusqlite::types::Value>,
{
    fn affinity(&self) -> Affinity {
        Affinity::Other(0)
    }
}

pub trait FromAffinityValue {
    fn from_affinity_value(
        _: Affinity,
        _: rusqlite::types::ValueRef,
    ) -> rusqlite::types::FromSqlResult<Self>
    where
        Self: Sized;

    fn read_with_affinity<'a>(
        affinity: Affinity,
        v_ref: rusqlite::types::ValueRef,
    ) -> rusqlite::Result<Self>
    where
        Self: Sized,
    {
        Self::from_affinity_value(affinity, v_ref)
            .with_context(|| format!("reading value {:?} with affinity {:?}", v_ref, affinity))
            .map_err(|e| rusqlite::types::FromSqlError::Other(e.into()).into())
    }

    fn read_affinity_value<'a>(c: &mut sql::RowCursor<'a>) -> rusqlite::Result<Self>
    where
        Self: Sized,
    {
        let affinity = c.get::<Affinity>()?;
        let v_ref = c.get_raw();
        let v = Self::read_with_affinity(affinity, v_ref)?;
        Ok(v)
    }
}

#[derive(Clone, PartialEq, Debug, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum Value {
    /// Anything that looks like a uuid ends up in here
    Entity(EntityId),
    #[serde(deserialize_with = "deserialize_with_attribute_prefix")]
    Attribute(String),
    Null,
    Integer(i64),
    Real(f64),
    // Instant(chrono::DateTime<chrono::Utc>),
    // Bool(bool), ???
    Text(String),
    Blob(Vec<u8>),
}

impl From<Entity> for Value {
    fn from(e: Entity) -> Self {
        Value::Entity(e.id)
    }
}

impl From<rusqlite::types::Value> for Value {
    fn from(v: rusqlite::types::Value) -> Self {
        match v {
            rusqlite::types::Value::Null => Value::Null,
            rusqlite::types::Value::Integer(i) => Value::Integer(i),
            rusqlite::types::Value::Real(i) => Value::Real(i),
            rusqlite::types::Value::Text(i) => Value::Text(i),
            rusqlite::types::Value::Blob(i) => Value::Blob(i),
        }
    }
}

impl FromAffinityValue for Value {
    fn from_affinity_value(
        t: Affinity,
        v: rusqlite::types::ValueRef,
    ) -> rusqlite::types::FromSqlResult<Self>
    where
        Self: Sized,
    {
        use rusqlite::types::FromSql;
        Ok(match t {
            Affinity::Entity => Value::Entity(EntityId(uuid::Uuid::column_result(v)?)),
            Affinity::Attribute => {
                Value::Attribute(AttributeName::column_result(v)?.0.into_owned())
            }
            Affinity::Other(_) => Value::from(rusqlite::types::Value::column_result(v)?),
        })
    }
}

impl Assertable for Value {
    fn affinity(&self) -> Affinity {
        match self {
            Value::Entity(_) => Affinity::Entity,
            Value::Attribute(_) => Affinity::Attribute,
            Value::Null | Value::Integer(_) | Value::Real(_) | Value::Text(_) | Value::Blob(_) => {
                Affinity::Other(0)
            }
        }
    }
}

impl rusqlite::ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput> {
        match self {
            Value::Entity(v) => v.to_sql(),
            Value::Attribute(v) => {
                let (colon, name) = v.split_at(1);
                assert_eq!(colon, ":");
                name.to_sql()
            }
            Value::Null => rusqlite::types::Null.to_sql(),
            Value::Integer(v) => v.to_sql(),
            Value::Real(v) => v.to_sql(),
            Value::Text(v) => v.to_sql(),
            Value::Blob(v) => v.to_sql(),
        }
    }
}

pub mod _serde {
    pub mod entity {
        pub fn serialize<S>(e: &uuid::Uuid, ser: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            // nasty ......
            let buf = &mut [0; 37];
            buf[0] = 0x23; // #
            e.to_hyphenated_ref().encode_lower(&mut buf[1..]);
            let s = std::str::from_utf8(buf).unwrap();
            ser.serialize_str(s)
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<uuid::Uuid, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            use serde::Deserialize;
            let s = String::deserialize(deserializer)?;
            if !s.starts_with('#') {
                return Err(serde::de::Error::custom(format!(
                    "expected '#' found {}",
                    s.get(0..1).unwrap_or("nothing")
                )));
            }
            let (_, uuid) = s.split_at(1);
            return uuid.parse().map_err(serde::de::Error::custom);
        }
    }
}

pub fn deserialize_with_attribute_prefix<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let s = String::deserialize(deserializer)?;
    if !s.starts_with(':') {
        return Err(serde::de::Error::custom(format!(
            "expected ':' found {}",
            s.get(0..1).unwrap_or("nothing")
        )));
    }
    return Ok(s);
}

pub fn deserialize_cow_with_attribute_prefix<'de, 'a, D>(
    deserializer: D,
) -> Result<Cow<'a, str>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_with_attribute_prefix(deserializer).map(Cow::from)
}

// #[derive(Clone, PartialEq, Debug, serde::Serialize, serde::Deserialize)]
// #[serde(untagged)]
// pub enum ???Value {
//     Map(HashMap<String, Value>),
//     Scalar(Value),
// }

// #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
// struct MemeTime<T>(T); // &'a chrono::DateTime<chrono::Utc>);
//
// impl<T> Deref for MemeTime<T> {
//     type Target = T;
//
//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }
//
// impl rusqlite::types::ToSql for MemeTime<&chrono::DateTime<chrono::Utc>> {
//     fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput> {
//         Ok(self.timestamp_millis().into())
//     }
// }
//
// impl rusqlite::types::FromSql for MemeTime<chrono::DateTime<chrono::Utc>> {
//     fn column_result(value: rusqlite::types::ValueRef) -> rusqlite::types::FromSqlResult<Self> {
//         use chrono::TimeZone;
//         value.as_i64().and_then(|v| {
//             chrono::Utc
//                 .timestamp_millis_opt(v)
//                 .single()
//                 .ok_or_else(|| rusqlite::types::FromSqlError::OutOfRange(v))
//                 .map(chrono::DateTime::<chrono::Utc>::from)
//                 .map(MemeTime)
//         })
//     }
// }
