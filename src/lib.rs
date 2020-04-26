//! How is this organized?
//!
//! - lib.rs - Session & Datom
//! - matter.rs - Projection? Borrows patterns into datom sets & constraints
//!             - 3-tuple of variable or entity, variable or attribute, variable or value
//! - sql.rs - render a SQL query for a Projection
//!
//! The names are terrible, iirc there are two interesting ways to view a list
//! of patterns as a graph.
//!
//! 1. bijection between graph vectors and variables
//! 2. bijection between graph vectors and datom sets/the 3-tuple pattern
//!
//! The Projection sort of models #2.
//!
//! (?e _ (cast date $))
//! (?e _ #date $)
//! (?e _ $::date))
//!
//!-----------------------------------------------------------------------------
//! DSL:
//!
//! Only bad people like avocado.
//! (All people who like avocado are bad.)
//! (All things with a positive opinion of avocados are bad.)
//! (Opinion objects and subjects are unique together?)
//!
//! where  (?o :opinion/subject ?p)
//!        (?o :opinion/object ?a)
//!        (?o :opinion/disposition "positive")
//!        (?a :thing/name "avocado")
//! assert (?p :person/alignment "bad")
//!
//! where  (?p :person/name "Spongebob")
//!        (?n :pants/shape "square")
//! assert (?p :person/pants ?n)
//!
//! where (?p :person/name "Spongebob")
//!       (?p :person/pants ?n)
//!       (?p :pants/shape ?s)
//! shape {?p ?s}
//!
//! Show three most recent articles, the authors, and three most recent comments.
//!
//! shape * (
//!     :article/title
//!     {:article/author :person/name}
//!     {(:article/comments limit 3) :comment/content}
//!     )
//!
//! where ?c :person/dob ?d
//!       ?p :person/parent ?c
//!   for "Jan 1 2020" =.. ?d .. "Jan 1 2021"
//!  show ?c :person/name
//!       ?p :person/name
//! (or)
//! where ?c :person/dob ?d | "Jan 1 2020" <= ?d < "Jan 1 2021"
//!       ?p :person/parent ?c
//!
#![allow(dead_code)]
#![allow(unused_imports)]
// ^^^ todo; get your shit together ^^^
#![allow(clippy::many_single_char_names)]
#![allow(clippy::redundant_closure)]
#![allow(clippy::needless_return)]

mod matter;
mod sql;

use std::collections::HashMap;
use std::{
    borrow::{Borrow, Cow},
    convert::TryFrom,
    fmt,
    ops::Deref,
};

// use rusqlite::types::Value as SqlValue;

use anyhow::Context;

pub use matter::{Ordering, Pattern, Projection, Selection, VariableOr};

pub(crate) const SCHEMA: &str = include_str!("../schema.sql");

/// Hard coded entity row ID for attribute of the identifier "entity/uuid" ...
pub(crate) const ENTITY_UUID_ROWID: i64 = -1;
/// Hard coded entity row ID for attribute of the identifier "attr/ident" ...
/// This is referenced _literally_ in the "attributes" database view.
pub(crate) const ATTR_IDENT_ROWID: i64 = -2;

pub(crate) const T_ENTITY: i64 = -1;
/// This is referenced _literally_ in the "attributes" database view.
pub(crate) const T_ATTRIBUTE: i64 = -2;

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
pub struct EntityName(#[serde(deserialize_with = "deserialize_with_entity_prefix")] uuid::Uuid);

impl Deref for EntityName {
    type Target = uuid::Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<I> From<I> for EntityName
where
    I: Into<uuid::Uuid>,
{
    fn from(i: I) -> Self {
        EntityName(i.into())
    }
}

impl rusqlite::ToSql for EntityName {
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
    fn from_static(s: &'static str) -> Self {
        AttributeName::try_from(Cow::from(s)).unwrap()
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

impl RowIdOr<EntityName> for EntityName {
    fn row_id_or(&self) -> either::Either<i64, &EntityName> {
        either::Right(self)
    }
}

impl<'a> RowIdOr<AttributeName<'a>> for AttributeName<'a> {
    fn row_id_or(&self) -> either::Either<i64, &AttributeName<'a>> {
        either::Right(self)
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Entity {
    rowid: i64,
    pub name: EntityName,
}

/// Not implemented for all T, because Entity and Attribute rowids are not the same!
impl RowIdOr<EntityName> for Entity {
    fn row_id_or(&self) -> either::Either<i64, &EntityName> {
        either::Left(self.rowid)
    }
}

impl Deref for Entity {
    type Target = EntityName; //uuid::Uuid;

    fn deref(&self) -> &Self::Target {
        &self.name
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
    fn t_and_bind(&self) -> (i64, &'static str) {
        match self {
            Affinity::Entity => (T_ENTITY, sql::bind_entity()),
            Affinity::Attribute => (T_ATTRIBUTE, sql::bind_attribute()),
            Affinity::Other(t) => (*t, "?"),
        }
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

impl Assertable for EntityName {
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

    fn read_affinity_value<'a>(c: &mut sql::RowCursor<'a>) -> rusqlite::Result<Self>
    where
        Self: Sized,
    {
        let affinity = Affinity::try_from(c.get::<i64>()?)
            .map_err(|e| anyhow::format_err!("affinity out of range: {}", e).into())
            .map_err(|e| rusqlite::types::FromSqlError::Other(e))?;
        let v_ref = c.get_raw();
        let v = Self::from_affinity_value(affinity, v_ref)?;
        Ok(v)
    }
}

#[derive(Clone, PartialEq, Debug, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum Value {
    /// Anything that looks like a uuid ends up in here
    Entity(EntityName),
    #[serde(deserialize_with = "deserialize_with_attribute_prefix")]
    Attribute(String),
    Null,
    Integer(i64),
    Real(f64),
    // Instant(chrono::DateTime<chrono::Utc>),
    Text(String),
    Blob(Vec<u8>),
}

impl From<Entity> for Value {
    fn from(e: Entity) -> Self {
        Value::Entity(e.name)
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
            Affinity::Entity => Value::Entity(EntityName(uuid::Uuid::column_result(v)?)),
            Affinity::Attribute => Value::Attribute(String::column_result(v)?),
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
            Value::Attribute(v) => v.to_sql(),
            Value::Null => rusqlite::types::Null.to_sql(),
            Value::Integer(v) => v.to_sql(),
            Value::Real(v) => v.to_sql(),
            Value::Text(v) => v.to_sql(),
            Value::Blob(v) => v.to_sql(),
        }
    }
}

impl Value {}

pub fn deserialize_with_entity_prefix<'de, D>(deserializer: D) -> Result<uuid::Uuid, D::Error>
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

// pub trait SqlValue: rusqlite::types::FromSql + rusqlite::types::ToSql {}

// impl<T> SqlValue for T where T: rusqlite::types::FromSql + rusqlite::types::ToSql {}

/// An entity-attribute-value tuple thing.
///
/// attribute is parameterized as to use a owned or borrowed string TODO this is stupid
#[derive(Debug)]
pub struct Datom<'a, T> {
    pub entity: EntityName,
    pub attribute: AttributeName<'a>, //&'s str,
    pub value: T,
}

impl<'a, T> Datom<'a, T> {
    pub fn from_eav(entity: EntityName, attribute: AttributeName<'a>, value: T) -> Self {
        Self {
            entity,
            attribute,
            value,
        }
    }

    pub fn eav(self) -> (EntityName, AttributeName<'a>, T) {
        (self.entity, self.attribute, self.value)
    }

    pub fn from_row(row: &rusqlite::Row) -> rusqlite::Result<Self>
    where
        T: FromAffinityValue,
    {
        Self::from_columns(&mut sql::RowCursor::from(row))
    }

    /// Expects a columns in the form of `e a is_ref v`
    pub fn from_columns<'c>(c: &mut sql::RowCursor<'c>) -> rusqlite::Result<Self>
    where
        T: FromAffinityValue,
    {
        let e = EntityName(c.get()?);
        let a = AttributeName(c.get::<String>()?.into());
        let v = T::read_affinity_value(c)?;
        Ok(Datom::from_eav(e, a, v))
    }
}

/// Wraps a rusqlite transaction to provide this crate's semantics to sqlite.
pub struct Session<'db> {
    tx: rusqlite::Transaction<'db>,
}

impl<'db> Session<'db> {
    pub fn new(db: &'db mut rusqlite::Connection) -> rusqlite::Result<Self> {
        let tx = db.transaction()?;
        // This view will be attached to the lifetime of the connection, not the transaction ...
        // ... so guard with IF NOT EXISTS.
        tx.execute(
            &format!(
                "CREATE TEMPORARY VIEW IF NOT EXISTS
                    attributes (rowid, ident)
                 AS select e, v FROM datoms WHERE a = {} AND t = {}",
                ATTR_IDENT_ROWID, T_ATTRIBUTE,
            ),
            rusqlite::NO_PARAMS,
        )?;
        Ok(Session { tx })
    }

    pub fn init_schema(db: &'db mut rusqlite::Connection) -> rusqlite::Result<()> {
        let tx = db.transaction()?;
        tx.execute_batch(SCHEMA)?;

        // Create the initial attributes, this isn't part of SCHEMA because sqlite can't make its
        // own UUIDs (unless we just use random 128 bit blobs ...) and I'm avoiding database
        // functions for now.

        // entity/uuid & attr/ident
        tx.execute(
            "INSERT INTO entities (rowid, uuid) VALUES (?, ?), (?, ?)",
            rusqlite::params![
                ENTITY_UUID_ROWID,
                uuid::Uuid::new_v4(),
                ATTR_IDENT_ROWID,
                uuid::Uuid::new_v4(),
            ],
        )
        .map(|n| assert_eq!(n, 2))?;

        tx.execute(
            "INSERT INTO datoms (e, a, t, v)
                  VALUES (?, ?, ?, ?)
                       , (?, ?, ?, ?)",
            rusqlite::params![
                ENTITY_UUID_ROWID, // the entity/uuid attribute's rowid
                ATTR_IDENT_ROWID,  // has a attr/ident attribue
                T_ATTRIBUTE,       //
                "entity/uuid",     // of this string
                ATTR_IDENT_ROWID,
                ATTR_IDENT_ROWID,
                T_ATTRIBUTE,
                "attr/ident",
            ],
        )
        .map(|n| assert_eq!(n, 2))?;

        tx.commit()?;

        Ok(())
    }

    pub fn commit(self) -> rusqlite::Result<()> {
        self.tx.commit()
    }

    pub fn new_entity(&self) -> rusqlite::Result<Entity> {
        let uuid = uuid::Uuid::new_v4();
        self.new_entity_at(EntityName(uuid))
    }

    pub fn new_entity_at(&self, name: EntityName) -> rusqlite::Result<Entity> {
        let n = self.tx.execute(
            "INSERT INTO entities (uuid) VALUES (?)",
            rusqlite::params![name],
        )?;
        assert_eq!(n, 1);
        let rowid = self.tx.last_insert_rowid();
        Ok(Entity { rowid, name })
    }

    pub fn find_entity(&self, name: EntityName) -> rusqlite::Result<Option<Entity>> {
        use rusqlite::OptionalExtension;
        self.tx
            .query_row(
                "SELECT rowid FROM entities WHERE uuid = ?",
                rusqlite::params![name],
                |row| row.get(0),
            )
            .optional()?
            .map(|rowid| Ok(Entity { rowid, name }))
            .transpose()
    }

    pub fn new_attribute<'a, S>(&self, ident: S) -> rusqlite::Result<Attribute<'a>>
    where
        S: Into<AttributeName<'a>>,
    {
        let ident: AttributeName = ident.into();

        let entity = self.new_entity()?;

        let n = self.tx.execute(
            "INSERT INTO datoms (e, a, t, v) VALUES (?, ?, ?, ?)",
            rusqlite::params![entity.rowid, ATTR_IDENT_ROWID, T_ATTRIBUTE, &ident],
        )?;
        assert_eq!(n, 1);

        Ok(Attribute { entity, ident })
    }

    // todo implement for generic Assertable ToSql thing?
    pub fn assert_obj(&self, obj: &HashMap<AttributeName, Value>) -> rusqlite::Result<Entity> {
        let entity_uuid = AttributeName::from_static(":entity/uuid");

        // application-level upsert so that we can get the rowid if the entity exists
        let entity = match obj.get(&entity_uuid) {
            Some(Value::Entity(name)) => match self.find_entity(*name)? {
                Some(entity) => entity,
                None => self.new_entity_at(*name)?,
            },
            Some(_) => panic!("expected uuid for :entity/uuid"),
            None => self.new_entity()?,
        };

        for (a, v) in obj.iter() {
            if a == &entity_uuid {
                continue;
            }
            self.assert(&entity, a, v)?;
        }

        Ok(entity)
    }

    pub fn assert<'z, E, A, T>(&self, e: &E, a: &A, v: &T) -> rusqlite::Result<()>
    where
        E: RowIdOr<EntityName>,
        A: RowIdOr<AttributeName<'z>>,
        T: Assertable + rusqlite::ToSql + fmt::Debug,
    {
        let e_variant = e.row_id_or();
        let (e, e_bind_str) = match e_variant.as_ref() {
            either::Left(rowid) => (rowid as &dyn rusqlite::ToSql, "?"),
            either::Right(e) => (e as &dyn rusqlite::ToSql, sql::bind_entity()),
        };

        let a_variant = a.row_id_or();
        let (a, a_bind_str) = match a_variant.as_ref() {
            either::Left(rowid) => (rowid as &dyn rusqlite::ToSql, "?"),
            either::Right(a) => (a as &dyn rusqlite::ToSql, sql::bind_attribute()),
        };

        let (t, v_bind_str) = v.affinity().t_and_bind();

        let sql = format!(
            r#"INSERT INTO datoms (e, a, t, v)
                    VALUES ( {e}, {a}, ?, {v} )"#,
            e = e_bind_str,
            a = a_bind_str,
            v = v_bind_str,
        );

        let mut stmt = self.tx.prepare(&sql)?;
        let n = stmt.execute(&[
            e as &dyn rusqlite::ToSql,
            a as &dyn rusqlite::ToSql,
            &t as &dyn rusqlite::ToSql,
            v,
        ])?;
        assert_eq!(n, 1);
        Ok(())
    }

    /// for debugging ... use with T as rusqlite::types::Value
    fn all_datoms<T>(&self) -> rusqlite::Result<Vec<Datom<T>>>
    where
        T: FromAffinityValue,
    {
        // TODO this ignores the t value
        let sql = r#"
            SELECT entities.uuid, attributes.ident, t, v
              FROM datoms
              JOIN entities   ON datoms.e = entities.rowid
              JOIN attributes ON datoms.a = attributes.rowid"#;
        let mut stmt = self.tx.prepare(sql)?;
        let rows = stmt.query_map(rusqlite::NO_PARAMS, Datom::from_row)?;
        rows.collect::<_>()
    }

    fn select(&self, s: &Selection<rusqlite::types::Value>) -> rusqlite::Result<Vec<Vec<Value>>> {
        let mut q = sql::Query::default();
        let _ = sql::selection_sql(&s, &mut q);

        let mut stmt = self.tx.prepare(q.as_str())?;

        let rows = stmt.query_map(q.params(), |row| {
            let mut c = sql::RowCursor::from(row);
            s.columns()
                .iter()
                .map(|loc| -> rusqlite::Result<Value> {
                    use matter::Field;
                    Ok(match loc.field {
                        Field::Entity => Value::Entity(EntityName(c.get()?)),
                        Field::Attribute => Value::Attribute(c.get()?),
                        Field::Value => Value::read_affinity_value(&mut c)?,
                    })
                })
                .collect::<rusqlite::Result<Vec<Value>>>()
        })?;

        rows.collect()
    }
}

pub fn query_attribute_map<'db, 'a, V>(
    attrs: &matter::AttributeMap<'a, V>,
    sess: &mut Session<'db>,
) -> rusqlite::Result<Vec<HashMap<&'a AttributeName<'a>, V>>>
where
    V: FromAffinityValue + Assertable + rusqlite::ToSql + fmt::Debug,
{
    let mut q = sql::Query::default();

    sql::attribute_map_sql(attrs, &mut q).unwrap();

    eprintln!("[DEBUG] sql:");
    eprintln!("{}", q.as_str());
    eprintln!("[DEBUG] par: {:?}", q.params());

    let mut stmt = sess.tx.prepare(q.as_str())?;

    let rows = stmt.query_map(q.params(), |row| {
        let mut c = sql::RowCursor::from(row);
        attrs
            .map
            .iter()
            .map(|(attr, _)| -> rusqlite::Result<(_, _)> {
                let value = V::read_affinity_value(&mut c)?;
                Ok((*attr, value))
            })
            .collect::<rusqlite::Result<HashMap<_, _>>>()
    })?;

    let wow = rows.collect::<rusqlite::Result<Vec<HashMap<&AttributeName, _>>>>()?;
    Ok(wow)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn deserialization() -> Result<()> {
        use serde::{de::value::Error, de::IntoDeserializer, Deserialize};

        assert_eq!(
            value_from("#03b48f17-7f0a-455f-af1d-ba946f762090")?,
            Value::Entity(
                "03b48f17-7f0a-455f-af1d-ba946f762090"
                    .parse()
                    .map(EntityName)
                    .unwrap()
            )
        );

        assert_eq!(
            value_from(":entity/uuid")?,
            Value::Attribute(":entity/uuid".to_owned())
        );

        assert_eq!(
            value_from("anything else")?,
            Value::Text("anything else".to_owned())
        );

        fn value_from<T>(t: T) -> Result<Value, Error>
        where
            for<'de> T: IntoDeserializer<'de>,
        {
            let wat = IntoDeserializer::<Error>::into_deserializer(t);
            Value::deserialize(wat)
        }

        Ok(())
    }

    #[test]
    fn attribute_sql() -> Result<()> {
        use rusqlite::types::{FromSql, FromSqlResult, ToSql, ToSqlOutput, ValueRef};

        let attr = AttributeName::from_static(":foo/bar");
        assert_eq!(attr.to_sql()?, "foo/bar".to_sql()?);

        let v = ValueRef::Text("foo/bar".as_bytes());
        assert_eq!(attr, AttributeName::column_result(v)?);

        Ok(())
    }

    pub(crate) fn test_conn() -> Result<rusqlite::Connection> {
        let mut conn = rusqlite::Connection::open_in_memory()?;
        Session::init_schema(&mut conn)?;
        Ok(conn)
    }

    pub(crate) fn goodbooks() -> Result<rusqlite::Connection> {
        let mut db = test_conn()?;
        let s = Session::new(&mut db)?;

        let mut books = HashMap::<i64, EntityName>::new();

        {
            let title = s.new_attribute(":book/title")?;
            let avg_rating = s.new_attribute(":book/avg-rating")?;
            let isbn = s.new_attribute(":book/isbn")?;
            let authors = s.new_attribute(":book/authors")?;

            let mut r = csv::Reader::from_path("goodbooks-10k/books.csv")?;
            for result in r.deserialize().take(4000) {
                let book: Book = result?;

                let e = s.new_entity()?;
                s.assert(&e, &title, &book.title)?;
                s.assert(&e, &avg_rating, &book.average_rating)?;
                s.assert(&e, &isbn, &book.isbn)?;
                s.assert(&e, &authors, &book.authors)?;

                books.insert(book.book_id, e.name);
            }
        }

        {
            let score = s.new_attribute(":rating/score")?; // aka one-to-five
            let book = s.new_attribute(":rating/book")?;
            let user = s.new_attribute(":rating/user")?;

            let mut r = csv::Reader::from_path("goodbooks-10k/ratings.csv")?;
            for result in r.deserialize().take(8000) {
                let rating: Rating = result?;

                // if this is a rating for a book we didn't add, ignore it
                let book_ref = match books.get(&rating.book_id) {
                    None => continue,
                    Some(v) => v,
                };

                let e = s.new_entity()?;
                s.assert(&e, &book, book_ref)?;
                s.assert(&e, &user, &rating.user_id)?;
                s.assert(&e, &score, &rating.rating)?;
            }
        }

        s.commit()?;
        return Ok(db);

        #[derive(Debug, serde::Deserialize)]
        struct Book {
            book_id: i64,
            title: String,
            isbn: String,
            authors: String,
            average_rating: f64,
        }

        #[derive(Debug, serde::Deserialize)]
        struct Rating {
            user_id: i64,
            book_id: i64,
            rating: i64,
        }
    }

    #[test]
    fn wow() -> Result<()> {
        let mut db = goodbooks()?;

        let n_datoms: i64 =
            db.query_row("SELECT count(*) FROM datoms", rusqlite::NO_PARAMS, |row| {
                row.get(0)
            })?;
        eprintln!("there are {} datoms ... wow", n_datoms);

        let sess = Session::new(&mut db)?;

        // eprintln!("all datoms: {:#?}", sess.all_datoms::<Value>());

        let patterns = vec![
            // pat!(?r "rating/book" ?b),
            pat!(?b ":book/avg-rating" ?v),
            // pat!(?b "book/title" ?t),
        ];
        let max_rating = 4.0.into();

        let mut p = Projection::<rusqlite::types::Value>::default();
        p.add_patterns(&patterns);
        p.add_constraint(
            p.variable("v")
                .cloned()
                .unwrap()
                .le(matter::Concept::Value(&max_rating)),
        );

        // let book = p.variable("b").cloned().unwrap();
        let attrs = vec![
            // "entity/uuid".into(),
            AttributeName::from_static(":book/title"),
            AttributeName::from_static(":book/isbn"),
            AttributeName::from_static(":book/avg-rating"),
        ];
        let mut attrs = p.attribute_map("b", &attrs);
        attrs.order_by.push(attrs.map[2].1.value_field().desc());
        attrs.limit = 12;

        // eprintln!("{:#?}", attrs_map);
        let mut q = sql::Query::default();
        sql::attribute_map_sql(&attrs, &mut q).unwrap();
        eprintln!("{}", q);
        eprintln!("{:?}", q.params());

        let mut stmt = sess.tx.prepare(q.as_str())?;

        let rows = stmt.query_map(q.params(), |row| {
            let mut c = sql::RowCursor::from(row);
            attrs
                .map
                .iter()
                .map(|(attr, _)| -> rusqlite::Result<(_, _)> {
                    let value = Value::read_affinity_value(&mut c)?;
                    Ok((*attr, value))
                })
                .collect::<rusqlite::Result<HashMap<_, _>>>()
        })?;

        let wow = rows.collect::<rusqlite::Result<Vec<HashMap<&AttributeName, Value>>>>()?;

        let jaysons = serde_json::to_string_pretty(&wow)?;
        println!("{}", jaysons);

        Ok(())
    }
}
