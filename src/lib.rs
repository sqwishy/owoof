//! How is this organized?
//!
//! - lib.rs - Session
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

pub mod explain;
pub mod matter;
pub mod sql;
pub mod types;

use std::collections::HashMap;
use std::{
    borrow::{Borrow, Cow},
    convert::TryFrom,
    fmt,
    ops::Deref,
};

// use rusqlite::types::Value as SqlValue;

use anyhow::Context;
use uuid::Uuid;

use explain::{ExplainLine, Explanation, PlanExplainLine, PlanExplanation};
pub use matter::{AttributeMap, Location, Ordering, Pattern, Projection, Selection, VariableOr};
pub use types::{
    Affinity, Assertable, Attribute, AttributeName, Entity, EntityId, FromAffinityValue, RowIdOr,
    Value,
};

pub(crate) const SCHEMA: &str = include_str!("../schema.sql");

/// Hard coded entity row ID for attribute of the identifier "entity/uuid" ...
pub(crate) const ENTITY_UUID_ROWID: i64 = -1;
/// Hard coded entity row ID for attribute of the identifier "attr/ident" ...
/// This is referenced _literally_ in the "attributes" database view.
pub(crate) const ATTR_IDENT_ROWID: i64 = -2;
/// Hard coded entity row ID for attribute of the identifier "attr/unique" ...
pub(crate) const ATTR_UNIQUE_ROWID: i64 = -3;

pub(crate) static ENTITY_UUID: AttributeName = AttributeName::from_static_unchecked(":entity/uuid");
pub(crate) static ATTR_UNIQUE: AttributeName = AttributeName::from_static_unchecked(":attr/unique");

pub(crate) const T_ENTITY: i64 = -1;
/// This is referenced _literally_ in the "attributes" database view.
pub(crate) const T_ATTRIBUTE: i64 = -2;

/// https://sqlite.org/rescode.html#extrc
const SQLITE_CONSTRAINT_TRIGGER: i32 = 1811;
const SQLITE_CONSTRAINT_UNIQUE: i32 = 2067;

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum Error {
    #[error("attribute must be unique for the entity")]
    NotUniqueForEntity,
    #[error("value must be unique for the attribute")]
    NotUniqueForAttribute,
    #[error("{0} is immutable")]
    Immutable(&'static str),
    #[error("this value is protected from modification")]
    Protected,
    #[error("could not retract what doesn't exist")]
    NotFound,
    #[error("sql error")]
    Sql(#[from] rusqlite::Error),
}

impl Error {
    fn promote_rusqlite(e: rusqlite::Error) -> Self {
        match &e {
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error {
                    code: rusqlite::ffi::ErrorCode::ConstraintViolation,
                    extended_code,
                },
                Some(msg),
            ) => match (extended_code, msg.as_str()) {
                (&SQLITE_CONSTRAINT_TRIGGER, ":entity/id is immutable") => {
                    Error::Immutable(":entity/id")
                }
                (
                    &SQLITE_CONSTRAINT_UNIQUE,
                    "UNIQUE constraint failed: datoms.a, datoms.t, datoms.v",
                ) => Error::NotUniqueForAttribute,
                (&SQLITE_CONSTRAINT_UNIQUE, "UNIQUE constraint failed: datoms.e, datoms.a") => {
                    Error::NotUniqueForEntity
                }
                _ => e.into(),
            },
            _ => e.into(),
        }
    }
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Wraps a rusqlite transaction to provide this crate's semantics to sqlite.
pub struct Session<'db> {
    tx: rusqlite::Transaction<'db>,
}

impl<'db> Session<'db> {
    pub fn new(db: &'db mut rusqlite::Connection) -> rusqlite::Result<Self> {
        let mut tx = db.transaction()?;
        Self::setup_views(&mut tx)?;
        Ok(Session { tx })
    }

    /// Views are attached to the lifetime of the connection, not the transaction ...
    /// ... so guard with IF NOT EXISTS.
    fn setup_views(db: &mut rusqlite::Transaction<'_>) -> rusqlite::Result<()> {
        /* attributes view */
        db.execute(
            &format!(
                "CREATE TEMPORARY VIEW IF NOT EXISTS
                    attributes (rowid, ident)
                 AS SELECT e, v FROM datoms WHERE a = {} AND t = {}",
                ATTR_IDENT_ROWID, T_ATTRIBUTE,
            ),
            rusqlite::NO_PARAMS,
        )?;

        /* attr_unique view */
        db.execute(
            &format!(
                "CREATE TEMPORARY VIEW IF NOT EXISTS
                    attr_unique (rowid, v)
                 AS SELECT e, v FROM datoms WHERE a = {}",
                ATTR_UNIQUE_ROWID,
            ),
            rusqlite::NO_PARAMS,
        )?;

        /* :entity/id datoms */

        db.execute(
            &format!(
                "CREATE TEMPORARY TRIGGER
                  IF NOT EXISTS create_entity_uuid_datoms
                AFTER INSERT ON entities
                   FOR EACH ROW
                BEGIN
                    INSERT INTO datoms (e, a, t, v) VALUES (new.rowid, {}, {}, new.rowid);
                END",
                ENTITY_UUID_ROWID, T_ENTITY
            ),
            rusqlite::NO_PARAMS,
        )?;

        /* TODO I don't know that this ever happens,
         * we don't write queries that would do this, right? */
        db.execute(
            &format!(
                "CREATE TEMPORARY TRIGGER
                  IF NOT EXISTS immutable_entity_uuid_datoms
                BEFORE UPDATE ON entities
                    FOR EACH ROW WHEN new.a = {ENTITY_UUID_ROWID}
                BEGIN SELECT RAISE(FAIL, ':entity/id is immutable');
                END",
                ENTITY_UUID_ROWID = ENTITY_UUID_ROWID,
            ),
            rusqlite::NO_PARAMS,
        )?;

        Ok(())
    }

    pub fn init_schema(db: &'db mut rusqlite::Connection) -> rusqlite::Result<()> {
        use rusqlite::params;

        let mut tx = db.transaction()?;
        tx.execute_batch(SCHEMA)?;

        Self::setup_views(&mut tx)?;

        // Create the initial attributes, this isn't part of SCHEMA because sqlite can't make its
        // own UUIDs (unless we just use random 128 bit blobs ...) and I'm avoiding database
        // functions for now.

        for (ent_rowid, ident) in &[
            (ENTITY_UUID_ROWID, "entity/uuid"),
            (ATTR_IDENT_ROWID, "attr/ident"),
            (ATTR_UNIQUE_ROWID, "attr/unique"),
        ] {
            let p = params![ent_rowid, Uuid::new_v4()];
            tx.execute("INSERT INTO entities (rowid, uuid) VALUES (?, ?)", p)
                .map(|n| assert_eq!(n, 1))?;

            let p = params![ent_rowid, ATTR_IDENT_ROWID, T_ATTRIBUTE, ident];
            tx.execute("INSERT INTO datoms (e, a, t, v) VALUES (?, ?, ?, ?)", p)
                .map(|n| assert_eq!(n, 1))?;
        }

        // assert :attr/ident :attr/unique true
        let p = params![ATTR_IDENT_ROWID, ATTR_UNIQUE_ROWID, 0, true];
        tx.execute("INSERT INTO datoms (e, a, t, v) VALUES (?, ?, ?, ?)", p)
            .map(|n| assert_eq!(n, 1))?;

        // assert :entity/uuid :attr/unique true
        let p = params![ENTITY_UUID_ROWID, ATTR_UNIQUE_ROWID, 0, true];
        tx.execute("INSERT INTO datoms (e, a, t, v) VALUES (?, ?, ?, ?)", p)
            .map(|n| assert_eq!(n, 1))?;

        tx.execute(
            "UPDATE datoms SET unique_for_attribute = 1 WHERE a = ? OR a = ?",
            params![ATTR_IDENT_ROWID, ENTITY_UUID_ROWID],
        )?;

        tx.commit()?;

        Ok(())
    }

    /// Run PRAGMA optimize;
    ///
    /// https://sqlite.org/lang_analyze.html
    pub fn optimize(&self) -> rusqlite::Result<()> {
        let n = self
            .tx
            .execute("SELECT * FROM pragma_optimize()", rusqlite::NO_PARAMS)?;
        assert_eq!(n, 1);
        Ok(())
    }

    pub fn commit(self) -> rusqlite::Result<()> {
        self.tx.commit()
    }

    pub fn new_entity(&self) -> rusqlite::Result<Entity> {
        let uuid = uuid::Uuid::new_v4();
        self.new_entity_at(EntityId::from(uuid))
    }

    pub fn new_entity_at(&self, id: EntityId) -> rusqlite::Result<Entity> {
        let n = self.tx.execute(
            "INSERT INTO entities (uuid) VALUES (?)",
            rusqlite::params![id],
        )?;
        assert_eq!(n, 1);
        let rowid = self.tx.last_insert_rowid();
        Ok(Entity { rowid, id })
    }

    pub(crate) fn find_entity(&self, id: EntityId) -> rusqlite::Result<Option<Entity>> {
        use rusqlite::OptionalExtension;
        self.tx
            .query_row(
                "SELECT rowid FROM entities WHERE uuid = ?",
                rusqlite::params![id],
                |row| row.get(0),
            )
            .optional()?
            .map(|rowid| Ok(Entity { rowid, id }))
            .transpose()
    }

    pub fn new_attribute<'a, S>(&self, ident: S) -> Result<Attribute<'a>>
    where
        S: Into<AttributeName<'a>>,
    {
        let ident: AttributeName = ident.into();

        let entity = self.new_entity()?;

        use types::RowId;
        self.assert(&entity, &RowId(ATTR_IDENT_ROWID), &ident)?;

        Ok(Attribute { entity, ident })
    }

    // todo implement for generic Assertable ToSql thing?
    pub fn assert_obj(&self, obj: &HashMap<AttributeName, Value>) -> Result<Entity> {
        // application-level upsert so that we can get the rowid if the entity exists
        let entity = match obj.get(&ENTITY_UUID) {
            Some(Value::Entity(name)) => match self.find_entity(*name)? {
                Some(entity) => entity,
                None => self.new_entity_at(*name)?,
            },
            Some(_) => panic!("expected uuid for :entity/uuid"),
            None => self.new_entity()?,
        };

        for (a, v) in obj.iter() {
            if a == &ENTITY_UUID {
                continue;
            }
            self.assert(&entity, a, v)?;
        }

        Ok(entity)
    }

    pub fn assert<'z, E, A, T>(&self, e: &E, a: &A, v: &T) -> Result<()>
    where
        E: RowIdOr<EntityId>,
        A: RowIdOr<AttributeName<'z>>,
        T: Assertable + rusqlite::ToSql + fmt::Debug,
    {
        self.check_protected_datom(e, a)?;

        let e_variant = e.row_id_or();
        let a_variant = a.row_id_or();
        let v_affinity = v.affinity();

        /* If we're setting the :attr/unique attribute on some entity,
         * denormalize the new value to the unique_for_attribute column in datoms.
         *
         * This is done instead of a SQL trigger for performance reasons.
         * It appears as if having ANY triggers on a table does some slow-path to happen.
         * For example:
         *
         *       CREATE TRIGGER asdf
         *     BEFORE INSERT ON datoms
         *                 WHEN false
         *                BEGIN select 0; END;
         *
         * ... causes population of a test database to take ~180ms from ~130ms. */
        match a_variant.as_ref() {
            either::Left(&rowid) if rowid == ATTR_UNIQUE_ROWID => {
                self.set_datoms_unique_for_attribute(e, true)?;
            }
            either::Right(&a) if a == &ATTR_UNIQUE => {
                self.set_datoms_unique_for_attribute(e, true)?;
            }
            _ => (),
        };

        let (e, e_bind_str) = match e_variant.as_ref() {
            either::Left(rowid) => (rowid as &dyn rusqlite::ToSql, "?"),
            either::Right(e) => (e as &dyn rusqlite::ToSql, sql::bind_entity()),
        };

        let (a, a_bind_str) = match a_variant.as_ref() {
            either::Left(rowid) => (rowid as &dyn rusqlite::ToSql, "?"),
            either::Right(a) => (a as &dyn rusqlite::ToSql, sql::bind_attribute()),
        };

        let (t, v_bind_str) = v_affinity.t_and_bind();

        let sql = format!(
            r#"
        WITH yooneek AS (SELECT v FROM attr_unique WHERE rowid = {a} LIMIT 1)
        INSERT INTO datoms (e, a, t, v, unique_for_attribute)
             VALUES ( {e}
                    , {a}
                    , ?
                    , {v}
                    , ifnull((SELECT v FROM yooneek), false) )"#,
            e = e_bind_str,
            a = a_bind_str,
            v = v_bind_str,
        );
        // eprintln!("[DEBUG] sql: ...");
        // eprintln!("{}", sql);
        // eprintln!(
        //     "[DEBUG] par: {:?} {:?} {:?} {:?} {:?}",
        //     a.to_sql(),
        //     e.to_sql(),
        //     a.to_sql(),
        //     t,
        //     v,
        // );

        let mut stmt = self.tx.prepare(&sql)?;
        match stmt.execute(&[
            a as &dyn rusqlite::ToSql,
            e as &dyn rusqlite::ToSql,
            a as &dyn rusqlite::ToSql,
            t as &dyn rusqlite::ToSql,
            v,
        ]) {
            // Ok(0) => todo!("something about an upsert?"),
            Ok(n) => Ok(assert_eq!(n, 1)),
            Err(e) => Err(Error::promote_rusqlite(e)),
        }
    }

    /// How should this even work?
    /// - Should we find the datoms and then delete them by rowid?
    pub fn retract<'z, E, A, T>(&self, e: &E, a: &A, v: &T) -> Result<()>
    where
        E: RowIdOr<EntityId>,
        A: RowIdOr<AttributeName<'z>>,
        T: Assertable + rusqlite::ToSql + fmt::Debug,
    {
        self.check_protected_datom(e, a)?;

        let e_variant = e.row_id_or();
        let a_variant = a.row_id_or();
        let v_affinity = v.affinity();

        /* denormalize modification of :attr/unique to datoms.unique_for_attribute
         * (see assert() for more) */
        match a_variant.as_ref() {
            either::Left(&rowid) if rowid == ATTR_UNIQUE_ROWID => {
                self.set_datoms_unique_for_attribute(e, false)?;
            }
            either::Right(&a) if a == &ATTR_UNIQUE => {
                self.set_datoms_unique_for_attribute(e, false)?;
            }
            _ => (),
        };

        let (e, e_bind_str) = match e_variant.as_ref() {
            either::Left(rowid) => (rowid as &dyn rusqlite::ToSql, "?"),
            either::Right(e) => (e as &dyn rusqlite::ToSql, sql::bind_entity()),
        };

        let (a, a_bind_str) = match a_variant.as_ref() {
            either::Left(rowid) => (rowid as &dyn rusqlite::ToSql, "?"),
            either::Right(a) => (a as &dyn rusqlite::ToSql, sql::bind_attribute()),
        };

        let (t, v_bind_str) = v_affinity.t_and_bind();

        let sql = format!(
            r#"
        DELETE FROM datoms
              WHERE e={e}
                AND a={a}
                AND t=?
                AND v={v}"#,
            e = e_bind_str,
            a = a_bind_str,
            v = v_bind_str,
        );

        let mut stmt = self.tx.prepare(&sql)?;
        match stmt.execute(&[
            e as &dyn rusqlite::ToSql,
            a as &dyn rusqlite::ToSql,
            t as &dyn rusqlite::ToSql,
            v,
        ]) {
            Ok(0) => Err(Error::NotFound),
            Ok(n) => Ok(assert_eq!(n, 1)),
            Err(e) => Err(Error::promote_rusqlite(e)),
        }
    }

    fn check_protected_datom<'z, E, A>(&self, e: &E, a: &A) -> Result<()>
    where
        E: RowIdOr<EntityId>,
        A: RowIdOr<AttributeName<'z>>,
    {
        let e_variant = e.row_id_or();
        let a_variant = a.row_id_or();

        match (e_variant, a_variant) {
            /* enforce immutability of datoms with :entity/uuid attributes */
            (_, either::Left(rowid)) if rowid == ENTITY_UUID_ROWID => {
                return Err(Error::Immutable(":entity/uuid"))
            }
            (_, either::Right(a)) if a == &ENTITY_UUID => {
                return Err(Error::Immutable(":entity/uuid")); /* rustfmt plz no */
            }
            // TODO we can't properly prohibit modification of special datoms like entity/uuid
            // unless we know what uuid it has ahead of time?
            // (e, a) if a == &ATTR_UNIQUE => {
            //     return Err(Error::Protected)
            // }
            _ => return Ok(()),
        }
    }

    fn set_datoms_unique_for_attribute<E>(&self, e: &E, v: bool) -> Result<usize>
    where
        E: RowIdOr<EntityId>,
    {
        let e_variant = e.row_id_or();

        let (e, e_bind_str) = match e_variant.as_ref() {
            either::Left(rowid) => (rowid as &dyn rusqlite::ToSql, "?"),
            either::Right(e) => (e as &dyn rusqlite::ToSql, sql::bind_entity()),
        };

        let sql = format!(
            r#"
             UPDATE datoms
                SET unique_for_attribute = ?
              WHERE datoms.a = {e};"#,
            e = e_bind_str,
        );
        let params = rusqlite::params![v, e];
        let mut stmt = self.tx.prepare(&sql)?;
        match stmt.execute(params) {
            Ok(n) => Ok(n),
            Err(e) => Err(Error::promote_rusqlite(e)),
        }
    }

    /// for debugging ... use with T as rusqlite::types::Value
    pub fn all_datoms(
        &self,
    ) -> rusqlite::Result<Vec<(i64, String, Affinity, rusqlite::types::Value, i64)>> {
        let sql = r#"
            SELECT e, attributes.ident, t, v, unique_for_attribute
              FROM datoms
              JOIN attributes ON datoms.a = attributes.rowid"#;
        let mut stmt = self.tx.prepare(sql)?;
        let rows = stmt.query_map(rusqlite::NO_PARAMS, |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })?;
        rows.collect::<_>()
    }

    /// TODO use trait for return type so the user can avoid double-vectorings and write things
    /// that can read from column?
    pub fn find<'s, 'p, V, S>(
        &self,
        s: &'s Selection<'s, 'p, V, S>,
    ) -> rusqlite::Result<Vec<<S as sql::ReadFromRow>::Out>>
    where
        V: FromAffinityValue + Assertable + rusqlite::ToSql + fmt::Debug,
        S: sql::AddToQuery<&'s dyn sql::ToSqlDebug> + sql::ReadFromRow,
    {
        let mut q = sql::Query::default();
        let _ = sql::selection_sql(s, &mut q);

        eprintln!("[DEBUG] sql: ...");
        eprintln!("{}", q.as_str());
        eprintln!("[DEBUG] par: {:?}", q.params());

        let mut stmt = self.tx.prepare(q.as_str())?;

        let rows = stmt.query_map(q.params(), |row| {
            let mut c = sql::RowCursor::from(row);
            s.columns.read_from_row(&mut c)
        })?;

        rows.collect()
    }

    pub fn explain<'s, 'p, V, S>(
        &self,
        s: &'s Selection<'s, 'p, V, S>,
    ) -> rusqlite::Result<Explanation>
    where
        V: FromAffinityValue + Assertable + rusqlite::ToSql + fmt::Debug,
        S: sql::AddToQuery<&'s dyn sql::ToSqlDebug>,
    {
        let mut q = sql::Query::default();
        let _ = sql::selection_sql(s, &mut q);

        let sql = format!("EXPLAIN\n{}", q);
        let mut stmt = self.tx.prepare(&sql)?;

        let rows = stmt.query_map(q.params(), |row| {
            let mut c = sql::RowCursor::from(row);
            ExplainLine::from_row_cursor(&mut c)
        })?;

        rows.collect::<Result<Vec<_>, _>>()
            .map(|lines| Explanation { lines })
    }

    pub fn explain_plan<'s, 'p, V, S>(
        &self,
        s: &'s Selection<'s, 'p, V, S>,
    ) -> rusqlite::Result<PlanExplanation>
    where
        V: FromAffinityValue + Assertable + rusqlite::ToSql + fmt::Debug,
        S: sql::AddToQuery<&'s dyn sql::ToSqlDebug>,
    {
        let mut q = sql::Query::default();
        let _ = sql::selection_sql(s, &mut q);

        let sql = format!("EXPLAIN QUERY PLAN\n{}", q);
        let mut stmt = self.tx.prepare(&sql)?;

        let rows = stmt.query_map(q.params(), |row| {
            let mut c = sql::RowCursor::from(row);
            PlanExplainLine::from_row_cursor(&mut c)
        })?;

        rows.collect::<Result<Vec<_>, _>>()
            .map(|lines| PlanExplanation { lines })
    }
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
                    .parse::<uuid::Uuid>()
                    .map(EntityId::from)
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
        assert_eq!(AttributeName::column_result(v)?, attr);

        let attr = Value::Attribute(":foo/bar".to_owned());
        assert_eq!(attr.to_sql()?, "foo/bar".to_sql()?);

        let v = ValueRef::Text("foo/bar".as_bytes());
        assert_eq!(Value::from_affinity_value(Affinity::Attribute, v)?, attr);

        Ok(())
    }

    pub(crate) fn blank_db() -> Result<rusqlite::Connection> {
        let mut conn = rusqlite::Connection::open_in_memory()?;
        Session::init_schema(&mut conn)?;
        Ok(conn)
    }

    pub(crate) fn goodbooks() -> Result<rusqlite::Connection> {
        let mut db = blank_db()?;
        let s = Session::new(&mut db)?;

        let mut books = HashMap::<i64, EntityId>::new();

        {
            let title = s.new_attribute(":book/title")?;
            let avg_rating = s.new_attribute(":book/avg-rating")?;
            let isbn = s.new_attribute(":book/isbn")?;
            let authors = s.new_attribute(":book/authors")?;

            let mut r = csv::Reader::from_path("goodbooks-10k/books.csv")?;
            for result in r.deserialize().take(1_000) {
                let book: Book = result?;

                let e = s.new_entity()?;
                s.assert(&e, &title, &book.title)?;
                s.assert(&e, &avg_rating, &book.average_rating)?;
                s.assert(&e, &isbn, &book.isbn)?;
                s.assert(&e, &authors, &book.authors)?;

                books.insert(book.book_id, e.id);
            }
        }

        {
            let score = s.new_attribute(":rating/score")?; // aka one-to-five
            let book = s.new_attribute(":rating/book")?;
            let user = s.new_attribute(":rating/user")?;

            let mut r = csv::Reader::from_path("goodbooks-10k/ratings.csv")?;
            for result in r.deserialize().take(2_000) {
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

        s.optimize()?;
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
    fn explain() -> Result<()> {
        let mut db = goodbooks()?;
        let sess = Session::new(&mut db)?;

        let mut p = Projection::<Value>::default();
        let book_attrs = &[AttributeName::from_static(":book/title")];
        let book = p.entity_group("b").unwrap().attribute_map(book_attrs);
        let sel = p.select(&book);
        println!("{}", sess.explain(&sel)?);

        Ok(())
    }

    #[test]
    fn explain_plan() -> Result<()> {
        let mut db = goodbooks()?;
        let sess = Session::new(&mut db)?;

        let mut p = Projection::<Value>::default();
        let book_attrs = &[AttributeName::from_static(":book/title")];
        let book = p.entity_group("b").unwrap().attribute_map(book_attrs);
        let sel = p.select(&book);
        println!("{}", sess.explain_plan(&sel)?);

        Ok(())
    }

    #[test]
    fn unique_for_attribute() {
        let mut db = blank_db().unwrap();

        /* :attr/ident is unique for attribute */
        {
            let session = Session::new(&mut db).unwrap();
            session.new_attribute(":foo/bar").unwrap();
            assert_eq!(
                session.new_attribute(":foo/bar"),
                Err(Error::NotUniqueForAttribute)
            );
        };

        let session = Session::new(&mut db).unwrap();
        let name = session.new_attribute(":person/name").unwrap();
        let bob1 = session.new_entity().unwrap();
        let bob2 = session.new_entity().unwrap();
        let bob_name = Value::Text("Bob".to_owned());
        session.assert(&bob1, &name, &bob_name).unwrap();
        session.commit().unwrap();

        /* make :person/name unique for attribute _after_ non-unique data exists */
        {
            let session = Session::new(&mut db).unwrap();
            session.assert(&bob2, &name, &bob_name).unwrap();
            let res = session.assert(&name, &ATTR_UNIQUE, &Value::Integer(1));
            assert_eq!(res, Err(Error::NotUniqueForAttribute));
        };

        /* make :person/name unique for attribute and then add data */
        {
            let session = Session::new(&mut db).unwrap();
            session
                .assert(&name, &ATTR_UNIQUE, &Value::Integer(1))
                .unwrap();
            session.commit().unwrap();
        }

        {
            let session = Session::new(&mut db).unwrap();
            let res = session.assert(&bob2, &name, &bob_name);
            assert_eq!(res, Err(Error::NotUniqueForAttribute));
        }

        /* retract and try again */
        {
            let session = Session::new(&mut db).unwrap();
            session
                .retract(&name, &ATTR_UNIQUE, &Value::Integer(1))
                .unwrap();
            session.commit().unwrap();
        }

        {
            let session = Session::new(&mut db).unwrap();
            session.assert(&bob2, &name, &bob_name).unwrap();
            session.commit().unwrap();
        }
    }

    #[test]
    fn fuck_with_entity_uuid() {
        let mut db = blank_db().unwrap();
        let session = Session::new(&mut db).unwrap();

        // assert_eq!(
        //     session.retract(
        //         &types::RowId(ENTITY_UUID_ROWID),
        //         &ATTR_UNIQUE,
        //         &Value::Integer(1),
        //     ),
        //     Err(Error::Protected)
        // );

        let e = session.new_entity().unwrap();
        assert_eq!(
            session.retract(&e, &ENTITY_UUID, &e.id),
            Err(Error::Immutable(":entity/uuid"))
        );
        assert_eq!(
            session.assert(&e, &ENTITY_UUID, &Value::Integer(420)),
            Err(Error::Immutable(":entity/uuid"))
        );
    }

    #[test]
    fn upsert_maybe_i_guess() -> Result<()> {
        let mut db = blank_db()?;

        let session = Session::new(&mut db)?;
        let name = session.new_attribute(":person/name")?;
        let bob = session.new_entity()?;
        session.assert(&bob, &name, &Value::Text("bob".to_owned()))?;

        let res = session.assert(&bob, &name, &Value::Text("jim".to_owned()));
        assert_eq!(res, Err(Error::NotUniqueForEntity));

        session.commit()?;
        Ok(())
    }

    #[test]
    fn retract() {
        let mut db = blank_db().unwrap();

        let session = Session::new(&mut db).unwrap();

        let name = session.new_attribute(":person/name").unwrap();
        let bob = session.new_entity().unwrap();
        let bobs_name = Value::Text("bob".to_owned());

        session.assert(&bob, &name, &bobs_name).unwrap();

        let pat = [pat!(?p ":person/name" ?n)];
        let mut prj = Projection::<Value>::from_patterns(&pat);
        let sel = prj.select((prj.var("p").unwrap(), prj.var("n").unwrap()));

        assert_eq!(
            session.find(&sel).unwrap(),
            vec![(Value::Entity(bob.id), bobs_name.clone())],
        );

        session.retract(&bob, &name, &bobs_name).unwrap();

        assert_eq!(session.find(&sel).unwrap(), vec![]);

        assert_eq!(
            Err(Error::NotFound),
            session.retract(&bob, &name, &bobs_name)
        );
    }

    #[test]
    fn select_location() -> Result<()> {
        let mut db = blank_db()?;

        let session = Session::new(&mut db)?;

        let name = session.new_attribute(":person/name")?;
        let bob = session.new_entity()?;

        session.assert(&bob, &name, &Value::Text("bob".to_owned()))?;

        let pat = [pat!(?p ?a ?n)];
        let mut prj = Projection::<Value>::from_patterns(&pat);
        let sel = prj.select((
            prj.var("p").unwrap(),
            prj.var("a").unwrap(),
            prj.var("n").unwrap(),
        ));
        let _wow = session.find(&sel)?;
        Ok(())
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
            /* */
            pat!(?b ":book/avg-rating" ?v),
            pat!(?r ":rating/book" ?b),
        ];
        let max_rating = Value::Real(4.0);

        let mut p = Projection::<Value>::default();
        p.add_patterns(&patterns);

        let var_v = p.var("v").unwrap();
        p.add_constraint(var_v.le(matter::Concept::value(&max_rating)));

        let book_attrs = [
            AttributeName::from_static(":book/title"),
            AttributeName::from_static(":book/isbn"),
            AttributeName::from_static(":book/avg-rating"),
        ];
        let book_map = p.entity_group("b").unwrap().attribute_map(&book_attrs);

        let rate_attrs = [
            AttributeName::from_static(":rating/user"),
            AttributeName::from_static(":rating/score"),
        ];
        let rate_map = p.entity_group("r").unwrap().attribute_map(&rate_attrs);

        // just make sure this compiles ...
        let _ = p.select(&book_map);

        let mut sel = p.select((&book_map, &rate_map));
        sel.limit(8);
        let wow = sess.find(&sel)?;

        let jaysons = serde_json::to_string_pretty(&wow)?;
        println!("{}", jaysons);

        Ok(())
    }
}
