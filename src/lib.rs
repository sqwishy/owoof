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

mod explain;
mod matter;
mod sql;
mod types;

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
pub use matter::{Ordering, Pattern, Projection, Selection, VariableOr};
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
    #[error("could not retract what doesn't exist")]
    NotFound,
    #[error("sql error")]
    Sql(#[from] rusqlite::Error),
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

        db.execute(
            &format!(
                "CREATE TEMPORARY TRIGGER
                  IF NOT EXISTS accurate_entity_uuid_datoms
                BEFORE INSERT ON datoms
                    FOR EACH ROW WHEN new.a = {ENTITY_UUID_ROWID}
                                  AND new.t = {T_ENTITY}
                                  AND new.e != new.v
                BEGIN SELECT RAISE(FAIL, ':entity/id is immutable');
                END",
                ENTITY_UUID_ROWID = ENTITY_UUID_ROWID,
                T_ENTITY = T_ENTITY,
            ),
            rusqlite::NO_PARAMS,
        )?;

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

        /* unique_for_attribute */

        db.execute(
            &format!(
                "CREATE TEMPORARY TRIGGER
                  IF NOT EXISTS denormalize_ins_unique_for_attribute
                BEFORE INSERT ON datoms
                    FOR EACH ROW WHEN new.a = {ATTR_UNIQUE_ROWID}
                BEGIN
                    UPDATE datoms
                       SET unique_for_attribute = new.v
                     WHERE datoms.a = new.e;
                END",
                ATTR_UNIQUE_ROWID = ATTR_UNIQUE_ROWID,
            ),
            rusqlite::NO_PARAMS,
        )?;

        db.execute(
            &format!(
                "CREATE TEMPORARY TRIGGER
                  IF NOT EXISTS denormalize_upd_unique_for_attribute
                BEFORE UPDATE ON datoms
                    FOR EACH ROW WHEN new.a = {ATTR_UNIQUE_ROWID}
                                  AND new.v != old.v
                BEGIN
                    UPDATE datoms
                       SET unique_for_attribute = new.v
                     WHERE datoms.a = new.e;
                END",
                ATTR_UNIQUE_ROWID = ATTR_UNIQUE_ROWID,
            ),
            rusqlite::NO_PARAMS,
        )?;

        db.execute(
            &format!(
                "CREATE TEMPORARY TRIGGER
                  IF NOT EXISTS denormalize_del_unique_for_attribute
                BEFORE DELETE ON datoms
                    FOR EACH ROW WHEN old.a = {ATTR_UNIQUE_ROWID}
                BEGIN
                    UPDATE datoms
                       SET unique_for_attribute = false
                     WHERE datoms.a = old.e;
                END",
                ATTR_UNIQUE_ROWID = ATTR_UNIQUE_ROWID,
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

        // (ATTR_IDENT_ROWID, ATTR_UNIQUE_ROWID, true);
        let p = params![ATTR_IDENT_ROWID, ATTR_UNIQUE_ROWID, 0, true];
        tx.execute("INSERT INTO datoms (e, a, t, v) VALUES (?, ?, ?, ?)", p)
            .map(|n| assert_eq!(n, 1))?;

        tx.commit()?;

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

    pub fn assert<'z, E, A, T>(&self, e: &E, a: &A, v: &T) -> Result<()>
    where
        E: RowIdOr<EntityId>,
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
        let n = match stmt.execute(&[
            a as &dyn rusqlite::ToSql,
            e as &dyn rusqlite::ToSql,
            a as &dyn rusqlite::ToSql,
            &t as &dyn rusqlite::ToSql,
            v,
        ]) {
            Ok(n) => Ok(n),
            Err(e) => match &e {
                rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error {
                        code: rusqlite::ffi::ErrorCode::ConstraintViolation,
                        extended_code,
                    },
                    Some(msg),
                ) => match (extended_code, msg.as_str()) {
                    (&SQLITE_CONSTRAINT_TRIGGER, ":entity/id is immutable") => {
                        Err(Error::Immutable(":entity/id"))
                    }
                    (
                        &SQLITE_CONSTRAINT_UNIQUE,
                        "UNIQUE constraint failed: datoms.a, datoms.t, datoms.v",
                    ) => Err(Error::NotUniqueForAttribute),
                    (&SQLITE_CONSTRAINT_UNIQUE, "UNIQUE constraint failed: datoms.e, datoms.a") => {
                        Err(Error::NotUniqueForEntity)
                    }
                    _ => Err(e.into()),
                },
                _ => Err(e.into()),
            },
        }?;
        assert_eq!(n, 1);
        Ok(())
    }

    /// How should this even work?
    /// - Should we find the datoms and then delete them by rowid?
    pub fn retract<'z, E, A, T>(&self, e: &E, a: &A, v: &T) -> Result<()>
    where
        E: RowIdOr<EntityId>,
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
            &t as &dyn rusqlite::ToSql,
            v,
        ]) {
            /* TODO reuse error handling elsewhere  */
            Ok(0) => Err(Error::NotFound),
            Ok(n) => Ok(assert_eq!(n, 1)),
            Err(e) => match &e {
                rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error {
                        code: rusqlite::ffi::ErrorCode::ConstraintViolation,
                        extended_code,
                    },
                    Some(msg),
                ) => match (extended_code, msg.as_str()) {
                    (&SQLITE_CONSTRAINT_TRIGGER, ":entity/id is immutable") => {
                        Err(Error::Immutable(":entity/id"))
                    }
                    (&SQLITE_CONSTRAINT_UNIQUE, msg) if msg.starts_with("UNIQUE") => {
                        Err(Error::NotUniqueForEntity)
                    }
                    _ => Err(e.into()),
                },
                _ => Err(e.into()),
            },
        }
    }

    /// for debugging ... use with T as rusqlite::types::Value
    pub fn all_datoms(
        &self,
    ) -> rusqlite::Result<Vec<(EntityId, String, Affinity, rusqlite::types::Value)>> {
        // TODO this ignores the t value
        let sql = r#"
            SELECT entities.uuid, attributes.ident, t, v
              FROM datoms
              JOIN entities   ON datoms.e = entities.rowid
              JOIN attributes ON datoms.a = attributes.rowid"#;
        let mut stmt = self.tx.prepare(sql)?;
        let rows = stmt.query_map(rusqlite::NO_PARAMS, |row| {
            Ok((
                row.get::<_, uuid::Uuid>(0)?.into(),
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
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

            // TODO UUHSDFUSDHFJ
            // s.columns()
            //     .iter()
            //     .map(|loc| -> rusqlite::Result<Value> {
            //         use matter::Field;
            //         Ok(match loc.field {
            //             Field::Entity => Value::Entity(EntityId(c.get()?)),
            //             Field::Attribute => Value::Attribute(c.get()?),
            //             Field::Value => Value::read_affinity_value(&mut c)?,
            //         })
            //     })
            //     .collect::<rusqlite::Result<Vec<Value>>>()
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
            for result in r.deserialize().take(1_000) {
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
    fn unique_for_attribute() -> Result<()> {
        let mut db = blank_db()?;
        let session = Session::new(&mut db)?;
        session.new_attribute(":person/name")?;
        assert_eq!(
            session.new_attribute(":person/name"),
            Err(Error::NotUniqueForAttribute)
        );
        session.commit()?;
        Ok(())
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
        p.add_constraint(var_v.le(matter::Concept::Value(&max_rating)));

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
