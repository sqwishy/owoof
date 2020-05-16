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

use explain::{ExplainLine, Explanation, PlanExplainLine, PlanExplanation};
pub use matter::{Ordering, Pattern, Projection, Selection, VariableOr};
pub use types::{
    Affinity, Assertable, Attribute, AttributeName, Entity, EntityName, FromAffinityValue, RowIdOr,
    Value,
};

pub(crate) const SCHEMA: &str = include_str!("../schema.sql");

/// Hard coded entity row ID for attribute of the identifier "entity/uuid" ...
pub(crate) const ENTITY_UUID_ROWID: i64 = -1;
/// Hard coded entity row ID for attribute of the identifier "attr/ident" ...
/// This is referenced _literally_ in the "attributes" database view.
pub(crate) const ATTR_IDENT_ROWID: i64 = -2;

pub(crate) const T_ENTITY: i64 = -1;
/// This is referenced _literally_ in the "attributes" database view.
pub(crate) const T_ATTRIBUTE: i64 = -2;

/// https://sqlite.org/rescode.html#extrc
const SQLITE_CONSTRAINT_TRIGGER: i32 = 1811;
const SQLITE_CONSTRAINT_UNIQUE: i32 = 2067;

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum Error {
    #[error("value must be unique the entity")]
    NotUniqueForEntity,
    #[error("{0} is immutable")]
    Immutable(&'static str),
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
        let tx = db.transaction()?;

        // Views are attached to the lifetime of the connection, not the transaction ...
        // ... so guard with IF NOT EXISTS.

        let attributes = format!(
            "CREATE TEMPORARY VIEW IF NOT EXISTS attributes (rowid, ident)
                 AS SELECT e, v FROM datoms WHERE a = {} AND t = {}",
            ATTR_IDENT_ROWID, T_ATTRIBUTE,
        );
        // eprintln!("[DEBUG] sql: ...");
        // eprintln!("{}", attributes);
        tx.execute(&attributes, rusqlite::NO_PARAMS)?;

        tx.execute(
            &format!(
                "CREATE TRIGGER
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

        tx.execute(
            &format!(
                "CREATE TEMPORARY TRIGGER
                  IF NOT EXISTS accurate_entity_uuid_datoms
                BEFORE INSERT ON datoms
                    FOR EACH ROW WHEN new.a = {entity_uuid}
                                  AND new.t = {t_entity}
                                  AND new.e != new.v
                BEGIN SELECT RAISE(FAIL, ':entity/id is immutable');
                END",
                entity_uuid = ENTITY_UUID_ROWID,
                t_entity = T_ENTITY,
            ),
            rusqlite::NO_PARAMS,
        )?;

        tx.execute(
            &format!(
                "CREATE TEMPORARY TRIGGER
                  IF NOT EXISTS immutable_entity_uuid_datoms
                BEFORE UPDATE ON entities
                    FOR EACH ROW WHEN a = {entity_uuid}
                BEGIN SELECT RAISE(FAIL, ':entity/id is immutable');
                END",
                entity_uuid = ENTITY_UUID_ROWID,
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
        self.new_entity_at(EntityName::from(uuid))
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
            r#"
        INSERT INTO datoms (e, a, t, v)
             VALUES ( {e}
                    , {a}
                    , ?
                    , {v} )"#,
            e = e_bind_str,
            a = a_bind_str,
            v = v_bind_str,
        );
        // eprintln!("[DEBUG] sql: ...");
        // eprintln!("{}", sql);
        // eprintln!(
        //     "[DEBUG] par: {:?} {:?} {:?} {:?}",
        //     e.to_sql(),
        //     a.to_sql(),
        //     t,
        //     v
        // );

        let mut stmt = self.tx.prepare(&sql)?;
        let n = match stmt.execute(&[
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
                    (&SQLITE_CONSTRAINT_UNIQUE, msg) if msg.starts_with("UNIQUE") => {
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

    pub fn retract<'z, E, A, T>(&self, e: &E, a: &A, v: &T) -> Result<()>
    where
        E: RowIdOr<EntityName>,
        A: RowIdOr<AttributeName<'z>>,
        T: Assertable + rusqlite::ToSql + fmt::Debug,
    {
        todo!()
    }

    /// for debugging ... use with T as rusqlite::types::Value
    pub fn all_datoms(
        &self,
    ) -> rusqlite::Result<Vec<(EntityName, String, Affinity, rusqlite::types::Value)>> {
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
    pub fn find<'s, V, S>(
        &self,
        s: &'s Selection<'s, V, S>,
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

            // s.attrs
            //     .iter()
            //     .map(|a| {
            //         a.map
            //             .iter()
            //             .map(|(attr, _)| -> rusqlite::Result<(_, _)> {
            //                 let value = V::read_affinity_value(&mut c)?;
            //                 Ok((*attr, value))
            //             })
            //             .collect::<rusqlite::Result<HashMap<_, _>>>()
            //     })
            //     .collect::<rusqlite::Result<Vec<_>>>()

            // TODO UUHSDFUSDHFJ
            // s.columns()
            //     .iter()
            //     .map(|loc| -> rusqlite::Result<Value> {
            //         use matter::Field;
            //         Ok(match loc.field {
            //             Field::Entity => Value::Entity(EntityName(c.get()?)),
            //             Field::Attribute => Value::Attribute(c.get()?),
            //             Field::Value => Value::read_affinity_value(&mut c)?,
            //         })
            //     })
            //     .collect::<rusqlite::Result<Vec<Value>>>()
        })?;

        rows.collect()
    }

    pub fn explain<'s, V, S>(&self, s: &'s Selection<'s, V, S>) -> rusqlite::Result<Explanation>
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

    pub fn explain_plan<'s, V, S>(
        &self,
        s: &'s Selection<'s, V, S>,
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
                    .map(EntityName::from)
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

        let mut books = HashMap::<i64, EntityName>::new();

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

                books.insert(book.book_id, e.name);
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
    fn upsert_maybe_i_guess() -> Result<()> {
        let mut db = blank_db()?;

        let session = Session::new(&mut db)?;
        let name = session.new_attribute(":person/name")?;
        let bob = session.new_entity()?;
        session.assert(&bob, &name, &Value::Text("bob".to_owned()))?;

        let res = session.assert(&bob, &name, &Value::Text("jim".to_owned()));
        assert_eq!(res, Err(Error::NotUniqueForEntity));

        // let d = session.all_datoms()?;
        // eprintln!("{:#?}", d);

        session.commit()?;
        Ok(())
    }

    #[test]
    fn retract() -> Result<()> {
        let mut db = blank_db()?;

        let session = Session::new(&mut db)?;

        let name = session.new_attribute(":person/name")?;
        let bob = session.new_entity()?;

        session.assert(&bob, &name, &Value::Text("bob".to_owned()))?;

        let pat = [pat!(?p name ?n)];
        let prj = Projection::<Value>::from_patterns(&pat);
        todo!();
        // prj.selection()
        // session.find

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

        let var_v = p.variable("v").cloned().unwrap();
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

        // let mut sel = p.selection();
        // sel.attrs.push(&book_map);
        // sel.attrs.push(&rate_map);
        // // sel.order_by.push(book_map.map[2].1.value_field().desc());
        // sel.limit(8);

        let mut sel = p.select(&book_map);
        sel.limit(8);
        let wow = sess.find(&sel)?;

        let jaysons = serde_json::to_string_pretty(&wow)?;
        println!("{}", jaysons);

        Ok(())
    }
}
