//! How is this organized?
//!
//! - lib.rs - Session & Datom
//! - dialog.rs - mostly tests; Pattern 3-tuple of ?t/T
//! - matter.rs - Projection? Borrows patterns into datom sets & constraints
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
#![allow(unused_variables)]
// ^^^ todo; get your shit together ^^^

mod dialogue;
mod matter;
mod sql;

use std::collections::HashMap;
use std::{
    borrow::{Borrow, Cow},
    fmt,
    ops::Deref,
};

use rusqlite::types::Value as SqlValue;

use anyhow::Context;

pub use dialogue::Pattern;
pub use matter::{Projection, Selection};

pub(crate) const SCHEMA: &'static str = include_str!("../schema.sql");

/// Hard coded entity row ID for attribute of the identifier "entity/uuid" ...
pub(crate) const ENTITY_UUID: i64 = -1;
/// Hard coded entity row ID for attribute of the identifier "attr/ident" ...
/// This is referenced _literally_ in the "attributes" database view.
pub(crate) const ATTR_IDENT: i64 = -2;

pub(crate) const T_PLAIN: i64 = 0;
pub(crate) const T_ENTITY: i64 = -1;
pub(crate) const T_ATTRIBUTE: i64 = -2;

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum Affinity {
    Entity,
    Attribute,
    Other(u32),
}

impl Affinity {
    fn t_and_bind(&self) -> (i64, &'static str) {
        match self {
            Affinity::Entity => (T_ENTITY, sql::bind_entity()),
            Affinity::Attribute => (T_ATTRIBUTE, sql::bind_attribute()),
            Affinity::Other(t) => (*t as i64, "?"),
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
        Affinity::Entity
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

/// Wraps a rusqlite transaction to provide this crate's semantics to sqlite.
pub struct Session<'tx> {
    tx: &'tx rusqlite::Transaction<'tx>,
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct EntityName(uuid::Uuid);

impl Deref for EntityName {
    type Target = uuid::Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl rusqlite::ToSql for EntityName {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput> {
        self.0.to_sql()
    }
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct AttributeName<'a>(Cow<'a, str>);

impl<'a> Deref for AttributeName<'a> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0.borrow()
    }
}

impl<'a, I> From<I> for AttributeName<'a>
where
    I: Into<Cow<'a, str>>,
{
    fn from(i: I) -> Self {
        let cow: Cow<str> = i.into();
        AttributeName(cow)
    }
}

impl<'a> rusqlite::ToSql for AttributeName<'a> {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput> {
        self.0.to_sql()
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Entity {
    // pub rowid: i64, todo why?
    pub name: EntityName,
}

impl Deref for Entity {
    type Target = EntityName; //uuid::Uuid;

    fn deref(&self) -> &Self::Target {
        &self.name
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Attribute<'a> {
    // TODO these member names are confusing and awful
    pub name: EntityName,
    pub ident: AttributeName<'a>,
}

impl<'a> Deref for Attribute<'a> {
    type Target = AttributeName<'a>;

    fn deref(&self) -> &Self::Target {
        &self.ident
    }
}

pub trait Valuable {
    fn t(&self) -> i64;
    fn to_sql(&self) -> &dyn rusqlite::types::ToSql;
    fn to_sql_dbg(&self) -> &dyn sql::ToSqlDebug;
    // fn from_sql(t: i64, v: rusqlite::types::ValueRef) -> Result<Self, ValueError>
    // where
    //     Self: rusqlite::types::FromSql + Sized,
    // {
    //     Ok(rusqlite::types::FromSql::column_result(v)?)
    // }
}

impl Valuable for EntityName {
    fn t(&self) -> i64 {
        T_ENTITY
    }

    fn to_sql(&self) -> &dyn rusqlite::types::ToSql {
        &self.0
    }

    fn to_sql_dbg(&self) -> &dyn sql::ToSqlDebug {
        &self.0
    }
}

// impl<T: rusqlite::types::ToSql + fmt::Debug> Valuable for T {
impl Valuable for rusqlite::types::Value {
    fn t(&self) -> i64 {
        T_PLAIN
    }

    fn to_sql(&self) -> &dyn rusqlite::types::ToSql {
        self
    }

    fn to_sql_dbg(&self) -> &dyn sql::ToSqlDebug {
        self
    }
}

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

// TODO get rid of the T? this is super annoying?
#[derive(Clone, PartialEq, Debug)]
pub enum Value<T> {
    AsIs(T),
    Entity(EntityName),
    // Attribute(AttributeName),
    Null,
    Text(String),
    Blob(Vec<u8>),
    Integer(i64),
    Real(f64),
    // Uuid(uuid::Uuid),
    // DateTime(chrono::DateTime<chrono::Utc>),
}

impl<T> From<Entity> for Value<T> {
    fn from(e: Entity) -> Self {
        Value::Entity(e.name)
    }
}

impl From<rusqlite::types::Value> for Value<rusqlite::types::Value> {
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

impl<T> Value<T> {
    // fn t(&self) -> i64 {
    //     match self {
    //         Value::AsIs(i) => todo!(),
    //         Value::Entity(i) => i.t(),
    //         Value::DateTime(i) => i.t(),
    //         Value::Uuid(_)
    //         | Value::Null
    //         | Value::Text(_)
    //         | Value::Blob(_)
    //         | Value::Integer(_)
    //         | Value::Real(_) => T_PLAIN,
    //     }
    // }

    fn read_t_v_sql<'s>(t_col: &'s str, v_col: &'s str) -> Cow<'s, str> {
        sql::read_v(t_col, v_col).into()
    }

    // fn bind_str(&self) -> &'static str {
    //     "?"
    // }
}

// #[derive(Debug, thiserror::Error)]
// pub enum ValueError {
//     #[error("invalid type header sentinel thing")]
//     InvalidT,
//     #[error("database type error")]
//     Sql(#[from] rusqlite::types::FromSqlError),
// }

// pub trait SqlValue: rusqlite::types::FromSql + rusqlite::types::ToSql {}

// impl<T> SqlValue for T where T: rusqlite::types::FromSql + rusqlite::types::ToSql {}

/// An entity-attribute-value tuple thing.
///
/// attribute is parameterized as to use a owned or borrowed string TODO this is stupid
#[derive(Debug)]
pub struct Datom<'a, T> {
    pub entity: EntityName,
    pub attribute: AttributeName<'a>, //&'s str,
    pub value: Value<T>,
}

impl<'a, T> Datom<'a, T> {
    pub fn from_eav<S>(entity: EntityName, attribute: AttributeName<'a>, value: Value<T>) -> Self {
        Self {
            entity,
            attribute,
            value,
        }
    }

    pub fn eav(self) -> (EntityName, AttributeName<'a>, Value<T>) {
        (self.entity, self.attribute, self.value)
    }

    pub fn from_row(row: &rusqlite::Row) -> rusqlite::Result<Self>
where
        // S: rusqlite::types::FromSql,
        // T: rusqlite::types::FromSql,
    {
        Self::from_columns(&mut sql::RowCursor::from(row))
    }

    /// Expects a columns in the form of `e a is_ref v`
    pub fn from_columns<'c>(c: &mut sql::RowCursor<'c>) -> rusqlite::Result<Self>
where
        // S: rusqlite::types::FromSql,
        // T: rusqlite::types::FromSql,
    {
        let e = EntityName(c.get()?);
        let a = AttributeName(c.get::<String>()?.into());
        let v = match c.get()? {
            T_ENTITY => Value::Entity(EntityName(c.get()?)),
            // T_DATETIME => Value::DateTime(c.get()?),
            _ => Value::from(c.get::<rusqlite::types::Value>()?),
        };
        todo!()
        // Ok(Datom::from_eav(e, a, v))
    }
}

impl<'tx> Session<'tx> {
    pub fn new(tx: &'tx rusqlite::Transaction) -> Self {
        Session { tx }
    }

    pub fn new_entity(&self) -> rusqlite::Result<Entity> {
        let uuid = uuid::Uuid::new_v4();
        let n = self.tx.execute(
            "INSERT INTO entities (uuid) VALUES (?)",
            rusqlite::params![uuid],
        )?;
        assert_eq!(n, 1);
        let name = EntityName(uuid);
        Ok(Entity { name })
    }

    pub fn new_attribute<'a, S>(&self, ident: S) -> rusqlite::Result<Attribute<'a>>
    where
        S: Into<AttributeName<'a>>,
    {
        let ident = ident.into();

        let e = self.new_entity()?;
        let rowid = self.tx.last_insert_rowid();

        let n = self.tx.execute(
            "INSERT INTO datoms (e, a, t, v) VALUES (?, ?, ?, ?)",
            rusqlite::params![rowid, ATTR_IDENT, T_ATTRIBUTE, &*ident],
        )?;
        assert_eq!(n, 1);

        Ok(Attribute {
            name: e.name,
            ident,
        })
    }

    pub fn assert<T>(&self, e: &EntityName, a: &AttributeName<'_>, v: &T) -> rusqlite::Result<()>
    where
        T: Assertable + rusqlite::ToSql,
    {
        let (t, bind_str) = v.affinity().t_and_bind();

        let sql = format!(
            r#"
    INSERT INTO datoms (e, a, t, v)
         VALUES ( (SELECT rowid FROM entities   WHERE uuid = ?)
                , (SELECT rowid FROM attributes WHERE ident = ?)
                , ?
                , {} )
         "#,
            bind_str,
        );

        let mut stmt = self.tx.prepare(&sql)?;
        let n = stmt.execute(&[
            &e.deref() as &dyn rusqlite::ToSql,
            &a.deref() as &dyn rusqlite::ToSql,
            &t as &dyn rusqlite::ToSql,
            v,
        ])?;
        assert_eq!(n, 1);
        Ok(())
    }

    /// for debugging ... use with T as rusqlite::types::Value
    fn all_datoms<T>(&self) -> rusqlite::Result<Vec<Datom<T>>>
    where
        T: rusqlite::types::FromSql,
    {
        // TODO this ignores the t value
        let sql = r#"
            SELECT entities.uuid, attributes.ident, 0, v
              FROM datoms
              JOIN entities   ON datoms.e = entities.rowid
              JOIN attributes ON datoms.a = attributes.rowid"#;
        let mut stmt = self.tx.prepare(sql)?;
        let rows = stmt.query_map(rusqlite::NO_PARAMS, Datom::from_row)?;
        rows.collect::<_>()
    }

    fn select(
        &self,
        s: &Selection<rusqlite::types::Value>,
    ) -> rusqlite::Result<Vec<Vec<Value<rusqlite::types::Value>>>> {
        let mut q = sql::Query::default();
        let _ = sql::selection_sql(&s, &mut q);

        let mut stmt = self.tx.prepare(q.as_str())?;

        let rows = stmt.query_map(q.params(), |row| {
            let mut c = sql::RowCursor::from(row);
            s.columns()
                .iter()
                .map(|loc| -> rusqlite::Result<Value<_>> {
                    Ok(match loc.field {
                        matter::Field::Entity => Value::Entity(EntityName(c.get()?)),
                        matter::Field::Attribute => todo!(), // Value::Attribute(AttributeName(c.get()?)),
                        matter::Field::Value => Value::from(c.get::<rusqlite::types::Value>()?),
                    })
                })
                .collect::<rusqlite::Result<Vec<Value<rusqlite::types::Value>>>>()
        })?;

        rows.collect()
    }

    fn find<T>(
        &self,
        top: &str,
        terms: Vec<dialogue::Pattern<'_, T>>,
    ) -> anyhow::Result<Vec<HashMap<String, Value<T>>>>
    where
        // T: Valuable + fmt::Debug,
        T: rusqlite::types::FromSql + rusqlite::types::ToSql + std::fmt::Debug,
    {
        let p = Projection::from_patterns(&terms);
        let top = p.variables().get(top).expect("undefined variable?");

        let mut query = sql::Query::default();

        // This is mostly dead code, but it sort of fetched all the datoms in all the datomsets
        // that were involved in the projection and selected them. But maybe this should use a
        // Selection object instead? Or maybe we don't support this at all?

        let mut pre = std::iter::once("SELECT ").chain(std::iter::repeat("     , "));
        for n in 0..p.datomsets() {
            use std::fmt::Write;
            // I think if we use writeln! we insert carriage returns on windoge. I don't know
            // if that will fuck up the query but lets just not do that at all ever anyway.
            write!(
                query,
                "{pre}{read_ent} -- {comment}\n",
                pre = pre.next().unwrap(),
                read_ent = sql::read_entity(&format!("_dtm{}.e", n)),
                comment = n,
            )?;
            write!(
                query,
                "{pre}{read_atr}\n",
                pre = pre.next().unwrap(),
                read_atr = sql::read_attribute(&format!("_dtm{}.a", n)),
            )?;
            write!(query, "{}_dtm{}.t\n", pre.next().unwrap(), n)?;
            // TODO this is horribly wrong
            write!(
                query,
                "{}{}\n",
                pre.next().unwrap(),
                Value::<T>::read_t_v_sql(&format!("_dtm{}.t", n), &format!("_dtm{}.v", n),),
            )?;
        }

        // add FROM and WHERE clause to query
        sql::projection_sql(&p, &mut query).unwrap();

        // TODO
        query.push_str(" LIMIT 10");

        eprintln!("{}", query);
        eprintln!(">>> {:?}", query.params());

        let mut stmt = self.tx.prepare(query.as_str())?;

        let rows = stmt.query_map(query.params(), |row| {
            let mut c = sql::RowCursor::from(row);
            (0..p.datomsets())
                .map(|_| Datom::from_columns(&mut c))
                .collect::<rusqlite::Result<Vec<Datom<'_, _>>>>()
        })?;

        weird_grouping(top, rows)
    }
}

fn weird_grouping<'a, T, I>(
    top: &matter::Location,
    rows: I,
) -> anyhow::Result<Vec<HashMap<String, Value<T>>>>
where
    T: fmt::Debug,
    I: Iterator<Item = rusqlite::Result<Vec<Datom<'a, T>>>>,
{
    rows.map(|datoms| {
        let datoms: Vec<Datom<'_, T>> = datoms?;
        // group datoms from each row by entity
        let mut by_ent: HashMap<EntityName, HashMap<String, Value<T>>> = HashMap::new();
        // later return the entity for the `top` variable
        let mut top_ent = Option::<EntityName>::None;

        for (n, datom) in datoms.into_iter().enumerate() {
            let (entity, attribute, value) = datom.eav();

            if n == top.datomset.0 {
                let e = match top.field {
                    matter::Field::Entity => entity,
                    matter::Field::Value => match value {
                        Value::Entity(e) => e,
                        _ => continue,
                    },
                    _ => continue,
                };
                top_ent.replace(e);
            }

            match by_ent.get_mut(&entity) {
                Some(map) => {
                    map.insert(attribute.to_string(), value);
                }
                None => {
                    // map.insert("entity/uuid".to_owned(), Value::Entity(entity));
                    let mut map: HashMap<String, Value<T>> = HashMap::new();
                    map.insert(attribute.to_string(), value);
                    by_ent.insert(entity, map);
                }
            }
        }

        let top_ent = top_ent.expect("variable did not match an entity");
        let top_map = by_ent
            .remove(&top_ent)
            .expect("this is an actual panic; todo explain why");

        // for attr, val top_map.iter_mut()

        return Ok(top_map);

        // TODO this can't work because Value can't contain more hashmaps ...
        // fn reassemble<K, V>(to: &mut HashMap, from: &mut HashMap
    })
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    // struct Film {
    //     // #[oof("film/title")]
    //     title: String,
    //     year: u16,
    //     rating: f32,
    // }

    pub(crate) fn test_conn() -> Result<rusqlite::Connection> {
        let mut conn = rusqlite::Connection::open_in_memory()?;
        // conn.create_scalar_function("uuid_generate_v4", 0, false, move |_| {
        //     Ok(uuid::Uuid::new_v4())
        // })?;
        let tx = conn.transaction()?;
        tx.execute_batch(SCHEMA)?;

        // Create the initial attributes, this isn't part of SCHEMA because sqlite can't make its
        // own UUIDs (unless we just use random 128 bit blobs ...) and I'm avoiding database
        // functions for now.

        // entity/uuid & attr/ident
        tx.execute(
            "INSERT INTO entities (rowid, uuid) VALUES (?, ?), (?, ?)",
            rusqlite::params![
                ENTITY_UUID,
                uuid::Uuid::new_v4(),
                ATTR_IDENT,
                uuid::Uuid::new_v4(),
            ],
        )
        .map(|n| assert_eq!(n, 2))?;

        tx.execute(
            "INSERT INTO datoms (e, a, t, v)
                  VALUES (?, ?, ?, ?)
                       , (?, ?, ?, ?)",
            rusqlite::params![
                ENTITY_UUID, // the entity/uuid attribute's rowid
                ATTR_IDENT,  // has a attr/ident attribue
                T_ATTRIBUTE,
                "entity/uuid", // of this string
                ATTR_IDENT,
                ATTR_IDENT,
                T_ATTRIBUTE,
                "attr/ident",
            ],
        )
        .map(|n| assert_eq!(n, 2))?;

        tx.execute(
            &format!(
                "CREATE VIEW attributes (rowid, ident)
                          AS select e, v FROM datoms WHERE a = {}",
                ATTR_IDENT,
            ),
            rusqlite::NO_PARAMS,
        )?;

        tx.commit()?;

        Ok(conn)
    }

    pub(crate) fn goodbooks() -> Result<rusqlite::Connection> {
        let mut db = test_conn()?;
        let tx = db.transaction()?;
        let s = Session::new(&tx);

        let mut books = HashMap::<i64, EntityName>::new();

        {
            let title = s.new_attribute("book/title")?;
            let avg_rating = s.new_attribute("book/avg-rating")?;
            let isbn = s.new_attribute("book/isbn")?;
            let authors = s.new_attribute("book/authors")?;

            let mut r = csv::Reader::from_path("/home/sqwishy/src/goodbooks-10k/books.csv")?;
            for result in r.deserialize().take(500) {
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
            let rank = s.new_attribute("rating/rank")?; // aka one-to-five
            let book = s.new_attribute("rating/book")?;
            let user = s.new_attribute("rating/user")?;

            let mut r = csv::Reader::from_path("/home/sqwishy/src/goodbooks-10k/ratings.csv")?;
            for result in r.deserialize().take(1500) {
                let rating: Rating = result?;

                // if this is a rating for a book we didn't add, ignore it
                let book_ref = match books.get(&rating.book_id) {
                    None => continue,
                    Some(v) => v,
                };

                let e = s.new_entity()?;
                s.assert(&e, &book, book_ref)?;
                s.assert(&e, &user, &rating.user_id)?;
                s.assert(&e, &rank, &rating.rating)?;
            }
        }

        tx.commit()?;
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
        let tx = db.transaction()?;
        let sess = Session::new(&tx);

        // eprintln!(
        //     "all datoms: {:#?}",
        //     s.all_datoms::<rusqlite::types::Value>()
        // );

        // let mut f = Where::<rusqlite::types::Value>::from(vec![
        //     pat!(?b "book/title" ?t),
        //     pat!(?b "book/avg-rating" ?v),
        //     pat!(?r "rating/book" ?b),
        //     pat!(?r "rating/user" ?u),
        //     pat!(?r "rating/rank" ?k),
        // ]);
        // f.preds.push(prd!(?v < 1));

        // let mut q = dialogue::query::<rusqlite::types::Value>(vec![
        //     pat!(?b "book/title" ?t),
        //     pat!(?b "book/avg-rating" ?v),
        // ]);
        // q.such_that(vec![prd!(?v < 1)]);
        // q.show(vec!["t"]);

        // eprintln!("{:#?}", q);

        let mut p = Projection::<rusqlite::types::Value>::default();

        let pats = vec![
            pat!(?b "book/title" ?t),
            pat!(?b "book/avg-rating" ?v),
            // pat!(?r "rating/book" ?b),
        ];
        p.add_patterns(&pats);

        let max_rating = 4.0.into();
        p.add_constraint(
            p.variable("v")
                .cloned()
                .unwrap()
                .le(matter::Concept::Value(&max_rating)),
        );

        let mut s = Selection::new(&p);
        s.columns.push(p.variable("b").cloned().unwrap());
        s.columns.push(p.variable("t").cloned().unwrap());
        s.columns.push(p.variable("v").cloned().unwrap());
        s.limit = 8;

        eprintln!("{:#?}", s);

        let mut q = sql::Query::default();
        sql::selection_sql(&s, &mut q).unwrap();
        eprintln!("{}", q);
        eprintln!("{:?}", q.params());

        let wow = sess.select(&s);
        eprintln!("{:#?}", wow);

        Ok(())
    }
}
