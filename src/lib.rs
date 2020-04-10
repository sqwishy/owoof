//! How is this organized?
//!
//! lib - Session & Datom
//! dialog - mostly tests; Pattern 3-tuple of ?t/T
//! matter - Projection? Borrows patterns into datom sets & constraints
//! sql - render a SQL query for a Projection
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
use std::{borrow::Cow, fmt, ops::Deref};

use anyhow::Context;

pub use dialogue::{Pattern, Predicate};
pub use matter::{Projection, Selection, Where};

pub const SCHEMA: &'static str = include_str!("../schema.sql");

/// Hard coded entity row ID for attribute of the identifier "entity/uuid" ...
pub const ENTITY_UUID: i64 = -1;
/// Hard coded entity row ID for attribute of the identifier "attr/ident" ...
/// This is referenced _literally_ in the "attributes" database view.
pub const ATTR_IDENT: i64 = -2;

pub const PLAIN_T: i64 = 0;
pub const ENTITY_T: i64 = -1;

/// Wraps a rusqlite transaction to provide this crate's semantics to sqlite.
pub struct Session<'tx> {
    tx: &'tx rusqlite::Transaction<'tx>,
}

pub type EntityId = i64;
pub type EntityName = uuid::Uuid;
pub type AttributeId = i64;
pub type AttributeName = str;

// #[derive(Copy, Clone, PartialEq, Eq, Debug)]
// pub enum Unique {
//     ForEntity,
//     ForAttribute,
// }

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Entity {
    pub uuid: EntityName,
}

impl Deref for Entity {
    type Target = EntityName;

    fn deref(&self) -> &Self::Target {
        &self.uuid
    }
}

// TODO get rid of this? this is super annoying?
#[derive(Clone, PartialEq, Debug)]
pub enum Value<T> {
    AsIs(T),
    Entity(EntityName),
    Text(String),
    Real(f64),
    Integer(i64),
    Uuid(uuid::Uuid),
}

impl<T> From<Entity> for Value<T> {
    fn from(e: Entity) -> Self {
        Value::Entity(e.uuid)
    }
}

impl From<rusqlite::types::Value> for Value<rusqlite::types::Value> {
    fn from(v: rusqlite::types::Value) -> Self {
        Value::AsIs(v)
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Attribute<S> {
    pub uuid: EntityName,
    pub ident: S,
}

pub trait Valuable {
    fn t(&self) -> i64;
    fn read_column<'s>(col: &'s str) -> Cow<'s, str>;
    fn bind_str(&self) -> &'static str;
    fn to_sql(&self) -> &dyn rusqlite::types::ToSql;
    // fn from_sql(t: i64, v: rusqlite::types::ValueRef) -> Result<Self, ValueError>
    // where
    //     Self: rusqlite::types::FromSql + Sized,
    // {
    //     Ok(rusqlite::types::FromSql::column_result(v)?)
    // }
}

impl Valuable for Entity {
    fn t(&self) -> i64 {
        ENTITY_T
    }

    fn read_column<'s>(col: &'s str) -> Cow<'s, str> {
        format!("(SELECT uuid FROM entities WHERE rowid = {})", col).into()
    }

    fn bind_str(&self) -> &'static str {
        "(SELECT rowid FROM entities WHERE uuid = ?)"
    }

    fn to_sql(&self) -> &dyn rusqlite::types::ToSql {
        &self.uuid
    }
}

impl<T: rusqlite::types::ToSql + fmt::Debug> Valuable for T {
    fn t(&self) -> i64 {
        PLAIN_T
    }

    fn read_column<'s>(col: &'s str) -> Cow<'s, str> {
        col.into()
    }

    fn bind_str(&self) -> &'static str {
        "?"
    }

    fn to_sql(&self) -> &dyn rusqlite::types::ToSql {
        self
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ValueError {
    #[error("invalid type header sentinel thing")]
    InvalidT,
    #[error("database type error")]
    Sql(#[from] rusqlite::types::FromSqlError),
}

// pub trait SqlValue: rusqlite::types::FromSql + rusqlite::types::ToSql {}

// impl<T> SqlValue for T where T: rusqlite::types::FromSql + rusqlite::types::ToSql {}

/// A version of DbDatom referencing entities and attributes by public handles.
///
/// attribute is parameterized as to use a owned or borrowed string
#[derive(Debug)]
pub struct Datom<S, T> {
    pub entity: EntityName,
    pub attribute: S, //&'s str,
    pub value: Value<T>,
}

pub type OwnedDatom<T> = Datom<String, T>;

impl<S, T> Datom<S, T> {
    pub fn from_eav(entity: EntityName, attribute: S, value: Value<T>) -> Self {
        Self {
            entity,
            attribute,
            value,
        }
    }

    pub fn eav(self) -> (EntityName, S, Value<T>) {
        (self.entity, self.attribute, self.value)
    }

    pub fn from_row(row: &rusqlite::Row) -> rusqlite::Result<Self>
    where
        S: rusqlite::types::FromSql,
        T: rusqlite::types::FromSql,
    {
        Self::from_columns(&mut sql::RowCursor::from(row))
    }

    /// Expects a columns in the form of `e a is_ref v`
    pub fn from_columns<'a>(c: &mut sql::RowCursor<'a>) -> rusqlite::Result<Self>
    where
        S: rusqlite::types::FromSql,
        T: rusqlite::types::FromSql,
    {
        let e = c.get()?;
        let a = c.get()?;
        let v = match c.get()? {
            ENTITY_T => Value::Entity(c.get()?),
            _ => Value::AsIs(c.get()?),
        };
        Ok(Datom::from_eav(e, a, v))
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
        Ok(Entity { uuid })
    }

    pub fn new_attribute<'i>(&self, ident: &'i str) -> rusqlite::Result<Attribute<&'i str>> {
        let e = self.new_entity()?;
        let rowid = self.tx.last_insert_rowid();
        let n = self.tx.execute(
            "INSERT INTO datoms (e, a, t, v) VALUES (?, ?, ?, ?)",
            rusqlite::params![rowid, ATTR_IDENT, PLAIN_T, ident],
        )?;
        assert_eq!(n, 1);
        Ok(Attribute {
            uuid: e.uuid,
            ident,
        })
    }

    pub fn assert<T>(&self, e: &EntityName, a: &AttributeName, v: &T) -> rusqlite::Result<()>
    where
        T: Valuable, // T: rusqlite::ToSql + fmt::Debug + Valuable,
    {
        let sql = format!(
            r#"
    INSERT INTO datoms (e, a, t, v)
         VALUES ( (SELECT rowid FROM entities   WHERE uuid = ?)
                , (SELECT rowid FROM attributes WHERE ident = ?)
                , ?
                , {} )
         "#,
            v.bind_str(),
        );

        let mut stmt = self.tx.prepare(&sql)?;
        let n = stmt.execute(&[
            e as &dyn rusqlite::ToSql,
            &a as &dyn rusqlite::ToSql,
            &v.t() as &dyn rusqlite::ToSql,
            v.to_sql(),
        ])?;
        assert_eq!(n, 1);
        Ok(())
    }

    /// for debugging ... use with T as rusqlite::types::Value
    fn all_datoms<T>(&self) -> rusqlite::Result<Vec<OwnedDatom<T>>>
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

    fn find<'s, 'p, T>(
        &'s self,
        top: &'p str,
        terms: Vec<dialogue::Pattern<'p, T>>,
    ) -> anyhow::Result<Vec<HashMap<String, Value<T>>>>
    where
        T: rusqlite::types::FromSql + rusqlite::types::ToSql + std::fmt::Debug,
    {
        let wh = dialogue::Where::from(terms);
        let p = Projection::of(&wh);
        let top = p.variables().get(top).expect("undefined variable?");

        let mut query = sql::Query::default();

        let mut pre = std::iter::once("SELECT ").chain(std::iter::repeat("     , "));
        for n in 0..p.datomsets() {
            use std::fmt::Write;
            // I think if we use writeln! we insert carriage returns on windoge. I don't know
            // if that will fuck up the query but lets just not do that at all ever anyway.
            write!(
                query,
                "{}{} -- {}\n",
                pre.next().unwrap(),
                Entity::read_column(&format!("_dtm{}.e", n)),
                n,
            )?;
            write!(
                query,
                "{}(SELECT ident FROM attributes WHERE rowid = _dtm{}.a)\n",
                pre.next().unwrap(),
                n,
            )?;
            write!(query, "{}_dtm{}.t\n", pre.next().unwrap(), n)?;
            write!(
                query,
                "{}CASE _dtm{}.t WHEN 1 THEN {} ELSE _dtm{}.v END\n",
                pre.next().unwrap(),
                n,
                Entity::read_column(&format!("_dtm{}.e", n)),
                n,
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
                .collect::<rusqlite::Result<Vec<OwnedDatom<_>>>>()
        })?;

        weird_grouping(top, rows)
    }
}

fn weird_grouping<T, I>(
    top: &matter::Location,
    rows: I,
) -> anyhow::Result<Vec<HashMap<String, Value<T>>>>
where
    T: fmt::Debug,
    I: Iterator<Item = rusqlite::Result<Vec<OwnedDatom<T>>>>,
{
    rows.map(|datoms| {
        let datoms: Vec<OwnedDatom<T>> = datoms?;
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
                    map.insert(attribute, value);
                }
                None => {
                    // map.insert("entity/uuid".to_owned(), Value::Entity(entity));
                    let mut map: HashMap<String, Value<T>> = HashMap::new();
                    map.insert(attribute, value);
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
                ENTITY_UUID, // the entity/uuid attribute
                ATTR_IDENT,  // has a attr/ident attribue
                PLAIN_T,
                "entity/uuid", // of this string
                ATTR_IDENT,
                ATTR_IDENT,
                PLAIN_T,
                "attr/ident",
            ],
        )
        .map(|n| assert_eq!(n, 2))?;

        tx.execute(
            &format!(
                "create view attributes (rowid, ident)
                          as select e, v from datoms where a = {}",
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

        let mut books = HashMap::<i64, Entity>::new();

        {
            let title = s.new_attribute("book/title")?;
            let avg_rating = s.new_attribute("book/avg_rating")?;
            let isbn = s.new_attribute("book/isbn")?;
            let authors = s.new_attribute("book/authors")?;

            let mut r = csv::Reader::from_path("/home/sqwishy/src/goodbooks-10k/books.csv")?;
            for result in r.deserialize().take(500) {
                let book: Book = result?;

                let e = s.new_entity()?;
                s.assert(&e, title.ident, &book.title)?;
                s.assert(&e, avg_rating.ident, &book.average_rating)?;
                s.assert(&e, isbn.ident, &book.isbn)?;
                s.assert(&e, authors.ident, &book.authors)?;

                books.insert(book.book_id, e);
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
                s.assert(&e, book.ident, book_ref)?;
                s.assert(&e, user.ident, &rating.user_id)?;
                s.assert(&e, rank.ident, &rating.rating)?;
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
        let s = Session::new(&tx);

        // eprintln!(
        //     "all datoms: {:#?}",
        //     s.all_datoms::<rusqlite::types::Value>()
        // );

        let mut f = Where::<rusqlite::types::Value>::from(vec![
            pat!(?b "book/title" ?t),
            pat!(?b "book/avg_rating" ?v),
        ]);
        f.preds.push(prd!(?v < 1));

        // let wow = s.find::<rusqlite::types::Value>(
        //     "b",
        //     vec![
        //         pat!(?b "book/title" ?t),
        //         pat!(?b "book/avg_rating" ?v),
        //         // pat!(?r "rating/book" ?b),
        //         // pat!(?r "rating/user" ?u),
        //         // pat!(?r "rating/rank" ?k),
        //     ],
        // );

        eprintln!("{:#?}", f);

        Ok(())
    }
}
