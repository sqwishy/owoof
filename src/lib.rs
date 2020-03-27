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
//!    if ?d > "Jan 1 2020"
//!  show ?c :person/name
//!       ?p :person/name
//!
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
// ^^^ todo; get your shit together ^^^

mod dialogue;
mod matter;
mod sql;

use std::collections::HashMap;

use anyhow::Context;

pub use dialogue::Pattern;
pub use matter::{Projection, Selection, Where};

pub const SCHEMA: &'static str = include_str!("../schema.sql");

/// Wraps a rusqlite transaction to provide this crate's semantics to sqlite.
pub struct Session<'tx> {
    tx: &'tx rusqlite::Transaction<'tx>,
}

pub type EntityId = i64;
pub type EntityHandle = uuid::Uuid;
pub type AttributeId = i64;
pub type AttributeHandle = str;

// TODO get rid of this? this is super annoying?
#[derive(Copy, Clone, PartialEq, Debug)]
pub enum Value<T> {
    Entity(EntityId),
    Other(T),
}

/// reflects the "datoms" table in the database,
/// which references entities and attributes by rowid
#[derive(Debug)]
pub struct DbDatom<T> {
    entity: EntityId,
    attribute: AttributeId,
    value: Value<T>,
}

impl<T> DbDatom<T> {
    // pub fn new(entity: EntityId, attribute: AttributeId, value: T) -> Self {
    //     DbDatom {
    //         entity,
    //         attribute,
    //         value,
    //     }
    // }

    pub fn val(entity: EntityId, attribute: AttributeId, value: T) -> Self {
        DbDatom {
            entity,
            attribute,
            value: Value::Other(value),
        }
    }

    pub fn ent(entity: EntityId, attribute: AttributeId, value: EntityId) -> Self {
        DbDatom {
            entity,
            attribute,
            value: Value::Entity(value),
        }
    }

    pub fn from_row(row: &rusqlite::Row) -> rusqlite::Result<Self>
    where
        T: rusqlite::types::FromSql,
    {
        Self::from_columns(&mut sql::RowCursor::from(row))
    }

    pub fn from_columns<'a>(row: &mut sql::RowCursor<'a>) -> rusqlite::Result<Self>
    where
        T: rusqlite::types::FromSql,
    {
        let entity = row.get()?;
        let attribute = row.get()?;
        let is_ref = row.get()?;
        let value = if is_ref {
            Value::Entity(row.get()?)
        } else {
            Value::Other(row.get()?)
        };
        Ok(DbDatom {
            entity,
            attribute,
            value,
        })
    }
}

/// A version of DbDatom referencing entities and attributes by public handles.
///
/// attribute is parameterized as to use a owned or borrowed string
#[derive(Debug)]
pub struct Datom<S, T> {
    entity: EntityHandle,
    // TODO, the attribute should probably be an attribute(/entity) id or something
    attribute: S, //&'s str,
    value: T,
}

pub type OwnedDatom<T> = Datom<String, T>;

impl<S, T> Datom<S, T> {
    pub fn new(entity: EntityHandle, attribute: S, value: T) -> Self {
        Datom {
            entity,
            attribute,
            value,
        }
    }

    pub fn from_row(row: &rusqlite::Row) -> rusqlite::Result<Self>
    where
        S: rusqlite::types::FromSql,
        T: rusqlite::types::FromSql,
    {
        let entity = row.get(0)?;
        let attribute = row.get(1)?;
        let value: T = row.get(2)?;
        Ok(Datom {
            entity,
            attribute,
            value,
        })
    }
}

impl<'tx> Session<'tx> {
    pub fn new(tx: &'tx rusqlite::Transaction) -> Self {
        Session { tx }
    }

    pub fn new_entity(&self) -> rusqlite::Result<EntityId> {
        let uuid = uuid::Uuid::new_v4();
        let n = self.tx.execute(
            "INSERT INTO entities (uuid) VALUES (?)",
            rusqlite::params![uuid],
        )?;
        assert_eq!(n, 1);
        Ok(self.tx.last_insert_rowid())
    }

    pub fn new_attribute(&self, ident: &str) -> rusqlite::Result<AttributeId> {
        let n = self.tx.execute(
            "INSERT INTO attributes (ident) VALUES (?)",
            rusqlite::params![ident],
        )?;
        assert_eq!(n, 1);
        Ok(self.tx.last_insert_rowid())
    }

    pub fn assert<T>(&self, datom: &DbDatom<T>) -> rusqlite::Result<()>
    where
        T: rusqlite::ToSql,
    {
        let sql = r#"INSERT INTO datoms (e, a, v, is_ref) VALUES (?, ?, ?, ?)"#;
        let mut stmt = self.tx.prepare(sql)?;
        let n = stmt.execute(&[
            &datom.entity as &dyn rusqlite::ToSql,
            &datom.attribute as &dyn rusqlite::ToSql,
            match &datom.value {
                Value::Entity(v) => v as &dyn rusqlite::ToSql,
                Value::Other(v) => v as &dyn rusqlite::ToSql,
            },
            match &datom.value {
                Value::Entity(_) => &true as &dyn rusqlite::ToSql,
                Value::Other(_) => &false as &dyn rusqlite::ToSql,
            },
        ])?;
        assert_eq!(n, 1);
        Ok(())
    }

    /// mostly for debugging ...
    fn all_datoms<T>(&self) -> rusqlite::Result<Vec<OwnedDatom<T>>>
    where
        T: rusqlite::types::FromSql,
    {
        let sql = r#"
            SELECT entities.uuid, attributes.ident, v
              FROM datoms
              JOIN entities ON datoms.e = entities.rowid
              JOIN attributes ON datoms.a = attributes.rowid"#;
        let mut stmt = self.tx.prepare(sql)?;
        let rows = stmt.query_map(rusqlite::NO_PARAMS, Datom::from_row)?;
        rows.collect::<_>()
    }

    fn find<'p, T>(
        &self,
        top: &'p str,
        terms: Vec<dialogue::Pattern<'p, T>>,
    ) -> anyhow::Result<Vec<HashMap<AttributeId, Value<T>>>>
    where
        T: rusqlite::types::FromSql + rusqlite::types::ToSql + std::fmt::Debug,
    {
        let mut query = sql::GenericQuery::default();

        let wh = dialogue::Where { terms };
        let p = Projection::of(&wh);
        let top = p.variables().get(top).expect("undefined variable?");

        sql::select_datomsets(&p, &mut query).unwrap();
        sql::projection_sql(&p, &mut query).unwrap();

        eprintln!("{}", query);

        let mut stmt = self.tx.prepare(query.as_str())?;
        let rows = stmt.query_map(query.params(), |row| {
            let mut c = sql::RowCursor::from(row);
            (0..p.datomsets())
                .map(|_| DbDatom::<T>::from_columns(&mut c))
                .collect::<rusqlite::Result<Vec<DbDatom<_>>>>()
        })?;

        rows.map(|datoms| {
            let datoms: Vec<DbDatom<_>> = datoms?;
            // group datoms from each row by entity
            let mut by_ent: HashMap<EntityId, HashMap<AttributeId, Value<T>>> = HashMap::new();
            // later return the entity for the `top` variable
            let mut top_ent = Option::<EntityId>::None;

            for (n, datom) in datoms.into_iter().enumerate() {
                if n == top.datomset.0 {
                    let e = match top.field {
                        matter::Field::Entity => datom.entity,
                        matter::Field::Value => match datom.value {
                            Value::Entity(e) => e,
                            _ => continue,
                        },
                        _ => continue,
                    };
                    top_ent.replace(e);
                }

                match by_ent.get_mut(&datom.entity) {
                    Some(map) => {
                        map.insert(datom.attribute, datom.value);
                    }
                    None => {
                        let mut map: HashMap<AttributeId, _> = HashMap::new();
                        map.insert(datom.attribute, datom.value);
                        by_ent.insert(datom.entity, map);
                    }
                }
            }

            let top_ent = top_ent.expect("variable did not match an entity");
            let top_map = by_ent
                .remove(&top_ent)
                .expect("this is an actual panic; todo explain why");

            // for attr, val top_map.iter_mut()

            return Ok(top_map);

            // fn reassemble<K, V>(to: &mut HashMap, from: &mut HashMap
        })
        .collect()
    }
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

    pub(crate) fn test_conn() -> rusqlite::Result<rusqlite::Connection> {
        let conn = rusqlite::Connection::open_in_memory()?;
        // conn.create_scalar_function("uuid_generate_v4", 0, false, move |_| {
        //     Ok(uuid::Uuid::new_v4())
        // })?;
        conn.execute_batch(SCHEMA)?;
        Ok(conn)
    }

    pub(crate) fn goodbooks() -> Result<rusqlite::Connection> {
        let mut db = test_conn()?;
        let tx = db.transaction()?;
        let s = Session::new(&tx);

        let mut books = HashMap::<i64, EntityId>::new();

        {
            let title = s.new_attribute("book/title")?;
            let rating = s.new_attribute("book/rating")?;
            let isbn = s.new_attribute("book/isbn")?;
            let authors = s.new_attribute("book/authors")?;

            let mut r = csv::Reader::from_path("/home/sqwishy/src/goodbooks-10k/books.csv")?;
            for result in r.deserialize().take(500) {
                let book: Book = result?;

                let e = s.new_entity()?;
                s.assert(&DbDatom::val(e, title, book.title))?;
                s.assert(&DbDatom::val(e, rating, book.average_rating))?;
                s.assert(&DbDatom::val(e, isbn, book.isbn))?;
                s.assert(&DbDatom::val(e, authors, book.authors))?;

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
                s.assert(&DbDatom::<rusqlite::types::Value>::ent(e, book, *book_ref))?;
                s.assert(&DbDatom::val(e, user, rating.user_id))?;
                s.assert(&DbDatom::val(e, rank, rating.rating))?;
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

        let wow = s.find::<rusqlite::types::Value>(
            "r",
            vec![
                pat!(?b "book/title" ?t),
                pat!(?b "book/rating" ?v),
                pat!(?r "rating/book" ?b),
                pat!(?r "rating/user" ?u),
            ],
        );

        eprintln!("{:#?}", wow);

        Ok(())
    }
}
