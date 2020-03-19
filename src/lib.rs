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
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
// ^^^ todo; get your shit together ^^^

mod dialogue;
mod matter;
mod sql;

use std::collections::HashMap;

pub use dialogue::Pattern;
pub use matter::Projection;

pub const SCHEMA: &'static str = include_str!("../schema.sql");

/// Wraps a rusqlite transaction to provide this crate's semantics to sqlite.
pub struct Session<'tx> {
    tx: &'tx rusqlite::Transaction<'tx>,
}

pub type EntityId = i64;
pub type EntityHandle = uuid::Uuid;
pub type AttributeId = i64;
pub type AttributeHandle = str;

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

    pub fn assert<S, T>(&self, datom: &Datom<S, T>) -> rusqlite::Result<()>
    where
        S: rusqlite::ToSql,
        T: rusqlite::ToSql,
    {
        // TODO this does nothing if the attribute is missing
        let sql = r#"
               WITH new (e, a, v)
                 AS (SELECT ?, attributes.rowid, ? FROM attributes WHERE ident = ?)
        INSERT INTO datoms (e, a, v)
             SELECT * FROM new;
        "#;
        let mut stmt = self.tx.prepare(sql)?;
        let n = stmt.execute(rusqlite::params![
            datom.entity,
            datom.value,
            datom.attribute
        ])?;
        assert_eq!(n, 1);
        Ok(())
    }

    fn all_datoms<T>(&self) -> rusqlite::Result<Vec<OwnedDatom<T>>>
    where
        T: rusqlite::types::FromSql,
    {
        let sql = r#"
            SELECT e, attributes.ident, v
              FROM datoms
              JOIN attributes ON datoms.a = attributes.rowid"#;
        let mut stmt = self.tx.prepare(sql)?;
        let rows = stmt.query_map(rusqlite::NO_PARAMS, |row| {
            let entity = row.get(0)?;
            let attribute = row.get(1)?;
            let value: T = row.get(2)?;
            Ok(Datom {
                entity,
                attribute,
                value,
            })
        })?;
        rows.collect::<_>()
    }
}

// pub struct Transaction<'s> {
//     // ts: time::DateTime,
//     datoms: Vec<Datom<'s, std::rc::Rc<dyn std::any::Any>>>,
// }

// pub enum Change<'s, T> {
//     Assert(Datom<'s, T>),
//     Retract(Datom<'s, T>),
// }

/// Super generic type, not used much except I think to represent what we store in sqlite
#[derive(Debug)]
pub struct Datom<S, T> {
    entity: EntityId,
    // TODO, the attribute should probably be an attribute(/entity) id or something
    attribute: S, //&'s str,
    value: T,
}

pub type OwnedDatom<T> = Datom<String, T>;

impl<S, T> Datom<S, T> {
    pub fn new(entity: EntityId, attribute: S, value: T) -> Self {
        Datom {
            entity,
            attribute,
            value,
        }
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

    #[test]
    fn it_works() -> Result<()> {
        let mut conn = test_conn()?;
        let tx = conn.transaction()?;
        let db = Session::new(&tx);

        let e = db.new_entity()?;
        db.new_attribute("article/title")?;
        db.assert(&Datom::new(e, "article/title", "Nice Meme"))?;

        let anchorman: HashMap<_, _> = {
            let mut v = Vec::<(_, Box<dyn rusqlite::ToSql>)>::new();
            v.push((
                "film/title",
                Box::new("Anchorman: The Legend of Ron Burgundy"),
            ));
            v.push(("film/year", Box::new(2004)));
            v.push(("film/rating", Box::new(7.2)));
            v.into_iter().collect()
        };

        let pirates: HashMap<_, _> = {
            let mut v = Vec::<(_, Box<dyn rusqlite::ToSql>)>::new();
            v.push((
                "film/title",
                Box::new("Pirates of the Caribbean: The Curse of the Black Pearl"),
            ));
            v.push(("film/year", Box::new(2003)));
            v.push(("film/rating", Box::new(8.0)));
            v.into_iter().collect()
        };

        Ok(())
    }

    pub(crate) fn goodbooks() -> Result<rusqlite::Connection> {
        let mut db = test_conn()?;
        let tx = db.transaction()?;
        let s = Session::new(&tx);

        s.new_attribute("book/title")?;
        s.new_attribute("book/rating")?;
        s.new_attribute("book/isbn")?;
        s.new_attribute("book/authors")?;

        let mut r = csv::Reader::from_path("/home/sqwishy/src/goodbooks-10k/books.csv")?;
        for result in r.deserialize().take(500) {
            let book: Book = result?;

            let e = s.new_entity()?;
            s.assert(&Datom::new(e, "book/title", book.title))?;
            s.assert(&Datom::new(e, "book/isbn", book.isbn))?;
            s.assert(&Datom::new(e, "book/authors", book.authors))?;
        }

        drop(tx);
        return Ok(db);

        #[derive(Debug, serde::Deserialize)]
        struct Book {
            title: String,
            isbn: String,
            authors: String,
        }
    }

    #[test]
    fn wow() -> Result<()> {
        let mut db = goodbooks()?;
        let tx = db.transaction()?;
        let s = Session::new(&tx);

        let p: Pattern<&'static str> = pat!(?b "book/title" ?t);

        // Where { terms: vec![p] }

        Ok(())
    }
}
