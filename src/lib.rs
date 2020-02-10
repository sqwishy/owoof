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
//! shape ?n (:pants/shape)
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

use std::collections::HashMap;

pub const SCHEMA: &'static str = include_str!("../schema.sql");

pub mod time;

pub struct Session<'tx> {
    tx: &'tx rusqlite::Transaction<'tx>,
}

pub type EntityId = i64;
pub type EntityHandle = i64;

impl<'tx> Session<'tx> {
    fn new_entity(&self) -> rusqlite::Result<EntityId> {
        let uuid = uuid::Uuid::new_v4();
        self.tx.execute(
            "INSERT INTO entities (uuid) VALUES (?)",
            rusqlite::params![uuid],
        )?;
        Ok(self.tx.last_insert_rowid())
    }

    fn assert<'s, T>(&self, datom: &Datom<'s, T>) -> rusqlite::Result<()>
    where
        T: rusqlite::ToSql,
    {
        let sql = r#"
               WITH new (e, a, v)
                 AS (SELECT ?, attributes.rowid, ? FROM attributes WHERE ident = ?)
        INSERT INTO datoms (e, a, v)
             SELECT * FROM new;
        "#;
        let mut stmt = self.tx.prepare(sql)?;
        stmt.execute(rusqlite::params![
            datom.entity,
            datom.value,
            datom.attribute
        ])?;
        Ok(())
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

#[derive(Debug)]
pub struct Datom<'s, T> {
    entity: EntityId,
    attribute: &'s str,
    value: T,
}

impl<'s, T> Datom<'s, T> {
    pub fn new(entity: EntityId, attribute: &'s str, value: T) -> Self {
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

    #[test]
    fn it_works() -> Result<()> {
        let mut conn = rusqlite::Connection::open_in_memory()?;
        // conn.create_scalar_function("uuid_generate_v4", 0, false, move |_| {
        //     Ok(uuid::Uuid::new_v4())
        // })?;
        conn.execute_batch(SCHEMA)?;

        let tx = conn.transaction()?;
        let db = Session { tx: &tx };

        let e = db.new_entity()?;
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
}
