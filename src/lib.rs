#![allow(unused)]
//! owoof is about interacting with a SQLite-backed database using entity-attribute-value triples
//! and pattern matching.
//!
//! It is inspired by Datomic and I wrote [a wordy post on my
//! blog](https://froghat.ca/blag/dont-woof/) that explains some of the motivation.  Although, that
//! post is about an earlier version of this library.
//!
//! ## tldr
//!
//! Consider a database of triplets that looks like ...
//! ```ignore
//! 100 :animal/name  "Cat"
//! 100 :pet/name     "Garfield"
//! 101 :animal/name  "Dog"
//! 101 :pet/name     "Odie"
//! 102 :person/name  "John Arbuckle"
//! 100 :pet/human    102
//! 101 :pet/human    102
//! ```
//!
//! And consider this pattern ...
//! ```ignore
//! ?_ :pet/name ?_
//! ```
//! If we were to match triplets using this pattern, we'd be asking for triplets with: any
//! entity, for the *:pet/name* attribute, and any value. And we would get the following
//! two triplets.
//! ```ignore
//! 100 :pet/name    "Garfield"
//! 101 :pet/name    "Odie"
//! ```
//! Those are the triplets that exist that match that pattern.
//!
//! Now consider this **pair** of patterns ...
//! ```ignore
//! ?a :pet/name     ?_
//! ?a :animal/name  "Cat"
//! ```
//! The **first** pattern matches the two triplets from earlier.
//! But the **second** matches just:
//! ```ignore
//! 100 :animal/name "Cat"
//! ```
//! But, we are not interested in *every* combination of triplets in the sets that matched our
//! patterns.  We related the patterns by constraining the *entity* to the same variable, *?a*.
//! (This is often called unification.)  This means that we only match the combinations of triplets
//! between sets when they share the same entity.  So we end up with *one* result with *two*
//! triplets.
//! ```ignore
//! 100 :pet/name    "Garfield"
//! 100 :animal/name "Cat"
//! ```
//!
//! It's like if we were to say:
//! > Match every triplet having the attribute *:pet/name* with any value and for some any
//! > entity named *?a*, match also triplets on the same entity *?a* having the attribute
//! > *:animal/name* with the value "Cat".
//!
//! Another one might be:
//! ```ignore
//! ?p :person/name ?person
//! ?a :pet/human   ?p
//! ?a :pet/name    ?pet
//! ```
//! Here, the variables *?human* and *?pet* don't relate triplets together, but they're
//! there so we can refer to them when we want to get information out.
//!
//! These patterns are saying:
//! > Given each entity *?a* having the *:pet/name* *?pet*
//! >   and each entity *?p* having the *:person/name* *?person*,
//! > match only combinations of these where there exists some triplet *?a* *:pet/human*
//! > *?p*. That is, where *?a*'s human is *?p*.
//!
//! The point of owoof is to allow us to build a database of triplets and to use pattern matching
//! to ask it questions and get values out -- like the values of *?person* and *?pet* in the above
//! query:
//! ```ignore
//! "John Arbuckle" "Garfield"
//! "John Arbuckle" "Odie"
//! ```
//!
//! Here's a kind of WIP example of the rust API corresponding to the patterns above:
//!
//! ```
//! use owoof::{Network, ValueRef, AttributeRef};
//!
//! let mut network = Network::<ValueRef>::default();
//!
//! let (p, _, person) = network
//!     .fluent_triples()
//!     .match_attribute(":person/name")
//!     .eav();
//!
//! let (a, _, _) = network
//!     .fluent_triples()
//!     .match_attribute(":pet/human")
//!     .link_value(p)
//!     .eav();
//!
//! let (a, _, pet) = network
//!     .fluent_triples()
//!     .match_attribute(":pet/name")
//!     .eav();
//!     
//! // TODO finish this example with
//! // owoof.do_meme(network.select().field(person).field(pet))?
//! // or something
//! ```
//!
//! Check out the [`network`] module for some memes ... TODO
//!
//! The [`DontWoof`] type is the main interface around talking to SQLite.
use thiserror::Error;

pub mod driver;
#[cfg(feature = "explain")]
pub mod explain;
pub mod network;
pub mod select;
pub mod soup;
pub mod sql;
// pub mod sync;
pub mod types;

use rusqlite::hooks::Action;
use rusqlite::{OptionalExtension, ToSql};

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

pub use network::{GenericNetwork, Network, OwnedNetwork};
pub use select::Select;
pub use soup::Encoded;
use types::TypeTag;
pub use types::{Attribute, AttributeRef, Entity, Value, ValueRef};

pub(crate) const SCHEMA: &str = include_str!("../schema.sql");

#[derive(Debug, Error)]
pub enum Error {
    #[error("sql error")]
    Sql(#[from] rusqlite::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

type Change = (Action, i64);

#[derive(Debug)]
pub struct DontWoof<'tx> {
    tx: HookedTransaction<'tx>,
    changes: Arc<Mutex<Vec<Change>>>,
    changes_swap: RefCell<Vec<Change>>,
    changes_failed: Arc<AtomicBool>,
}

impl<'tx> DontWoof<'tx> {
    /// Look up an attribute by its identifier.
    ///
    /// In other words, find ?e given ?a where ?e :db/attribute ?a.
    pub fn attribute<'a, A: Into<&'a AttributeRef>>(
        &self,
        a: Encoded<A>,
    ) -> Result<Encoded<Entity>> {
        let sql = r#"SELECT rowid FROM "attributes" WHERE ident = ?"#;
        self.tx
            .query_row(sql, &[&a.rowid], |row| row.get::<_, i64>(0))
            // .optional()
            .map(Encoded::from_rowid)
            .map_err(Error::from)
    }

    pub fn new_entity(&self) -> Result<Encoded<Entity>> {
        let n = self.tx.execute(
            r#"INSERT INTO "soup" (t, v) VALUES (?, randomblob(16))"#,
            rusqlite::params![types::ENTITY_ID_TAG],
        )?;
        assert_eq!(n, 1);
        let rowid = self.tx.last_insert_rowid();
        Ok(Encoded::from_rowid(rowid))
    }

    pub fn fluent_entity(&self) -> Result<FluentEntity> {
        let e = self.new_entity()?;
        Ok(FluentEntity { woof: self, e })
    }

    pub fn encode<V: TypeTag + ToSql>(&self, val: V) -> Result<Encoded<V>> {
        let rowid: i64 = self._encode(val.type_tag(), &val as &dyn ToSql)?;
        Ok(Encoded::from_rowid(rowid))
    }

    fn _encode(&self, tag: i64, val: &dyn ToSql) -> Result<i64> {
        let params = rusqlite::params![tag, val];

        /* some brief testing suggests this is faster than INSERT ON CONFLICT RETURNING */

        let select = r#"SELECT rowid
                          FROM "soup"
                         WHERE t = ?
                           AND v = ?"#;
        let rowid = match self
            .tx
            .query_row(select, params, |row| row.get::<_, i64>(0))
            .optional()?
        {
            Some(rowid) => rowid,
            None => {
                let insert = r#"INSERT INTO "soup" (t, v) VALUES (?, ?)"#;
                let n = self.tx.execute(insert, params)?;
                assert_eq!(n, 1);
                self.tx.last_insert_rowid()
            }
        };

        Ok(rowid)
    }

    pub fn assert<V: TypeTag>(
        &self,
        e: Encoded<Entity>,
        a: Encoded<Entity>,
        v: Encoded<V>,
    ) -> Result<()> {
        /* triples is WITHOUT ROWID so don't try to read the last rowid after an insert */
        let n = self.tx.execute(
            r#"INSERT INTO "triples" (e,a,v) VALUES (?, ?, ?)"#,
            &[&e.rowid, &a.rowid, &v.rowid],
        )?;
        assert_eq!(n, 1);

        /* This kind of sucks because it's a super rare event but requires accessing a RefCell
         * and unlocking a Mutex.  Using an AtomicBool to flag buffer emptiness allow an early exit
         * doesn't improve performance much (~8ms down to ~6ms) and overall this check is ~less
         * than %1 of an import.  So it's not worth worrying about this too much. */
        self._update_attribute_indexes()?;

        Ok(())
    }

    fn _update_attribute_indexes(&self) -> rusqlite::Result<()> {
        /* Since Connection is Send, this can fail to lock.  And we don't have a way to recover and
         * try again if that happens.  For now, there should be a big warning about this in the
         * API.  TODO XXX FIXME */
        if let Ok(mut swap) = self.changes_swap.try_borrow_mut() {
            debug_assert!(swap.is_empty());
            if let Ok(ref mut mutex) = self.changes.try_lock() {
                if mutex.is_empty() {
                    return Ok(());
                }
                std::mem::swap::<Vec<Change>>(mutex.as_mut(), swap.as_mut());
            } else {
                debug_assert!(false, "failed to lock changes");
                self.changes_failed.store(true, Ordering::SeqCst);
                return Ok(());
            }

            let result = self._execute_attribute_index_changes(swap.as_slice());

            swap.clear();

            /* If this is Err(_), don't set self.changes_failed, that refers to synchronization
             * issues.  This is a rusqlite query failure or whatever. */
            result.map(drop)
        } else {
            debug_assert!(false, "failed to borrow changes_swap");
            self.changes_failed.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    fn _execute_attribute_index_changes(&self, swap: &[Change]) -> rusqlite::Result<()> {
        swap.iter()
            .filter_map(|(action, rowid)| match action {
                Action::SQLITE_INSERT => Some(format!(
                    r#"CREATE INDEX "triples-ave-{rowid}" ON "triples" (v, e) WHERE a = {rowid}"#,
                    rowid = rowid
                )),
                Action::SQLITE_DELETE => Some(format!(
                    r#"DROP INDEX "triples-ave-{rowid}""#,
                    rowid = rowid
                )),
                _ => None,
            })
            .try_for_each(|sql| self.tx.execute(&sql, []).map(drop))
    }

    /// Run `PRAGMA optimize;`.
    ///
    /// This is mostly for troubleshooting but may change performance by updating index statistics
    /// used by the query planner.
    ///
    /// See <https://sqlite.org/lang_analyze.html>
    pub fn optimize(&self) -> rusqlite::Result<()> {
        let n = self.tx.execute("SELECT * FROM pragma_optimize()", [])?;
        assert_eq!(n, 1);
        Ok(())
    }

    pub fn prefetch_attributes<V>(&self, network: &mut Network<V>) -> Result<()>
    where
        V: TypeTag + ToSql + std::fmt::Debug,
    {
        use crate::network::{Constraint, Field, Match};

        network
            .constraints_mut()
            .iter_mut()
            .try_for_each(|constraint| match constraint {
                &mut Constraint::Eq { lh, rh: Match::Value(ref v) }
                    if lh.field() == Field::Attribute =>
                {
                    let mut stmt = self.tx.prepare_cached(
                        r#"
                    SELECT a.rowid
                      FROM attributes a
                      JOIN soup s ON a.ident = s.rowid
                     WHERE s.t = ? AND s.v = ?
                     LIMIT 1
                    "#,
                    )?;
                    let type_tag = v.type_tag();
                    let rh = stmt
                        .query_row(rusqlite::params![type_tag, v], |row| row.get(0))
                        .map(Encoded::from_rowid)
                        .map(Match::Encoded)
                        .optional()?;
                    // If a lookup failed, the query probably won't succeed, but whatever ...
                    if let Some(rh) = rh {
                        *constraint = Constraint::Eq { lh, rh };
                    }
                    Ok(())
                }
                _ => Result::<(), Error>::Ok(()),
            })?;

        Ok(())
    }

    pub fn into_tx(self) -> rusqlite::Transaction<'tx> {
        self.tx.unwrap()
    }
}

#[derive(Debug)]
struct HookedTransaction<'tx>(Option<rusqlite::Transaction<'tx>>);

impl<'tx> std::ops::Deref for HookedTransaction<'tx> {
    type Target = rusqlite::Transaction<'tx>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap()
    }
}

impl<'tx> HookedTransaction<'tx> {
    fn new<F>(tx: rusqlite::Transaction<'tx>, hook: F) -> Self
    where
        F: FnMut(Action, &str, &str, i64) + Send,
    {
        tx.update_hook(Some(hook));
        HookedTransaction(Some(tx))
    }

    fn unwrap(mut self) -> rusqlite::Transaction<'tx> {
        let tx = self.0.take().unwrap();
        HookedTransaction::_unhook(&tx);
        tx
    }

    fn _unhook(db: &rusqlite::Connection) {
        let no_hook = None::<fn(_: Action, _: &str, _: &str, _: i64)>;
        db.update_hook(no_hook);
    }
}

impl<'tx> Drop for HookedTransaction<'tx> {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            HookedTransaction::_unhook(&tx);
        }
    }
}

impl<'tx> From<rusqlite::Transaction<'tx>> for DontWoof<'tx> {
    fn from(tx: rusqlite::Transaction<'tx>) -> Self {
        /* irc this must be Send because this hook is placed on the Connnection which can be shared
         * by multiple threads.  So Arc and other Send-able primitives are required instead of
         * their !Send counterparts.  */
        let changes = Arc::new(Mutex::new(Vec::<Change>::default()));
        let changes_failed = Arc::new(AtomicBool::new(false));

        /* We have to be careful here, the borrow checker doesn't know that this function must be
         * valid after calling update_hook().  We use `move` to make sure we don't borrow anything
         * that will be dropped in this scope ... */
        let hook = {
            let changes = Arc::clone(&changes);
            let changes_failed = Arc::clone(&changes_failed);
            move |action: Action, _database: &str, table: &str, rowid: i64| {
                if table == "attributes" {
                    if let Ok(ref mut mutex) = changes.try_lock() {
                        mutex.push((action, rowid));
                    } else {
                        changes_failed.store(true, Ordering::SeqCst);
                    }
                }
            }
        };

        DontWoof {
            tx: HookedTransaction::new(tx, hook),
            changes,
            changes_swap: RefCell::new(Vec::<Change>::default()),
            changes_failed,
        }
    }
}

impl<'tx> std::ops::Deref for DontWoof<'tx> {
    type Target = rusqlite::Transaction<'tx>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

/// ???
pub struct FluentEntity<'w, 'tx> {
    woof: &'w DontWoof<'tx>,
    e: Encoded<Entity>,
}

impl FluentEntity<'_, '_> {
    pub fn assert<V: TypeTag>(&self, a: Encoded<Entity>, v: Encoded<V>) -> Result<&Self> {
        self.woof.assert(self.e, a, v)?;
        Ok(self)
    }
}

impl From<&FluentEntity<'_, '_>> for Encoded<Entity> {
    fn from(fl: &FluentEntity) -> Self {
        fl.e
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;

    fn rusqlite_in_memory() -> Result<rusqlite::Connection> {
        let mut db = rusqlite::Connection::open_in_memory()?;
        {
            let mut tx = db.transaction()?;
            tx.execute_batch(SCHEMA)?;
            tx.commit()?;
        }
        Ok(db)
    }

    #[test]
    fn test() -> anyhow::Result<()> {
        let mut db = rusqlite_in_memory()?;
        let mut tx = db.transaction()?;
        let woof = DontWoof::from(tx);

        let db_attr = woof.attribute(woof.encode(AttributeRef::from_static(":db/attribute"))?)?;

        let pet_name = woof
            .fluent_entity()?
            .assert(db_attr, woof.encode(":pet/name".parse::<Attribute>()?)?)?
            .into();

        let animal_name: Encoded<Entity> = woof
            .fluent_entity()?
            .assert(db_attr, woof.encode(":animal/name".parse::<Attribute>()?)?)?
            .into();

        let garfield: Encoded<Entity> = woof
            .fluent_entity()?
            .assert(pet_name, woof.encode(ValueRef::from("Garfield"))?)?
            .assert(animal_name, woof.encode(ValueRef::from("Cat"))?)?
            .into();

        Ok(())
    }

    #[test]
    fn test_books() -> anyhow::Result<()> {
        let mut db = rusqlite::Connection::open("/tmp/owoof-test.sqlite")?;
        let mut tx = db.transaction()?;
        let mut woof = DontWoof::from(tx);
        // woof.execute_batch(SCHEMA)?;
        // import_books(&woof)?;
        // woof.into_tx().commit()?;

        // return Ok(());

        use crate::network::*;

        let mut network = Network::<ValueRef>::default();

        let (e, _, v) = network
            .fluent_triples()
            .match_attribute(AttributeRef::from_static(":db/attribute"))
            .eav();

        woof.prefetch_attributes(&mut network)?;

        {
            let mut q = sql::Query::default();
            use sql::PushToQuery;
            Select::from(&network)
                .field(e)
                .field(v)
                // .order_by(v.asc())
                .push_to_query(&mut q);

            eprintln!(">>> {}", q);

            let mut stmt = woof.prepare(q.as_str())?;
            let rows = stmt.query_map(q.params(), |row| {
                use crate::driver::FromSqlAndTypeTag;

                let type_tag = row.get::<_, i64>(0)?;
                let value = row.get_ref(1)?;
                let a = Value::column_result(type_tag, value)?;

                let type_tag = row.get::<_, i64>(2)?;
                let value = row.get_ref(3)?;
                let b = Value::column_result(type_tag, value)?;

                Ok((a, b))
            })?;
            for result in rows {
                let (a, b) = result.context("row result")?;
                eprintln!("{:?} {:?}", a, b);
            }
        }

        // return Ok(());

        // TODO
        let mut network: Network = Default::default();

        // ?calvin :book/title "The Complete Calvin and Hobbes"
        let calvin = network
            .fluent_triples()
            .match_attribute(AttributeRef::from_static(":book/title"))
            .match_value("The Complete Calvin and Hobbes")
            .entity();
        // ?rating :rating/book ?calvin
        let rating = network
            .fluent_triples()
            .match_attribute(AttributeRef::from_static(":rating/book"))
            .link_value(calvin)
            .entity();
        // ?rating :rating/score 1
        network
            .fluent_triples()
            .link_entity(rating)
            .match_attribute(AttributeRef::from_static(":rating/score"))
            .match_value(1);
        // ?rating :rating/user ?user
        let user = network
            .fluent_triples()
            .link_entity(rating)
            .match_attribute(AttributeRef::from_static(":rating/user"))
            .value();
        // ?more-great-takes :rating/user ?user
        let more_great_takes = network
            .fluent_triples()
            .match_attribute(AttributeRef::from_static(":rating/user"))
            .link_value(user)
            .entity();
        // ?more-great-takes :rating/book ?book
        let book = network
            .fluent_triples()
            .link_entity(more_great_takes)
            .match_attribute(AttributeRef::from_static(":rating/book"))
            .value();
        // ?more-great-takes :rating/score 5
        network
            .fluent_triples()
            .link_entity(more_great_takes)
            .match_attribute(AttributeRef::from_static(":rating/score"))
            .match_value(5);
        // ?book :book/title
        let title = network
            .fluent_triples()
            .link_entity(book)
            .match_attribute(AttributeRef::from_static(":book/title"))
            .value();
        // ?book :book/avg-rating
        let rating = network
            .fluent_triples()
            .link_entity(book)
            .match_attribute(AttributeRef::from_static(":book/avg-rating"))
            .value();

        woof.prefetch_attributes(&mut network)?;

        let e = woof
            .explain_plan(&Select::from(&network).field(title).field(rating))
            .context("explain")?;
        eprintln!("{}", e);

        {
            let mut q = sql::Query::default();
            use sql::PushToQuery;
            Select::from(&network)
                .field(title)
                .field(rating)
                // .limit(10)
                .push_to_query(&mut q);
            eprintln!(">>> {}", q);

            let mut stmt = woof.prepare(q.as_str())?;
            let rows = stmt.query_map(q.params(), |row| {
                use crate::driver::FromSqlAndTypeTag;

                let type_tag = row.get::<_, i64>(0)?;
                let value = row.get_ref(1)?;
                let a = Value::column_result(type_tag, value)?;

                let type_tag = row.get::<_, i64>(2)?;
                let value = row.get_ref(3)?;
                let b = Value::column_result(type_tag, value)?;

                Ok((a, b))
            })?;
            for result in rows {
                let (a, b) = result.context("row result")?;
                eprintln!("{:?} {:?}", a, b);
            }
        }

        Ok(())
    }

    fn import_books(woof: &DontWoof) -> anyhow::Result<()> {
        let mut books = std::collections::BTreeMap::<i64, Encoded<Entity>>::new();

        let db_attr = woof.attribute(woof.encode(AttributeRef::from_static(":db/attribute"))?)?;

        let title: Encoded<Entity> = woof
            .fluent_entity()?
            .assert(db_attr, woof.encode(":book/title".parse::<Attribute>()?)?)?
            .into();
        let isbn: Encoded<Entity> = woof
            .fluent_entity()?
            .assert(db_attr, woof.encode(":book/isbn".parse::<Attribute>()?)?)?
            .into();
        let authors: Encoded<Entity> = woof
            .fluent_entity()?
            .assert(db_attr, woof.encode(":book/authors".parse::<Attribute>()?)?)?
            .into();
        let avg_rating: Encoded<Entity> = woof
            .fluent_entity()?
            .assert(
                db_attr,
                woof.encode(":book/avg-rating".parse::<Attribute>()?)?,
            )?
            .into();

        let mut r = csv::Reader::from_path("goodbooks-10k/books.csv")?;
        for result in r.deserialize() {
            let book: Book = result?;
            let entity = woof
                .fluent_entity()?
                .assert(title, woof.encode(Value::from(book.title))?)?
                .assert(isbn, woof.encode(Value::from(book.isbn))?)?
                .assert(authors, woof.encode(Value::from(book.authors))?)?
                .assert(avg_rating, woof.encode(Value::from(book.average_rating))?)?
                .into();
            books.insert(book.book_id, entity);
        }

        let score: Encoded<Entity> = woof // aka one-to-five
            .fluent_entity()?
            .assert(db_attr, woof.encode(":rating/score".parse::<Attribute>()?)?)?
            .into();
        let book: Encoded<Entity> = woof
            .fluent_entity()?
            .assert(db_attr, woof.encode(":rating/book".parse::<Attribute>()?)?)?
            .into();
        let user: Encoded<Entity> = woof
            .fluent_entity()?
            .assert(db_attr, woof.encode(":rating/user".parse::<Attribute>()?)?)?
            .into();

        let mut r = csv::Reader::from_path("goodbooks-10k/ratings.csv")?;
        for result in r.deserialize().take(5_000) {
            let rating: Rating = result?;
            let book_ref = books[&rating.book_id];

            woof.fluent_entity()?
                .assert(book, book_ref)?
                .assert(user, woof.encode(ValueRef::from(rating.user_id))?)?
                .assert(score, woof.encode(ValueRef::from(rating.rating))?)?;
        }

        return Ok(());

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
}
