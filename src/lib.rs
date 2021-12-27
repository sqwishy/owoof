#![allow(clippy::match_ref_pats)]
#![allow(clippy::option_map_unit_fn)]
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
//! > Match every triplet where some entity named *?a* has the attribute *:pet/name*
//! > with any value.  And match triplets on the same entity *?a* having the attribute
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
//! use owoof::{Network, Value, ValueRef, AttributeRef, disperse::just, traits::*};
//!
//! let mut network = Network::<ValueRef>::default();
//!
//! let (p, _, person) = network
//!     .fluent_triples()
//!     .match_attribute(AttributeRef::from_static(":person/name"))
//!     .eav();
//!
//! let (a, _, _) = network
//!     .fluent_triples()
//!     .match_attribute(AttributeRef::from_static(":pet/human"))
//!     .link_value(p)
//!     .eav();
//!
//! let (a, _, pet) = network
//!     .fluent_triples()
//!     .match_attribute(AttributeRef::from_static(":pet/name"))
//!     .eav();
//!
//! # let mut db = owoof::new_in_memory().unwrap();
//! # let woof = owoof::DontWoof::new(&mut db).unwrap();
//! let _: Vec<(Value, Value)> = network.select()
//!        .field(person)
//!        .field(pet)
//!        .to_query()
//!        .disperse((just(), just()), &woof)
//!        .unwrap();
//! # // assert_eq!(
//! # //     res,
//! # //     vec![
//! # //         (Value::Text("John Arbuckle".to_owned()), Value::Text("Garfield".to_owned())),
//! # //         (Value::Text("John Arbuckle".to_owned()), Value::Text("Oide".to_owned())),
//! # //     ],
//! # // );
//! ```
//!
//! Check out the [`network`] module for some stuff about querying.
//!
//! The [`DontWoof`] type is useful for mutating data.
//!
//! ## Crate Features
//!
//! - explain -- Adds `DontWoof::explain()` to do EXPLAIN QUERY PLAN. *enabled by default*
//! - cli -- Required for `bin/owoof`. Enables serde & serde_json.
//! - serde & serde_json -- Required for `parse_value()` & `parse_pattern()` and for serializing [`Value`]
//!   and [`ValueRef`]

use thiserror::Error;

pub mod disperse;
pub mod driver;
#[cfg(feature = "explain")]
pub mod explain;
pub mod network;
pub mod retrieve;
pub mod soup;
pub mod sql;
pub mod types;

use rusqlite::hooks::Action;
use rusqlite::{OptionalExtension, ToSql};

use std::cell::RefCell;
use std::sync::atomic::{self, AtomicBool};
use std::sync::{Arc, Mutex};

use crate::driver::{TypeTag, ENTITY_ID_TAG};

pub use crate::either::Either;
pub use crate::network::{GenericNetwork, Network, Ordering, OwnedNetwork};
pub use crate::retrieve::{NamedNetwork, Pattern};
pub use crate::soup::Encoded;
pub use crate::types::{Attribute, AttributeRef, Entity, Value, ValueRef};

/// This is just supposed to be some helpful traits re-exported but there's only the one thing in
/// it so there's not much point...
pub mod traits {
    pub use crate::sql::PushToQuery;
}

pub(crate) const SCHEMA: &str = include_str!("../schema.sql");

/// Simply executes all the statements required to build the schema against the given connection.
/// Run this under a transaction that you manage or use [`create_schema_in_transaction`].
pub fn create_schema(db: &rusqlite::Connection) -> rusqlite::Result<()> {
    db.execute_batch(SCHEMA)
}

pub fn create_schema_in_transaction(db: &mut rusqlite::Connection) -> rusqlite::Result<()> {
    let tx = db.transaction()?;
    create_schema(&tx)?;
    tx.commit()
}

pub fn new_in_memory() -> rusqlite::Result<rusqlite::Connection> {
    let mut db = rusqlite::Connection::open_in_memory()?;
    create_schema_in_transaction(&mut db)?;
    Ok(db)
}

/// TODO we only have one variant so what's the point?
#[derive(Debug, Error)]
pub enum Error {
    #[error("sql error")]
    Sql(#[from] rusqlite::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

type Change = i64;

/// This has a bunch of logic for changing data, use [`DontWoof::new`] to make an instance.
///
/// `DontWoof::from(rusqlite::Transaction)` is bad, don't use it.
#[derive(Debug)]
pub struct DontWoof<'tx> {
    tx: HookedTransaction<'tx>,
    changes: Arc<Mutex<Vec<Change>>>,
    changes_swap: RefCell<Vec<Change>>,
    changes_failed: Arc<AtomicBool>,
}

impl<'tx> DontWoof<'tx> {
    /// `DontWoof::from(rusqlite::Transaction)` is bad, don't use it.
    pub fn new(db: &'tx mut rusqlite::Connection) -> Result<Self> {
        db.execute("pragma foreign_keys=on", [])?;
        let tx = db.transaction()?;
        Ok(Self::from(tx))
    }

    /// Look up an attribute by its identifier.
    ///
    /// In other words, find ?e given ?a where ?e :db/attribute ?a.
    pub fn attribute<A: AsRef<AttributeRef>>(&self, a: Encoded<A>) -> Result<Encoded<Entity>> {
        let sql = r#"SELECT rowid FROM "attributes" WHERE ident = ?"#;
        self.tx
            .query_row(sql, &[&a.rowid], |row| row.get::<_, i64>(0))
            .map(Encoded::from_rowid)
            .map_err(Error::from)
    }

    // /// Panics if the given identifier is not valid.
    // pub fn attribute_for(&self, identifier: &'static str) -> Result<Encoded<Entity>> {
    //     let a = self.encode(AttributeRef::from_static(identifier))?;
    //     let e = self.attribute(a)?;
    //     Ok(e)
    // }

    pub fn new_entity(&self) -> Result<Encoded<Entity>> {
        let insert = r#"INSERT INTO "soup" (t, v) VALUES (?, randomblob(16))"#;
        let mut insert = self.tx.prepare_cached(insert)?;
        let n = insert.execute(rusqlite::params![ENTITY_ID_TAG])?;
        assert_eq!(n, 1);
        let rowid = self.tx.last_insert_rowid();
        Ok(Encoded::from_rowid(rowid))
    }

    pub fn decode<T: driver::FromTypeTagAndSqlValue>(&self, e: Encoded<T>) -> Result<T> {
        use driver::FromSqlRow;
        let select = r#"SELECT t, v
                          FROM "soup"
                         WHERE rowid = ?"#;
        let mut select = self.tx.prepare_cached(select)?;
        let t = select.query_row(rusqlite::params![e.rowid], |row| {
            driver::just::<T>().from_start_of_row(row)
        })?;
        Ok(t)
    }

    pub fn fluent_entity(&self) -> Result<FluentEntity> {
        let e = self.new_entity()?;
        Ok(FluentEntity { woof: self, e })
    }

    pub fn encode<V: TypeTag + ToSql>(&self, val: V) -> Result<Encoded<<V as TypeTag>::Factory>> {
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
        let mut select = self.tx.prepare_cached(select)?;
        let rowid = match select
            .query_row(params, |row| row.get::<_, i64>(0))
            .optional()?
        {
            Some(rowid) => rowid,
            None => {
                let insert = r#"INSERT INTO "soup" (t, v) VALUES (?, ?)"#;
                let mut insert = self.tx.prepare_cached(insert)?;
                let n = insert.execute(params)?;
                assert_eq!(n, 1);
                self.tx.last_insert_rowid()
            }
        };

        Ok(rowid)
    }

    /// Insert a single triplet.
    pub fn assert<V: TypeTag>(
        &self,
        e: Encoded<Entity>,
        a: Encoded<Entity>,
        v: Encoded<V>,
    ) -> Result<&Self> {
        /* triples is WITHOUT ROWID so don't try to read the last rowid after an insert */
        let mut stmt = self
            .tx
            .prepare_cached(r#"INSERT INTO "triples" (e,a,v) VALUES (?, ?, ?)"#)?;
        let n = stmt.execute(&[&e.rowid, &a.rowid, &v.rowid])?;
        assert_eq!(n, 1);

        /* This kind of sucks because it's a super rare event but requires accessing a RefCell
         * and unlocking a Mutex.  Using an AtomicBool to flag buffer emptiness allow an early exit
         * doesn't improve performance much (~8ms down to ~6ms) and overall this check is ~less
         * than %1 of an import.  So it's not worth worrying about this too much. */
        self._update_attribute_indexes()?;

        Ok(self)
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
                self.changes_failed.store(true, atomic::Ordering::SeqCst);
                return Ok(());
            }

            swap.sort_unstable();
            swap.dedup();

            let result = self._execute_attribute_index_changes(swap.as_slice());

            swap.clear();

            /* If this is Err(_), don't set self.changes_failed, that refers to synchronization
             * issues.  This is a rusqlite query failure or whatever. */
            result.map(drop)
        } else {
            debug_assert!(false, "failed to borrow changes_swap");
            self.changes_failed.store(true, atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    fn _execute_attribute_index_changes(&self, swap: &[Change]) -> rusqlite::Result<()> {
        /* The changes list just a rowid.  We don't assume from the action whether an attribute has
         * actually been created or removed because we may get change notifications for failed
         * commands that are rolled back -- such as removing an attribute that is in use.
         *
         * So we're paranoid and any change notification just means to recheck the state. */
        swap.iter().try_for_each(|rowid| {
            let mut stmt = self
                .tx
                .prepare_cached(r#"SELECT count(*) FROM "attributes" WHERE rowid = ?"#)?;
            let c: i64 = stmt.query_row(&[rowid], |row| row.get(0))?;
            let sql = if 0 < c {
                format!(
                    r#"CREATE INDEX
                          IF NOT EXISTS "triples-ave-{rowid}"
                                     ON "triples" (v, e)
                                  WHERE a = {rowid}"#,
                    rowid = rowid
                )
            } else {
                format!(
                    r#"DROP INDEX IF EXISTS "triples-ave-{rowid}""#,
                    rowid = rowid
                )
            };
            self.tx.execute(&sql, []).map(drop)
        })
    }

    /// Delete a single triplet.
    pub fn retract<V: TypeTag>(
        &self,
        e: Encoded<Entity>,
        a: Encoded<Entity>,
        v: Encoded<V>,
    ) -> Result<&Self> {
        let mut stmt = self.tx.prepare_cached(
            r#"DELETE FROM "triples"
                WHERE e = ?
                  AND a = ?
                  AND v = ?"#,
        )?;
        let n = stmt.execute(&[&e.rowid, &a.rowid, &v.rowid])?;

        if 0 < n {
            self._update_attribute_indexes()?;
        }

        Ok(self)
    }

    /// Run `PRAGMA optimize;`.  May update indexes and promote better queries.
    ///
    /// The SQLite documentation recommends calling this before closing a connection. (TODO where?)
    ///
    /// See <https://sqlite.org/lang_analyze.html>
    pub fn optimize(&self) -> rusqlite::Result<()> {
        self.tx
            .execute("SELECT * FROM pragma_optimize()", [])
            .map(drop)
    }

    pub fn prefetch_attributes<V>(&self, network: &mut Network<V>) -> Result<()>
    where
        V: TypeTag + ToSql,
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
        F: FnMut(Action, &str, &str, i64) + Send + 'static,
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
        let foreign_keys: i64 = tx
            .query_row("pragma foreign_keys", [], |row| row.get(0))
            .unwrap();
        assert!(1 == foreign_keys);

        /* irc this must be Send because this hook is placed on the Connnection which can be shared
         * by multiple threads.  So Arc and other Send-able primitives are required instead of
         * their !Send counterparts.  */
        let changes = Arc::new(Mutex::new(Vec::<Change>::default()));
        let changes_failed = Arc::new(AtomicBool::new(false));

        let hook = {
            let changes = Arc::clone(&changes);
            let changes_failed = Arc::clone(&changes_failed);
            move |_action: Action, _database: &str, table: &str, rowid: i64| {
                if table == "attributes" {
                    if let Ok(ref mut mutex) = changes.try_lock() {
                        mutex.push(rowid);
                    } else {
                        changes_failed.store(true, atomic::Ordering::SeqCst);
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

pub mod either {
    pub use Either::{Left, Left as left, Right, Right as right};

    #[cfg_attr(
        feature = "serde",
        derive(serde::Serialize, serde::Deserialize),
        serde(untagged)
    )]
    #[derive(Debug, PartialEq)]
    pub enum Either<L, R> {
        Left(L),
        Right(R),
    }

    impl<L, R> Either<L, R> {
        pub fn map_left<LL, F: FnOnce(L) -> LL>(self, f: F) -> Either<LL, R> {
            match self {
                Either::Left(l) => Either::Left(f(l)),
                Either::Right(r) => Either::Right(r),
            }
        }

        pub fn map_right<RR, F: FnOnce(R) -> RR>(self, f: F) -> Either<L, RR> {
            match self {
                Either::Left(l) => Either::Left(l),
                Either::Right(r) => Either::Right(f(r)),
            }
        }
    }

    impl<'a, L, R> TryFrom<&'a str> for Either<L, R>
    where
        L: TryFrom<&'a str>,
        R: TryFrom<&'a str>,
    {
        type Error = (
            <L as TryFrom<&'a str>>::Error,
            <R as TryFrom<&'a str>>::Error,
        );

        fn try_from(s: &'a str) -> Result<Self, Self::Error> {
            L::try_from(s).map(Either::Left).or_else(|a_err| {
                R::try_from(s)
                    .map(Either::Right)
                    .map_err(|b_err| (a_err, b_err))
            })
        }
    }
}

/// A derpy meme that copies [`rusqlite::OptionalExtension`].
pub trait Optional<T> {
    fn optional(self) -> Result<Option<T>>;
}

impl<T> Optional<T> for Result<T, Error> {
    fn optional(self) -> Result<Option<T>> {
        self.map_err(|Error::Sql(err)| err)
            .optional()
            .map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn rusqlite_in_memory() -> Result<rusqlite::Connection> {
        let mut db = rusqlite::Connection::open_in_memory()?;
        {
            let tx = db.transaction()?;
            tx.execute_batch(SCHEMA)?;
            tx.commit()?;
        }
        Ok(db)
    }

    #[test]
    fn test_decode_new_entity() -> anyhow::Result<()> {
        let mut db = rusqlite_in_memory()?;
        let woof = DontWoof::new(&mut db)?;

        let e = woof.new_entity()?;
        let _ = woof.decode(e)?;
        Ok(())
    }

    #[test]
    fn test_decode() -> anyhow::Result<()> {
        let mut db = rusqlite_in_memory()?;
        let woof = DontWoof::new(&mut db)?;

        let v = woof.encode(ValueRef::from("hello world"))?;
        assert_eq!(Value::Text("hello world".to_owned()), woof.decode(v)?);
        Ok(())
    }

    #[test]
    fn test_retract() -> anyhow::Result<()> {
        let mut db = rusqlite_in_memory()?;
        let woof = DontWoof::new(&mut db)?;

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

        assert!(woof
            .retract(
                animal_name,
                db_attr,
                woof.encode(":animal/name".parse::<Attribute>()?)?,
            )
            .is_err());

        woof.retract(garfield, animal_name, woof.encode(ValueRef::from("Cat"))?)?;

        woof.retract(
            animal_name,
            db_attr,
            woof.encode(":animal/name".parse::<Attribute>()?)?,
        )?;

        Ok(())
    }
}
