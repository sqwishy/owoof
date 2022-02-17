//! To do with reading values off of [`rusqlite::Row`] using [`FromSqlRow`].
//!
//! You can call [`Query::disperse`] to execute it in SQLite.  That function takes a [`FromSqlRow`]
//! that is used to get nice value from a [`rusqlite::Row`].
//!
//! This module is supposed to be stuff implementing [`FromSqlRow`] but right now it's just
//! [`zip_with_keys`], which lets you get a kind of key-value mapping from a row.
//!
//! Otherwise, you can get a single value using [`just`] (which this module exports)
//! and sequences using slices or tuples & arrays up to `[_;9]` or do a bit of branching using [`Either`].
//! ```
//! # use owoof::{AttributeRef, Attribute, sql::QueryWriter, either::{left, right}, Value};
//! # use owoof::disperse::{just, zip_with_keys, Query};
//! # use rusqlite::Connection;
//! #
//! # let mut db = owoof::new_in_memory().unwrap();
//! # let woof = owoof::DontWoof::new(&mut db).unwrap();
//! #
//! // FromSqlRow is implemented on tuples & arrays.
//! let mut fromsql = (
//!     just::<Value>(),
//!     [just::<Value>(), just::<Value>()],
//!     zip_with_keys([Attribute::from_static(":db/id")]),
//! );
//!
//! let results = Query::default()
//!     .push_sql(
//!         r#"
//! SELECT 0, 123
//!      , 0, "is your refrigerator running?"
//!      , 0, "better go catch itjasdkfjlsdfjalskdfjdklsf"
//!      , 1, x'b3ddeb4ca61f44338acd7e10117f142e'
//!      "#,
//!     )
//!     .disperse(fromsql, &woof)
//!     .unwrap();
//!
//! assert_eq!(results.len(), 1); // one result returned
//!
//! match results.into_iter().next().unwrap() {
//!     (Value::Integer(123), texts, obj) => {
//!         assert_eq!(
//!             texts,
//!             vec![
//!                 Value::Text(String::from(
//!                     "is your refrigerator running?"
//!                 )),
//!                 Value::Text(String::from(
//!                     "better go catch itjasdkfjlsdfjalskdfjdklsf"
//!                 )),
//!             ]
//!         );
//!         assert_eq!(
//!             obj.into_iter().collect::<Vec<_>>(),
//!             vec![(
//!                 ":db/id".parse::<Attribute>().unwrap(),
//!                 "#b3ddeb4c-a61f-4433-8acd-7e10117f142e"
//!                     .parse()
//!                     .map(Value::Entity)
//!                     .unwrap(),
//!             ),]
//!         );
//!     }
//!     result => assert!(false, "{:#?}", result),
//! }
//! ```
//!
//! See [`crate::driver`] for a bit more information about [`FromSqlRow`].
use rusqlite::{Row, ToSql};

use crate::either::Either;
use crate::types::Value;
use crate::DontWoof;

pub use crate::driver::{just, ColumnIndex, FromSqlRow, Result};
pub use crate::sql::Query;

impl Query<&dyn ToSql> {
    /// [`FromSqlRow`]
    pub fn disperse<'tx, D: FromSqlRow>(
        &self,
        mut wat: D,
        db: &DontWoof<'tx>,
    ) -> rusqlite::Result<Vec<<D as FromSqlRow>::Out>> {
        let mut stmt = db.prepare(self.as_str())?;
        let query = stmt.query_map(self.params(), |row| wat.from_start_of_row(&row))?;
        query.collect::<rusqlite::Result<Vec<_>>>()
    }

    pub fn count<'tx>(&self, db: &DontWoof<'tx>) -> rusqlite::Result<usize> {
        let mut stmt = db.prepare(self.as_str())?;
        let query = stmt.query_map(self.params(), |_| Ok(()))?;
        Ok(query.count())
    }
}

/// Given a sequence of keys (like attributes) returns an implementation of [`FromSqlRow`] that
/// reads one [`Value`] per key and outputs an [`ObjectMap`], a type that can
/// `serde::Serialize` to a map of keys zipped with values.
///
/// For example, with a suitable query, you might pass two attributes `":db/id"` and
/// `":db/attribute"` you get a map like:
///
/// ```skip
/// {
///   ":db/id": "#b181a977-a8a1-2998-16df-a314c607ecde",
///   ":db/attribute": ":db/attribute"
/// }
/// ```
pub fn zip_with_keys<K>(
    keys: K,
) -> impl FromSqlRow<Out = ObjectMap<<K as IntoIterator>::IntoIter, Vec<Value>>>
where
    K: IntoIterator,
    <K as IntoIterator>::IntoIter: Clone + ExactSizeIterator,
{
    let keys = keys.into_iter();
    row_fn(move |row, idx| {
        let keys = keys.clone();
        std::iter::repeat(just::<Value>())
            .map(|mut v| v.from_sql_row(row, idx))
            .take(keys.len())
            .collect::<Result<Vec<_>, _>>()
            .map(|values| ObjectMap::new(keys, values))
    })
}

/// Create an implementation of [`FromSqlRow`] from a function.
pub fn row_fn<O, F>(f: F) -> RowFn<F>
where
    F: FnMut(&Row, &mut ColumnIndex) -> Result<O>,
{
    RowFn(f)
}

#[derive(Debug, Copy, Clone)]
pub struct RowFn<F>(F);

impl<F, O> FromSqlRow for RowFn<F>
where
    F: FnMut(&Row, &mut ColumnIndex) -> Result<O>,
{
    type Out = O;

    fn from_sql_row(&mut self, row: &Row, idx: &mut ColumnIndex) -> Result<Self::Out> {
        (self.0)(row, idx)
    }
}

/// Serializes a map by zipping `K` and `V`.
/// Initialize this with `zip_with_keys`.
#[derive(Debug)]
pub struct ObjectMap<K, V>(K, V);

impl<K, V> ObjectMap<K, V> {
    pub fn new(k: K, v: V) -> ObjectMap<K, V> {
        ObjectMap(k, v)
    }
}

impl<K, V> ObjectMap<K, V>
where
    K: Iterator,
    V: IntoIterator,
{
    pub fn into_iter(
        self,
    ) -> impl Iterator<Item = (<K as Iterator>::Item, <V as IntoIterator>::Item)> {
        let ObjectMap(key, value) = self;
        key.zip(value.into_iter())
    }
}

#[cfg(feature = "serde")]
mod _serde {
    use super::ObjectMap;
    use serde::ser::{Serialize, SerializeMap, Serializer};
    impl<K, V> Serialize for ObjectMap<K, V>
    where
        K: Clone + ExactSizeIterator,
        <K as Iterator>::Item: Serialize,
        V: Clone + IntoIterator,
        <V as IntoIterator>::Item: Serialize,
    {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let ObjectMap(key, value) = self;
            let mut map = serializer.serialize_map(Some(key.len()))?;
            for (k, v) in key.clone().zip(value.clone().into_iter()) {
                map.serialize_entry(&k, &v)?;
            }
            map.end()
        }
    }
}

impl<L, R> FromSqlRow for Either<L, R>
where
    L: FromSqlRow,
    R: FromSqlRow,
{
    type Out = Either<<L as FromSqlRow>::Out, <R as FromSqlRow>::Out>;

    fn from_sql_row(&mut self, row: &Row, idx: &mut ColumnIndex) -> Result<Self::Out> {
        match self {
            Either::Left(l) => l.from_sql_row(row, idx).map(Either::Left),
            Either::Right(l) => l.from_sql_row(row, idx).map(Either::Right),
        }
    }
}
