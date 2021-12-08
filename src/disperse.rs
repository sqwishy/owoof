//! To do with reading values off of `rusqlite::Row`s
use rusqlite::Row;

use crate::driver::{just, ColumnIndex, FromSqlRow, Result};
use crate::either::Either;
use crate::types::Value;

/// Given a sequence of keys (like attributes) returns an implementation of [`FromSqlRow`] that
/// reads one [`Value`] per key and outputs an [`ObjectMap`], a type that can
/// [`serde::Serialize`] to a map of keys zipped with values.
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
