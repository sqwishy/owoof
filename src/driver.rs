//! [`rusqlite::types::ToSql`] and [`rusqlite::types::FromSql`] implementations on [`crate::types`]
use rusqlite::{
    types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef as SqlValueRef},
    Result, Row,
};

use crate::types::{Attribute, AttributeRef, Entity, Value, ValueRef};

impl ToSql for Value {
    fn to_sql(&self) -> Result<ToSqlOutput> {
        match self {
            Value::Entity(e) => e.to_sql(),
            Value::Attribute(a) => a.to_sql(),
            Value::Text(s) => s.to_sql(),
            Value::Integer(i) => i.to_sql(),
            Value::Float(f) => f.to_sql(),
            Value::Boolean(b) => b.to_sql(),
            Value::Uuid(u) => u.to_sql(),
            Value::Blob(b) => b.to_sql(),
        }
    }
}

impl ToSql for ValueRef<'_> {
    fn to_sql(&self) -> Result<ToSqlOutput> {
        match self {
            ValueRef::Entity(e) => e.to_sql(),
            ValueRef::Attribute(a) => a.to_sql(),
            ValueRef::Text(s) => s.to_sql(),
            ValueRef::Integer(i) => i.to_sql(),
            ValueRef::Float(f) => f.to_sql(),
            ValueRef::Boolean(b) => b.to_sql(),
            ValueRef::Uuid(u) => u.to_sql(),
            ValueRef::Blob(b) => b.to_sql(),
        }
    }
}

impl ToSql for Entity {
    fn to_sql(&self) -> Result<ToSqlOutput> {
        (**self).to_sql()
    }
}

impl<'a> FromSql for Entity {
    fn column_result(value: SqlValueRef) -> FromSqlResult<Self> {
        uuid::Uuid::column_result(value).map(Entity::from)
    }
}

impl ToSql for Attribute {
    fn to_sql(&self) -> Result<ToSqlOutput> {
        self.just_the_identifier().to_sql()
    }
}

impl<'a> ToSql for &'a AttributeRef {
    fn to_sql(&self) -> Result<ToSqlOutput> {
        self.just_the_identifier().to_sql()
    }
}

impl<'a> FromSql for Attribute {
    fn column_result(value: SqlValueRef) -> FromSqlResult<Self> {
        value.as_str().map(Attribute::from_ident)
    }
}

pub trait FromTypeTagAndSqlValue: Sized {
    fn from_type_tag_and_sql_value(type_tag: i64, value: SqlValueRef<'_>) -> FromSqlResult<Self>;
}

impl FromTypeTagAndSqlValue for Value {
    fn from_type_tag_and_sql_value(type_tag: i64, value: SqlValueRef<'_>) -> FromSqlResult<Self> {
        match type_tag {
            crate::types::ENTITY_ID_TAG => Entity::column_result(value).map(Value::Entity),
            crate::types::ATTRIBUTE_IDENTIFIER_TAG => {
                Attribute::column_result(value).map(Value::Attribute)
            }
            crate::types::PLAIN_TAG => match value {
                SqlValueRef::Null => todo!(),
                SqlValueRef::Integer(i) => Ok(Value::Integer(i)),
                SqlValueRef::Real(f) => Ok(Value::Float(f)),
                SqlValueRef::Text(t) => String::from_utf8(t.to_vec())
                    .map_err(|e| FromSqlError::Other(Box::new(e)))
                    .map(Value::Text),
                SqlValueRef::Blob(b) => Ok(Value::Blob(b.to_vec())),
            },
            /* TODO probably could use a more informative custom type here ... */
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

#[derive(Debug, Default)]
pub struct ColumnIndex(usize);

impl ColumnIndex {
    /// Return the current value and advance the index.
    ///
    /// Returns [`rusqlite::Error::InvalidColumnIndex`] if it can't advance the index because it's
    /// [`usize::MAX`] or whatever but that will never happen so I don't know why I even exist.
    pub fn bump(&mut self) -> Result<usize> {
        let idx = self.0;
        match self.0.checked_add(1) {
            Some(next) => self.0 = next,
            None => return Err(rusqlite::Error::InvalidColumnIndex(self.0)),
        };
        Ok(idx)
    }
}

/// Adapts a [`rusqlite::Row`] to a rust type implementing [`FromTypeTagAndSqlValue`]
pub trait FromSqlRow {
    type Out;

    fn from_sql_row(&mut self, row: &Row, idx: &mut ColumnIndex) -> Result<Self::Out>;

    fn from_start_of_row(&mut self, row: &Row) -> Result<Self::Out> {
        self.from_sql_row(row, &mut ColumnIndex::default())
    }
}

impl<T> FromSqlRow for &mut [T]
where
    T: FromSqlRow,
{
    type Out = Vec<<T as FromSqlRow>::Out>;

    fn from_sql_row(&mut self, row: &Row, idx: &mut ColumnIndex) -> Result<Self::Out> {
        self.iter_mut()
            .map(|item| item.from_sql_row(row, idx))
            .collect()
    }
}

#[cfg_attr(feature = "serde", derive(serde::Serialize), serde(untagged))]
#[derive(Debug)]
pub enum Either<L, R> {
    Left(L),
    Right(R),
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

/// Implements [`FromSqlRow`] for just one [`FromTypeTagAndSqlValue`].
#[derive(Debug)]
pub struct Just<T: FromTypeTagAndSqlValue>(std::marker::PhantomData<T>);

impl<T: FromTypeTagAndSqlValue> Clone for Just<T> {
    fn clone(&self) -> Self {
        just()
    }
}

pub fn just<T: FromTypeTagAndSqlValue>() -> Just<T> {
    Just(std::marker::PhantomData::<T>)
}

impl<T> FromSqlRow for Just<T>
where
    T: FromTypeTagAndSqlValue + Sized,
{
    type Out = T;

    fn from_sql_row(&mut self, row: &Row, idx: &mut ColumnIndex) -> Result<Self::Out> {
        let type_tag = row.get::<_, i64>(idx.bump()?)?;
        let sql_value = row.get_ref(idx.bump()?)?;
        T::from_type_tag_and_sql_value(type_tag, sql_value).map_err(From::from)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct RowFn<F>(F);

/// Create an implementation of [`FromSqlRow`] from a function.
pub fn row_fn<O, F>(f: F) -> RowFn<F>
where
    F: FnMut(&Row, &mut ColumnIndex) -> Result<O>,
{
    RowFn(f)
}

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
