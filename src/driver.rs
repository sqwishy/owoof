//! [`rusqlite::types::ToSql`] and [`rusqlite::types::FromSql`] implementations on [`crate::types`]
use rusqlite::types::{
    FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef as SqlValueRef,
};

pub use rusqlite::{Result, Row};

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
