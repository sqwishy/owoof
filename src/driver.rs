//! [`rusqlite::types::ToSql`] and [`rusqlite::types::FromSql`] implementations on
//! [`crate::types`].  And the [`TypeTag`] & [`FromTypeTagAndSqlValue`] traits for reading for
//! loading application types from a type-tag and SQLite value pair.
//!
//! ## The TypeTag trait
//!
//! Values are meant to be stored along with enough information to describe what corresponding rust
//! type they were before serialized into the SQLite database.
//!
//! For instance, we might store a date-time as an integer of the number of milliseconds from an
//! epoch.  But, when we get it back, we don't want an integer, we want our date-time.
//!
//! **The type tag is in-band information that allows us to discriminate between integers and
//! date-times, or other scalar types with the same SQLite representation.**  This way, users don't
//! need to know or expect anything about the type of what they're querying and they should get the
//! same values out as what was put in.
//!
//! **Another consideration for this feature is just to implement orderability properly.**  If I query
//! date-times since `A` I don't also want to search for integers greater than the integer
//! representation of the date-time `A`.
use rusqlite::types::{
    FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef as SqlValueRef,
};

pub use rusqlite::{Result, Row};

use crate::types::{Attribute, AttributeRef, Entity, Value, ValueRef};

pub(crate) const PLAIN_TAG: i64 = 0;
pub(crate) const ENTITY_ID_TAG: i64 = 1;
pub(crate) const ATTRIBUTE_IDENTIFIER_TAG: i64 = 2;
// pub const USER_TAG: i64 = 256;

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

/// See the module level documentation in [`crate::driver`] about this.
pub trait TypeTag {
    /// Used to help map borrowing and owning instances to a single type.  To avoid discriminating
    /// between `&Value`, `Value`, and `ValueRef` in cases where we want to treat them the same.
    ///
    /// I don't really like this solution but I don't know of a more normal trait to use.
    type Factory;

    fn type_tag(&self) -> i64;
}

// Wow! Excellent meme!
impl<T: TypeTag> TypeTag for &'_ T {
    type Factory = <T as TypeTag>::Factory;

    fn type_tag(&self) -> i64 {
        (*self).type_tag()
    }
}

impl TypeTag for Entity {
    type Factory = Self;

    fn type_tag(&self) -> i64 {
        ENTITY_ID_TAG
    }
}

impl TypeTag for Attribute {
    type Factory = Self;

    fn type_tag(&self) -> i64 {
        ATTRIBUTE_IDENTIFIER_TAG
    }
}

impl TypeTag for &'_ AttributeRef {
    type Factory = Attribute;

    fn type_tag(&self) -> i64 {
        ATTRIBUTE_IDENTIFIER_TAG
    }
}

impl TypeTag for Value {
    type Factory = Self;

    fn type_tag(&self) -> i64 {
        match self {
            Value::Entity(e) => e.type_tag(),
            Value::Attribute(a) => a.type_tag(),
            _ => PLAIN_TAG,
        }
    }
}

impl TypeTag for ValueRef<'_> {
    type Factory = Value;

    fn type_tag(&self) -> i64 {
        match self {
            ValueRef::Entity(e) => e.type_tag(),
            ValueRef::Attribute(a) => a.type_tag(),
            _ => PLAIN_TAG,
        }
    }
}

/// Make `Self` from a type tag (`i64`) and a [`rusqlite::types::ValueRef`].
pub trait FromTypeTagAndSqlValue: Sized {
    fn from_type_tag_and_sql_value(type_tag: i64, value: SqlValueRef<'_>) -> FromSqlResult<Self>;
}

impl FromTypeTagAndSqlValue for Value {
    fn from_type_tag_and_sql_value(type_tag: i64, value: SqlValueRef<'_>) -> FromSqlResult<Self> {
        match type_tag {
            ENTITY_ID_TAG => Entity::column_result(value).map(Value::Entity),
            ATTRIBUTE_IDENTIFIER_TAG => Attribute::column_result(value).map(Value::Attribute),
            PLAIN_TAG => match value {
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

impl FromTypeTagAndSqlValue for Entity {
    fn from_type_tag_and_sql_value(type_tag: i64, value: SqlValueRef<'_>) -> FromSqlResult<Self> {
        match type_tag {
            ENTITY_ID_TAG => Entity::column_result(value),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl FromTypeTagAndSqlValue for Attribute {
    fn from_type_tag_and_sql_value(type_tag: i64, value: SqlValueRef<'_>) -> FromSqlResult<Self> {
        match type_tag {
            ATTRIBUTE_IDENTIFIER_TAG => Attribute::column_result(value),
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

/// A factory to make a [`FromSqlRow::Out`] from a [`rusqlite::Row`] using
/// [`FromSqlRow::/rom_start_of_row`].
/// For example `&[T]` (where `T` implements [`FromSqlRow`])
/// also implements [`FromSqlRow`] where
/// `FromSqlRow::Out = Vec<<T as FromSqlRow>::Out>`
pub trait FromSqlRow {
    type Out;

    fn from_sql_row(&mut self, row: &Row, idx: &mut ColumnIndex) -> Result<Self::Out>;

    fn from_start_of_row(&mut self, row: &Row) -> Result<Self::Out> {
        self.from_sql_row(row, &mut ColumnIndex::default())
    }
}

/// You might want [`Query<&dyn ToSql>::count`] instead?
impl FromSqlRow for () {
    type Out = ();

    fn from_sql_row(&mut self, _row: &Row, _idx: &mut ColumnIndex) -> Result<Self::Out> {
        Ok(())
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

macro_rules! _from_sql_row_fixed {
    ( $($n:expr)* ) => {
        $(
        impl<T: FromSqlRow> FromSqlRow for [T; $n] {
            type Out = Vec<<T as FromSqlRow>::Out>;

            fn from_sql_row(&mut self, row: &Row, idx: &mut ColumnIndex) -> Result<Self::Out> {
                self.as_mut_slice().from_sql_row(row, idx)
            }
        }
        )*
    };
}

_from_sql_row_fixed!(0 1 2 3 4 5 6 7 8 9);

macro_rules! _from_sql_row_tuple {
    ( ) => {};
    ( $t:ident $( $rest:ident )* ) => {
        impl<$t: FromSqlRow, $($rest: FromSqlRow),*> FromSqlRow for ($t, $($rest),*)
        {
            type Out = (<$t as FromSqlRow>::Out, $(<$rest as FromSqlRow>::Out),*);

            fn from_sql_row(&mut self, row: &Row, idx: &mut ColumnIndex) -> Result<Self::Out> {
                #[allow(non_snake_case)]
                let ($t, $($rest),*) = self;
                Ok(($t.from_sql_row(row, idx)?, $( $rest.from_sql_row(row, idx)? ),*))
            }
        }

        _from_sql_row_tuple!($($rest)*);
    };
}

_from_sql_row_tuple!(A B C D E F G H I);

/// Implements [`FromSqlRow`] for just one [`FromTypeTagAndSqlValue`].
#[derive(Debug)]
pub struct Just<T: FromTypeTagAndSqlValue>(std::marker::PhantomData<T>);

impl<T: FromTypeTagAndSqlValue> Clone for Just<T> {
    fn clone(&self) -> Self {
        just()
    }
}

impl<T: FromTypeTagAndSqlValue> Copy for Just<T> {}

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
