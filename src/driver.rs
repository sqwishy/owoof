use rusqlite::{
    types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef as SqlValueRef},
    Result,
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
        self.without_prefix().to_sql()
    }
}

impl<'a> ToSql for AttributeRef<'a> {
    fn to_sql(&self) -> Result<ToSqlOutput> {
        self.without_prefix().to_sql()
    }
}

impl<'a> FromSql for Attribute {
    fn column_result(value: SqlValueRef) -> FromSqlResult<Self> {
        let s = value.as_str()?;
        Ok(Attribute::from_string_unchecked(format!(":{}", s)))
    }
}

pub trait FromSqlAndTypeTag: Sized {
    fn column_result(type_tag: i64, value: SqlValueRef<'_>) -> FromSqlResult<Self>;
}

impl FromSqlAndTypeTag for Value {
    fn column_result(type_tag: i64, value: SqlValueRef<'_>) -> FromSqlResult<Self> {
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
            _ => todo!(),
        }
    }
}
