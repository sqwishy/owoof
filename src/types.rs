//! Relating to types for entities, attributes, and other database values.

use std::{
    borrow::{Borrow, ToOwned},
    convert::TryFrom,
    ops::Deref,
    str::FromStr,
};
use thiserror::Error;

use uuid::Uuid;

pub(crate) const PLAIN_TAG: i64 = 0;
pub(crate) const ENTITY_ID_TAG: i64 = 1;
pub(crate) const ATTRIBUTE_IDENTIFIER_TAG: i64 = 2;
// pub const USER_TAG: i64 = 256;

pub trait TypeTag {
    // type Meme;
    // fn new(i64) -> Self;
    fn type_tag(&self) -> i64;
}

// Wow! Excellent meme!
impl<T: TypeTag> TypeTag for &'_ T {
    fn type_tag(&self) -> i64 {
        (*self).type_tag()
    }
}

#[cfg_attr(feature = "serde", derive(serde::Serialize), serde(untagged))]
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Entity(Entity),
    Attribute(Attribute),
    Text(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Uuid(Uuid),
    Blob(Vec<u8>),
    // Timestamp(...),
}

/// A borrowing version of [Value]
#[derive(Debug, Clone, PartialEq, Copy)]
pub enum ValueRef<'a> {
    Entity(Entity),
    Attribute(AttributeRef<'a>),
    Text(&'a str),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Uuid(Uuid),
    Blob(&'a [u8]),
}

impl TypeTag for Value {
    fn type_tag(&self) -> i64 {
        match self {
            Value::Entity(e) => e.type_tag(),
            Value::Attribute(a) => a.type_tag(),
            _ => PLAIN_TAG,
        }
    }
}

impl TypeTag for ValueRef<'_> {
    fn type_tag(&self) -> i64 {
        match self {
            ValueRef::Entity(e) => e.type_tag(),
            ValueRef::Attribute(a) => a.type_tag(),
            _ => PLAIN_TAG,
        }
    }
}

impl<'a> From<&'a Value> for ValueRef<'a> {
    fn from(value: &'a Value) -> ValueRef<'a> {
        match *value {
            Value::Entity(e) => ValueRef::Entity(e),
            Value::Attribute(ref a) => ValueRef::Attribute(a.into()),
            Value::Text(ref s) => ValueRef::Text(&s),
            Value::Integer(i) => ValueRef::Integer(i),
            Value::Float(f) => ValueRef::Float(f),
            Value::Boolean(b) => ValueRef::Boolean(b),
            Value::Uuid(u) => ValueRef::Uuid(u),
            Value::Blob(ref b) => ValueRef::Blob(b.as_slice()),
        }
    }
}

impl From<ValueRef<'_>> for Value {
    fn from(borrowed: ValueRef<'_>) -> Value {
        match borrowed {
            ValueRef::Entity(e) => Value::Entity(e),
            ValueRef::Attribute(a) => Value::Attribute(a.into()),
            ValueRef::Text(s) => Value::Text(s.to_owned()),
            ValueRef::Integer(i) => Value::Integer(i),
            ValueRef::Float(f) => Value::Float(f),
            ValueRef::Boolean(b) => Value::Boolean(b),
            ValueRef::Uuid(u) => Value::Uuid(u),
            ValueRef::Blob(b) => Value::Blob(b.to_owned()),
        }
    }
}

impl From<Entity> for Value {
    fn from(v: Entity) -> Self {
        Value::Entity(v)
    }
}

impl From<Entity> for ValueRef<'_> {
    fn from(v: Entity) -> Self {
        ValueRef::Entity(v)
    }
}

impl From<Attribute> for Value {
    fn from(v: Attribute) -> Self {
        Value::Attribute(v)
    }
}

impl<'a> From<AttributeRef<'a>> for ValueRef<'a> {
    fn from(v: AttributeRef<'a>) -> Self {
        ValueRef::Attribute(v)
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Text(s)
    }
}

impl<'a> From<&'a str> for ValueRef<'a> {
    fn from(s: &'a str) -> Self {
        ValueRef::Text(s)
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::Integer(i)
    }
}

impl From<i64> for ValueRef<'_> {
    fn from(i: i64) -> Self {
        ValueRef::Integer(i)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Float(f)
    }
}

impl From<f64> for ValueRef<'_> {
    fn from(f: f64) -> Self {
        ValueRef::Float(f)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Boolean(b)
    }
}

impl From<bool> for ValueRef<'_> {
    fn from(b: bool) -> Self {
        ValueRef::Boolean(b)
    }
}

impl From<Uuid> for Value {
    fn from(u: Uuid) -> Self {
        Value::Uuid(u)
    }
}

impl From<Uuid> for ValueRef<'_> {
    fn from(u: Uuid) -> Self {
        ValueRef::Uuid(u)
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Blob(v)
    }
}

impl<'a> From<&'a [u8]> for ValueRef<'a> {
    fn from(v: &'a [u8]) -> Self {
        ValueRef::Blob(v)
    }
}

/// A uuid referring to an entity.
#[derive(Debug, Clone, PartialEq, Copy)]
#[repr(transparent)]
pub struct Entity(Uuid);

impl std::fmt::Display for Entity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let buf = &mut [0; 37];
        buf[0] = 0x23; // a # character
        self.0.to_hyphenated_ref().encode_lower(&mut buf[1..]);
        let s = std::str::from_utf8(buf).unwrap();
        write!(f, "{}", s)
    }
}

impl Deref for Entity {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<&'_ str> for Entity {
    type Error = EntityParseError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s.chars().next() {
            Some('#') => (),
            Some(_) => return Err(EntityParseError::InvalidLeader),
            None => return Err(EntityParseError::MissingLeader),
        }

        let (_, uuid) = s.split_at(1);
        uuid.parse().map(Entity).map_err(EntityParseError::Uuid)
    }
}

impl FromStr for Entity {
    type Err = EntityParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Entity::try_from(s)
    }
}

impl From<Uuid> for Entity {
    fn from(u: Uuid) -> Self {
        Entity(u)
    }
}

impl TypeTag for Entity {
    fn type_tag(&self) -> i64 {
        ENTITY_ID_TAG
    }
}

#[derive(Debug, Error)]
pub enum EntityParseError {
    #[error("expected leading '#' but found something else instead")]
    InvalidLeader,
    #[error("expected leading '#' but found nothing")]
    MissingLeader,
    #[error("invalid uuid")]
    Uuid(#[from] uuid::Error),
}

/// An attribute name or identifier, like :db/id or :pet/name
/// TODO provide an interface to create these from strings with no leading :
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[derive(Debug, Clone, PartialEq)]
#[repr(transparent)]
pub struct Attribute(String);

impl Attribute {
    pub fn from_string_unchecked(s: String) -> Self {
        Attribute(s)
    }

    pub fn without_prefix(&'_ self) -> &'_ str {
        AttributeRef(&self).without_prefix()
    }
}

// impl std::fmt::Display for Attribute {
// }

impl Deref for Attribute {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0.borrow()
    }
}

impl TryFrom<String> for Attribute {
    type Error = AttributeParseError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        parse_attribute(&s).map(drop).map(|_| Attribute(s))
    }
}

impl<'a> TryFrom<&'a str> for Attribute {
    type Error = AttributeParseError;

    fn try_from(s: &'a str) -> Result<Self, Self::Error> {
        parse_attribute(&s).map(Attribute::from)
    }
}

impl From<AttributeRef<'_>> for Attribute {
    fn from(borrowed: AttributeRef<'_>) -> Attribute {
        let AttributeRef(s) = borrowed;
        Attribute(s.to_owned())
    }
}

/// A borrowing version of [Attribute]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(transparent)]
pub struct AttributeRef<'a>(&'a str);

impl<'a> AttributeRef<'a> {
    pub fn without_prefix(&self) -> &'a str {
        &self.0[1..]
    }
}

// TODO do something similar as Path does ...
// pub struct AttributeRef(str);
//
// see path.rs:
//   pub fn new<S: AsRef<OsStr> + ?Sized>(s: &S) -> &Path {
//       unsafe { &*(s.as_ref() as *const OsStr as *const Path) }
//   }
//
// And implement ToOwned and Borrow and finally be happy...

impl<'a> From<&'a Attribute> for AttributeRef<'a> {
    fn from(attribute: &'a Attribute) -> AttributeRef<'a> {
        let Attribute(s) = attribute;
        AttributeRef(s)
    }
}

impl<'a> TryFrom<&'a str> for AttributeRef<'a> {
    type Error = AttributeParseError;

    fn try_from(s: &'a str) -> Result<Self, Self::Error> {
        parse_attribute(s)
    }
}

fn parse_attribute<'a>(s: &'a str) -> Result<AttributeRef, AttributeParseError> {
    let rest = match s.chars().next() {
        Some(':') => &s[1..],
        Some(_) => return Err(AttributeParseError::InvalidLeader),
        None => return Err(AttributeParseError::MissingLeader),
    };

    if let Some(_) = rest.find(|c: char| c.is_whitespace()) {
        return Err(AttributeParseError::InvalidWhitespace);
    }

    match rest.len() {
        1..=255 => Ok(AttributeRef(s)),
        _ => Err(AttributeParseError::InvalidLength),
    }
}

#[derive(Debug, Error)]
pub enum AttributeParseError {
    #[error("expected leading ':' but found something else instead")]
    InvalidLeader,
    #[error("expected leading ':' but found nothing")]
    MissingLeader,
    #[error("whitespace not allowed")]
    InvalidWhitespace,
    #[error("identifier is either too long or too short (1..=255)")]
    InvalidLength,
}

impl FromStr for Attribute {
    type Err = AttributeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Attribute::try_from(s)
    }
}

impl AttributeRef<'static> {
    /// Panics if the attribute is invalid
    pub fn from_static(s: &'static str) -> Self {
        AttributeRef::try_from(s).unwrap()
    }

    pub const fn from_static_unchecked(s: &'static str) -> Self {
        AttributeRef(s)
    }
}

impl<'a> Deref for AttributeRef<'a> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0.borrow()
    }
}

impl TypeTag for Attribute {
    fn type_tag(&self) -> i64 {
        ATTRIBUTE_IDENTIFIER_TAG
    }
}

impl TypeTag for AttributeRef<'_> {
    fn type_tag(&self) -> i64 {
        ATTRIBUTE_IDENTIFIER_TAG
    }
}

#[cfg(feature = "serde")]
pub mod _serde {
    use super::*;
    use serde::{Serialize, Serializer};

    impl Serialize for Entity {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.collect_str(&self)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_attribute_validation() {
        assert!(Attribute::try_from("foo/meme".to_string()).is_err());
        assert!(Attribute::try_from(":foo/meme extra".to_string()).is_err());
        assert!(Attribute::try_from(":foo/meme".to_string()).is_ok());

        assert!(AttributeRef::try_from("foo/meme").is_err());
        assert!(AttributeRef::try_from(":foo/meme extra").is_err());
        assert!(AttributeRef::try_from(":foo/meme").is_ok());

        assert!(AttributeRef::try_from(":f").is_ok());
        assert!(AttributeRef::try_from(":").is_err());
    }

    #[test]
    fn test_entity_validation() {
        assert!(Entity::try_from("#not-a-uuid").is_err());
        assert!(Entity::try_from("b3ddeb4c-a61f-4433-8acd-7e10117f142e").is_err());
        assert!("#b3ddeb4c-a61f-4433-8acd-7e10117f142e"
            .parse::<Entity>()
            .is_ok());
    }
}
