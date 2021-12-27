//! Relating to types for entities, attributes, and other database values.

use std::{
    borrow::{Borrow, ToOwned},
    convert::{AsRef, TryFrom},
    fmt,
    ops::Deref,
    str::FromStr,
};
use thiserror::Error;

#[cfg(feature = "serde")]
use std::borrow::Cow;

use uuid::Uuid;

/// A owning version of [`ValueRef`]
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

/// A borrowing version of [`Value`]
#[cfg_attr(feature = "serde", derive(serde::Serialize), serde(untagged))]
#[derive(Debug, Clone, PartialEq, Copy)]
pub enum ValueRef<'a> {
    Entity(Entity),
    Attribute(&'a AttributeRef),
    Text(&'a str),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Uuid(Uuid),
    Blob(&'a [u8]),
}

impl<'a> From<&'a Value> for ValueRef<'a> {
    fn from(value: &'a Value) -> ValueRef<'a> {
        match *value {
            Value::Entity(e) => ValueRef::Entity(e),
            Value::Attribute(ref a) => ValueRef::Attribute(a),
            Value::Text(ref s) => ValueRef::Text(s),
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
            ValueRef::Attribute(a) => Value::Attribute(a.to_owned()),
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

impl<'a> From<&'a AttributeRef> for ValueRef<'a> {
    fn from(v: &'a AttributeRef) -> Self {
        ValueRef::Attribute(v)
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Text(s)
    }
}

/* Conflicts with TryFrom<&str> ... */
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

impl<'a> From<&'_ ValueRef<'a>> for ValueRef<'a> {
    fn from(value: &'_ ValueRef<'a>) -> ValueRef<'a> {
        value.clone()
    }
}

/// Figure out an appropriate [`Value`] variant from some text.
///
/// Tries to read it as an #entity :attribute number boolean uuid and if all those fail it just
/// eats the whole input as a text value.  Uses [`FromStr`] so refer to that implementation on
pub fn parse_value(s: &str) -> Value {
    Option::<Value>::None
        .or_else(|| s.parse::<Entity>().map(Value::from).ok())
        .or_else(|| s.parse::<Attribute>().map(Value::from).ok())
        .or_else(|| s.parse::<i64>().map(Value::from).ok())
        .or_else(|| s.parse::<f64>().map(Value::from).ok())
        .or_else(|| s.parse::<bool>().map(Value::from).ok())
        .or_else(|| s.parse::<uuid::Uuid>().map(Value::from).ok())
        .unwrap_or_else(|| Value::Text(s.to_owned()))
}

#[cfg(feature = "serde_json")]
/// Requires the `serde_json` feature.
impl FromStr for Value {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(parse_value(s))
    }
}

impl TryFrom<&str> for Value {
    type Error = ();

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        s.parse().map_err(|_| ())
    }
}

// #[cfg(feature = "serde_json")]
// /// Requires the `serde_json` feature.
// impl<'a> TryFrom<&'a str> for ValueRef<'a> {
//     type Error = ();
//
//     fn try_from(s: &'a str) -> Result<Self, Self::Error> {
//         use serde_json::from_str as json;
//
//         Option::<ValueRef>::None
//             .or_else(|| s.parse::<Entity>().map(ValueRef::from).ok())
//             .or_else(|| AttributeRef::from_str(s).map(ValueRef::from).ok())
//             .or_else(|| json::<&str>(s).map(ValueRef::Text).ok())
//             .or_else(|| json::<i64>(s).map(ValueRef::from).ok())
//             .or_else(|| json::<f64>(s).map(ValueRef::from).ok())
//             .or_else(|| json::<bool>(s).map(ValueRef::from).ok())
//             .or_else(|| s.parse::<uuid::Uuid>().map(ValueRef::from).ok())
//             .ok_or(())
//     }
// }

/// A uuid referring to an entity.
#[derive(Debug, Clone, PartialEq, Copy)]
#[repr(transparent)]
pub struct Entity(Uuid);

impl fmt::Display for Entity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
            Some(_) | None => return Err(EntityParseError::Leader),
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

#[derive(Debug, Error, Clone, PartialEq)]
pub enum EntityParseError {
    #[error("expected leading `#`")]
    Leader,
    #[error("invalid uuid")]
    Uuid(#[from] uuid::Error),
}

/// An attribute name or identifier, like `:db/id` or `:pet/name`.
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Attribute(String);

impl Attribute {
    /// Copies the given identifier and prepends a `:` to create an Attribute.
    pub fn from_ident(ident: &str) -> Self {
        Self::from_string_unchecked(format!(":{}", ident))
    }

    pub fn from_string_unchecked(s: String) -> Self {
        Attribute(s)
    }

    /// Panics if the attribute is invalid.
    pub fn from_static(s: &'static str) -> Self {
        AttributeRef::from_static(s).to_owned()
    }
}

impl Deref for Attribute {
    type Target = AttributeRef;

    fn deref(&self) -> &Self::Target {
        AttributeRef::new(&self.0)
    }
}

impl TryFrom<String> for Attribute {
    type Error = AttributeParseError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl<'a> TryFrom<&'a str> for Attribute {
    type Error = AttributeParseError;

    fn try_from(s: &'a str) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl fmt::Display for Attribute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl AsRef<AttributeRef> for Attribute {
    fn as_ref(&self) -> &AttributeRef {
        self
    }
}

/// A borrowing version of [`Attribute`], like `Path` is to `PathBuf`.
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct AttributeRef(str);

impl AttributeRef {
    /// uses unsafe copied from std lib's path.rs ¯\\_(ツ)_/¯
    /// ```ignore
    /// pub fn new<S: AsRef<OsStr> + ?Sized>(s: &S) -> &Path {
    ///     unsafe { &*(s.as_ref() as *const OsStr as *const Path) }
    /// }
    /// ```
    fn new<S: AsRef<str> + ?Sized>(s: &S) -> &Self {
        unsafe { &*(s.as_ref() as *const str as *const AttributeRef) }
    }

    /// TODO needs a better name so this doesn't collide with `FromStr`.
    pub fn from_str<S: AsRef<str> + ?Sized>(s: &S) -> Result<&Self, AttributeParseError> {
        parse_attribute(s.as_ref())
    }

    /// Without the leading `:`.
    pub fn just_the_identifier(&self) -> &str {
        &self.0[1..]
    }

    /// The identifier with the leading `:`.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Everything after the last `/` in the identifier,
    /// or just the identifier if no `/` is in the attribute.
    pub fn tail(&self) -> &str {
        self.just_the_identifier()
            .rsplit('/')
            .next()
            .unwrap_or(self.just_the_identifier())
    }
}

impl Borrow<AttributeRef> for Attribute {
    fn borrow(&self) -> &AttributeRef {
        AttributeRef::new(&self.0)
    }
}

impl ToOwned for AttributeRef {
    type Owned = Attribute;

    fn to_owned(&self) -> Self::Owned {
        Attribute(self.0.to_owned())
    }
}

impl AsRef<AttributeRef> for &AttributeRef {
    fn as_ref(&self) -> &AttributeRef {
        self
    }
}

impl fmt::Display for AttributeRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/* FromStr doesn't let us borrow from the input string so we can't use it for AttributeRef */
impl<'a> TryFrom<&'a str> for &'a AttributeRef {
    type Error = AttributeParseError;

    fn try_from(s: &'a str) -> Result<Self, Self::Error> {
        parse_attribute(s)
    }
}

impl FromStr for Attribute {
    type Err = AttributeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_attribute(s).map(ToOwned::to_owned)
    }
}

fn parse_attribute(s: &str) -> Result<&AttributeRef, AttributeParseError> {
    let rest = match s.chars().next() {
        Some(':') => &s[1..],
        Some(_) | None => return Err(AttributeParseError::Leader),
    };

    if rest.contains(char::is_whitespace) {
        return Err(AttributeParseError::Whitespace);
    }

    match rest.len() {
        1..=255 => Ok(AttributeRef::new(s)),
        _ => Err(AttributeParseError::Length),
    }
}

#[derive(Debug, Error, Clone, PartialEq)]
pub enum AttributeParseError {
    #[error("expected leading `:`")]
    Leader,
    #[error("whitespace not allowed")]
    Whitespace,
    #[error("identifier is either too long or too short (1..=255)")]
    Length,
}

impl AttributeRef {
    /// Panics if the attribute is invalid
    pub fn from_static(s: &'static str) -> &'static Self {
        TryFrom::try_from(s).unwrap()
    }
}

#[cfg(feature = "serde")]
pub mod _serde {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    impl Serialize for Entity {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.collect_str(&self)
        }
    }

    impl<'de> Deserialize<'de> for Entity {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            let s: Cow<str> = Deserialize::deserialize(deserializer)?;
            s.parse().map_err(serde::de::Error::custom)
        }
    }

    impl<'de> Deserialize<'de> for Attribute {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            let s: Cow<str> = Deserialize::deserialize(deserializer)?;
            s.parse().map_err(serde::de::Error::custom)
        }
    }

    impl<'de> Deserialize<'de> for Value {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            #[derive(Deserialize)]
            #[serde(untagged)]
            enum Container<'a> {
                Float(f64),
                Integer(i64),
                Boolean(bool),
                Text(Cow<'a, str>),
                Bytes(Cow<'a, [u8]>),
            }

            let s: Container = Deserialize::deserialize(deserializer)?;
            Ok(match s {
                Container::Float(f) => Value::from(f),
                Container::Integer(i) => Value::from(i),
                Container::Boolean(b) => Value::from(b),
                Container::Text(t) => Option::<Value>::None
                    .or_else(|| t.parse::<Entity>().map(Value::from).ok())
                    .or_else(|| t.parse::<Attribute>().map(Value::from).ok())
                    .or_else(|| t.parse::<uuid::Uuid>().map(Value::from).ok())
                    .unwrap_or_else(|| Value::Text(t.into())),
                Container::Bytes(b) => Value::Blob(b.into()),
            })
        }
    }

    #[feature("serde_json")]
    #[test]
    fn test_parsing() {
        assert_eq!(
            "b3ddeb4c-a61f-4433-8acd-7e10117f142e"
                .parse::<Value>()
                .unwrap(),
            "b3ddeb4c-a61f-4433-8acd-7e10117f142e"
                .parse::<uuid::Uuid>()
                .map(Value::Uuid)
                .unwrap(),
        );
        assert_eq!(
            "#b3ddeb4c-a61f-4433-8acd-7e10117f142e"
                .parse::<Value>()
                .unwrap(),
            "#b3ddeb4c-a61f-4433-8acd-7e10117f142e"
                .parse::<Entity>()
                .map(Value::Entity)
                .unwrap(),
        );
        assert_eq!(
            "\"db/id\"".parse::<Value>().unwrap(),
            Value::Text("db/id".to_owned()),
        );
        assert_eq!(
            ":db/id".parse::<Value>().unwrap(),
            ":db/id".parse::<Attribute>().map(Value::Attribute).unwrap(),
        );

        // assert_eq!(
        //     ValueRef::try_from("\"foo\"").unwrap(),
        //     ValueRef::Text("foo"),
        // );
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

        assert!(AttributeRef::from_str("foo/meme").is_err());
        assert!(AttributeRef::from_str(":foo/meme extra").is_err());
        assert!(AttributeRef::from_str(":foo/meme").is_ok());

        assert!(AttributeRef::from_str(":f").is_ok());
        assert!(AttributeRef::from_str(":").is_err());
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
