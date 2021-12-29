//! Relating to types for entities, attributes, and other database values.
//!
//! If the `serde` feature is enabled, should be able to serialize and deserialize to a thing.  But
//! it might not be very coherent.  [`Attribute`] and [`Entity`] instances are serialized to
//! text with a leading symbol.  So, the serialized representation of `Value::Attribute(a)` is the
//! same as `Value::Text(a.to_string())`.  So if you serialize the latter, you will deserialize
//! into the former.
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

#[cfg(feature = "serde_json")]
use crate::FromBorrowedStr;

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

/// Opposite of [`FromStr`] and [`FromBorrowedStr`].  Requires the `serde_json` feature.
///
/// Formatting rules are something like:
/// - #entity-id
/// - :some/attribute
/// - "text is a json string"
/// - some-hyphenated-uuid
#[cfg(feature = "serde_json")]
impl<'v> fmt::Display for ValueRef<'v> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use serde_json::to_string as json;

        match self {
            ValueRef::Entity(v) => v.fmt(f),
            ValueRef::Attribute(v) => v.fmt(f),
            ValueRef::Text(v) => json(v).map_err(|_| fmt::Error)?.fmt(f),
            ValueRef::Integer(v) => v.fmt(f),
            ValueRef::Float(v) => v.fmt(f),
            ValueRef::Boolean(v) => v.fmt(f),
            ValueRef::Uuid(v) => v.fmt(f),
            ValueRef::Blob(v) => json(v).map_err(|_| fmt::Error)?.fmt(f),
        }
    }
}

/// See [`ValueRef`]'s [`fmt::Display`] implementation.
#[cfg(feature = "serde_json")]
impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        ValueRef::from(self).fmt(f)
    }
}

/// Opposite of [`fmt::Display`].  Requires the `serde_json` feature.
#[cfg(feature = "serde_json")]
pub fn parse_value(s: &str) -> Result<Value, ()> {
    use serde_json::from_str as json;

    match s.chars().next().ok_or(())? {
        '#' => s.parse::<Entity>().map_err(drop).map(Value::Entity),
        ':' => s.parse::<Attribute>().map_err(drop).map(Value::Attribute),
        '"' => json::<String>(s).map_err(drop).map(Value::Text),
        '[' => json::<Vec<u8>>(s).map_err(drop).map(Value::Blob),
        _ => Result::<Value, _>::Err(())
            .or_else(|_| s.parse::<i64>().map(From::from))
            .or_else(|_| s.parse::<f64>().map(From::from))
            .or_else(|_| s.parse::<bool>().map(From::from))
            .or_else(|_| s.parse::<uuid::Uuid>().map(From::from))
            .map_err(drop),
    }
}

/// Opposite of [`fmt::Display`].  Requires the `serde_json` feature.
#[cfg(feature = "serde_json")]
pub fn parse_value_ref(s: &str) -> Result<ValueRef<'_>, ()> {
    use serde_json::from_str as json;

    match s.chars().next().ok_or(())? {
        '#' => s.parse::<Entity>().map_err(drop).map(ValueRef::Entity),
        ':' => AttributeRef::from_str(s)
            .map_err(drop)
            .map(ValueRef::Attribute),
        '"' => json::<&str>(s).map_err(drop).map(ValueRef::Text),
        _ => Result::<ValueRef, _>::Err(())
            .or_else(|_| s.parse::<i64>().map(From::from))
            .or_else(|_| s.parse::<f64>().map(From::from))
            .or_else(|_| s.parse::<bool>().map(From::from))
            .or_else(|_| s.parse::<uuid::Uuid>().map(From::from))
            .map_err(drop),
    }
}

/// Opposite of [`fmt::Display`].  Requires the `serde_json` feature.
#[cfg(feature = "serde_json")]
impl FromStr for Value {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_value(s)
    }
}

/// Opposite of [`fmt::Display`].  Requires the `serde_json` feature.
#[cfg(feature = "serde_json")]
impl<'a> FromBorrowedStr<'a> for ValueRef<'a> {
    type Err = ();

    fn from_borrowed_str(s: &'a str) -> Result<Self, Self::Err> {
        parse_value_ref(s)
    }
}

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
                Integer(i64),
                Float(f64),
                Boolean(bool),
                Text(Cow<'a, str>),
                Bytes(Cow<'a, [u8]>),
            }

            let s: Container = Deserialize::deserialize(deserializer)?;
            Ok(match s {
                Container::Integer(i) => Value::from(i),
                Container::Float(f) => Value::from(f),
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

    #[cfg(feature = "serde_json")]
    #[test]
    fn test_parsing() {
        use crate::BorrowedParse;

        let data = [
            (ValueRef::Integer(123), "123"),
            (ValueRef::Float(0.12), "0.12"),
            (ValueRef::Boolean(true), "true"),
            (ValueRef::Text("hello world"), "\"hello world\""),
            (
                ValueRef::Uuid(
                    "b3ddeb4c-a61f-4433-8acd-7e10117f142e"
                        .parse::<uuid::Uuid>()
                        .unwrap(),
                ),
                "b3ddeb4c-a61f-4433-8acd-7e10117f142e",
            ),
            (
                ValueRef::Entity(
                    "#b3ddeb4c-a61f-4433-8acd-7e10117f142e"
                        .parse::<Entity>()
                        .unwrap(),
                ),
                "#b3ddeb4c-a61f-4433-8acd-7e10117f142e",
            ),
            (
                ValueRef::Attribute(AttributeRef::from_str(":foo/bar").unwrap()),
                ":foo/bar",
            ),
        ];

        for (value, formatted) in data.into_iter() {
            let displayed = value.to_string();
            assert_eq!(displayed.as_str(), formatted, "{:?}", value);

            let parsed: ValueRef = displayed.borrowed_parse().unwrap();
            assert_eq!(parsed, value);
        }

        {
            let blob = ValueRef::Blob("bytes".as_bytes());
            let formatted = "[98,121,116,101,115]";
            assert_eq!(blob.to_string(), formatted);
            assert!(formatted.borrowed_parse::<ValueRef>().is_err());
            assert_eq!(formatted.parse::<Value>().unwrap(), blob.into());
        }

        {
            let blob = ValueRef::Text("needs \"escaping\"");
            let formatted = r#""needs \"escaping\"""#;
            assert_eq!(blob.to_string(), formatted);
            assert!(formatted.borrowed_parse::<ValueRef>().is_err());
            assert_eq!(formatted.parse::<Value>().unwrap(), blob.into());
        }
    }

    #[cfg(feature = "serde")]
    #[cfg(feature = "serde_json")]
    #[test]
    fn test_json() {
        let data = [
            (ValueRef::Text("some text"), "\"some text\""),
            (ValueRef::Integer(123), "123"),
            (ValueRef::Float(0.12), "0.12"),
            (ValueRef::Boolean(true), "true"),
            (
                ValueRef::Uuid(
                    "b3ddeb4c-a61f-4433-8acd-7e10117f142e"
                        .parse::<uuid::Uuid>()
                        .unwrap(),
                ),
                "\"b3ddeb4c-a61f-4433-8acd-7e10117f142e\"",
            ),
            (
                ValueRef::Entity(
                    "#b3ddeb4c-a61f-4433-8acd-7e10117f142e"
                        .parse::<Entity>()
                        .unwrap(),
                ),
                "\"#b3ddeb4c-a61f-4433-8acd-7e10117f142e\"",
            ),
            (
                ValueRef::Attribute(AttributeRef::from_str(":foo/bar").unwrap()),
                "\":foo/bar\"",
            ),
        ];

        for (value, json) in data.into_iter() {
            let ser = serde_json::to_string(&value).unwrap();
            assert_eq!(&ser, json);

            let deser: Value = serde_json::from_str(&ser).unwrap();
            assert_eq!(ValueRef::from(&deser), value);
        }
    }

    #[cfg(feature = "serde")]
    #[cfg(feature = "serde_json")]
    #[test]
    fn test_json_incoherence() {
        /* We serialize Text but get an Attribute back */
        let attribute_like_text = ValueRef::Text(":looks/like-an-attribute");
        let json = serde_json::to_string(&attribute_like_text).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&json).unwrap(),
            Value::from(Attribute::from_static(":looks/like-an-attribute"))
        );

        /* We serialize Text but get an Entity */
        let entity_like_text = ValueRef::Text("#b3ddeb4c-a61f-4433-8acd-7e10117f142e");
        let json = serde_json::to_string(&entity_like_text).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&json).unwrap(),
            Value::Entity(
                "#b3ddeb4c-a61f-4433-8acd-7e10117f142e"
                    .parse::<Entity>()
                    .unwrap()
            ),
        );
    }
}
