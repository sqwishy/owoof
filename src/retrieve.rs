//! A higher-level way of adding constraints to a [`GenericNetwork`].
//!
//! [`NamedNetwork`] is just a network paired with a list of variables that map to triples-fields
//! in the network.
//!
//! [`NamedNetwork::add_pattern`] allows adding a constraint from a [`Pattern`] which can be
//! parsed from a string. See [`types::parse_value`] on how parsing is attempted.
//!
//! ```
//! # use owoof::{NamedNetwork, Value, ValueRef, Attribute, Pattern, BorrowedParse};
//! # use owoof::{Variable, either::{Left, Right}};
//! let mut network = NamedNetwork::<ValueRef>::default();
//! let pattern = r#"?p :pet/name "Garfield""#
//!         .borrowed_parse()
//!         .expect("parse pattern");
//! assert_eq!(
//!     pattern,
//!     Pattern {
//!         entity: Left(Variable::Unify("?p")),
//!         attribute: Right(Value::Attribute(Attribute::from_static(":pet/name"))),
//!         value: Right(Value::Text("Garfield".to_owned())),
//!     }
//! );
//! network.add_pattern(&pattern);
//! ```
//!
//! One nice thing about this is that variables are automatically unified.
//!
//! So if I add another pattern `?p :animal/name "Cat"`, there will be a constraint linking the
//! `?p` variables together.
//!
//! Also, a [`GenericNetwork`] specifies the FROM clause of a SELECT in SQL, everything else goes on
//! to a [`Select`] object that can be returned by [`GenericNetwork::select()`].
//!
//! A [`Select`] has [`crate::sql::PushToQuery::to_query`] returning a
//! [`crate::sql::Query`].  And you can execute a [`crate::sql::Query`] with
//! [`crate::sql::Query::disperse`].  (There should be an example of this at the root of the
//! documentation in lib.rs.)
//!
//! See the [dispersal](crate::disperse) module about executing a query and reading data back from
//! SQLite.
//!
//! ---
//!
//! > "I need some information."
//! >
//! > "This is Information Retrieval, not Information [Dispersal](crate::disperse)."

use std::fmt;
use std::ops::{Deref, DerefMut};

use thiserror::Error;

use crate::either::Either;
use crate::network::{GenericNetwork, Ordering, Triples, TriplesField};
use crate::types::{Attribute, AttributeParseError, Entity, EntityParseError};
use crate::FromBorrowedStr;

#[cfg(feature = "serde_json")]
use crate::Value;

/* TODO call this Shape or Gather to sound less SQL? */
/// LIMIT, ORDER BY, and SELECT clauses forming an entire SELECT statement.
/// Needed to actually query for stuff using a [`GenericNetwork`].
#[derive(Debug, Clone, PartialEq)]
pub struct Select<'n, V> {
    /* TODO Does this really needs to be here? */
    pub(crate) network: &'n GenericNetwork<V>,
    pub(crate) selection: Vec<TriplesField>,
    pub(crate) order_by: Vec<(TriplesField, Ordering)>,
    pub(crate) limit: i64,
}

impl<'n, V> From<&'n GenericNetwork<V>> for Select<'n, V> {
    fn from(network: &'n GenericNetwork<V>) -> Self {
        Select { network, selection: vec![], order_by: vec![], limit: 0 }
    }
}

impl<'n, V> GenericNetwork<V> {
    pub fn select(&'n self) -> Select<'n, V> {
        Select::from(self)
    }
}

impl<'n, V> Deref for Select<'n, V> {
    type Target = GenericNetwork<V>;

    fn deref(&self) -> &Self::Target {
        self.network
    }
}

impl<'n, V> Select<'n, V> {
    pub fn fields(&self) -> &[TriplesField] {
        self.selection.as_slice()
    }

    pub fn field(&mut self, field: TriplesField) -> &mut Self {
        self.selection.push(field);
        self
    }

    pub fn limit(&mut self, limit: i64) -> &mut Self {
        self.limit = limit;
        self
    }

    pub fn order_by(&mut self, ord: (TriplesField, Ordering)) -> &mut Self {
        self.order_by.push(ord);
        self
    }
}

/// A [`GenericNetwork`] paired with [`Names`] to associate variables to
/// their first occurrence in a network.
#[derive(Debug)]
pub struct NamedNetwork<'n, V> {
    pub network: GenericNetwork<V>,
    pub names: Names<'n>,
}

impl<'n, V> NamedNetwork<'n, V> {
    pub fn new(network: GenericNetwork<V>, names: Names<'n>) -> Self {
        NamedNetwork { network, names }
    }

    pub fn split(self) -> (GenericNetwork<V>, Names<'n>) {
        let NamedNetwork { network, names } = self;
        (network, names)
    }
}

/* TODO XXX FIXME -- implement this more generally? */
impl<'n, 'v> NamedNetwork<'n, crate::ValueRef<'v>> {
    /// Constrain a [`GenericNetwork`] using the given [`Pattern`], unifying variables where
    /// appropriate.
    pub fn add_pattern<V>(&mut self, pattern: &'v Pattern<'n, V>) -> Triples
    where
        crate::ValueRef<'v>: From<&'v V>,
    {
        use crate::network::Match;

        let t = self.network.add_triples();
        [
            (t.entity(), &pattern.entity),
            (t.attribute(), &pattern.attribute),
            (t.value(), &pattern.value),
        ]
        .into_iter()
        .filter_map(|(field, part)| match part {
            Either::Left(Variable::Any) => None,
            Either::Left(Variable::Unify(unify)) => {
                if let Some(link_to) = self.names.get(unify) {
                    Some(field.eq(link_to.into()))
                } else {
                    self.names.append(unify, field);
                    None
                }
            }
            Either::Right(v) => Some(field.eq(Match::Value(v.into()))),
        })
        .for_each(|constraint| self.network.add_constraint(constraint));

        t
    }
}

impl<'n, V> Deref for NamedNetwork<'n, V> {
    type Target = GenericNetwork<V>;

    fn deref(&self) -> &Self::Target {
        &self.network
    }
}

impl<'n, V> DerefMut for NamedNetwork<'n, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.network
    }
}

impl<'n, V> Default for NamedNetwork<'n, V> {
    fn default() -> Self {
        NamedNetwork { network: GenericNetwork::default(), names: Names::default() }
    }
}

/// A mapping of names or variables to the [`TriplesField`] of their location in a network.
///
/// This uses a `Vec` as it'll be faster than a `BTreeMap` or `HashMap` for smol collections as
/// with our use case.
#[derive(Debug, Default)]
pub struct Names<'n> {
    vec: Vec<(&'n str, TriplesField)>,
}

impl<'n> Names<'n> {
    pub fn get(&self, looking_for: &str) -> Option<TriplesField> {
        self.vec
            .iter()
            .find_map(|&(ref has, found)| (has == &looking_for).then(|| found))
    }

    /// There is no point in appending the same `name` multiple times,
    /// only the first will be returned by [`Self::get()`].
    pub fn append(&mut self, name: &'n str, location: TriplesField) {
        self.vec.push((name, location));
    }

    pub fn iter(&self) -> impl Iterator<Item = &(&'n str, TriplesField)> + '_ {
        self.vec.iter()
    }

    pub fn lookup(&self, v: &Variable<'_>) -> Result<TriplesField, NamesLookupError> {
        match v {
            Variable::Any => Err(NamesLookupError::DoesNotUnify),
            Variable::Unify(unify) => self.get(unify).ok_or(NamesLookupError::Unmatched),
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq)]
pub enum NamesLookupError {
    #[error("does not unify")]
    DoesNotUnify,
    #[error("variable not previously declared")]
    Unmatched,
}

/// A data structure for specifying high-level constraints with [`NamedNetwork::add_pattern`].
///
/// - [`Variable::Any`] adds no constraint.
/// - [`Variable::Unify`] constrains the field to the first occurrence of the variable.
/// - [`Either::Right`] holds a [`crate::Value`] or [`crate::ValueRef`] or something and will match
///   that field to that value using equality.
///
/// FYI: `entity` and `attribute` *should* use [`crate::Entity`] and [`crate::Attribute`]
/// respectively instead, but it's a bit convenient for all the fields to be homogeneous or
/// whatever so you can add them to a `Vec` or iterate over them or otherwise interact with them
/// all the same.
///
/// Can be parsed but must borrow the input string because [`Variable`]s always borrow.  See
/// [`BorrowedParse`] about that.  Also requires the `serde_json` feature to parse a [`Value`].
#[derive(Debug, PartialEq)]
pub struct Pattern<'a, V> {
    pub entity: Either<Variable<'a>, V>,
    pub attribute: Either<Variable<'a>, V>,
    pub value: Either<Variable<'a>, V>,
}

/// Requires the `serde_json` feature.
#[cfg(feature = "serde_json")]
impl<'a> FromBorrowedStr<'a> for Pattern<'a, Value> {
    type Err = PatternParseError;

    fn from_borrowed_str(s: &'a str) -> Result<Self, Self::Err> {
        parse_pattern(s)
    }
}

/// Requires the `serde_json` feature.
#[cfg(feature = "serde_json")]
pub fn parse_pattern<'a>(s: &'a str) -> Result<Pattern<'a, Value>, PatternParseError> {
    let (s, e) = take_no_whitespace(s);
    let (s, a) = take_no_whitespace(s);
    let v = s.trim();

    return Ok(Pattern {
        entity: parse_variable_or_entity(e)
            .map_err(|(v, e)| PatternParseError::Entity(v, e))?
            .map_right(Value::Entity),
        attribute: parse_variable_or_attribute(a)
            .map_err(|(v, a)| PatternParseError::Attribute(v, a))?
            .map_right(Value::Attribute),
        value: parse_variable_or_value(v).map_err(|(v, ())| PatternParseError::Value(v))?,
    });

    fn take_no_whitespace(s: &str) -> (&str, &str) {
        let s = s.trim_start();
        let next = s.split_whitespace().next().unwrap_or_default();
        (&s[next.len()..], next)
    }
}

pub fn parse_variable_or_entity<'a>(
    s: &'a str,
) -> Result<Either<Variable<'a>, Entity>, (VariableParseError, EntityParseError)> {
    Either::from_borrowed_str(s)
}

pub fn parse_variable_or_attribute<'a>(
    s: &'a str,
) -> Result<Either<Variable<'a>, Attribute>, (VariableParseError, AttributeParseError)> {
    Either::from_borrowed_str(s)
}

#[cfg(feature = "serde_json")]
pub fn parse_variable_or_value<'a>(
    s: &'a str,
) -> Result<Either<Variable<'a>, Value>, (VariableParseError, ())> {
    Either::from_borrowed_str(s)
}

#[derive(Debug, Error)]
pub enum PatternParseError {
    #[error("not a variable ({}) and not an entity ({})", .0, .1)]
    Entity(VariableParseError, EntityParseError),
    #[error("not a variable ({}) and not an attribute ({})", .0, .1)]
    Attribute(VariableParseError, AttributeParseError),
    #[error("not a variable ({}) and not a value", .0)]
    Value(VariableParseError),
}

#[derive(Debug, PartialEq)]
pub enum Variable<'a> {
    Any,
    Unify(&'a str),
}

impl<'a> fmt::Display for Variable<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Variable::Any => write!(f, "?"),
            Variable::Unify(v) => write!(f, "{}", v),
        }
    }
}

/// Allows `Either<Variable, _>` to implement [`FromBorrowedStr`].
impl<'a> FromBorrowedStr<'a> for Variable<'a> {
    type Err = VariableParseError;

    fn from_borrowed_str(s: &'a str) -> Result<Self, Self::Err> {
        parse_variable(s)
    }
}

pub fn parse_variable<'a>(s: &'a str) -> Result<Variable<'a>, VariableParseError> {
    match s {
        "" => return Err(VariableParseError::Leader),
        "?" => return Ok(Variable::Any),
        _ if !s.starts_with("?") => return Err(VariableParseError::Leader),
        _ => (),
    };

    if s.contains(char::is_whitespace) {
        return Err(VariableParseError::Whitespace);
    }

    if 256 < s.len() {
        return Err(VariableParseError::Length);
    }

    Ok(Variable::Unify(s))
}

#[derive(Debug, Error, Clone, PartialEq)]
pub enum VariableParseError {
    #[error("expected leading `?`")]
    Leader,
    #[error("whitespace not allowed")]
    Whitespace,
    #[error("name is either too long or too short (0..=255)")]
    Length,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        assert_eq!(parse_variable("1234"), Err(VariableParseError::Leader));
        assert_eq!(parse_variable("?foo"), Ok(Variable::Unify("?foo")));
        assert_eq!(parse_variable("?"), Ok(Variable::Any));
    }

    #[cfg(feature = "serde_json")]
    #[test]
    fn test_parse_pattern() {
        assert_eq!(
            parse_pattern("? ? ?asdf").unwrap(),
            Pattern {
                entity: Either::Left(Variable::Any),
                attribute: Either::Left(Variable::Any),
                value: Either::Left(Variable::Unify("?asdf")),
            }
        );
    }
}
