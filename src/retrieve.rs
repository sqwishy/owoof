//! A higher-level way of adding constraints to a [`GenericNetwork`].
//!
//! "I need some information."
//! "This is Information Retrieval, not Information Dispersal."

use std::ops::{Deref, DerefMut};

use thiserror::Error;

use crate::either::Either;
use crate::network::{GenericNetwork, Ordering, TriplesField};

/* TODO call this Shape or Gather to sound less SQL? */
#[derive(Debug, Clone, PartialEq)]
pub struct Select<'n, V> {
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
    pub fn add_pattern(&mut self, pattern: &'v Pattern<'n, crate::Value>) -> &mut Self {
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

        self
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
/// all the same. */
#[derive(Debug, PartialEq)]
pub struct Pattern<'a, V> {
    pub entity: Either<Variable<'a>, V>,
    pub attribute: Either<Variable<'a>, V>,
    pub value: Either<Variable<'a>, V>,
}

use crate::types::{Attribute, AttributeParseError, Entity, EntityParseError};

pub fn parse_variable_or_entity<'a>(
    s: &'a str,
) -> Result<Either<Variable<'a>, Entity>, (VariableParseError, EntityParseError)> {
    s.try_into()
}

pub fn parse_variable_or_attribute<'a>(
    s: &'a str,
) -> Result<Either<Variable<'a>, Attribute>, (VariableParseError, AttributeParseError)> {
    s.try_into()
}

#[derive(Debug, PartialEq)]
pub enum Variable<'a> {
    Any,
    Unify(&'a str),
}

pub fn parse_variable<'a>(s: &'a str) -> Result<Variable<'a>, VariableParseError> {
    match s {
        "" => return Err(VariableParseError::MissingLeader),
        "?" => return Ok(Variable::Any),
        _ if !s.starts_with("?") => return Err(VariableParseError::InvalidLeader),
        _ => (),
    };

    if s.contains(char::is_whitespace) {
        return Err(VariableParseError::InvalidWhitespace);
    }

    if 256 < s.len() {
        return Err(VariableParseError::InvalidLength);
    }

    Ok(Variable::Unify(s))
}

impl<'a> TryFrom<&'a str> for Variable<'a> {
    type Error = VariableParseError;

    fn try_from(s: &'a str) -> Result<Self, Self::Error> {
        parse_variable(s)
    }
}

#[derive(Debug, Error)]
pub enum VariableParseError {
    #[error("expected leading `?` but found something else instead")]
    InvalidLeader,
    #[error("expected leading `?` but found nothing")]
    MissingLeader,
    #[error("whitespace not allowed")]
    InvalidWhitespace,
    #[error("name is either too long or too short (0..=255)")]
    InvalidLength,
}
