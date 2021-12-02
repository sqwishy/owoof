//! For querying data.  Describe groups of entity-attribute-value triples and constraints to
//! search.  
//!
//! Suppose you want:
//! ```ignore
//! ?b :book/title "The Complete Calvin and Hobbes"
//! ?r :review/book ?b
//! ?r :review/user ?u
//! ?r :review/score 1
//! ```
//!
//! But now imagine it like:
//! ```text
//!                 +---+
//!                 | e |<-
//!  :review/user<->| a |  \  
//!                 | v |   \  +---+   ->:review/book
//!                 +---+    ->| e |  /
//!                         /  | a |<-    +---+
//!                 +---+  /   | v |<---->| e |
//!                 | e |<-    +---+      | a |<->:book/title
//! :review/score<->| a |                 | v |<->"The Complete Calvin and Hobbes"
//!             1<->| v |                 +---+
//!                 +---+             
//! ```
//! That's a [`Network`], each block is a group of database [`Triples`], the lines are constraints
//! defining what data the triples can match.
//!
//! This is how you might use the API to build that [`Network`].
//! ```
//! # use owoof::{Network, AttributeRef};
//! let mut network: Network = Default::default();
//! // ?b :book/title "The Complete Calvin and Hobbes"
//! let b = network
//!     .fluent_triples()
//!     .match_attribute(AttributeRef::from_static(":book/title"))
//!     .match_value("The Complete Calvin and Hobbes")
//!     .entity();
//! // ?r :review/book ?b
//! let r = network
//!     .fluent_triples()
//!     .match_attribute(AttributeRef::from_static(":review/book"))
//!     .link_value(b)
//!     .entity();
//! // ?r :review/user ?u
//! let u = network
//!     .fluent_triples()
//!     .link_entity(r)
//!     .match_attribute(AttributeRef::from_static(":review/user"))
//!     .value();
//! // ?r :review/score 1
//! network
//!     .fluent_triples()
//!     .link_entity(r)
//!     .match_attribute(AttributeRef::from_static(":review/book"))
//!     .match_value(1)
//!     .entity();
//! ```
use std::ops::Deref;

use crate::{soup::Encoded, Value, ValueRef};

/// A borrowing type alias for [`GenericNetwork`] using [`ValueRef`].
pub type Network<'a, T = ValueRef<'a>> = GenericNetwork<T>;

/// A owning types alias for [`GenericNetwork`] using [`Value`].
pub type OwnedNetwork<T = Value> = GenericNetwork<T>;

/// A plan or projection of entity-attribute-value sets with constraints between them.
///
/// - See [`Network`] for type alias that borrows using [`ValueRef`].
/// - Or [`OwnedNetwork`] for a type alias that owns using [`Value`].
#[derive(Debug, Clone, PartialEq)]
pub struct GenericNetwork<V> {
    triples: usize,
    constraints: Vec<Constraint<V>>,
    // decodes: Vec<()>,
}

impl<V> Default for GenericNetwork<V> {
    fn default() -> Self {
        GenericNetwork { triples: 0usize, constraints: vec![] }
    }
}

impl<V> GenericNetwork<V> {
    /// Adds one group of entity-attribute-value triples that can be constrained with values or
    /// by fields on other triples groups.
    pub fn add_triples(&mut self) -> Triples {
        let t = Triples(self.triples);
        self.triples += 1;
        t
    }

    /// Adds a [`Constraint`].
    pub fn constrain(&mut self, c: Constraint<V>) {
        self.add_constraint(c)
    }

    /// Adds a [`Constraint`].
    pub fn add_constraint(&mut self, c: Constraint<V>) {
        self.constraints.push(c)
    }

    /// The number of triples added into this network.
    pub fn triples(&self) -> usize {
        self.triples
    }

    pub fn constraints(&self) -> &[Constraint<V>] {
        &self.constraints
    }

    pub fn constraints_mut(&mut self) -> &mut [Constraint<V>] {
        &mut self.constraints
    }

    pub fn fluent_triples(&mut self) -> FluentTriples<'_, V> {
        let triples = self.add_triples();
        FluentTriples { network: self, triples }
    }
}

impl<V> GenericNetwork<V>
where
    V: PartialEq,
{
    pub fn is_linked(&self, a: TriplesField, b: TriplesField) -> Option<&Constraint<V>> {
        self.constraints.iter().find(|c| match c {
            &&Constraint::Eq { lh, rh: Match::Field(rh) } if lh == a && rh == b => true,
            &&Constraint::Eq { lh, rh: Match::Field(rh) } if lh == b && rh == a => true,
            _ => false,
        })
    }

    pub fn constraint_value_matches<'s>(
        &'s self,
        v: V,
    ) -> impl Iterator<Item = TriplesField> + 's {
        let v = Match::Value(v);
        self.constraints.iter().filter_map(move |c| match c {
            Constraint::Eq { lh, rh } if rh == &v => Some(*lh),
            _ => None,
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Triples(usize);

impl Triples {
    pub fn entity(self) -> TriplesField {
        TriplesField(self, Field::Entity)
    }

    pub fn attribute(self) -> TriplesField {
        TriplesField(self, Field::Attribute)
    }

    pub fn value(self) -> TriplesField {
        TriplesField(self, Field::Value)
    }

    pub fn eav(self) -> (TriplesField, TriplesField, TriplesField) {
        (self.entity(), self.attribute(), self.value())
    }

    pub fn usize(self) -> usize {
        self.0
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct TriplesField(pub(crate) Triples, pub(crate) Field);

impl TriplesField {
    pub fn triples(self) -> Triples {
        self.0
    }

    pub fn field(self) -> Field {
        self.1
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Field {
    Entity,
    Attribute,
    Value,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Ordering {
    Asc,
    Desc,
}

impl TriplesField {
    pub fn asc(self) -> (Self, Ordering) {
        (self, Ordering::Asc)
    }

    pub fn desc(self) -> (Self, Ordering) {
        (self, Ordering::Desc)
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Constraint<V> {
    Eq { lh: TriplesField, rh: Match<V> },
}

impl TriplesField {
    pub fn eq<V>(self, rh: Match<V>) -> Constraint<V> {
        Constraint::Eq { lh: self, rh }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Match<V> {
    Field(TriplesField),
    Encoded(Encoded<V>),
    Value(V),
}

impl<V> From<TriplesField> for Match<V> {
    fn from(f: TriplesField) -> Self {
        Match::Field(f)
    }
}

impl<V> From<Encoded<V>> for Match<V> {
    fn from(e: Encoded<V>) -> Self {
        Match::Encoded(e)
    }
}

pub struct FluentTriples<'n, V> {
    network: &'n mut GenericNetwork<V>,
    triples: Triples,
}

impl<'n, V> FluentTriples<'n, V> {
    pub fn match_entity<I: Into<V>>(&mut self, i: I) -> &mut Self {
        self.network.add_constraint(Constraint::Eq {
            lh: self.triples.entity(),
            rh: Match::Value(i.into()),
        });
        self
    }

    pub fn match_attribute<I: Into<V>>(&mut self, i: I) -> &mut Self {
        self.network.add_constraint(Constraint::Eq {
            lh: self.triples.attribute(),
            rh: Match::Value(i.into()),
        });
        self
    }

    pub fn match_value<I: Into<V>>(&mut self, i: I) -> &mut Self {
        self.network.add_constraint(Constraint::Eq {
            lh: self.triples.value(),
            rh: Match::Value(i.into()),
        });
        self
    }

    pub fn link_entity<I: Into<Match<V>>>(&mut self, i: I) -> &mut Self {
        self.network
            .add_constraint(Constraint::Eq { lh: self.triples.entity(), rh: i.into() });
        self
    }

    pub fn link_attribute<I: Into<Match<V>>>(&mut self, i: I) -> &mut Self {
        self.network
            .add_constraint(Constraint::Eq { lh: self.triples.attribute(), rh: i.into() });
        self
    }

    pub fn link_value<I: Into<Match<V>>>(&mut self, i: I) -> &mut Self {
        self.network
            .add_constraint(Constraint::Eq { lh: self.triples.value(), rh: i.into() });
        self
    }
}

impl<'n, V> Deref for FluentTriples<'n, V> {
    type Target = Triples;

    fn deref(&self) -> &Self::Target {
        &self.triples
    }
}
