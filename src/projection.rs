//! So if you read the essay over in lib.rs we saw that sequence of patterns could be
//! used to gather sets of datoms and relate them to each other.
//!
//! Consider this example ...
//! ```ignore
//! ?p :person/name "John Arbuckle"
//! ?a :pet/human   ?p
//! ?a ?t           ?v
//! ```
//!
//! There are there different patterns here, but we can express them all with the
//! following...
//!
//! ```
//! # use owoof::{Pattern, Match, Value};
//! vec![
//!     Pattern {
//!         entity:    Match::Variable("p".into()),
//!         attribute: Match::Value(":person/human".into()),
//!         value:     Match::Value(Value::Text("John Arbuckle".to_owned())),
//!     },
//!     Pattern {
//!         entity:    Match::Variable("a".into()),
//!         attribute: Match::Value(":pet/human".into()),
//!         value:     Match::Variable("p".into()),
//!     },
//!     Pattern {
//!         entity:    Match::Variable("a".into()),
//!         attribute: Match::Variable("t".into()),
//!         value:     Match::Variable("v".into()),
//!     },
//! ];
//! ```
//!
//! Then, we can initialize a [`Projection`] by calling [`Projection::from_patterns`] using
//! a slice of that that vector.
//!
//! The [`pat`] macro can also be used as a shorthand for the above. Although,
//! typically that's only useful if you're writing literals.
//!
//! A [`Projection`] rendered to a SQL *FROM* & *WHERE* clause using
//! [`crate::sql::projection_sql`].
//!
//! What you probably want is to, get a [`Selection`] (from [`Projection::select`]) or
//! an [`AttributeMap`] (from [`Projection::entity_group`] &
//! [`EntityGroup::attribute_map`]) and pass that to [`crate::Session::find`] to do a
//! search.
//!
//! Here is an example!
//! ```
//! # use std::collections::HashMap;
//! # use owoof::{pat, AttributeName};
//! # let mut db = rusqlite::Connection::open_in_memory()?;
//! # owoof::Session::init_schema(&mut db)?;
//!   let s = owoof::Session::new(&mut db)?;
//!   let person_name = s.new_attribute(":person/name")?;
//!   let pet_human   = s.new_attribute(":pet/human")?;
//!   let pet_name    = s.new_attribute(":pet/name")?;
//!
//!   let john = s.new_entity()?;
//!   s.assert(&john, &person_name, &"John Arbuckle".to_owned())?;
//!
//!   let garfield = s.new_entity()?;
//!   s.assert(&garfield, &pet_human, &*john)?;
//!   s.assert(&garfield, &pet_name,  &"Garfield".to_owned())?;
//!
//!   let odie = s.new_entity()?;
//!   s.assert(&odie, &pet_human, &*john)?;
//!   s.assert(&odie, &pet_name,  &"Odie".to_owned())?;
//!
//!   s.commit()?;
//!
//!   use owoof::{Value, Value::Text};
//!   let pat = &[ // ~~~ give pats ~~~
//!       pat!(?person ":person/name" ?person_name),
//!       pat!(?pet    ":pet/human"   ?person),
//!       pat!(?pet    ":pet/name"    ?pet_name),
//!   ];
//!   let mut projection = owoof::Projection::<Value>::from_patterns(pat);
//!
//!   // select and fetch two-tuples of values at the location of these variables
//!   let selection = projection.select((
//!       projection.var("person_name").unwrap(),
//!       projection.var("pet_name").unwrap(),
//!   ));
//!   let res = owoof::Session::new(&mut db)?.find(&selection)?;
//!   assert_eq!(res, vec![
//!       (Text("John Arbuckle".to_owned()), Text("Garfield".to_owned())),
//!       (Text("John Arbuckle".to_owned()), Text("Odie".to_owned())),
//!   ]);
//!
//!   // select and fetch attribute-value maps grouped by ?pet
//!   let pet_stuff = projection.entity_group("pet").unwrap()
//!                             .attribute_map(vec![&*pet_human, &*pet_name]);
//!   let selection = projection.select(&pet_stuff);
//!   let mut res = owoof::Session::new(&mut db)?.find(&selection)?;
//!   assert_eq!(
//!       res,
//!       vec![
//!           [
//!               (&AttributeName::from_static(":pet/name"), Text("Garfield".to_owned())),
//!               (&AttributeName::from_static(":pet/human"), Value::from(john.clone())),
//!           ].iter().cloned().collect(),
//!           [
//!               (&AttributeName::from_static(":pet/name"), Text("Odie".to_owned())),
//!               (&AttributeName::from_static(":pet/human"), Value::from(john.clone())),
//!           ].iter().cloned().collect(),
//!       ],
//!   );
//!
//! # let s = owoof::Session::new(&mut db)?;
//! # Ok::<_, anyhow::Error>(())
//! ```
use std::ops::{Deref, DerefMut};
use std::{borrow::Cow, collections::HashMap, fmt::Debug, iter};

use crate::{
    types::{Affinity, HasAffinity},
    AttributeName, EntityId,
};

/// It's debatable whether this should be public ... but there is some special behavior
/// in here where it shouldn't be possible to use the empty string as a variable name.
///
/// And by special behavior I mean a couple functions do nothing instead of something if
/// you pass `""`.  But I don't know why that's a feature in here. This should be a
/// bug...
#[deprecated]
pub const ANONYMOUS: &str = "";

/// Just a usize with a tiny bit of sugar/behavior on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatomSet(pub usize);

impl DatomSet {
    pub fn entity_field(self) -> Location {
        Location {
            datomset: self,
            field: Field::Entity,
        }
    }

    pub fn attribute_field(self) -> Location {
        Location {
            datomset: self,
            field: Field::Attribute,
        }
    }

    pub fn value_field(self) -> Location {
        Location {
            datomset: self,
            field: Field::Value,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Field {
    Entity,
    Attribute,
    Value,
}

/// Points to a datomset field (row & column) in a projection. A datomset is just an
/// entity-attribute-value set so the field is one of those.
///
/// The [DatomSet] has constructors for this type.
///
/// And this has constructors for [Constraint].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Location {
    pub datomset: DatomSet,
    pub field: Field,
}

macro_rules! _constrain_impl {
    ($name:ident) => {
        pub fn $name<'a, V, I>(self, v: I) -> Constraint<'a, V>
        where
            V: 'a,
            I: Into<Concept<'a, V>>,
        {
            Constraint::$name(self, v.into())
        }
    };
}

impl Location {
    /// longhand for [`Location::eq`]; that's hard to call because of the [`PartialEq`] trait or something
    pub fn constrained_to<'a, V, I>(self, v: I) -> Constraint<'a, V>
    where
        V: 'a,
        I: Into<Concept<'a, V>>,
    {
        Constraint::eq(self, v.into())
    }

    _constrain_impl!(eq);
    _constrain_impl!(ne);
    _constrain_impl!(gt);
    _constrain_impl!(ge);
    _constrain_impl!(lt);
    _constrain_impl!(le);
}

// /// TODO I'm not sure it's useful to have this type, the only thing I can think of if we want to
// /// guarantee that someone is referencing a Location that it got from `Projection::variable()`
// #[derive(Debug, Clone, Copy, PartialEq, Eq)]
// pub struct Variable(Location);
//
// impl From<Location> for Variable {
//     fn from(l: Location) -> Self {
//         Variable(l)
//     }
// }

#[derive(Debug, PartialEq, Eq)]
pub enum Match<S, T> {
    Variable(S),
    Value(T),
}

/// An entity-attribute-value tuple, with the possibility of variables for each element.
/// The values for entity and attribute are public handles, a uuid and string
/// respectively, rather than the private/internal database ids.
/// (var|entity, var|attribute, var|value)
#[derive(Debug)]
pub struct Pattern<'a, V> {
    pub entity: Match<Cow<'a, str>, EntityId>,
    pub attribute: Match<Cow<'a, str>, AttributeName<'a>>,
    pub value: Match<Cow<'a, str>, V>,
}

#[macro_export]
macro_rules! pat {
    (?$e:ident $a:tt ?$v:ident) => {{
       $crate::projection::Pattern {
            entity: $crate::_varorval!(:var $e),
            attribute: $crate::_varorval!(:val $a),
            value: $crate::_varorval!(:var $v),
        }
    }};
    (?$e:ident ?$a:ident $v:tt) => {{
       $crate::projection::Pattern {
            entity: $crate::_varorval!(:var $e),
            attribute: $crate::_varorval!(:var $a),
            value: $crate::_varorval!(:val $v),
        }
    }};
    (?$e:ident $a:tt $v:tt) => {{
       $crate::projection::Pattern {
            entity: $crate::_varorval!(:var $e),
            attribute: $crate::_varorval!(:val $a),
            value: $crate::_varorval!(:val $v),
        }
    }};
    (?$e:ident ?$a:ident ?$v:ident) => {{
       $crate::projection::Pattern {
            entity: $crate::_varorval!(:var $e),
            attribute: $crate::_varorval!(:var $a),
            value: $crate::_varorval!(:var $v),
        }
    }};
    ($e:tt $a:tt ?$v:ident) => {{
       $crate::projection::Pattern {
            entity: $crate::_varorval!(:val $e),
            attribute: $crate::_varorval!(:val $a),
            value: $crate::_varorval!(:var $v),
        }
    }};
    ($e:tt ?$a:ident $v:tt) => {{
       $crate::projection::Pattern {
            entity: $crate::_varorval!(:val $e),
            attribute: $crate::_varorval!(:var $a),
            value: $crate::_varorval!(:val $v),
        }
    }};
    ($e:tt $a:tt $v:tt) => {{
       $crate::projection::Pattern {
            entity: $crate::_varorval!(:val $e),
            attribute: $crate::_varorval!(:val $a),
            value: $crate::_varorval!(:val $v),
        }
    }};
    ($e:tt ?$a:ident ?$v:ident) => {{
       $crate::projection::Pattern {
            entity: $crate::_varorval!(:val $e),
            attribute: $crate::_varorval!(:var $a),
            value: $crate::_varorval!(:var $v),
        }
    }};
}

#[macro_export]
macro_rules! _varorval {
    (:var $v:ident) => {
        $crate::projection::Match::Variable(stringify!($v).into())
    };
    (:val $v:tt) => {
        $crate::projection::Match::Value($v.into())
    };
}

#[derive(Debug, PartialEq, Eq)]
pub enum ConstraintOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

/// Express a relationship between a location/field in a datom-set and something else, possibly
/// another location.
///
/// Technically, attribute and entity references should not orderable, and we shouldn't permit
/// greater-than or less-than constraints on those but that might be a pain to implement.
///
/// This is used a lot by [`Projection`]. Typically, the lifetime on this thing borrows
/// from the [`Pattern`] that the [`Projection`] is using.
#[derive(Debug)]
pub struct Constraint<'a, V> {
    pub lh: Location,
    pub op: ConstraintOp,
    pub rh: Concept<'a, V>,
}

/// Honestly, I don't even know what this is about. The `V` type typically implements
/// [`HasAffinity`] so we could just about discriminate on [`Affinity::Entity`] and
/// [`Affinity::Attribute`] instead?
///
/// I'm almost certain this pre-dates that trait and the [`crate::Value`] type though so maybe
/// the hope was I could just use this instead of having those and keep things simpler?
#[derive(Debug)]
pub enum Concept<'a, V> {
    Location(Location),
    Entity(&'a EntityId),
    Attribute(&'a AttributeName<'a>),
    Value(Affinity, &'a V),
}

impl<'a, V> Constraint<'a, V> {
    pub fn eq(lh: Location, rh: Concept<'a, V>) -> Self {
        let op = ConstraintOp::Eq;
        Constraint { op, lh, rh }
    }

    pub fn ne(lh: Location, rh: Concept<'a, V>) -> Self {
        let op = ConstraintOp::Ne;
        Constraint { op, lh, rh }
    }

    pub fn gt(lh: Location, rh: Concept<'a, V>) -> Self {
        let op = ConstraintOp::Gt;
        Constraint { op, lh, rh }
    }

    pub fn ge(lh: Location, rh: Concept<'a, V>) -> Self {
        let op = ConstraintOp::Ge;
        Constraint { op, lh, rh }
    }

    pub fn lt(lh: Location, rh: Concept<'a, V>) -> Self {
        let op = ConstraintOp::Lt;
        Constraint { op, lh, rh }
    }

    pub fn le(lh: Location, rh: Concept<'a, V>) -> Self {
        let op = ConstraintOp::Le;
        Constraint { op, lh, rh }
    }
}

impl<'a, V> Concept<'a, V>
where
    V: HasAffinity,
{
    pub fn value(v: &'a V) -> Self {
        Concept::Value(v.affinity(), v)
    }
}

impl<'a, V> From<Location> for Concept<'a, V> {
    fn from(o: Location) -> Concept<'a, V> {
        Concept::Location(o)
    }
}

impl<'a, V> From<&'a EntityId> for Concept<'a, V> {
    fn from(o: &'a EntityId) -> Concept<'a, V> {
        Concept::Entity(o)
    }
}

impl<'a, V> From<&'a AttributeName<'a>> for Concept<'a, V> {
    fn from(o: &'a AttributeName<'a>) -> Concept<'a, V> {
        Concept::Attribute(o)
    }
}

/// A collection of datom-sets with constraints relating them.
///
/// This references attributes and entities by their handles/public representations.
#[derive(Debug)]
pub struct Projection<'a, V> {
    sets: usize,
    /// I believe this only contains the _first_ occurrence of some variable
    variables: HashMap<&'a str, Location>,
    constraints: Vec<Constraint<'a, V>>,
}

impl<'a, V> Default for Projection<'a, V> {
    fn default() -> Self {
        Projection {
            sets: 0usize,
            variables: HashMap::new(),
            constraints: Vec::new(),
        }
    }
}

impl<'a, V> Projection<'a, V>
where
    V: Debug + HasAffinity,
{
    pub fn from_patterns(patterns: &'a [Pattern<V>]) -> Self {
        let mut p = Self::default();
        p.add_patterns(patterns);
        p
    }

    // pub fn datomsets(&self) -> impl Iterator<Item = DatomSet> {
    //     (0..self.sets).map(DatomSet)
    // }

    pub fn datomsets(&self) -> usize {
        self.sets
    }

    pub fn variables(&self) -> &HashMap<&'a str, Location> {
        &self.variables
    }

    pub fn constraints(&self) -> &Vec<Constraint<'a, V>> {
        &self.constraints
    }

    /// Return the first location (if any) where this variable was used.
    pub fn variable(&self, n: &str) -> Option<Location> {
        self.variables.get(n).cloned()
    }

    pub fn var(&self, n: &str) -> Option<Location> {
        self.variable(n)
    }

    /// As [`Projection::constrained_to`] but looks up the variable's location of first use.
    ///
    /// Note, this doesn't search *every* spot the variable might have been used; we
    /// don't track that. It will only return locations constrainted the the location of
    /// the variable's first use.
    ///
    /// This _should_ be fine if you're using [`Projection::constrain_variable()`] or
    /// [`Projection::variable()`] to get a variable's location when building
    /// constraints, since those will always return the variable's location of first
    /// use.
    pub fn variable_locations(&self, n: &str) -> impl Iterator<Item = Location> + '_ {
        self.variables
            .get(n)
            .into_iter()
            .map(move |&first| Some(first).into_iter().chain(self.constrained_to(first)))
            .flatten()
    }

    /// Find [`Location`]s with an equality constraint on this location.
    pub fn constrained_to(&self, loc: Location) -> impl Iterator<Item = Location> + '_ {
        self.constraints().iter().filter_map(move |c| match c {
            Constraint {
                lh,
                op: ConstraintOp::Eq,
                rh: Concept::Location(rh),
            } if lh == &loc => Some(*rh),
            Constraint {
                lh,
                op: ConstraintOp::Eq,
                rh: Concept::Location(rh),
            } if rh == &loc => Some(*lh),
            _ => None,
        })
    }

    pub fn add_datomset(&mut self) -> DatomSet {
        let datomset = DatomSet(self.sets);
        self.sets += 1;
        datomset
    }

    fn constrain(&mut self, c: Constraint<'a, V>) {
        self.constraints.push(c)
    }

    /// This is needed to add non-equality constraints
    pub fn add_constraint(&mut self, c: Constraint<'a, V>) {
        self.constrain(c)
    }

    /// Register a variable at some location.
    /// If it was registered before, constraint the existing variable to the given location.
    ///
    /// Nothing happens if this receives the anonymous variable (the empty string).
    /// TODO this may never be what anybody wants at all ever...
    pub fn constrain_variable(&mut self, variable: &'a str, location: Location) {
        if variable == ANONYMOUS {
        } else if let Some(prior) = self.variable(variable) {
            let equal_to_prior = location.constrained_to(prior);
            self.constraints.push(equal_to_prior);
        } else {
            self.variables.insert(variable, location);
        }
    }

    pub fn constrain_field<I>(&mut self, location: Location, i: I)
    where
        I: Into<Concept<'a, V>>,
    {
        self.constrain(location.constrained_to(i))
    }

    pub fn add_patterns(&mut self, patterns: &'a [Pattern<V>]) {
        for pattern in patterns {
            self.add_pattern(pattern);
        }
    }

    /// Appends a datomset to the projection with [`Projection::add_datomset`],
    /// constraining each entity-attribute-value to either a variable or a value based
    /// on the given [`Pattern`].
    pub fn add_pattern<'p: 'a>(&mut self, pattern: &'p Pattern<V>) -> DatomSet {
        let Pattern {
            entity,
            attribute,
            value,
        } = pattern;

        let datomset = self.add_datomset();
        let entity_field = datomset.entity_field();
        let attribute_field = datomset.attribute_field();
        let value_field = datomset.value_field();
        // constrain the entity ...
        match entity {
            Match::Variable(e) => self.constrain_variable(e, entity_field),
            Match::Value(e) => self.constrain_field(entity_field, e),
        };
        // constrain the attribute ...
        match attribute {
            Match::Variable(a) => self.constrain_variable(a, attribute_field),
            Match::Value(a) => self.constrain_field(attribute_field, a),
        };
        // ... and the value
        match value {
            Match::Variable(v) => self.constrain_variable(v, value_field),
            Match::Value(v) => self.constrain_field(value_field, Concept::Value(v.affinity(), v)),
        };
        datomset
    }

    /// Select allows us to get an object we can pass to [`crate::Session::find`] that
    /// can resolve query results into a vector or maybe tuples I think too.
    ///
    /// See the [owoof::projection] module documentation for an example.
    pub fn select<'s, S>(&'s mut self, s: S) -> Selection<'s, 'a, V, S> {
        Selection {
            projection: self,
            columns: s,
            order_by: vec![],
            limit: 0,
        }
    }

    // TODO this should take a location, not a variable
    /// This is part of representing a bunch of datomsets all constrained to the same
    /// entity. Suppose you've got ...
    /// ```ignore
    /// ?birthday-person :person/birthday ?today
    /// ?birthday-person :person/name ?name
    /// ?birthday-person :person/age ?age
    /// ```
    /// ... using [`Projection::select`] we can ask for the values of specific locations and
    /// end up with a sequence or `[?name ?age]` for each `?birthday-person` or something.
    ///
    /// But using an [`EntityGroup`] allows us to construct an [`AttributeMap`] which
    /// implements [`crate::sql::ReadFromRow`] to produce a `HashMap` on attributes to
    /// values.
    ///
    /// So instead of ending up with a sequence for each matching "object" we get a map.
    /// Which might be nice sometimes like if you want to serialize a self-describing JSON
    /// document.
    ///
    /// See the [owoof::projection] module documentation for an example.
    pub fn entity_group<'p>(&'p mut self, var: &'a str) -> Option<EntityGroup<'p, 'a, V>>
    where
        'a: 'p,
    {
        if var == ANONYMOUS {
            return None;
        }

        let datoms = self
            .variable_locations(var)
            .filter(|l| l.field == Field::Entity)
            .map(|l| l.datomset)
            .collect::<Vec<_>>();

        Some(EntityGroup {
            projection: self,
            var,
            datoms,
        })
    }

    /// short-hand for [`Projection::entity_group`] & [`EntityGroup::attribute_map`].
    pub fn attribute_map<'p, I>(&mut self, top: &'a str, attrs: I) -> AttributeMap<'p, V>
    where
        'a: 'p,
        I: iter::IntoIterator<Item = &'a AttributeName<'a>>,
    {
        let mut eg = self.entity_group(top).expect("todo deanonymize variable");
        eg.attribute_map(attrs)
    }
}

/// See [`Projection::entity_group`] ...
#[derive(Debug)]
pub struct EntityGroup<'a, 'p, V> {
    projection: &'a mut Projection<'p, V>,
    var: &'p str,
    /// datoms where the entity is constrained to our variable
    datoms: Vec<DatomSet>,
}

impl<'a, 'p, V> EntityGroup<'a, 'p, V>
where
    V: Debug + HasAffinity,
{
    pub fn get_or_fetch_attribute<'t: 'p>(&mut self, attr: &'t AttributeName<'t>) -> DatomSet {
        let p = &mut self.projection;

        // Search our known datomsets where `top` is the entity
        // to see if it's already constrained to this attribute...
        let exists: Option<DatomSet> = self
            .datoms
            .iter()
            .find(|datomset| {
                p.constraints().iter().any(|c| match c {
                    Constraint {
                        lh,
                        op: ConstraintOp::Eq,
                        rh: Concept::Attribute(rh),
                    } => lh == &datomset.attribute_field() && rh == &attr,
                    _ => false,
                })
            })
            .cloned();

        if let Some(datomset) = exists {
            return datomset;
        }

        // Create a new datomset that fetches this attribute value
        let datomset = p.add_datomset();
        p.constrain_variable(self.var, datomset.entity_field());
        p.constrain(datomset.attribute_field().constrained_to(attr));
        self.datoms.push(datomset);
        return datomset;
    }

    pub fn attribute_map<'b: 'p, I>(&mut self, attrs: I) -> AttributeMap<'p, V>
    where
        I: iter::IntoIterator<Item = &'b AttributeName<'b>>,
    {
        let map = attrs
            .into_iter()
            .map(|attr| (attr, self.get_or_fetch_attribute(attr)))
            .collect();
        AttributeMap {
            map,
            p: Default::default(),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Ordering {
    Asc,
    Desc,
}

impl Location {
    pub fn asc(self) -> (Location, Ordering) {
        (self, Ordering::Asc)
    }

    pub fn desc(self) -> (Location, Ordering) {
        (self, Ordering::Desc)
    }
}

#[derive(Debug)]
pub struct AttributeMap<'a, V> {
    /// `map` is ordered, corresponding to query row column order
    pub(crate) map: Vec<(&'a AttributeName<'a>, DatomSet)>,
    p: std::marker::PhantomData<V>,
}

impl<'a, V> AttributeMap<'a, V> {
    pub fn into_value_locations(self) -> impl Iterator<Item = Location> + 'a {
        self.map
            .into_iter()
            .map(|(_, datomset)| datomset.value_field())
    }
}

#[derive(Debug)]
pub struct Selection<'a, 'p, V, S> {
    pub projection: &'a mut Projection<'p, V>,
    pub columns: S,
    pub order_by: Vec<(Location, Ordering)>,
    pub limit: i64,
}

impl<'a, 'p, V, S> Selection<'a, 'p, V, S> {
    pub fn limit(&mut self, limit: i64) -> &mut Self {
        self.limit = limit;
        self
    }

    pub fn order_by(&mut self, ord: (Location, Ordering)) -> &mut Self {
        self.order_by.push(ord);
        self
    }

    pub fn read_using<SS>(
        self,
        from_row: SS,
    ) -> Selection<'a, 'p, V, crate::sql::CrazyFuckingMemes<S, SS>> {
        Selection {
            projection: self.projection,
            columns: crate::sql::CrazyFuckingMemes::new(self.columns, from_row),
            order_by: self.order_by,
            limit: self.limit,
        }
    }
}

impl<'a, 'p, V, S> Deref for Selection<'a, 'p, V, S> {
    type Target = Projection<'p, V>;

    fn deref(&self) -> &Self::Target {
        &self.projection
    }
}

impl<'a, 'p, V, S> DerefMut for Selection<'a, 'p, V, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.projection
    }
}
