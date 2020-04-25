use std::{borrow::Cow, collections::HashMap, fmt::Debug, iter};

use crate::{AttributeName, EntityName};

const ANONYMOUS: &'static str = "";

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

/// A column of a datom-set.
///
/// This name is terrible ... call it DatomsColumn? Or something?
///
/// The [DatomSet] has constructors for this type.
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
    fn constrained_to<'a, V, I>(self, v: I) -> Constraint<'a, V>
    where
        V: 'a,
        I: Into<Concept<'a, V>>,
    {
        Constraint::eq(self, v.into())
    }

    // todo this is bad method name because it conflicts with the Eq trait or something
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
pub enum VariableOr<S, T> {
    Variable(S),
    Value(T),
}

/// An entity-attribute-value tuple, with the possibility of variables for each element.
/// The values for entity and attribute are public handles, a uuid and string
/// respectively, rather than the private/internal database ids.
/// (var|entity, var|attribute, var|value)
#[derive(Debug)]
pub struct Pattern<'a, V> {
    pub entity: VariableOr<Cow<'a, str>, EntityName>,
    pub attribute: VariableOr<Cow<'a, str>, AttributeName<'a>>,
    pub value: VariableOr<Cow<'a, str>, V>,
}

#[macro_export]
macro_rules! pat {
    (?$e:ident $a:tt ?$v:ident) => {{
       $crate::matter::Pattern {
            entity: _varorval!(:var $e),
            attribute: _varorval!(:val $a),
            value: _varorval!(:var $v),
        }
    }};
    (?$e:ident ?$a:ident $v:tt) => {{
       $crate::matter::Pattern {
            entity: _varorval!(:var $e),
            attribute: _varorval!(:var $a),
            value: _varorval!(:val $v),
        }
    }};
    (?$e:ident $a:tt $v:tt) => {{
       $crate::matter::Pattern {
            entity: _varorval!(:var $e),
            attribute: _varorval!(:val $a),
            value: _varorval!(:val $v),
        }
    }};
    (?$e:ident ?$a:ident ?$v:ident) => {{
       $crate::matter::Pattern {
            entity: _varorval!(:var $e),
            attribute: _varorval!(:var $a),
            value: _varorval!(:var $v),
        }
    }};
    ($e:tt $a:tt ?$v:ident) => {{
       $crate::matter::Pattern {
            entity: _varorval!(:val $e),
            attribute: _varorval!(:val $a),
            value: _varorval!(:var $v),
        }
    }};
    ($e:tt ?$a:ident $v:tt) => {{
       $crate::matter::Pattern {
            entity: _varorval!(:val $e),
            attribute: _varorval!(:var $a),
            value: _varorval!(:val $v),
        }
    }};
    ($e:tt $a:tt $v:tt) => {{
       $crate::matter::Pattern {
            entity: _varorval!(:val $e),
            attribute: _varorval!(:val $a),
            value: _varorval!(:val $v),
        }
    }};
    ($e:tt ?$a:ident ?$v:ident) => {{
       $crate::matter::Pattern {
            entity: _varorval!(:val $e),
            attribute: _varorval!(:var $a),
            value: _varorval!(:var $v),
        }
    }};
}

#[macro_export]
macro_rules! _varorval {
    (:var $v:ident) => {
        $crate::matter::VariableOr::Variable(stringify!($v).into())
    };
    (:val $v:tt) => {
        $crate::matter::VariableOr::Value($v.into())
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

/// Express a relationship between a field in a datom-set and something else, either another field
/// in a datom-set or a special(?) value.
///
/// All locations can be related to each other through equality with negation.
///
/// Technically, attribute and entity references should not orderable, and we shouldn't permit
/// greater-than or less-than constraints on those but that might be a pain to implement.
///
/// This typically borrows from Pattern
#[derive(Debug)]
pub struct Constraint<'a, V> {
    pub lh: Location,
    pub op: ConstraintOp,
    pub rh: Concept<'a, V>,
}

#[derive(Debug)]
pub enum Concept<'a, V> {
    Location(Location),
    Entity(&'a EntityName),
    Attribute(&'a AttributeName<'a>),
    Value(&'a V),
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

impl<'a, V> From<Location> for Concept<'a, V> {
    fn from(o: Location) -> Concept<'a, V> {
        Concept::Location(o)
    }
}

impl<'a, V> From<&'a EntityName> for Concept<'a, V> {
    fn from(o: &'a EntityName) -> Concept<'a, V> {
        Concept::Entity(o)
    }
}

impl<'a, V> From<&'a AttributeName<'a>> for Concept<'a, V> {
    fn from(o: &'a AttributeName<'a>) -> Concept<'a, V> {
        Concept::Attribute(o)
    }
}

/// A collection of datom-sets.  With constraints interlinking them.
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
    V: Debug,
{
    pub fn from_patterns(patterns: &'a Vec<Pattern<V>>) -> Self {
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
    pub fn variable(&self, n: &str) -> Option<&Location> {
        self.variables.get(n)
    }

    /// Find all locations constrained to this variable.
    ///
    /// As an implementation detail, this doesn't search recursively.
    ///
    /// If we say that x is at 0.v, we include search constraints that equal 0.v, but we don't
    /// recurse and search for things equal to those. That _should_ be fine based on the way that
    /// [constrain_variable()] works?
    pub fn variable_locations(&self, n: &str) -> impl Iterator<Item = &Location> {
        self.variables
            .get(n)
            .into_iter()
            .map(move |first| {
                Some(first)
                    .into_iter()
                    .chain(self.constraints().iter().filter_map(move |c| match c {
                        Constraint {
                            lh,
                            op: ConstraintOp::Eq,
                            rh: Concept::Location(rh),
                        } if lh == first => Some(rh),
                        Constraint {
                            lh,
                            op: ConstraintOp::Eq,
                            rh: Concept::Location(rh),
                        } if rh == first => Some(lh),
                        _ => None,
                    }))
            })
            .flatten()
    }

    fn add_datomset(&mut self) -> DatomSet {
        let datomset = DatomSet(self.sets);
        self.sets += 1;
        datomset
    }

    fn constrain(&mut self, c: Constraint<'a, V>) {
        self.constraints.push(c)
    }

    /// Register a variable at some location.
    /// If it was registered before, constraint the existing variable to the given location.
    ///
    /// The anonymous variable (the empty string) is
    fn constrain_variable(&mut self, variable: &'a str, location: Location) {
        if variable == ANONYMOUS {
            return;
        } else if let Some(prior) = self.variable(variable) {
            let equal_to_prior = location.constrained_to(prior.clone());
            self.constraints.push(equal_to_prior);
        } else {
            self.variables.insert(variable, location);
        }
    }

    fn constrain_field<I>(&mut self, location: Location, i: I)
    where
        I: Into<Concept<'a, V>>,
    {
        self.constrain(location.constrained_to(i))
    }

    pub fn add_patterns(&mut self, patterns: &'a Vec<Pattern<V>>) {
        for pattern in patterns {
            self.add_pattern(pattern);
        }
    }

    // TODO, does this actually have to borrow pattern like this or just Pattern<'p, ..>?
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
            VariableOr::Variable(e) => self.constrain_variable(e, entity_field),
            VariableOr::Value(e) => self.constrain_field(entity_field, e),
        };
        // constrain the attribute ...
        match attribute {
            VariableOr::Variable(a) => self.constrain_variable(a, attribute_field),
            VariableOr::Value(a) => self.constrain_field(attribute_field, a),
        };
        // ... and the value
        match value {
            VariableOr::Variable(v) => self.constrain_variable(v, value_field),
            VariableOr::Value(v) => self.constrain_field(value_field, Concept::Value(v)),
        };
        datomset
    }

    pub fn add_constraint<'c: 'a>(&mut self, c: Constraint<'a, V>) {
        self.constraints.push(c);
    }

    pub fn attribute_map<I>(&'a mut self, top: &'a str, attrs: I) -> AttributeMap<'a, V>
    where
        I: iter::IntoIterator<Item = &'a AttributeName<'a>>,
    {
        // TODO hrm .....
        let top = if top == ANONYMOUS { "_" } else { top };

        // Attempt to reuse datomsets where the datomset entity is
        // constrained to the `top` variable...
        let top_datoms = self
            .variable_locations(top)
            .filter(|l| l.field == Field::Entity)
            .map(|l| l.datomset)
            .collect::<Vec<_>>();

        let map = attrs
            .into_iter()
            .map(|attr| {
                // Search the datomsets where `top` is an entity of
                // and find one constrained to this attribute...
                let exists: Option<DatomSet> = top_datoms
                    .iter()
                    .find(|datomset| {
                        let datomset_attr = datomset.attribute_field();
                        self.constraints().iter().any(|c| match c {
                            Constraint {
                                lh,
                                op: ConstraintOp::Eq,
                                rh: Concept::Attribute(rh),
                            } => *lh == datomset_attr && *rh == attr,
                            _ => false,
                        })
                    })
                    .cloned();
                if let Some(datomset) = exists {
                    return (attr, datomset);
                }

                // Create a new datomset that fetches this attribute value
                let datomset = self.add_datomset();
                self.constrain_variable(top, datomset.entity_field());
                self.constrain(datomset.attribute_field().constrained_to(attr));
                (attr, datomset)
            })
            .collect();

        AttributeMap {
            map,
            projection: self,
            order_by: vec![],
            limit: 0,
        }
    }
}

#[derive(Debug)]
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
    pub projection: &'a Projection<'a, V>,
    /// This is ordered, corresponding to query row column order
    pub map: Vec<(&'a AttributeName<'a>, DatomSet)>,
    pub order_by: Vec<(Location, Ordering)>,
    pub limit: i64,
}

#[derive(Debug)]
pub struct Selection<'a, V> {
    pub projection: &'a Projection<'a, V>,
    pub columns: Vec<Location>,
    pub limit: i64,
}

impl<'a, V> Selection<'a, V> {
    pub fn new(projection: &'a Projection<'a, V>) -> Self {
        Selection {
            projection,
            columns: vec![],
            limit: 0,
        }
    }

    pub fn columns(&self) -> &Vec<Location> {
        &self.columns
    }

    pub fn limit(&self) -> i64 {
        self.limit
    }
}
