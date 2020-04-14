use std::{collections::HashMap, fmt::Debug, iter};

use crate::{dialogue, AttributeName, EntityName};

const ANONYMOUS: &'static str = "";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatomSet(pub usize);

impl DatomSet {
    fn entity_field(self) -> Location {
        Location {
            datomset: self,
            field: Field::Entity,
        }
    }

    fn attribute_field(self) -> Location {
        Location {
            datomset: self,
            field: Field::Attribute,
        }
    }

    fn value_field(self) -> Location {
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
    pub fn from_patterns(patterns: &'a Vec<dialogue::Pattern<V>>) -> Self {
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

    pub fn variable(&self, n: &str) -> Option<&Location> {
        self.variables.get(n)
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

    pub fn add_patterns(&mut self, patterns: &'a Vec<dialogue::Pattern<V>>) {
        for pattern in patterns {
            self.add_pattern(pattern);
        }
    }

    // TODO, does this actually have to borrow pattern like this or just Pattern<'p, ..>?
    pub fn add_pattern<'p: 'a>(&mut self, pattern: &'p dialogue::Pattern<V>) -> DatomSet {
        use dialogue::{Pattern, VariableOr};
        match pattern {
            // a :person/name "Spongebob"
            Pattern {
                entity: VariableOr::Variable(e),
                attribute: VariableOr::Value(a),
                value: VariableOr::Value(v),
            } => {
                let datomset = self.add_datomset();

                // constrain the entity ...
                self.constrain_variable(e, datomset.entity_field());

                // constrain the attribute ...
                self.constrain(datomset.attribute_field().constrained_to(a));

                // ... and value
                self.constrain(datomset.value_field().constrained_to(Concept::Value(v)));

                datomset
            }

            // a :person/name b
            Pattern {
                entity: VariableOr::Variable(e),
                attribute: VariableOr::Value(a),
                value: VariableOr::Variable(v),
            } => {
                let datomset = self.add_datomset();

                // constrain the entity ...
                self.constrain_variable(e, datomset.entity_field());

                // constrain the attribute ...
                self.constrain(datomset.attribute_field().constrained_to(a));

                // ... and the value
                self.constrain_variable(v, datomset.value_field());

                datomset
            }

            // // a b c
            // Pattern {
            //     attribute: Variable(a),
            //     ..
            // } => todo!("variable attribute {:?}", pattern),

            // // "some-hyphenated-uuid" :person/name b
            // Pattern {
            //     entity: Value(e), ..
            // } => todo!("explicit entity {:?}", pattern),
            _ => todo!("{:?}", pattern),
        }
    }

    pub fn add_constraint<'c: 'a>(&mut self, c: Constraint<'a, V>) {
        self.constraints.push(c);
    }

    pub fn attribute_map<I>(&'a mut self, top: &'a str, attrs: I) -> AttributeMap<'a, V>
    where
        I: iter::IntoIterator<Item = &'a AttributeName<'a>>,
    {
        use dialogue::{Pattern, VariableOr};
        let map = attrs
            .into_iter()
            .map(|attr| {
                // Create a new datomset that fetches this attribute value
                // TODO reuse existing constraints?
                let datomset = self.add_datomset();
                self.constrain_variable(top, datomset.entity_field());
                self.constrain(datomset.attribute_field().constrained_to(attr));
                (attr, datomset)
            })
            .collect();
        AttributeMap {
            map,
            projection: self,
        }
    }
}

#[derive(Debug)]
pub struct AttributeMap<'a, V> {
    pub projection: &'a Projection<'a, V>,
    pub map: Vec<(&'a AttributeName<'a>, DatomSet)>,
    // pub limit: i64,
}

#[derive(Debug)]
pub struct Selection<'a, V> {
    pub projection: &'a Projection<'a, V>,
    pub columns: Vec<Location>,
    pub limit: i64,
}

// impl<'a, V> From<Projection<'a, V>> for Selection<'a, V> {
//     fn from(projection: Projection<'a, V>) -> Self {
//         Selection {
//             projection,
//             columns: vec![],
//         }
//     }
// }

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
