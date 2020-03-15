use std::{collections::HashMap, fmt::Debug};

use crate::{dialogue, AttributeHandle, EntityHandle};

#[derive(Debug, Clone, Copy)]
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

#[derive(Debug, Clone)]
pub enum Field {
    Entity,
    Attribute,
    Value,
}

/// A column of a datom-set.
///
/// This name is terrible ... call it DatomsColumn? Or something?
#[derive(Debug, Clone)]
pub struct Location {
    pub datomset: DatomSet,
    pub field: Field,
}

impl Location {
    fn constrained<'a, V, I>(self, v: I) -> Constraint<'a, V>
    where
        V: 'a,
        I: Into<Value<'a, V>>,
    {
        Constraint(self, v.into())
    }
}

/// The field in some datom-set must match something, like another field in a datom-set.
#[derive(Debug)]
pub struct Constraint<'a, V>(pub Location, pub Value<'a, V>);

#[derive(Debug)]
pub enum Value<'a, V> {
    Location(Location),
    Entity(&'a EntityHandle),
    Attribute(&'a AttributeHandle),
    /// This borrows from Pattern
    EqValue(&'a V),
}

macro_rules! value_from {
    ($type:ty => $variant:ident) => {
        impl<'a, V> From<$type> for Value<'a, V> {
            fn from(o: $type) -> Value<'a, V> {
                Value::$variant(o)
            }
        }
    };
}

value_from!(Location => Location);
value_from!(&'a EntityHandle => Entity);
value_from!(&'a AttributeHandle => Attribute);
// This is too generic to receive a From impl ...
// value_from!(&'a V => EqValue);

/// A collection of datom-sets.  With constraints interlinking them.
///
/// This references attributes and entities by their handles/public representations.
#[derive(Default, Debug)]
pub struct Glyph<'a, V> {
    sets: usize,
    /// I believe this only contains the _first_ occurrence of some variable
    variables: HashMap<&'a str, Location>,
    constraints: Vec<Constraint<'a, V>>,
}

impl<'a, V> Glyph<'a, V>
where
    V: Debug,
{
    pub fn datomsets(&self) -> usize {
        self.sets
    }
    pub fn variables(&self) -> &HashMap<&'a str, Location> {
        &self.variables
    }
    pub fn constraints(&self) -> &Vec<Constraint<'a, V>> {
        &self.constraints
    }

    fn add_datomset(&mut self) -> DatomSet {
        let datomset = DatomSet(self.sets);
        self.sets += 1;
        datomset
    }

    /// Register a variable. If it was registered before, add a constraint to maintain
    /// that the variable share values.
    fn constrain_variable(&mut self, variable: &'a str, location: Location) {
        if let Some(prior) = self.variables.get(variable) {
            let equal_to_prior = location.constrained(prior.clone());
            self.constraints.push(equal_to_prior);
        } else {
            self.variables.insert(variable, location);
        }
    }

    pub fn add_pattern<'p: 'a>(&mut self, pattern: &'p dialogue::Pattern<'a, V>) {
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
                self.constraints
                    .push(Constraint(datomset.attribute_field(), Value::Attribute(a)));

                // ... and value
                self.constraints
                    .push(Constraint(datomset.value_field(), Value::EqValue(v)));
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
                self.constraints
                    .push(Constraint(datomset.attribute_field(), Value::Attribute(a)));

                // ... and the value
                self.constrain_variable(v, datomset.value_field())
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
}
