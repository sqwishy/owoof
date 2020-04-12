//! what are you doing here?
//!
//! TODO move this into matter? this is projection stuff?

use std::{borrow::Cow, fmt, iter};

use crate::{AttributeName, EntityName};

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
       $crate::dialogue::Pattern {
            entity: pat!(:var $e),
            attribute: pat!(:val $a),
            value: pat!(:var $v),
        }
    }};
    (?$e:ident $a:tt $v:tt) => {{
       $crate::dialogue::Pattern {
            entity: pat!(:var $e),
            attribute: pat!(:val $a),
            value: pat!(:val $v),
        }
    }};
    (:var $v:ident) => {
        $crate::dialogue::VariableOr::Variable(stringify!($v).into())
    };
    (:val $v:tt) => {
        $crate::dialogue::VariableOr::Value($v.into())
    };
}
