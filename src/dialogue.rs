//! what are you doing here?
//!
//! TODO move this into matter? this is projection stuff?

use std::{fmt, iter};

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
pub struct Pattern<S, V> {
    // todo, less borrowy?
    pub entity: VariableOr<S, EntityName>,
    pub attribute: VariableOr<S, AttributeName<S>>,
    pub value: VariableOr<S, V>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Predicate<S, V> {
    pub op: PredicateOp,
    pub lh: S,
    pub rh: VariableOr<S, V>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PredicateOp {
    Eq,
    Gt,
    Ge,
    Lt,
    Le,
}

impl<S, V> Predicate<S, V> {
    fn lt(lh: S, rh: VariableOr<S, V>) -> Self {
        let op = PredicateOp::Lt;
        Predicate { op, lh, rh }
    }
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
        $crate::dialogue::VariableOr::Variable(stringify!($v))
    };
    (:val $v:tt) => {
        $crate::dialogue::VariableOr::Value($v.into())
    };
}

#[macro_export]
macro_rules! prd {
    (?$e:ident $o:tt ?$v:ident) => {{
       $crate::dialogue::Predicate {
           op: prd!(:op $o),
           lh: stringify!($e),
           rh: prd!(:var $v),
        }
    }};
    (?$e:ident $o:tt $v:tt) => {{
       $crate::dialogue::Predicate {
           op: prd!(:op $o),
           lh: stringify!($e),
           rh: prd!(:val $v),
        }
    }};
    (:op <) => {
        $crate::dialogue::PredicateOp::Lt
    };
    (:var $v:ident) => {
        $crate::dialogue::VariableOr::Variable(stringify!($v))
    };
    (:val $v:tt) => {
        $crate::dialogue::VariableOr::Value($v.into())
    };
}
