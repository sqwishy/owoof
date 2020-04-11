//! what are you doing here?
//!
//! TODO move this into matter? this is projection stuff?

use std::{fmt, iter};

use crate::{AttributeName, EntityName};

#[derive(Debug, PartialEq, Eq)]
pub enum VariableOr<'a, T> {
    Variable(&'a str),
    Value(T),
}

/// An entity-attribute-value tuple, with the possibility of variables for each element.
/// The values for entity and attribute are public handles, a uuid and string
/// respectively, rather than the private/internal database ids.
/// (var|entity, var|attribute, var|value)
#[derive(Debug)]
pub struct Pattern<'a, V> {
    // todo, less borrowy?
    pub entity: VariableOr<'a, &'a EntityName>,
    pub attribute: VariableOr<'a, &'a AttributeName>,
    pub value: VariableOr<'a, V>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Predicate<'a, V> {
    pub op: PredicateOp,
    pub lh: &'a str,
    pub rh: VariableOr<'a, V>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PredicateOp {
    Eq,
    Gt,
    Ge,
    Lt,
    Le,
}

impl<'a, V> Predicate<'a, V> {
    fn lt(lh: &'a str, rh: VariableOr<'a, V>) -> Self {
        let op = PredicateOp::Lt;
        Predicate { op, lh, rh }
    }
}

/// TODO this entire type seems stupid. At the very least it should own the data so that a
/// Projection can borrow from it.
#[derive(Debug)]
pub struct Where<'a, V> {
    pub terms: Vec<Pattern<'a, V>>,
    pub preds: Vec<Predicate<'a, V>>,
    pub show: Vec<&'a str>,
}

impl<'a, V> Default for Where<'a, V> {
    fn default() -> Self {
        Where {
            terms: vec![],
            preds: vec![],
            show: vec![],
        }
    }
}

impl<'a, V> From<Vec<Pattern<'a, V>>> for Where<'a, V> {
    fn from(terms: Vec<Pattern<'a, V>>) -> Self {
        Where {
            terms,
            ..Where::default()
        }
    }
}

impl<'a, V> Where<'a, V> {
    pub fn such_that<P>(&mut self, preds: P) -> &mut Self
    where
        P: iter::IntoIterator<Item = Predicate<'a, V>>,
    {
        self.preds.extend(preds.into_iter());
        self
    }

    pub fn show<P>(&mut self, vars: P) -> &mut Self
    where
        P: iter::IntoIterator<Item = &'a str>,
    {
        self.show.extend(vars.into_iter());
        self
    }
}

pub fn query<'a, V>(terms: Vec<Pattern<'a, V>>) -> Where<'a, V> {
    terms.into()
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
