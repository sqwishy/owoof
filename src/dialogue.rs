//! what are you doing here?

use crate::EntityHandle;

#[derive(Debug)]
pub enum Face<'a> {
    Attribute(&'a str),
}

#[derive(Debug)]
pub struct Shape<'a> {
    faces: Vec<Face<'a>>,
}

#[derive(Debug)]
pub struct Where<'a> {
    terms: Vec<Pattern<'a, Box<dyn std::any::Any>>>,
}

/// a datom with ? bindings
/// [parameter|entity, attribute, parameter|value]
#[derive(Debug)]
pub struct Pattern<'a, T> {
    entity: VariableOr<'a, EntityHandle>,
    attribute: VariableOr<'a, &'a str>,
    value: VariableOr<'a, T>,
}

// impl<'a, T> Pattern<'a, T> {
// }

#[derive(Debug)]
pub enum VariableOr<'a, T> {
    Variable(&'a str),
    Value(T),
}

pub trait Dialogue {
    fn query(&mut self) -> anyhow::Result<()>;
}

#[cfg(test)]
mod tests {
    use super::VariableOr::*;
    use super::*;
    use anyhow::Result;

    #[test]
    fn it_works() -> Result<()> {
        //! where (?p :person/name "Spongebob")
        let w = Where {
            terms: vec![
                Pattern {
                    entity: Variable("s"),
                    attribute: Value("person/name"),
                    value: Value(Box::new("Spongebob")),
                },
                Pattern {
                    entity: Variable("s"),
                    attribute: Value("person/pants"),
                    value: Variable("p"),
                },
                Pattern {
                    entity: Variable("p"),
                    attribute: Value("geometry/shape"),
                    value: Variable("h"),
                },
            ],
        };

        w.compile();

        // let f = Shape {
        //     faces: vec![
        //         Face::Attribute("")
        //     ],
        // }

        Ok(())
    }
}
