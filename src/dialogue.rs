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

impl<'a> Where<'a> {
    fn compile() {
        /// with "s" as (select e from datoms where a = ? AND v = ?),
        ///      "s" as (select e, a, v from datoms where a = ? AND v = ?)
        todo!()
    }
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
    fn can_query() -> Result<()> {
        use crate::{tests::test_conn, Datom, Session};

        let mut conn = test_conn()?;
        let tx = conn.transaction()?;
        let db = Session::new(&tx);

        let person_name = db.new_attribute("person/name")?;
        let person_pants = db.new_attribute("person/pants")?;
        let object_geometry = db.new_attribute("object/geometry")?;

        let spongebob = db.new_entity()?;
        let pants = db.new_entity()?;
        db.assert(&Datom::new(spongebob, "person/name", "Spongebob"))?;
        db.assert(&Datom::new(spongebob, "person/pants", pants))?;
        db.assert(&Datom::new(pants, "object/geometry", "square"))?;

        let d = db.all_datoms::<rusqlite::types::Value>()?;
        eprintln!("everything {:#?}", d);

        let wow: Vec<String> = {
            let sql = r#"
            with "_s" as (select e from datoms where a = ? AND v = ?)
               , "_p" as (select v from datoms where e in "_s" AND a = ?)
               , "_g" as (select v from datoms where e in "_p" AND a = ?)
          select "v" from "_g";
            "#;
            let p = rusqlite::params![
                // "person/name",
                person_name,
                "Spongebob",
                // "person/pants",
                person_pants,
                // "object/geometry"
                object_geometry,
            ];

            let mut stmt = tx.prepare(sql)?;
            let wat = stmt.query_map(p, |row| row.get(0))?;
            wat.collect::<rusqlite::Result<Vec<String>>>()?
        };

        eprintln!("wow {:?}", wow);
        assert_eq!(wow, vec!["square"]);

        Ok(())
    }

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
                    // or pants/shape?
                    attribute: Value("object/geometry"),
                    value: Variable("g"),
                },
            ],
        };

        // w.compile();

        // let f = Shape {
        //     faces: vec![
        //         Face::Attribute("")
        //     ],
        // }

        Ok(())
    }
}
