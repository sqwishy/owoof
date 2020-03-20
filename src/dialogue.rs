//! what are you doing here?
//!
//! TODO move this into matter? this is projection stuff?

use std::fmt;

use crate::{AttributeHandle, EntityHandle};

#[derive(Debug)]
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
    pub entity: VariableOr<'a, &'a EntityHandle>,
    pub attribute: VariableOr<'a, &'a AttributeHandle>,
    pub value: VariableOr<'a, V>,
}

#[derive(Debug, PartialEq)]
pub enum Logic<T> {
    // Neg(T),
    Or(T),
    And(T),
}

#[derive(Debug)]
pub struct Where<'a, V> {
    pub terms: Vec<Pattern<'a, V>>,
}

#[macro_export]
macro_rules! pat {
    (?$e:ident $a:tt ?$v:tt) => {{
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
        $crate::dialogue::VariableOr::Value($v)
    };
}

#[cfg(test)]
mod tests {
    use super::VariableOr::*;
    use super::*;
    use anyhow::Result;

    fn bikini_bottom() -> rusqlite::Result<rusqlite::Connection> {
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

        tx.commit()?;
        Ok(conn)
    }

    /// where [(s :person/name "Spongebob"),
    ///        (s :person/pants p),
    ///        (p :object/geometry g)]
    /// shape g
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
            select "_p_g"."v"
              from datoms "_s"
                 , datoms "_s_p"
                 , datoms "_p_g"

                -- s :person/name "Spongebob"
              where "_s".a = ?
                and "_s".v = ?

                -- s :person/pants p
                and "_s_p".e = "_s".e
                and "_s_p".a = ?

                -- p :object/geometry g
                and "_p_g".e = "_s_p".v
                and "_p_g".a = ?
            "#;
            let p = rusqlite::params![person_name, "Spongebob", person_pants, object_geometry,];

            let mut stmt = tx.prepare(sql)?;
            let rows = stmt.query_map(p, |row| row.get(0))?;
            rows.collect::<rusqlite::Result<Vec<String>>>()?
        };

        assert_eq!(wow, vec!["square"]);

        Ok(())
    }

    /// ?p person/name ?n
    /// ?p person/favorite_color "blue"
    /// |> ?n
    #[test]
    fn simple_joining() -> Result<()> {
        use crate::{tests::test_conn, Datom, Session};

        let mut conn = test_conn()?;
        let tx = conn.transaction()?;
        let db = Session::new(&tx);

        let person_name = db.new_attribute("person/name")?;
        let favorite_color = db.new_attribute("person/favorite_color")?;

        let alice = db.new_entity()?;
        let bob = db.new_entity()?;
        let carol = db.new_entity()?;

        db.assert(&Datom::new(alice, "person/name", "Alice"))?;
        db.assert(&Datom::new(bob, "person/name", "Bob"))?;
        db.assert(&Datom::new(carol, "person/name", "Carol"))?;

        db.assert(&Datom::new(alice, "person/favorite_color", "blue"))?;
        db.assert(&Datom::new(bob, "person/favorite_color", "red"))?;
        db.assert(&Datom::new(carol, "person/favorite_color", "blue"))?;

        let blue_names: Vec<String> = {
            let sql = r#"
            select "_p_n"."v"
              from datoms "_p_n"
                 , datoms "_p_blue"
             where "_p_n"."e" = "_p_blue"."e"
               and "_p_n"."a" = ?
               and "_p_blue"."a" = ?
               and "_p_blue"."v" = ?
            "#;

            let p = rusqlite::params![person_name, favorite_color, "blue"];
            let mut stmt = tx.prepare(sql)?;
            let rows = stmt.query_map(p, |row| row.get(0))?;
            rows.collect::<rusqlite::Result<_>>()?
        };

        assert_eq!(
            blue_names.into_iter().map(|name| name).collect::<Vec<_>>(),
            vec!["Alice", "Carol"]
        );

        Ok(())
    }

    /// where [(?r review/book ?b),
    ///        (?r review/comment ?c),
    ///        (?r review/author ?a),
    /// TODO   (?b book/author ?a),
    ///        (?a person/name ?n)]
    /// ?n ?c
    ///
    /// b -> a -> n
    /// ^    ^
    /// +--- r -> c
    #[test]
    fn book_review() -> Result<()> {
        use crate::{tests::test_conn, Datom, Session};

        let mut conn = test_conn()?;
        let tx = conn.transaction()?;
        let db = Session::new(&tx);

        let book_author = db.new_attribute("book/author")?;
        let person_name = db.new_attribute("person/name")?;
        let review_book = db.new_attribute("review/book")?;
        let review_author = db.new_attribute("review/author")?;
        let review_comment = db.new_attribute("review/comment")?;

        let book = db.new_entity()?;
        let alice = db.new_entity()?;
        let bob = db.new_entity()?;

        db.assert(&Datom::new(alice, "person/name", "Alice"))?;
        db.assert(&Datom::new(bob, "person/name", "Bob"))?;

        db.assert(&Datom::new(book, "book/author", bob))?;

        let alice_review = db.new_entity()?;
        db.assert(&Datom::new(alice_review, "review/book", book))?;
        db.assert(&Datom::new(alice_review, "review/author", alice))?;
        db.assert(&Datom::new(alice_review, "review/comment", "Terrible"))?;

        let bob_review = db.new_entity()?;
        db.assert(&Datom::new(bob_review, "review/book", book))?;
        db.assert(&Datom::new(bob_review, "review/author", bob))?;
        db.assert(&Datom::new(bob_review, "review/comment", "My book!"))?;

        let wat: Vec<(String, String)> = {
            let sql = r#"
          select "_a_n".v, "_r_c".v
            from datoms "_r_b"
               , datoms "_r_c"
               , datoms "_r_a"
            -- , datoms "_b_a"
               , datoms "_a_n"

                 -- (?r review/book ?b)
           where "_r_b".a = ?
                 -- (?r review/comment ?c)
             and "_r_c".a = ?
             and "_r_c".e = "_r_b".e
                 -- (?r review/author ?a)
             and "_r_a".a = ?
             and "_r_a".e = "_r_b".e
                 -- (?a person/name ?n)
             and "_a_n".a = ?
             and "_a_n".e = "_r_a".v

        order by "_a_n".v ASC
            "#;

            let p = rusqlite::params![review_book, review_comment, review_author, person_name];

            let mut stmt = tx.prepare(sql)?;
            let rows = stmt.query_map(p, |row| Ok((row.get(0)?, row.get(1)?)))?;
            rows.collect::<rusqlite::Result<_>>()?
        };

        eprintln!("{:#?}", wat);
        assert_eq!(
            wat,
            vec![
                ("Alice".to_owned(), "Terrible".to_owned()),
                ("Bob".to_owned(), "My book!".to_owned())
            ]
        );

        Ok(())
    }

    /// What is the geometrical shape of spongebob's pants?
    ///
    /// where [(?s :person/name "Spongebob"),
    ///        (?s :person/pants ?p),
    ///        (?p :object/geometry ?g)]
    /// shape ?g
    #[test]
    fn it_works() -> Result<()> {
        use crate::{matter::Projection, sql};

        let w = Where {
            terms: vec![
                Pattern {
                    entity: Variable("s"),
                    attribute: Value("person/name"),
                    value: Value("Spongebob"),
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

        let mut g = crate::matter::Projection::default();

        for term in w.terms.iter() {
            g.add_pattern(term);
        }

        eprintln!("{:#?}", g);

        let mut query = sql::GenericQuery::from("select ");
        sql::location_sql(g.variables().get("g").unwrap(), &mut query)?;
        query.push_str("\n");

        sql::projection_sql(&g, &mut query).unwrap();
        eprintln!("query:\n{}", query);

        let mut conn = bikini_bottom()?;
        let tx = conn.transaction()?;
        let mut stmt = tx.prepare(query.as_str())?;
        let rows = stmt.query_map(query.params(), |row| row.get(0))?;
        let pants = rows.collect::<rusqlite::Result<Vec<String>>>()?;
        assert_eq!(pants, vec!["square"]);

        Ok(())
    }
}
