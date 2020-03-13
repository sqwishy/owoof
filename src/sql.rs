use std::fmt::{self, Debug, Write};
use std::ops::{Deref, DerefMut};

use rusqlite::types::ToSql;

use crate::matter::{self, Constraint, DatomSet, Glyph};

#[derive(Debug)]
pub struct GenericQuery<T> {
    string: String,
    params: Vec<T>,
}

// pub type Query = GenericQuery<Box<dyn ToSql>>;

impl<T> Default for GenericQuery<T> {
    fn default() -> Self {
        GenericQuery {
            string: String::new(),
            params: Vec::new(),
        }
    }
}

impl<T> fmt::Display for GenericQuery<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.string, f)
    }
}

impl<T> Deref for GenericQuery<T> {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.string
    }
}

impl<T> DerefMut for GenericQuery<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.string
    }
}

impl<T> GenericQuery<T> {
    // pub fn aliased_datomset(&mut self, n: DatomSet) -> fmt::Result {
    //     write!(self, "_d{}\n", n.0)
    // }

    pub fn add_param(&mut self, p: T) {
        self.params.push(p)
    }
}

/// todo this only supports owned queries, the params can't borrow from Glyph
pub fn glyph_sql<'g, 'q, V>(
    glyph: &'g Glyph<V>,
    query: &'q mut GenericQuery<&'g dyn ToSql>,
) -> fmt::Result
where
    'g: 'q,
    V: Debug + ToSql,
{
    assert!(glyph.datomsets() > 0);

    let mut query = query;

    for n in 0usize..glyph.datomsets() {
        if n == 0 {
            query.push_str("  select datoms ")
        } else {
            query.push_str("       , datoms ")
        }
        // write the alias
        write!(query, "_datoms{}\n", n).unwrap();
    }

    for (n, constraint) in glyph.constraints().iter().enumerate() {
        if n == 0 {
            query.push_str("   where ")
        } else {
            query.push_str("     and ")
        }

        let Constraint(on, to) = constraint;
        match to {
            matter::Value::Location(l) => {
                location_sql(on, &mut query)?;
                query.push_str(" = ");
                location_sql(l, &mut query)?;
            }
            matter::Value::Attribute(handle) => {
                location_sql(on, &mut query)?;
                query.push_str(" in ");
                query.push_str("(select id from attributes where ident = ?)");
                query.add_param(handle as &dyn ToSql);
            }
            matter::Value::EqValue(v) => {
                location_sql(on, &mut query)?;
                query.push_str(" = ?");
                query.add_param(v);
            }
            _ => todo!("sql: constrain {:?}", constraint),
        }

        query.push_str("\n");
    }

    Ok(())
}

fn location_sql<T>(l: &matter::Location, query: &mut GenericQuery<T>) -> fmt::Result {
    let column = match l.field {
        matter::Field::Entity => "e",
        matter::Field::Attribute => "a",
        matter::Field::Value => "v",
    };
    write!(query, "_datoms{}.{}", l.datomset.0, column)
}

// fn datomset_alias(d: DatomSet) -> String {
//     format!("_d{}", d.0)
// }
