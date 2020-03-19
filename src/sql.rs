use std::fmt::{self, Debug, Write};
use std::ops::{Deref, DerefMut};

use rusqlite::types::ToSql;

use crate::matter::{self, Constraint, DatomSet, Projection};

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

// todo; implement From for all ToString?

impl<T> From<String> for GenericQuery<T> {
    fn from(string: String) -> Self {
        GenericQuery {
            string,
            ..GenericQuery::default()
        }
    }
}

impl<'a, T> From<&'a str> for GenericQuery<T> {
    fn from(s: &'a str) -> Self {
        GenericQuery {
            string: s.to_string(),
            ..GenericQuery::default()
        }
    }
}

impl GenericQuery<&dyn ToSql> {
    pub fn params(&self) -> &[&dyn ToSql] {
        self.params.as_slice()
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

/// todo this only supports owned queries, the params can't borrow from Projection
pub fn projection_sql<'g, 'q, V>(
    projection: &'g Projection<V>,
    query: &'q mut GenericQuery<&'g dyn ToSql>,
) -> fmt::Result
where
    'g: 'q,
    V: Debug + ToSql,
{
    assert!(projection.datomsets() > 0);

    let mut query = query;

    for n in 0usize..projection.datomsets() {
        if n == 0 {
            query.push_str("from datoms ")
        } else {
            query.push_str("   , datoms ")
        }
        // write the alias
        write!(query, "_dtm{}\n", n).unwrap();
    }

    for (n, constraint) in projection.constraints().iter().enumerate() {
        if n == 0 {
            query.push_str("where ")
        } else {
            query.push_str("  and ")
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
                query.push_str("(select a.rowid from attributes a where a.ident = ?)");
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

pub fn location_sql<T>(l: &matter::Location, query: &mut GenericQuery<T>) -> fmt::Result {
    let column = match l.field {
        matter::Field::Entity => "e",
        matter::Field::Attribute => "a",
        matter::Field::Value => "v",
    };
    write!(query, "_dtm{}.{}", l.datomset.0, column)
}

// fn datomset_alias(d: DatomSet) -> String {
//     format!("_d{}", d.0)
// }
