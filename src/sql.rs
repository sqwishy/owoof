use std::fmt::{self, Debug, Write};
use std::ops::{Deref, DerefMut};

use rusqlite::types::ToSql;

use crate::matter::{self, Concept, Constraint, ConstraintOp, DatomSet, Field, Projection};

pub trait ToSqlDebug: ToSql + Debug {}
impl<T: ToSql + Debug> ToSqlDebug for T {}

#[derive(Debug)]
pub struct GenericQuery<T> {
    string: String,
    params: Vec<T>,
}

pub type Query<'a> = GenericQuery<&'a dyn ToSqlDebug>;

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

impl<T> Write for GenericQuery<T> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.string.write_str(s)
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

impl GenericQuery<&dyn ToSqlDebug> {
    pub fn params(&self) -> &[&dyn ToSqlDebug] {
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

    pub fn push(&mut self, s: &str) -> &mut Self {
        self.push_str(s);
        self
    }
}

// struct<T> Prefixed {
//     q: GenericQuery<T>
// }
//
// impl <T> Prefixed<T> {
// }

pub fn projection_sql<'q, 'a: 'q, V>(
    projection: &'a Projection<'a, V>,
    query: &'q mut GenericQuery<&'a dyn ToSqlDebug>,
) -> fmt::Result
where
    V: Debug + ToSql,
{
    assert!(projection.datomsets() > 0);

    let mut query = query;

    for n in 0usize..projection.datomsets() {
        if n == 0 {
            query.push_str("  FROM datoms ")
        } else {
            query.push_str("     , datoms ")
        }
        // write the alias
        write!(query, "_dtm{}\n", n).unwrap();
    }

    for (n, constraint) in projection.constraints().iter().enumerate() {
        if n == 0 {
            query.push_str(" WHERE ")
        } else {
            query.push_str("   AND ")
        }

        let Constraint { lh, op, rh } = constraint;

        location_sql(lh, &mut query)?;

        match op {
            ConstraintOp::Eq => query.push_str(" = "),
            ConstraintOp::Ne => query.push_str(" != "),
            ConstraintOp::Gt => query.push_str(" > "),
            ConstraintOp::Ge => query.push_str(" >= "),
            ConstraintOp::Lt => query.push_str(" < "),
            ConstraintOp::Le => query.push_str(" <= "),
        };

        match rh {
            Concept::Location(l) => {
                location_sql(l, &mut query)?;
            }
            Concept::Entity(ent) => {
                use crate::Valuable;
                let bind_str = <crate::EntityName as Valuable>::bind_str(ent);
                query.push_str(bind_str);
                query.add_param(ent.to_sql_dbg() as &dyn ToSqlDebug);
            }
            Concept::Attribute(handle) => {
                query.push_str("(SELECT a.rowid FROM attributes a WHERE a.ident = ?)");
                query.add_param(handle as &dyn ToSqlDebug);
            }
            Concept::Value(v) => {
                query.push_str("?");
                query.add_param(v);
            }
        }

        query.push_str("\n");
    }

    Ok(())
}

/// Write the sql result column expression for the given Location
pub fn location_sql<W: Write>(l: &matter::Location, query: &mut W) -> fmt::Result {
    let column = match l.field {
        Field::Entity => "e",
        Field::Attribute => "a",
        Field::Value => "v",
    };
    write!(query, "_dtm{}.{}", l.datomset.0, column)
}

pub fn location(l: &matter::Location) -> String {
    let mut s = String::new();
    let _ = location_sql(l, &mut s);
    s
}

pub fn selection_sql<'q, 'a: 'q, V>(
    s: &'a matter::Selection<'a, V>,
    query: &'q mut GenericQuery<&'a dyn ToSqlDebug>,
) -> fmt::Result
where
    V: Debug + ToSql,
{
    let mut pre = std::iter::once("SELECT ").chain(std::iter::repeat("     , "));
    for l in s.columns() {
        query.push_str(pre.next().unwrap());
        let col_str = match l.field {
            Field::Entity => read_entity(&location(l)),
            Field::Attribute => read_attribute(&location(l)),
            // TODO read the T/affinity field also?
            Field::Value => read_v(
                // TODO is this cheating?
                &format!("_dtm{}.t", l.datomset.0),
                &location(l),
            ),
        };
        query.push_str(&col_str);
        query.push_str("\n");
    }

    projection_sql(s.projection, query)?;

    if s.limit > 0 {
        query.push_str(" LIMIT ?\n");
        query.add_param(&s.limit as &dyn ToSqlDebug);
    }

    Ok(())
}

pub(crate) fn read_v(t_col: &str, v_col: &str) -> String {
    format!(
        "CASE {t}
         WHEN {t_ent} THEN {rd_ent}
         WHEN {t_atr} THEN {rd_atr}
         ELSE {v} END",
        t = t_col,
        t_ent = crate::T_ENTITY,
        rd_ent = read_entity(v_col),
        t_atr = crate::T_ATTRIBUTE,
        rd_atr = read_attribute(v_col),
        v = v_col,
    )
}

pub(crate) const fn bind_entity() -> &'static str {
    "(SELECT rowid FROM entities   WHERE uuid = ?)"
}

pub(crate) fn read_entity(col: &str) -> String {
    format!("(SELECT uuid  FROM entities   WHERE rowid = {})", col).into()
}

pub(crate) const fn bind_attribute() -> &'static str {
    "(SELECT rowid FROM attributes WHERE ident = ?)"
}

pub(crate) fn read_attribute(col: &str) -> String {
    format!("(SELECT ident FROM attributes WHERE rowid = {})", col).into()
}

pub struct RowCursor<'a> {
    pub row: &'a rusqlite::Row<'a>,
    pub cursor: usize,
}

impl<'a> RowCursor<'a> {
    pub fn get<T: rusqlite::types::FromSql>(&mut self) -> rusqlite::Result<T> {
        let res = self.row.get::<_, T>(self.cursor);
        self.cursor += 1;
        res
    }
}

impl<'a> From<&'a rusqlite::Row<'a>> for RowCursor<'a> {
    fn from(row: &'a rusqlite::Row<'a>) -> Self {
        RowCursor {
            row,
            cursor: Default::default(),
        }
    }
}

#[test]
fn test_cursored_get() -> rusqlite::Result<()> {
    let conn = rusqlite::Connection::open_in_memory()?;
    let mut stmt = conn.prepare("select 1, 2, 3")?;
    let foo = stmt.query_row(rusqlite::params![], |row| {
        let mut c = RowCursor::from(row);
        Ok((c.get()?, c.get()?, c.get()?))
    })?;
    assert_eq!(foo, (1, 2, 3));
    Ok(())
}
