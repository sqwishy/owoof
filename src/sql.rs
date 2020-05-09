#![allow(clippy::write_with_newline)]

use std::fmt::{self, Debug, Write};
use std::ops::{Deref, DerefMut};

use rusqlite::types::ToSql;

use crate::matter::{self, Concept, Constraint, ConstraintOp, DatomSet, Field, Projection};

pub trait ToSqlDebug: ToSql + Debug {}
impl<T: ToSql + Debug> ToSqlDebug for T {}

// pub trait SqlQueryable {
//     fn append_query<'s>(&'s self, _: &'s mut Query);
// }
//
// pub trait SqlStringable {
//     fn append_sql<W: Write>(&self, _: W) -> fmt::Result;
// }
//
// impl<T: SqlStringable> SqlQueryable for T {
//     fn append_query<'s>(&'s self, query: &'s mut Query) {
//         let _ = SqlStringable::append_sql(self, &mut query);
//     }
// }

// /// Gives us ToString for free ...
// impl<T: SqlStringable> fmt::Display for T {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         self.append_sql(f)
//     }
// }

#[derive(Debug)]
pub struct GenericQuery<T> {
    string: String,
    /// TODO limit this somehow because sqlite max params is like 999
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
    pub fn add_param(&mut self, p: T) {
        self.params.push(p)
    }

    pub fn push(&mut self, s: &str) -> &mut Self {
        self.push_str(s);
        self
    }
}

// TODO XXX FIXME require V to be assertable so we can get an affinity from it?
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
            query.push_str("  FROM all_datoms ")
        } else {
            query.push_str("     , all_datoms ")
        }
        // write the alias
        write!(query, "_dtm{}\n", n)?;
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
                query.push_str(bind_entity());
                query.add_param(ent as &dyn ToSqlDebug);
            }
            Concept::Attribute(handle) => {
                query.push_str(bind_attribute());
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

// impl SqlStringable for matter::Location {
//     fn append_sql<W: Write>(&self, w: W) -> fmt::Result {
//         let column = match self.field {
//             Field::Entity => "e",
//             Field::Attribute => "a",
//             Field::Value => "v",
//         };
//         write!(w, "_dtm{}.{}", self.datomset.0, column)
//     }
// }

pub fn location_sql<W: Write>(l: &matter::Location, w: &mut W) -> fmt::Result {
    write!(w, "{}", location(l))
}

pub fn location(l: &matter::Location) -> String {
    match l.field {
        Field::Entity => datomset_e(l.datomset),
        Field::Attribute => datomset_a(l.datomset),
        Field::Value => datomset_v(l.datomset),
    }
}

pub fn datomset_e(datomset: DatomSet) -> String {
    format!("_dtm{}.e", datomset.0)
}

pub fn datomset_a(datomset: DatomSet) -> String {
    format!("_dtm{}.a", datomset.0)
}

pub fn datomset_t(datomset: DatomSet) -> String {
    format!("_dtm{}.t", datomset.0)
}

pub fn datomset_v(datomset: DatomSet) -> String {
    format!("_dtm{}.v", datomset.0)
}

pub fn selection_sql<'q, 'a: 'q, V>(
    // TODO use different lifetimes for 'a?
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
            Field::Attribute => todo!("read_entity() instead?"), //read_attribute(&location(l)),
            Field::Value => read_value(&datomset_t(l.datomset), &datomset_v(l.datomset)),
        };
        query.push_str(&col_str);
        query.push_str("\n");
    }

    for (pre, col_fn) in pre.zip(s.attrs.iter().map(|a| a.result_columns()).flatten()) {
        query.push_str(pre);
        col_fn(query)?;
        query.deref_mut().push('\n');
    }

    projection_sql(s.projection, query)?;

    order_by_sql(&s.order_by, query)?;

    limit_sql(&s.limit, query)?;

    Ok(())
}

pub fn order_by_sql<W: Write>(
    order_by: &[(matter::Location, matter::Ordering)],
    w: &mut W,
) -> fmt::Result {
    let pre = std::iter::once("ORDER BY ").chain(std::iter::repeat(", "));
    for (pre, (term_location, ordering)) in pre.zip(order_by.iter()) {
        write!(
            w,
            "{pre}{col}{ord}",
            pre = pre,
            col = location(term_location),
            ord = match ordering {
                matter::Ordering::Asc => " ASC",
                matter::Ordering::Desc => " DESC",
            }
        )?;
    }
    if !order_by.is_empty() {
        write!(w, "\n")?
    }
    Ok(())
}

pub fn limit_sql<'a>(limit: &'a i64, query: &mut GenericQuery<&'a dyn ToSqlDebug>) -> fmt::Result {
    if *limit > 0 {
        query.push_str(" LIMIT ?\n");
        query.add_param(limit as &dyn ToSqlDebug);
    }
    Ok(())
}

pub(crate) fn read_value(t_col: &str, v_col: &str) -> String {
    format!(
        "CASE {t}
         WHEN {t_ent} THEN {rd_ent}
         ELSE {v} END",
        t = t_col,
        t_ent = crate::T_ENTITY,
        rd_ent = read_entity(v_col),
        v = v_col,
    )
}

pub(crate) const fn bind_entity() -> &'static str {
    "(SELECT rowid FROM entities   WHERE uuid = ?)"
}

pub(crate) fn read_entity(col: &str) -> String {
    format!("(SELECT uuid  FROM entities   WHERE rowid = {})", col)
}

pub(crate) const fn bind_attribute() -> &'static str {
    "(SELECT rowid FROM attributes WHERE ident = ?)"
}

pub(crate) fn read_attribute(col: &str) -> String {
    format!("(SELECT ident FROM attributes WHERE rowid = {})", col)
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

    pub fn get_raw(&mut self) -> rusqlite::types::ValueRef {
        let raw = self.row.get_raw(self.cursor);
        self.cursor += 1;
        raw
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
