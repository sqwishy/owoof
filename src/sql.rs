#![allow(clippy::write_with_newline)]

use std::fmt::{self, Debug, Write};
use std::ops::{Deref, DerefMut};

use rusqlite::types::ToSql;

use crate::matter::{self, Concept, Constraint, ConstraintOp, DatomSet, Field, Projection};

pub trait ToSqlDebug: ToSql + Debug {}
impl<T: ToSql + Debug> ToSqlDebug for T {}

pub trait AddToQuery<P> {
    fn add_to_query<W>(&self, _: &mut W)
    where
        W: QueryWriter<P>;
}

/// fuck
impl<T, P> AddToQuery<P> for &'_ T
where
    T: AddToQuery<P>,
{
    fn add_to_query<W>(&self, query: &mut W)
    where
        W: QueryWriter<P>,
    {
        (*self).add_to_query(query)
    }
}

impl<'a, P, T> AddToQuery<P> for matter::AttributeMap<'a, T> {
    fn add_to_query<W>(&self, query: &mut W)
    where
        W: QueryWriter<P>,
    {
        for &(_, datomset) in &self.map {
            query
                .nl()
                .push_sql(&datomset_t(datomset))
                .push_sql(", ")
                .push_sql(&read_value(&datomset_t(datomset), &datomset_v(datomset)));
        }
    }
}

impl<P> AddToQuery<P> for matter::Location {
    fn add_to_query<W>(&self, query: &mut W)
    where
        W: QueryWriter<P>,
    {
        let &matter::Location { field, datomset } = self;

        query.nl();
        match field {
            Field::Entity => query.push_sql(&read_entity(&datomset_e(datomset))),
            Field::Attribute => query.push_sql(&read_attribute(&datomset_a(datomset))),
            Field::Value => query
                .push_sql(&datomset_t(datomset))
                .push_sql(", ")
                .push_sql(&read_value(&datomset_t(datomset), &datomset_v(datomset))),
        };
    }
}

impl<T, P> AddToQuery<P> for &[T]
where
    T: AddToQuery<P>,
{
    fn add_to_query<W>(&self, query: &mut W)
    where
        W: QueryWriter<P>,
    {
        for t in self.iter() {
            t.add_to_query(query)
        }
    }
}

macro_rules! add_tuple_to_query {
    ( $( $t:ident )+ ) => {
        impl<_P, $($t: AddToQuery<_P>),+> AddToQuery<_P> for ($($t,)+)
        {
            fn add_to_query<W>(&self, query: &mut W)
            where
                W: QueryWriter<_P>
            {
                #[allow(non_snake_case)]
                let ($($t,)+) = self;
                $( $t.add_to_query(query); )+
            }
        }
    };
}

add_tuple_to_query!(A);
add_tuple_to_query!(A B);
add_tuple_to_query!(A B C);
add_tuple_to_query!(A B C D);
add_tuple_to_query!(A B C D E);
add_tuple_to_query!(A B C D E F);
add_tuple_to_query!(A B C D E F G);
add_tuple_to_query!(A B C D E F G H);
add_tuple_to_query!(A B C D E F G H I);

/// P is a generic for the query parameter type
pub trait QueryWriter<P> {
    fn add_param(&mut self, p: P) -> &mut Self;

    fn push_sql(&mut self, s: &str) -> &mut Self;

    fn nl(&mut self) -> &mut Self;

    fn with_indent<I>(&mut self, indent: I) -> IndentedQueryWriter<&mut Self, I>
    where
        Self: Sized,
        I: Iterator<Item = &'static str>,
    {
        let writer = self;
        IndentedQueryWriter { writer, indent }
    }
}

/// what the fucking fuck
impl<W, P> QueryWriter<P> for &'_ mut W
where
    W: QueryWriter<P>,
{
    fn add_param(&mut self, p: P) -> &mut Self {
        (*self).add_param(p);
        self
    }

    fn push_sql(&mut self, s: &str) -> &mut Self {
        (*self).push_sql(s);
        self
    }

    fn nl(&mut self) -> &mut Self {
        (*self).nl();
        self
    }
}

impl<P> QueryWriter<P> for GenericQuery<P> {
    fn add_param(&mut self, p: P) -> &mut Self {
        self.params.push(p);
        self
    }

    fn push_sql(&mut self, s: &str) -> &mut Self {
        self.string.push_str(s);
        self
    }

    fn nl(&mut self) -> &mut Self {
        self.string.push('\n');
        self
    }
}

#[derive(Debug)]
pub struct IndentedQueryWriter<W, I> {
    writer: W,
    indent: I,
}

impl<W, I, P> QueryWriter<P> for IndentedQueryWriter<W, I>
where
    W: QueryWriter<P>,
    /* I can't use a generic lifetime here for some reason?
     * Maybe because https://github.com/rust-lang/rust/issues/49601
     * I have no idea ... */
    I: Iterator<Item = &'static str>,
{
    fn add_param(&mut self, p: P) -> &mut Self {
        self.writer.add_param(p);
        self
    }

    fn push_sql(&mut self, s: &str) -> &mut Self {
        self.writer.push_sql(s);
        self
    }

    fn nl(&mut self) -> &mut Self {
        self.writer.nl();
        if let Some(i) = self.indent.next() {
            self.writer.push_sql(i);
        }
        self
    }
}

#[derive(Debug)]
pub struct GenericQuery<P> {
    string: String,
    /// TODO limit this somehow because sqlite max params is like 999
    params: Vec<P>,
    // params_exceeded: bool,
}

pub type Query<'a> = GenericQuery<&'a dyn ToSqlDebug>;

impl<P> Default for GenericQuery<P> {
    fn default() -> Self {
        GenericQuery {
            string: String::new(),
            params: Vec::new(),
        }
    }
}

impl<P> fmt::Display for GenericQuery<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.string, f)
    }
}

impl<P> Deref for GenericQuery<P> {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.string
    }
}

impl<P> DerefMut for GenericQuery<P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.string
    }
}

impl<P> Write for GenericQuery<P> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.string.write_str(s)
    }
}

impl<P> From<String> for GenericQuery<P> {
    fn from(string: String) -> Self {
        GenericQuery {
            string,
            ..GenericQuery::default()
        }
    }
}

impl<'a, P> From<&'a str> for GenericQuery<P> {
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

impl<P> GenericQuery<P> {
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
            query.push_str("  FROM datoms ")
        } else {
            query.push_str("     , datoms ")
        }
        // write the alias
        write!(query, "datoms{}\n", n)?;
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
    format!("datoms{}.e", datomset.0)
}

pub fn datomset_a(datomset: DatomSet) -> String {
    format!("datoms{}.a", datomset.0)
}

pub fn datomset_t(datomset: DatomSet) -> String {
    format!("datoms{}.t", datomset.0)
}

pub fn datomset_v(datomset: DatomSet) -> String {
    format!("datoms{}.v", datomset.0)
}

/// TODO XXX FIXME the V and S types are supposed to be related somehow?
///     S: AddToQuery<V> or S: AddToQuery<&V> ???
pub fn selection_sql<'q, 'a: 'q, 'p, V, S>(
    // TODO use different lifetimes for 'a?
    s: &'a matter::Selection<'a, 'p, V, S>,
    query: &'q mut GenericQuery<&'a dyn ToSqlDebug>,
) -> fmt::Result
where
    V: ToSqlDebug,
    S: AddToQuery<&'a dyn ToSqlDebug>,
{
    use std::iter::{once, repeat};

    let mut select_writer = query.with_indent(once("SELECT ").chain(repeat("     , ")));
    s.columns.add_to_query(&mut select_writer);
    query.nl();

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
        "CASE {t} WHEN {t_ent} THEN {rd_ent} ELSE {v} END",
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

    pub fn skip(&mut self, n: usize) -> &mut Self {
        self.cursor += n;
        self
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

pub trait ReadFromRow {
    type Out;

    fn read_from_row(&self, _: &mut RowCursor) -> rusqlite::Result<Self::Out>;
}

/// motherfuckingchrist
impl<T> ReadFromRow for &'_ T
where
    T: ReadFromRow,
{
    type Out = <T as ReadFromRow>::Out;

    fn read_from_row(&self, c: &mut RowCursor) -> rusqlite::Result<Self::Out> {
        (*self).read_from_row(c)
    }
}

impl<'a, V> ReadFromRow for matter::AttributeMap<'a, V>
where
    V: crate::FromAffinityValue,
{
    type Out = std::collections::HashMap<&'a crate::AttributeName<'a>, V>;

    fn read_from_row(&self, c: &mut RowCursor) -> rusqlite::Result<Self::Out> {
        self.map
            .iter()
            .map(|&(attr, _)| -> rusqlite::Result<(_, _)> {
                Ok((attr, V::read_affinity_value(c)?))
            })
            .collect()
    }
}

impl ReadFromRow for matter::Location {
    /// TODO the whole point of the phantom data in AttributeMap is to disambiguate this
    /// type ... so that we can share the same value types in both directions of the
    /// database.
    ///
    /// We don't have that here, so we need to modify the trait so that users can
    /// specify what kind of container things can be dumped into as long as it
    /// implements FromAffinityValue.
    type Out = crate::Value;

    fn read_from_row(&self, c: &mut RowCursor) -> rusqlite::Result<Self::Out> {
        use crate::types::{Affinity, FromAffinityValue, Value};

        match self.field {
            Field::Entity => Value::read_with_affinity(Affinity::Entity, c.get_raw()),
            Field::Attribute => Value::read_with_affinity(Affinity::Attribute, c.get_raw()),
            Field::Value => Value::read_affinity_value(c),
        }
    }
}

impl<T> ReadFromRow for &[T]
where
    T: ReadFromRow,
{
    type Out = Vec<<T as ReadFromRow>::Out>;

    fn read_from_row(&self, c: &mut RowCursor) -> rusqlite::Result<Self::Out> {
        self.iter().map(|t| t.read_from_row(c)).collect()
    }
}
macro_rules! read_tuple_from_row {
    ( $( $t:ident )+ ) => {
        impl<$($t: ReadFromRow),+> ReadFromRow for ($($t,)+)
        {
            type Out = ($(<$t as ReadFromRow>::Out,)+);

            fn read_from_row(&self, c: &mut RowCursor) -> rusqlite::Result<Self::Out> {
                #[allow(non_snake_case)]
                let ($($t,)+) = self;
                let boomer = ($( $t.read_from_row(c)?, )+);
                Ok(boomer)
            }
        }
    };
}

read_tuple_from_row!(A);
read_tuple_from_row!(A B);
read_tuple_from_row!(A B C);
read_tuple_from_row!(A B C D);
read_tuple_from_row!(A B C D E);
read_tuple_from_row!(A B C D E F);
read_tuple_from_row!(A B C D E F G);
read_tuple_from_row!(A B C D E F G H);
read_tuple_from_row!(A B C D E F G H I);

/* ??????????? */

#[derive(Debug)]
pub struct CrazyFuckingMemes<T, F>(T, F);

impl<T, F> CrazyFuckingMemes<T, F> {
    pub(crate) fn new(t: T, f: F) -> Self {
        CrazyFuckingMemes(t, f)
    }
}

impl<T, F> ReadFromRow for CrazyFuckingMemes<T, F>
where
    F: ReadFromRow,
{
    type Out = <F as ReadFromRow>::Out;

    fn read_from_row(&self, c: &mut RowCursor) -> rusqlite::Result<Self::Out> {
        self.1.read_from_row(c)
    }
}

impl<'a, P, T, Ph> AddToQuery<P> for CrazyFuckingMemes<T, Ph>
where
    T: AddToQuery<P>,
{
    fn add_to_query<W>(&self, query: &mut W)
    where
        W: QueryWriter<P>,
    {
        self.0.add_to_query(query)
    }
}
