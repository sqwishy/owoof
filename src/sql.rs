#![allow(clippy::write_with_newline)]

use std::fmt::{self, Debug, Display, Write};
use std::iter::{once, repeat};

use rusqlite::types::{FromSql, ToSql};

// use crate::projection::{self, Concept, Constraint, ConstraintOp, DatomSet, Field, Projection};
// use crate::types::HasAffinity;

use crate::network::{Field, Ordering, Triples, TriplesField};
use crate::select::Select;
use crate::soup::Encoded;
use crate::types::TypeTag;

/// A string buffer for a SQL query with a list of a values that should be passed along
/// as query parameters to [rusqlite] when querying.
#[derive(Debug)]
pub struct Query<P> {
    string: String,
    /// TODO limit this somehow because sqlite max params is like 32766 or sqlite3_limit()
    params: Vec<P>,
    // params_exceeded: bool,
}

impl<P> Query<P> {
    pub fn as_str(&self) -> &str {
        &self.string
    }

    pub fn params(&self) -> &[P] {
        self.params.as_slice()
    }
}

impl<P> Default for Query<P> {
    fn default() -> Self {
        Query { string: String::new(), params: Vec::new() }
    }
}

impl<P> Display for Query<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.string, f)
    }
}

impl<P> Write for Query<P> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.string.write_str(s)
    }
}

impl<P> QueryWriter<P> for Query<P> {
    fn push_param(&mut self, p: P) -> &mut Self {
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

/// what the fucking fuck
impl<W, P> QueryWriter<P> for &'_ mut W
where
    W: QueryWriter<P>,
{
    fn push_param(&mut self, p: P) -> &mut Self {
        (*self).push_param(p);
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

/// Allows query string building over [`Query`] and [`IndentedQueryWriter`]
///
/// P is the query parameter type.
pub trait QueryWriter<P>: Write {
    fn push_param(&mut self, p: P) -> &mut Self;

    fn push_sql(&mut self, s: &str) -> &mut Self;

    fn nl(&mut self) -> &mut Self;

    fn with_indent<I>(&mut self, indent: I) -> IndentedQueryWriter<&mut Self, I>
    where
        Self: Sized,
        I: Iterator<Item = &'static str>,
    {
        IndentedQueryWriter { writer: self, indent }
    }

    fn push<T>(&mut self, ptq: T) -> &mut Self
    where
        Self: Sized,
        T: PushToQuery<P>,
    {
        ptq.push_to_query(self);
        self
    }
}

#[derive(Debug)]
pub struct IndentedQueryWriter<W, I> {
    writer: W,
    indent: I,
}

impl<W, I> IndentedQueryWriter<W, I> {
    fn dedent(self) -> W {
        self.writer
    }
}

impl<W, I, P> QueryWriter<P> for IndentedQueryWriter<W, I>
where
    W: QueryWriter<P>,
    /* I can't use a generic lifetime here for some reason?
     * Maybe because https://github.com/rust-lang/rust/issues/49601
     * I have no idea ... */
    I: Iterator<Item = &'static str>,
{
    fn push_param(&mut self, p: P) -> &mut Self {
        self.writer.push_param(p);
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

impl<W, I> Write for IndentedQueryWriter<W, I>
where
    W: Write,
{
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.writer.write_str(s)
    }
}

/// Implemented by things that can write themselves into a SQL query
pub trait PushToQuery<P> {
    fn push_to_query<W>(&self, _: &mut W)
    where
        W: QueryWriter<P>;
}

/// srlsly what the fucking fuck
impl<T, P> PushToQuery<P> for &'_ T
where
    T: PushToQuery<P>,
{
    fn push_to_query<W>(&self, w: &mut W)
    where
        W: QueryWriter<P>,
    {
        (*self).push_to_query(w)
    }
}

impl<'n, V> PushToQuery<&'n dyn ToSql> for Select<'n, V>
where
    V: ToSql + TypeTag,
{
    fn push_to_query<W>(&self, writer: &mut W)
    where
        W: QueryWriter<&'n dyn ToSql>,
    {
        use crate::network::{Constraint, Match};

        /* Values are not stored directly on triplets, they instead are "encoded" into a `soup`
         * table.  So we never compare values or select against triples directly, but with the soup
         * rows that the triples point to. */
        let mut soup = SoupLookup::with_length(self.network.triples());
        let fromsoups = self
            .selection
            .iter()
            .cloned()
            .chain(self.network.constraints().iter().filter_map(|constraint| {
                if let &Constraint::Eq { lh, rh: Match::Value(_) } = constraint {
                    Some(lh)
                } else {
                    None
                }
            }))
            .chain(self.order_by.iter().map(|&(tf, _ordering)| tf))
            .filter(|tf| soup.mark(*tf))
            .map(FromSoup)
            .collect::<Vec<_>>();

        // ...

        let mut writer = writer.with_indent(once("SELECT ").chain(repeat("     , ")));
        for tf in self.selection.iter() {
            writer
                .nl()
                .push(FromSoup(*tf))
                .push_sql(".t, ")
                .push(FromSoup(*tf))
                .push_sql(".v");
        }

        let mut writer = writer
            .dedent()
            .with_indent(once("  FROM ").chain(repeat("     , ")));

        /* soup lookups first */
        for fromsoup in fromsoups.iter().cloned() {
            writer.nl().push_sql(r#"soup "#).push(fromsoup);
        }

        for n in 0..self.network.triples() {
            writer.nl().push_sql(&format!(r#"triples t{}"#, n));
        }

        let mut writer = writer
            .dedent()
            .with_indent(once(" WHERE ").chain(repeat("   AND ")));

        /* soup lookups first */
        for constraint in self.network.constraints().iter() {
            if let &Constraint::Eq { lh, rh: Match::Value(ref v) } = constraint {
                writer
                    .nl()
                    .push(FromSoup(lh))
                    // TODO Type tag written as literal to simplify borrowing and _maybe_ improve
                    // query planning at the statement prepare phase, although, I haven't found
                    // evidence of this in testing.
                    //
                    // This could be solve by holding the type tag and value in the
                    // Match::Value.
                    .push_sql(&format!(".t = {} AND ", v.type_tag()))
                    .push(FromSoup(lh))
                    .push_sql(".v = ?")
                    .push_param(v as &dyn ToSql);
            }
        }
        for fromsoup in fromsoups.iter().cloned() {
            writer
                .nl()
                .push(fromsoup.0)
                .push_sql(" = ")
                .push(fromsoup)
                .push_sql(".rowid");
        }

        /* constrain triples to each other or to the soup lookups */
        for constraint in self.network.constraints().iter() {
            match constraint {
                &Constraint::Eq { lh, ref rh } => match rh {
                    &Match::Field(rh) => {
                        writer.nl().push(lh).push_sql(" = ").push(rh);
                    }
                    Match::Encoded(Encoded { rowid, .. }) => {
                        // TODO check that parameter binding doesn't prevent a partial index from
                        // being used where a literal would use the index.
                        writer.nl().push(lh).push_sql(&format!(" = {}", rowid));
                    }
                    /* Skip this; this is handled by {fromsoups.0} = {fromsoup}.rowid above. */
                    Match::Value(_) => {}
                },
            }
        }

        let indent = once("ORDER BY ").chain(repeat("       , "));
        let mut writer = writer.dedent().with_indent(indent);
        for &(tf, ordering) in self.order_by.iter() {
            // TODO gather soups?
            writer
                .nl() /* */
                .push(FromSoup(tf))
                .push_sql(".v ")
                .push(ordering);
        }

        let writer = writer.dedent();

        if 0 < self.limit {
            // TODO The limit is written into the query instead of bound as a parameter because I
            // don't want to borrow from Select.
            writer.nl().push_sql(&format!(" LIMIT {}", self.limit));
        }

        return;

        #[derive(Copy, Clone)]
        struct FromSoup(TriplesField);

        impl<P> PushToQuery<P> for FromSoup {
            fn push_to_query<W>(&self, w: &mut W)
            where
                W: QueryWriter<P>,
            {
                let _ = match self.0.field() {
                    Field::Entity => write!(w, "s{}_e", self.0.triples().usize()),
                    Field::Attribute => write!(w, "s{}_a", self.0.triples().usize()),
                    Field::Value => write!(w, "s{}_v", self.0.triples().usize()),
                };
            }
        }

        struct SoupLookup(Vec<u8>);

        impl SoupLookup {
            fn with_length(len: usize) -> Self {
                SoupLookup(vec![0; len])
            }

            fn lookup(&mut self, t: Triples) -> &mut u8 {
                &mut self.0[t.usize()]
            }

            fn mark(&mut self, tf: TriplesField) -> bool {
                let cell = self.lookup(tf.triples());
                let flag: u8 = match tf.field() {
                    Field::Entity => 1,
                    Field::Attribute => 2,
                    Field::Value => 4,
                };
                if 0 == *cell & flag {
                    *cell |= flag;
                    return true;
                } else {
                    return false;
                }
            }
        }
    }
}

impl<P> PushToQuery<P> for TriplesField {
    fn push_to_query<W>(&self, w: &mut W)
    where
        W: QueryWriter<P>,
    {
        let _ = match self.field() {
            Field::Entity => write!(w, "t{}.e", self.triples().usize()),
            Field::Attribute => write!(w, "t{}.a", self.triples().usize()),
            Field::Value => write!(w, "t{}.v", self.triples().usize()),
        };
    }
}

impl<P> PushToQuery<P> for Ordering {
    fn push_to_query<W>(&self, w: &mut W)
    where
        W: QueryWriter<P>,
    {
        let _ = match self {
            Ordering::Asc => write!(w, "ASC"),
            Ordering::Desc => write!(w, "DESC"),
        };
    }
}

// ????????????

pub struct RowCursor<'a> {
    pub row: &'a rusqlite::Row<'a>,
    pub cursor: usize,
}

impl<'a> RowCursor<'a> {
    pub fn get<T: FromSql>(&mut self) -> rusqlite::Result<T> {
        let res = self.row.get::<_, T>(self.cursor);
        self.cursor += 1;
        res
    }

    // pub fn get_raw(&mut self) -> rusqlite::types::ValueRef {
    //     let raw = self.row.get_raw(self.cursor);
    //     self.cursor += 1;
    //     raw
    // }

    pub fn skip(&mut self, n: usize) -> &mut Self {
        self.cursor += n;
        self
    }
}

impl<'a> From<&'a rusqlite::Row<'a>> for RowCursor<'a> {
    fn from(row: &'a rusqlite::Row<'a>) -> Self {
        RowCursor { row, cursor: Default::default() }
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

// pub trait ReadFromRow {
//     type Out;
//
//     fn read_from_row(&self, _: &mut RowCursor) -> rusqlite::Result<Self::Out>;
// }
//
// /// motherfuckingchrist
// impl<T> ReadFromRow for &'_ T
// where
//     T: ReadFromRow,
// {
//     type Out = <T as ReadFromRow>::Out;
//
//     fn read_from_row(&self, c: &mut RowCursor) -> rusqlite::Result<Self::Out> {
//         (*self).read_from_row(c)
//     }
// }
//
// impl<'a, V> ReadFromRow for projection::AttributeMap<'a, V>
// where
//     V: crate::FromAffinityValue,
// {
//     type Out = std::collections::HashMap<&'a crate::AttributeName<'a>, V>;
//
//     fn read_from_row(&self, c: &mut RowCursor) -> rusqlite::Result<Self::Out> {
//         self.map
//             .iter()
//             .map(|&(attr, _)| -> rusqlite::Result<(_, _)> {
//                 Ok((attr, V::read_affinity_value(c)?))
//             })
//             .collect()
//     }
// }
//
// impl ReadFromRow for projection::Location {
//     /// TODO the whole point of the phantom data in AttributeMap is to disambiguate this
//     /// type ... so that we can share the same value types in both directions of the
//     /// database.
//     ///
//     /// We don't have that here, so we need to modify the trait so that users can
//     /// specify what kind of container things can be dumped into as long as it
//     /// implements FromAffinityValue.
//     type Out = crate::Value;
//
//     fn read_from_row(&self, c: &mut RowCursor) -> rusqlite::Result<Self::Out> {
//         use crate::types::{Affinity, FromAffinityValue, Value};
//
//         match self.field {
//             Field::Entity => Value::read_with_affinity(Affinity::Entity, c.get_raw()),
//             Field::Attribute => {
//                 Value::read_with_affinity(Affinity::Attribute, c.get_raw())
//             }
//             Field::Value => Value::read_affinity_value(c),
//         }
//     }
// }
//
// impl<T> ReadFromRow for &[T]
// where
//     T: ReadFromRow,
// {
//     type Out = Vec<<T as ReadFromRow>::Out>;
//
//     fn read_from_row(&self, c: &mut RowCursor) -> rusqlite::Result<Self::Out> {
//         self.iter().map(|t| t.read_from_row(c)).collect()
//     }
// }
// macro_rules! read_tuple_from_row {
//     ( $( $t:ident )+ ) => {
//         impl<$($t: ReadFromRow),+> ReadFromRow for ($($t,)+)
//         {
//             type Out = ($(<$t as ReadFromRow>::Out,)+);
//
//             fn read_from_row(&self, c: &mut RowCursor) -> rusqlite::Result<Self::Out> {
//                 #[allow(non_snake_case)]
//                 let ($($t,)+) = self;
//                 let boomer = ($( $t.read_from_row(c)?, )+);
//                 Ok(boomer)
//             }
//         }
//     };
// }
//
// read_tuple_from_row!(A);
// read_tuple_from_row!(A B);
// read_tuple_from_row!(A B C);
// read_tuple_from_row!(A B C D);
// read_tuple_from_row!(A B C D E);
// read_tuple_from_row!(A B C D E F);
// read_tuple_from_row!(A B C D E F G);
// read_tuple_from_row!(A B C D E F G H);
// read_tuple_from_row!(A B C D E F G H I);
//
// /* ??????????? */
//
// #[derive(Debug)]
// pub struct CrazyFuckingMemes<T, F>(T, F);
//
// impl<T, F> CrazyFuckingMemes<T, F> {
//     pub(crate) fn new(t: T, f: F) -> Self {
//         CrazyFuckingMemes(t, f)
//     }
// }
//
// impl<T, F> ReadFromRow for CrazyFuckingMemes<T, F>
// where
//     F: ReadFromRow,
// {
//     type Out = <F as ReadFromRow>::Out;
//
//     fn read_from_row(&self, c: &mut RowCursor) -> rusqlite::Result<Self::Out> {
//         self.1.read_from_row(c)
//     }
// }
//
// impl<'a, P, T, Ph> AddToQuery<P> for CrazyFuckingMemes<T, Ph>
// where
//     T: AddToQuery<P>,
// {
//     fn add_to_query<W>(&self, query: &mut W)
//     where
//         W: QueryWriter<P>,
//     {
//         self.0.add_to_query(query)
//     }
// }

// #[cfg(test)]
// mod test {
//     use super::*;
//     use crate::{AttributeRef, Network, Pattern};
//
//     #[test]
//     fn test() {
//         let mut network: Network = Default::default();
//         let (e, _, v) = network
//             .add_pattern(
//                 Pattern::default()
//                     .attribute(AttributeRef::from_static(":db/attribute")),
//             )
//             .eav();
//
//         let mut select = Select::from(&network);
//         select.order_by(v.desc());
//
//         let mut q = Query::<&dyn ToSql>::default();
//         select.push_to_query(&mut q);
//         eprintln!(">>> {}", q);
//     }
// }
