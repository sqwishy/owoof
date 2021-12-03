//! hi
#![allow(unused)]

use std::path::PathBuf;
use std::str::FromStr;

use owoof::{
    network::{Constraint, Match, Ordering, TriplesField},
    sql::PushToQuery,
    Attribute, AttributeRef, DontWoof, Entity, Network, Value, ValueRef,
};

use anyhow::Context;
use either::Either;

fn main() -> anyhow::Result<()> {
    let args_vec = std::env::args().collect::<Vec<String>>();
    let mut args = args_vec.iter().map(String::as_str);

    let exe = args
        .next()
        .map(|s| s.rsplit('/').next().unwrap_or(s))
        .unwrap_or("owoof");

    match parse_find(args) {
        Err(e) => {
            eprintln!("oof! {:#}\n", e);
            usage_and_exit(exe)
        }
        Ok(find) => {
            let mut db = rusqlite::Connection::open_with_flags(
                &find.path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?;
            let tx = db.transaction()?;
            let woof = DontWoof::from(tx);

            let mut network = Network::<ValueRef>::default();
            let mut vars: Vec<(&str, TriplesField)> = Vec::default();

            /* mend patterns into a network */
            find.patterns.iter().for_each(|pattern| {
                let mut t = network.add_triples();
                [
                    (t.entity(), &pattern.entity),
                    (t.attribute(), &pattern.attribute),
                    (t.value(), &pattern.value),
                ]
                .into_iter()
                .filter_map(|(field, part)| match &part {
                    Either::Left(Variable::Any) => None,
                    Either::Left(Variable::Unify(unify)) => {
                        linear_find_or_append(&mut vars, unify, field)
                            .map(|link_to| field.eq(link_to.into()))
                    }
                    Either::Right(entity) => Some(field.eq(Match::Value(ValueRef::from(entity)))),
                })
                .for_each(|constraint| network.add_constraint(constraint));
            });

            let mut selection = vec![];

            for show in find.show.iter() {
                let field = match &show.variable {
                    Variable::Any => Err(anyhow::anyhow!("? does not unify")),
                    Variable::Unify(unify) => linear_find(&vars, unify).ok_or_else(|| {
                        anyhow::anyhow!("{} wasn't found in the patterns to unify", unify)
                    }),
                }?;

                /* if there are no attributes, show ?var. but if attributes are given then show the
                 * value of those attributes where ?var is the entity. */

                if show.attributes.is_empty() {
                    selection.push(field);
                }

                for &attribute in show.attributes.iter() {
                    /* look for a triples t such that field = t.e & t.a = attribute */
                    let found = network
                        .constraint_value_matches(ValueRef::from(attribute))
                        .find(|other| network.is_linked(field, other.triples().entity()).is_some())
                        .map(|other| other.triples().value());

                    let show = found.unwrap_or_else(|| {
                        network
                            .fluent_triples()
                            .link_entity(field)
                            .match_attribute(attribute)
                            .value()
                    });

                    selection.push(show);
                }
            }

            woof.prefetch_attributes(&mut network)?;

            let mut select = network.select();

            select.limit(find.limit);

            for field in selection.into_iter() {
                select.field(field);
            }

            let q = select.to_query();
            eprintln!("{}", q.as_str());

            let mut stmt = woof.prepare(q.as_str())?;

            use owoof::driver::{just, zipmap, ColumnIndex, FromSqlRow};

            let mut memes = find
                .show
                .iter()
                .map(|show| {
                    if show.attributes.is_empty() {
                        owoof::driver::Either::Left(just::<Value>())
                    } else {
                        owoof::driver::Either::Right(zipmap::<
                            std::collections::BTreeMap<_, _>,
                            _,
                            _,
                        >(
                            show.attributes.as_slice().iter(),
                            just::<Value>(),
                        ))
                    }
                })
                .collect::<Vec<_>>();

            let results = stmt
                .query_map(q.params(), |row| {
                    memes.as_mut_slice().from_start_of_row(&row)
                    // just::<Value>().from_start_of_row(&row)
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let json = serde_json::to_string_pretty(&results)?;

            println!("{}", json);

            fn linear_find_or_append<K, V>(
                vec: &mut Vec<(K, V)>,
                looking_for: K,
                or_else: V,
            ) -> Option<V>
            where
                K: PartialEq,
                V: Copy,
            {
                match vec.iter().find(|(has, _)| has == &looking_for) {
                    Some(&(_, found)) => Some(found),
                    None => {
                        vec.push((looking_for, or_else));
                        None
                    }
                }
            }

            fn linear_find<K, V>(vec: &[(K, V)], looking_for: K) -> Option<V>
            where
                K: PartialEq,
                V: Copy,
            {
                vec.iter()
                    .find_map(|&(ref has, found)| (has == &looking_for).then(|| found))
            }
        }
    }

    Ok(())
}

fn usage_and_exit(exe: &str) -> ! {
    eprintln!("usage: {} [--db <path>] [<pattern>...] [--show <show>] [--limit <num>] [--asc <show>] [--desc <show>] [--find|--explain|--explain-plan]", exe);
    eprintln!("       {} [--db <path>] assert", exe);
    eprintln!("       {} [--db <path>] retract", exe);
    eprintln!("<pattern> is ?var|#some-entity-uuid ?var|:some/attribute ?var|json ");
    eprintln!("<show>    is ?var [:some/attribute...]");
    eprintln!(
        "the default path (set by OWOOF_DB) is {}",
        default_db_path().display()
    );
    std::process::exit(2);
}

#[derive(Debug)]
struct Find<'a> {
    path: PathBuf,
    show: Vec<Show<'a>>,
    patterns: Vec<Pattern<'a>>,
    // order: Vec<(&'a str, Vec<AttributeRef<'a>>, Ordering)>,
    limit: i64,
    explain: bool,
    // explain_plan: bool,
}

/* argument parsing ... */
fn parse_find<'a, I>(mut args: I) -> Result<Find<'a>, ArgError<'a>>
where
    I: Iterator<Item = &'a str>,
{
    let mut db = Option::<&str>::None;
    let mut limit = Option::<&str>::None;
    // let mut mode = QueryMode::Find;
    // let mut order = vec![];
    let mut patterns = vec![];
    let mut show = vec![];

    while let Some(arg) = args.next() {
        match arg {
            "-h" | "--help" => return Err(ArgError::Usage),
            "--db" => {
                db.replace(args.next().ok_or(ArgError::NeedsValue(arg))?);
            }
            "--limit" => {
                limit.replace(args.next().ok_or(ArgError::NeedsValue(arg))?);
            }
            "--show" => {
                show.push(args.next().ok_or(ArgError::NeedsValue(arg))?);
            }
            "--" => {
                patterns.extend(args);
                break;
            }
            _ if arg.starts_with("-") => return Err(ArgError::Unknown(arg)),
            _ => patterns.push(arg),
        }
    }

    Ok(Find {
        patterns: patterns
            .into_iter()
            .map(parse_pattern)
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(ArgError::invalid("<pattern>"))?,
        show: show
            .into_iter()
            .map(parse_show)
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(ArgError::invalid("--show"))?,

        path: db
            .map(|s| {
                s.parse()
                    .context("parse path")
                    .map_err(ArgError::invalid("--db"))
            })
            .unwrap_or_else(|| Ok(default_db_path()))?,
        // order: order
        //     .into_iter()
        //     .map(|(ordering, ord)| {
        //         parse_show(ordering)
        //             .map(|(var, attrs)| (var, attrs, ord))
        //             .map_err(ArgError::invalid(match ord {
        //                 owoof::Ordering::Asc => "--asc",
        //                 owoof::Ordering::Desc => "--desc",
        //             }))
        //     })
        //     .collect::<Result<Vec<_>, _>>()?,
        limit: limit
            .map(|s| {
                s.parse()
                    .context("parse number")
                    .map_err(ArgError::invalid("--limit"))
            })
            .unwrap_or_else(|| Ok(default_limit()))?,
        explain: false,
    })
}

#[derive(Debug, thiserror::Error)]
enum ArgError<'a> {
    #[error("ﾟ･✿ヾ╲(｡◕‿◕｡)╱✿･ﾟ")]
    Usage,
    #[error("unknown argument {}", .0)]
    Unknown(&'a str),
    #[error("expected value for {}", .0)]
    NeedsValue(&'a str),
    #[error("invalid value for {}: {}", .0, .1)]
    Invalid(&'a str, anyhow::Error),
}

impl<'a> ArgError<'a> {
    fn invalid(arg: &'a str) -> impl Fn(anyhow::Error) -> ArgError<'a> {
        move |e| ArgError::Invalid(arg, e)
    }
}

fn default_db_path() -> PathBuf {
    std::env::var_os("OWOOF_DB")
        .map(PathBuf::from)
        .unwrap_or("owoof.sqlite".into())
}

fn default_limit() -> i64 {
    std::env::var("OWOOF_LIMIT")
        .map(|s| s.parse().expect("parse OWOOF_LIMIT environment variable"))
        .unwrap_or(10)
}

#[derive(Debug, PartialEq)]
struct Pattern<'a> {
    /* These should use Entity and Attribute instead of value but it's a bit convenient for these
     * to be homomorphic or whatever so you can add them to a Vec or iterate over them and interact
     * with them all the same.  but idk */
    entity: Either<Variable<'a>, Value>,
    attribute: Either<Variable<'a>, Value>,
    value: Either<Variable<'a>, Value>,
}

fn parse_pattern<'a>(s: &'a str) -> anyhow::Result<Pattern<'a>> {
    // TODO FromStr and error type!
    (|| {
        let mut parts = s.splitn(3, |s: char| s.is_ascii_whitespace());
        let e = parts.next()?;
        let a = parts.next()?;
        let v = parts.next()?;
        Some(Pattern {
            entity: parse_variable_or_entity(e)?.map_right(Value::from),
            attribute: parse_variable_or_attribute(a)?.map_right(Value::from),
            value: parse_variable_or_value(v)?,
        })
    })()
    .ok_or_else(|| anyhow::anyhow!("expected entity attribute value triple"))
}

fn parse_variable_or_entity<'a>(s: &'a str) -> Option<Either<Variable<'a>, Entity>> {
    Option::<_>::None
        .or_else(|| parse_variable(s).map(Either::Left))
        .or_else(|| s.parse::<Entity>().ok().map(Either::Right))
}

fn parse_variable_or_attribute<'a>(s: &'a str) -> Option<Either<Variable<'a>, Attribute>> {
    Option::<_>::None
        .or_else(|| parse_variable(s).map(Either::Left))
        .or_else(|| s.parse::<Attribute>().ok().map(Either::Right))
}

fn parse_variable_or_value<'a>(s: &'a str) -> Option<Either<Variable<'a>, Value>> {
    Option::<_>::None
        .or_else(|| parse_variable(s).map(Either::Left))
        .or_else(|| parse_value(s).map(Either::Right))
}

// TODO I don't think borrowing is really worth it here and it makes FromStr kind of not work at
// all so that sucks .....
#[derive(Debug, PartialEq)]
enum Variable<'a> {
    Any,
    Unify(&'a str),
}

fn parse_variable<'a>(s: &'a str) -> Option<Variable<'a>> {
    match s {
        "?" => Some(Variable::Any),
        v if v.starts_with("?") => Some(Variable::Unify(v)),
        _ => None,
    }
}

// It would be nice to move this into the library but I don't think I want to depend on serde_json
// in there?  I'm not super duper sure about formalizing a text format for this.
fn parse_value(s: &str) -> Option<owoof::Value> {
    use serde_json::from_str as json;

    Option::<Value>::None
        .or_else(|| s.parse::<Entity>().map(Value::from).ok())
        .or_else(|| s.parse::<Attribute>().map(Value::from).ok())
        .or_else(|| json::<String>(s).map(Value::from).ok())
        .or_else(|| json::<i64>(s).map(Value::from).ok())
        .or_else(|| json::<f64>(s).map(Value::from).ok())
        .or_else(|| json::<bool>(s).map(Value::from).ok())
        .or_else(|| s.parse::<uuid::Uuid>().map(Value::from).ok())
}

#[derive(Debug, PartialEq)]
struct Show<'a> {
    variable: Variable<'a>,
    attributes: Vec<&'a AttributeRef>,
}

fn parse_show<'a>(s: &'a str) -> anyhow::Result<Show<'a>> {
    // TODO FromStr and error type!
    let mut parts = s.split(|s: char| s.is_ascii_whitespace());
    let variable = parts
        .next()
        .and_then(parse_variable)
        .ok_or_else(|| anyhow::anyhow!("expected ?var :some/attributes..."))?;
    let attributes = parts
        .map(|s| AttributeRef::from_str(s).context("parse attribute"))
        .collect::<anyhow::Result<_>>()?;
    Ok(Show { variable, attributes })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        assert_eq!(parse_variable("1234"), None);
        assert_eq!(parse_variable("?foo"), Some(Variable::Unify("?foo")));
        assert_eq!(parse_variable("?"), Some(Variable::Any));
    }

    #[test]
    fn test_parse_pattern() {
        assert_eq!(
            parse_pattern("? ? ?asdf").unwrap(),
            Pattern {
                entity: Either::Left(Variable::Any),
                attribute: Either::Left(Variable::Any),
                value: Either::Left(Variable::Unify("?asdf")),
            }
        );
    }
}
