//! hi

use std::path::PathBuf;

use owoof::{
    either, sql::PushToQuery, Attribute, AttributeRef, DontWoof, Either, Entity, Value, ValueRef,
};

use owoof::retrieve::{self, Pattern, Variable};

use anyhow::Context;

fn main() -> anyhow::Result<()> {
    let args_vec = std::env::args().collect::<Vec<String>>();
    let mut args = args_vec.iter().map(String::as_str);

    let exe = args
        .next()
        .map(|s| s.rsplit('/').next().unwrap_or(s))
        .unwrap_or("owoof");

    match parse_find(args) {
        Err(ArgError::Usage) => usage_and_exit(exe),
        Err(e) => {
            eprintln!("oof! {}", e);

            use std::error::Error;
            let mut source = e.source();
            while let Some(e) = source {
                eprintln!("   » {}", e);
                source = e.source();
            }

            eprintln!("");
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

            let mut network = retrieve::NamedNetwork::<ValueRef>::default();

            find.patterns
                .iter()
                .fold(&mut network, |n, pattern| n.add_pattern(pattern));

            let mut retreival = vec![];
            let mut dispersal = find
                .show
                .iter()
                .map(|show| -> anyhow::Result<_> {
                    let field = match &show.variable {
                        Variable::Any => Err(anyhow::anyhow!("? does not unify")),
                        Variable::Unify(unify) => network.names.get(unify).ok_or_else(|| {
                            anyhow::anyhow!("{} wasn't found in the patterns to unify", unify)
                        }),
                    }?;

                    /* If there are no attributes, show ?var.
                     * If attributes are given, then show the value of
                     * those attributes where ?var is the entity. */
                    if show.attributes.is_empty() {
                        retreival.push(field);

                        Ok(either::left(just::<Value>()))
                    } else {
                        for &attribute in show.attributes.iter() {
                            /* find or add triples `t`
                             * such that `t.e = field` and `t.a = attribute` */
                            let found = network
                                .constraint_value_matches(ValueRef::from(attribute))
                                .find(|other| {
                                    network.is_linked(field, other.triples().entity()).is_some()
                                })
                                .map(|other| other.triples().value());

                            let show = found.unwrap_or_else(|| {
                                network
                                    .fluent_triples()
                                    .link_entity(field)
                                    .match_attribute(attribute)
                                    .value()
                            });

                            retreival.push(show);
                        }

                        Ok(either::right(zip_with_keys(&show.attributes)))
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;

            /* Default --show is showing values for variables with the fewest constraints. */

            if dispersal.is_empty() {
                debug_assert!(retreival.is_empty());

                let var_constraint_counts = network
                    .names
                    .iter()
                    .map(|&(_, field)| (field, network.constraints_on(field).count()))
                    .collect::<Vec<_>>();

                if let Some(&(_, min)) =
                    var_constraint_counts.iter().min_by_key(|(_, count)| count)
                {
                    retreival = var_constraint_counts
                        .iter()
                        .cloned()
                        .filter_map(|(field, count)| (count == min).then(|| field))
                        .collect();
                    dispersal = retreival
                        .iter()
                        .map(|_| either::left(just::<Value>()))
                        .collect();
                }
            }

            /* TODO select makes network immutable (this is probably stupid),
             * so we can't select until after we prefetch, we can't prefetch
             * until after we've gone through the --show and made a selection */
            network.prefetch_attributes(&woof)?;

            let mut select = network.select();

            retreival
                .into_iter()
                .fold(&mut select, |s, field| s.field(field));

            select.limit(find.limit);

            if find.explain {
                let q = select.to_query();
                eprintln!("{}", q.as_str());

                let explain = woof.explain_plan(&q).context("explain")?;
                eprintln!("{}", explain);

                return Ok(());
            }

            use owoof::disperse::zip_with_keys;
            use owoof::driver::{just, FromSqlRow};

            let query = select.to_query();
            let mut stmt = woof.prepare(query.as_str())?;

            let results = stmt
                .query_map(query.params(), |row| {
                    dispersal.as_mut_slice().from_start_of_row(&row)
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let json = serde_json::to_string_pretty(&results)?;

            println!("{}", json);
        }
    }

    Ok(())
}

fn usage_and_exit(exe: &str) -> ! {
    eprintln!("usage: {} [--db <path>] [<pattern>...] [--show <show>] [--limit <num>] [--asc <show>] [--desc <show>] [--find|--explain|--explain-plan]", exe);
    eprintln!("       {} [--db <path>] assert", exe);
    eprintln!("       {} [--db <path>] retract", exe);
    eprintln!("");
    eprintln!("<pattern> is ?var|#some-entity-uuid ?var|:some/attribute ?var|json ");
    eprintln!("<show>    is ?var [:some/attribute...]");
    eprintln!("");
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
    patterns: Vec<Pattern<'a, Value>>,
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
    let mut explain = false;

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
            "--explain" => explain = true,
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
        explain,
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
    #[error("invalid option for {}", .0)]
    Invalid(&'a str, #[source] anyhow::Error),
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

fn parse_pattern<'a>(s: &'a str) -> anyhow::Result<Pattern<'a, Value>> {
    (|| {
        let (s, e) = take_no_whitespace(s)?;
        let (s, a) = take_no_whitespace(s)?;
        let v = s.trim();
        return Some(Pattern {
            entity: parse_variable_or_entity(e).ok()?.map_right(Value::from),
            attribute: parse_variable_or_attribute(a).ok()?.map_right(Value::from),
            value: parse_variable_or_value(v)?,
        });

        fn take_no_whitespace(s: &str) -> Option<(&str, &str)> {
            let s = s.trim_start();
            let next = s.split_whitespace().next()?;
            Some((&s[next.len()..], next))
        }
    })()
    .ok_or_else(|| anyhow::anyhow!("expected entity-attribute-value triple"))
}

use owoof::retrieve::{parse_variable, parse_variable_or_attribute, parse_variable_or_entity};

fn parse_variable_or_value<'a>(s: &'a str) -> Option<Either<Variable<'a>, Value>> {
    Option::<_>::None
        .or_else(|| parse_variable(s).ok().map(Either::Left))
        .or_else(|| parse_value(s).map(Either::Right))
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
    let mut parts = s.split_whitespace();
    Ok(parts.next().unwrap_or_default())
        .and_then(|s| parse_variable(s).with_context(|| format!("when reading {:?}", s)))
        .and_then(|variable| {
            parts
                .map(|s| {
                    AttributeRef::from_str(s).with_context(|| format!("when reading {:?}", s))
                })
                .collect::<anyhow::Result<_>>()
                .map(|attributes| Show { variable, attributes })
        })
        .context("expected ?var :some/attributes...")
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
