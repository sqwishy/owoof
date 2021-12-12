//! hi

use std::path::PathBuf;

use owoof::network::TriplesField;
use owoof::retrieve::{self, Pattern, Variable};
use owoof::{either, sql::PushToQuery, AttributeRef, DontWoof, Ordering, Value, ValueRef};

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

            for pattern in find.patterns.iter() {
                network.add_pattern(pattern);
            }

            use owoof::disperse::zip_with_keys;
            use owoof::driver::just;

            let mut retreival = vec![];
            let mut dispersal =
                find.show
                    .iter()
                    .map(|show| -> anyhow::Result<_> {
                        let field = network.names.lookup(&show.variable).with_context(|| {
                            anyhow::anyhow!("cannot show `{}`", &show.variable)
                        })?;

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
                                let value = network
                                    .this_and_links_to(field)
                                    .map(TriplesField::triples)
                                    .find(|t| {
                                        network
                                            .is_matched(t.attribute(), ValueRef::from(attribute))
                                            .is_some()
                                    })
                                    .map(|triples| triples.value());
                                let value = value.unwrap_or_else(|| {
                                    network
                                        .fluent_triples()
                                        .link_entity(field)
                                        .match_attribute(attribute)
                                        .value()
                                });
                                retreival.push(value);
                            }

                            Ok(either::right(zip_with_keys(&show.attributes)))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;

            let mut order_by = vec![];

            find.order.iter().try_for_each(|(show, ordering)| {
                let field = network
                    .names
                    .lookup(&show.variable)
                    .with_context(|| anyhow::anyhow!("cannot order by `{}`", &show.variable))?;

                if show.attributes.is_empty() {
                    order_by.push((field, *ordering));
                } else {
                    for &attribute in show.attributes.iter() {
                        let field = network.value_for_entity_attribute(field, attribute);
                        order_by.push((field, *ordering));
                    }
                }

                Result::<_, anyhow::Error>::Ok(())
            })?;

            /* Default --show is showing values for variables with the fewest constraints. */

            if dispersal.is_empty() {
                debug_assert!(retreival.is_empty());
                retreival = variables_with_fewest_constraints(&network)
                    .map(|(_, field)| field)
                    .collect();
                dispersal = retreival
                    .iter()
                    .map(|_| either::left(just::<Value>()))
                    .collect();
            }

            /* TODO select makes network immutable (this is probably stupid),
             * so we can't select until after we prefetch, we can't prefetch
             * until after we've gone through the --show and made a selection */
            network.prefetch_attributes(&woof)?;

            let mut select = network.select();

            for field in retreival.into_iter() {
                select.field(field);
            }

            for o in order_by.into_iter() {
                select.order_by(o);
            }

            select.limit(find.limit);

            if find.explain {
                let q = select.to_query();
                eprintln!("{}", q.as_str());

                let explain = woof.explain_plan(&q).context("explain")?;
                eprintln!("{}", explain);

                return Ok(());
            }

            let results = select.to_query().disperse(
                /* if we map a row to a single value, don't put that value in a list */
                if dispersal.len() == 1 {
                    either::left(dispersal.into_iter().next().unwrap())
                } else {
                    either::right(dispersal.as_mut_slice())
                },
                &woof,
            )?;
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
    order: Vec<(Show<'a>, Ordering)>,
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
    let mut patterns = vec![];
    let mut show = vec![];
    let mut order = vec![];
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
            "--asc" => {
                let arg = args.next().ok_or(ArgError::NeedsValue(arg))?;
                order.push((arg, Ordering::Asc));
            }
            "--desc" => {
                let arg = args.next().ok_or(ArgError::NeedsValue(arg))?;
                order.push((arg, Ordering::Desc));
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
            .map(|s| s.try_into().map_err(anyhow::Error::from))
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
        order: order
            .into_iter()
            .map(|(show, ord)| {
                parse_show(show)
                    .map(|show| (show, ord))
                    .map_err(ArgError::invalid(match ord {
                        Ordering::Asc => "--asc",
                        Ordering::Desc => "--desc",
                    }))
            })
            .collect::<Result<Vec<_>, _>>()?,
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

#[derive(Debug, PartialEq)]
struct Show<'a> {
    variable: Variable<'a>,
    attributes: Vec<&'a AttributeRef>,
}

fn parse_show<'a>(s: &'a str) -> anyhow::Result<Show<'a>> {
    let mut parts = s.split_whitespace();
    Ok(parts.next().unwrap_or_default())
        .and_then(|s| Variable::try_from(s).with_context(|| format!("when reading {:?}", s)))
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

pub fn variables_with_fewest_constraints<'a, 'n, V>(
    network: &'a retrieve::NamedNetwork<'n, V>,
) -> impl Iterator<Item = (&'n str, TriplesField)> + 'a
where
    V: PartialEq,
{
    let constraint_counts = network
        .names
        .iter()
        .map(|&(_, field)| network.constraints_on(field).count())
        .collect::<Vec<_>>();

    constraint_counts
        .iter()
        .cloned()
        .min()
        .map(move |min| {
            network
                .names
                .iter()
                .zip(constraint_counts.into_iter())
                .filter(move |&(_, count)| count == min)
                .map(|(v, _)| v)
        })
        .into_iter()
        .flatten()
        .cloned()
}
