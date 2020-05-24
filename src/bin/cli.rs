//! hi

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Context;

#[derive(Debug, thiserror::Error)]
enum ArgError<'a> {
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

#[derive(Debug)]
enum Show<'a> {
    Location(oof::Location),
    Map(oof::AttributeMap<'a, oof::Value>),
}

#[derive(Debug, serde::Serialize)]
#[serde(untagged)]
enum Shown<'a> {
    Value(oof::Value),
    Map(HashMap<&'a oof::AttributeName<'a>, oof::Value>),
}

impl<'a, P> oof::sql::AddToQuery<P> for Show<'a> {
    fn add_to_query<W>(&self, query: &mut W)
    where
        W: oof::sql::QueryWriter<P>,
    {
        match self {
            Show::Location(i) => i.add_to_query(query),
            Show::Map(i) => i.add_to_query(query),
        }
    }
}

impl<'a> oof::sql::ReadFromRow for Show<'a> {
    type Out = Shown<'a>;

    fn read_from_row(&self, c: &mut oof::sql::RowCursor) -> rusqlite::Result<Self::Out> {
        match self {
            Show::Location(i) => i.read_from_row(c).map(Shown::Value),
            Show::Map(i) => i.read_from_row(c).map(Shown::Map),
        }
    }
}

#[derive(Debug)]
enum QueryMode {
    Find,
    Explain,
    ExplainPlan,
}

#[derive(Debug)]
enum Command<'a> {
    Init {
        path: PathBuf,
    },
    Assert {
        path: PathBuf,
    },
    Query {
        mode: QueryMode,
        path: PathBuf,
        show: Vec<(&'a str, Vec<oof::AttributeName<'a>>)>,
        patterns: Vec<oof::Pattern<'a, oof::Value>>,
        order: Vec<(&'a str, Vec<oof::AttributeName<'a>>, oof::Ordering)>,
        limit: i64,
    },
}

impl<'a> Command<'a> {
    fn run(self) -> anyhow::Result<()> {
        match self {
            Command::Init { path } => {
                let mut conn = rusqlite::Connection::open(&path)?;
                oof::Session::init_schema(&mut conn)?;
                println!("New database created at {}", path.display());
                Ok(())
            }
            Command::Assert { path } => {
                let mut conn = rusqlite::Connection::open_with_flags(
                    &path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                        | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
                )?;
                let sess = oof::Session::new(&mut conn)?;

                let mut stdin = std::io::stdin();
                let stuff: OwO<std::collections::HashMap<oof::AttributeName, oof::Value>> =
                    serde_json::from_reader(&mut stdin)?;

                eprintln!("{:?}", stuff);
                match stuff {
                    OwO::Many(stuff) => todo!("{:?}", stuff),
                    OwO::One(stuff) => {
                        let ent = sess.assert_obj(&stuff).context("assert object")?;
                        let jaysons = serde_json::to_string_pretty(&ent.id)?;
                        println!("{}", jaysons);
                    }
                };

                return sess.commit().context("commit transaction");

                #[derive(serde::Deserialize, Debug)]
                #[serde(untagged)]
                enum OwO<T> {
                    Many(Vec<T>),
                    One(T),
                }
            }
            Command::Query {
                mode,
                path,
                show,
                patterns,
                order,
                limit,
            } => {
                let mut conn = rusqlite::Connection::open_with_flags(
                    &path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                        | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
                )?;

                let sess = oof::Session::new(&mut conn)?;

                let start = std::time::Instant::now();

                let mut p = oof::Projection::from_patterns(&patterns);

                let mut selection = show
                    .iter()
                    .map(|(var, attrs)| {
                        if attrs.is_empty() {
                            p.var(var)
                                .map(Show::Location)
                                .ok_or_else(|| anyhow::format_err!("unknown variable: {}", var))
                        } else {
                            Ok(Show::Map(p.attribute_map(var, attrs)))
                        }
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;

                if selection.is_empty() {
                    // Try to find a variable that is unconstrained ...
                    if let Some((_, loc)) = p
                        .variables()
                        .iter()
                        .find(|(_, &loc)| p.constrained_to(loc).next().is_none())
                    {
                        selection.push(Show::Location(*loc));
                    } else {
                        let map = p.attribute_map("_", std::iter::once(&oof::types::ENTITY_UUID));
                        selection.push(Show::Map(map));
                    }
                }

                assert!(selection.len() > 0);

                let mut sel = p.select(selection.as_slice());

                order.iter().try_for_each(|(var, attrs, ord)| {
                    if attrs.is_empty() {
                        if let Some(loc) = sel.var(var) {
                            sel.order_by((loc, *ord));
                        } else {
                            return Err(anyhow::format_err!("unknown variable: {}", var));
                        }
                    } else {
                        sel.attribute_map(var, attrs)
                            .into_value_locations()
                            .for_each(|loc| {
                                sel.order_by((loc, *ord));
                            })
                    }
                    Ok(())
                })?;

                sel.limit(limit);

                let output = match mode {
                    QueryMode::Find => {
                        if selection.len() == 1 {
                            let sel = sel.read_using(&selection[0]);
                            let results = sess.find(&sel).context("find")?;
                            serde_json::to_string_pretty(&results)
                        } else {
                            let results = sess.find(&sel).context("find")?;
                            serde_json::to_string_pretty(&results)
                        }?
                    }

                    QueryMode::Explain => if selection.len() == 1 {
                        let sel = sel.read_using(&selection[0]);
                        sess.explain(&sel)
                    } else {
                        sess.explain(&sel)
                    }
                    .context("explain")?
                    .to_string(),

                    QueryMode::ExplainPlan => if selection.len() == 1 {
                        let sel = sel.read_using(&selection[0]);
                        sess.explain_plan(&sel)
                    } else {
                        sess.explain_plan(&sel)
                    }
                    .context("explain plan")?
                    .to_string(),
                };

                let end = std::time::Instant::now();
                println!("{}", output);

                eprintln!("[DEBUG] duration: {:?}", end - start);

                Ok(())
            }
        }
    }
}

fn main() {
    let args = std::env::args().collect::<Vec<String>>(); // why is this not &'static str?
    let prog = args
        .first()
        .map(|s| s.rsplit('/').next().unwrap_or(s))
        .unwrap_or("???");

    match parse_args(args.iter().skip(1).map(|s| s.as_str())) {
        Err(e) => {
            eprintln!("{}\n", e);
            eprintln!(
                "usage: {} [--db <path>] [<pattern>...] [--show <show>] [--limit <num>] [--asc <show>] [--desc <show>] [--find|--explain|--explain-plan]",
                prog
            );
            eprintln!("       {} [--db <path>] assert", prog);
            eprintln!("       {} [--db <path>] init", prog);
            eprintln!("<pattern> is ...TODO");
            eprintln!("<show>    is ?var [:some/attribute...]");
            eprintln!(
                "the default path (set by OOF_DB) is {}",
                default_db_path().display()
            );
            std::process::exit(1);
        }
        Ok(cmd) => {
            /* eprintln!("{:#?}", cmd); */
            if let Err(e) = cmd.run() {
                print!("error");
                for cause in e.chain() {
                    print!(": {}", cause);
                }
                println!("");

                std::process::exit(1);
            }
        }
    }
}

fn default_db_path() -> PathBuf {
    std::env::var_os("OOF_DB")
        .map(PathBuf::from)
        .unwrap_or("oof.sqlite".into())
}

fn default_limit() -> i64 {
    std::env::var("OOF_LIMIT")
        .map(|s| s.parse().expect("parse OOF_LIMIT environment variable"))
        .unwrap_or(10)
}

fn parse_args<'a, I: Iterator<Item = &'a str>>(mut args: I) -> Result<Command<'a>, ArgError<'a>> {
    let mut patterns = vec![];
    let mut order = vec![];
    let mut db = Option::<&str>::None;
    let mut show = vec![];
    let mut limit = Option::<&str>::None;
    let mut mode = QueryMode::Find;

    let parse_db_path = |db: Option<&str>| -> Result<PathBuf, _> {
        db.map(|s| {
            s.parse()
                .map_err(|e| anyhow::format_err!("could not parse path: {}", e))
                .map_err(ArgError::invalid("--db"))
        })
        .unwrap_or_else(|| Ok(default_db_path()))
    };

    while let Some(arg) = args.next() {
        match arg {
            "assert" => {
                let path = parse_db_path(db)?;
                return Ok(Command::Assert { path });
            }
            "init" => {
                let path = parse_db_path(db)?;
                return Ok(Command::Init { path });
            }
            "--db" => {
                db = Some(args.next().ok_or(ArgError::NeedsValue(arg))?);
            }
            "--show" => {
                let v = args.next().ok_or(ArgError::NeedsValue(arg))?;
                show.push(v);
            }
            "--limit" => {
                limit = Some(args.next().ok_or(ArgError::NeedsValue(arg))?);
            }
            "--asc" => {
                let v = args.next().ok_or(ArgError::NeedsValue(arg))?;
                order.push((v, oof::Ordering::Asc));
            }
            "--desc" => {
                let v = args.next().ok_or(ArgError::NeedsValue(arg))?;
                order.push((v, oof::Ordering::Desc));
            }
            "--find" => mode = QueryMode::Find,
            "--explain" => mode = QueryMode::Explain,
            "--explain-plan" => mode = QueryMode::ExplainPlan,
            _ if arg.starts_with("-") => return Err(ArgError::Unknown(arg)),
            _ => patterns.push(arg),
        }
    }

    let path = parse_db_path(db)?;

    let patterns = patterns
        .into_iter()
        .map(parse_pattern)
        .collect::<anyhow::Result<Vec<_>>>()
        .map_err(ArgError::invalid("<pattern>"))?;

    let show = show
        .into_iter()
        .map(parse_show)
        .collect::<anyhow::Result<Vec<_>>>()
        .map_err(ArgError::invalid("--show"))?;

    let limit = limit
        .map(|s| {
            s.parse()
                .map_err(|e| anyhow::format_err!("could not parse i64: {}", e))
                .map_err(ArgError::invalid("--limit"))
        })
        .unwrap_or_else(|| Ok(default_limit()))?;

    let order = order
        .into_iter()
        .map(|(ordering, ord)| {
            parse_show(ordering)
                .map(|(var, attrs)| (var, attrs, ord))
                .map_err(ArgError::invalid(match ord {
                    oof::Ordering::Asc => "--asc",
                    oof::Ordering::Desc => "--desc",
                }))
        })
        .collect::<Result<Vec<_>, _>>()?;

    return Ok(Command::Query {
        mode,
        path,
        show,
        limit,
        patterns,
        order,
    });
}

fn parse_pattern<'a>(s: &'a str) -> anyhow::Result<oof::Pattern<'a, oof::Value>> {
    let mut parts = s.splitn(3, |s: char| s.is_ascii_whitespace());
    match (parts.next(), parts.next(), parts.next()) {
        (Some(e), Some(a), Some(v)) => {
            let entity = parse_variable(e)
                .map(|var| oof::VariableOr::Variable(std::borrow::Cow::from(var)))
                .or_else(|_| {
                    /* TODO this is fairly confusing, entity references are plain UUIDs in the
                     * entity part of the pattern but they are JSON strings with a leading # in the
                     * value field of the pattern. Maybe always require the leading # or whatever
                     * symbol? somewhat consistent with :attributes and ?variables */
                    e.parse::<uuid::Uuid>()
                        .map(oof::EntityId::from)
                        .map(oof::VariableOr::Value)
                })?;

            let attribute = parse_variable(a)
                .map(|var| oof::VariableOr::Variable(std::borrow::Cow::from(var)))
                .or_else(|_| parse_attribute(a).map(oof::VariableOr::Value))?;

            let value = parse_variable(v)
                .map(|var| oof::VariableOr::Variable(std::borrow::Cow::from(var)))
                .or_else(|_| serde_json::from_str::<oof::Value>(v).map(oof::VariableOr::Value))?;

            Ok(oof::Pattern {
                entity,
                attribute,
                value,
            })
        }
        _ => anyhow::bail!("expected a 3-tuple"),
    }
}

fn parse_show<'a>(s: &'a str) -> anyhow::Result<(&'a str, Vec<oof::AttributeName<'a>>)> {
    let mut terms = s.split_ascii_whitespace().filter(|s| !s.is_empty());
    let var = match terms.next().map(parse_variable) {
        None => anyhow::bail!("variable expected, none found"),
        Some(Err(e)) => anyhow::bail!("variable expected, {}", e),
        Some(Ok(v)) => v,
    };
    let attrs = terms
        .map(parse_attribute)
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok((var, attrs))
}

fn parse_variable<'a>(s: &'a str) -> anyhow::Result<&'a str> {
    if !s.starts_with("?") {
        anyhow::bail!("missing '?' prefix for {}", s)
    }
    Ok(&s[1..])
}

fn parse_attribute<'a>(s: &'a str) -> anyhow::Result<oof::AttributeName<'a>> {
    use std::borrow::Cow;
    use std::convert::TryFrom;
    oof::AttributeName::try_from(Cow::Borrowed(s)).map_err(anyhow::Error::msg)
}
