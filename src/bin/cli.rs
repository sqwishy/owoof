//! Usage:
//! cli '?b book/title "Starship Troopers"' \
//!     '?r rating/book ?b' \
//!     --show '?r rating/rank rating/user'
//!
//! ```
//!     [ {"rating/rank": 5, "rating/user": 1234} ]
//! ```
//!
//! ??? Not supported, maybe one day, but what's the syntax for controlling limits or orderings?
//! cli '?b book/avg-rating ?v' \
//!     '?r rating/book ?b' \
//!     --if '?v < 4.0'
//!     --show '?b book/title book/isbn ???
//!
//! ```
//!     [
//!       {
//!         "book/title": "Starship Troopers",
//!         "book/isbn": "441783589",
//!         "book/ratings": [
//!           {"rating/rank": 5, "rating/user": 1234}
//!         ]
//!       }
//!     ]
//! ```
//!
//! ?b (book/title book/isbn {book/ratings: (?r rating/book ?b)})
//! {book/title,
//!  book/isbn,
//!  book/ratings: {
//!     pull: (?r rating/book ?b)
//!  },
//!  }
//!
//! ?r (rating/book {book/ratings})

// use std::borrow::Cow;
// use std::convert::TryFrom;
// use std::str::FromStr;
use std::path::PathBuf;

// #[repr(transparent)]
// struct Entity<'a>(Cow<'a, str>);

// #[derive(Debug, Eq, PartialEq)]
// #[repr(transparent)]
// struct Attribute<'a>(Cow<'a, str>);
//
// // impl<'a> Clone for Attribute<'a> {
// //     fn clone(&self) -> Self {
// //         Attribute(self.0.clone())
// //     }
// // }
//
// impl<'a> TryFrom<&'a str> for Attribute<'a> {
//     type Error = ();
//
//     fn try_from(s: &'a str) -> Result<Self, Self::Error> {
//         if !s.starts_with(":") {
//             return Err(());
//         }
//         return Ok(Attribute(s.into()));
//     }
// }
//
// impl<'a> TryFrom<String> for Attribute<'a> {
//     type Error = ();
//
//     fn try_from(s: String) -> Result<Self, Self::Error> {
//         if !s.starts_with(":") {
//             return Err(());
//         }
//         return Ok(Attribute(s.into()));
//     }
// }
//
// #[derive(Debug, Eq, PartialEq)]
// #[repr(transparent)]
// struct Variable<'a>(Cow<'a, str>);

// #[test]
// fn parse() {
//     use std::convert::TryInto;
//     assert_eq!(
//         Ok(Attribute(":entity/uuid".into())),
//         ":entity/uuid".try_into(),
//     );
//
//     assert_eq!(
//         Ok(Attribute(":entity/uuid".into())),
//         ":entity/uuid".to_string().try_into(),
//     );
// }

#[derive(Debug, thiserror::Error)]
enum ArgError<'a> {
    #[error("unknown argument {}", .0)]
    Unknown(&'a str),
    #[error("expected value for {}", .0)]
    NeedsValue(&'a str),
    #[error("invaid value for {}: {}", .0, .1)]
    Invalid(&'a str, anyhow::Error),
}

impl<'a> ArgError<'a> {
    fn invalid(arg: &'a str) -> impl Fn(anyhow::Error) -> ArgError<'a> {
        move |e| ArgError::Invalid(arg, e)
    }
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
        path: PathBuf,
        map: (&'a str, Vec<&'a str>),
        patterns: Vec<oof::Pattern<'a, oof::Value>>,
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
                let mut sess = oof::Session::new(&mut conn)?;

                let mut stdin = std::io::stdin();
                let stuff: Woo<std::collections::HashMap<oof::AttributeName, oof::Value>> =
                    serde_json::from_reader(&mut stdin)?;

                match stuff {
                    Woo::Many(stuff) => eprintln!("{:?}", stuff),
                    Woo::One(stuff) => eprintln!("{:?}", stuff),
                };

                #[derive(serde::Deserialize)]
                #[serde(untagged)]
                enum Woo<T> {
                    Many(Vec<T>),
                    One(T),
                }

                Ok(())
            }
            Command::Query {
                path,
                map: (map_var, map_attrs),
                patterns,
                limit,
            } => {
                let mut conn = rusqlite::Connection::open_with_flags(
                    &path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                        | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
                )?;
                let mut sess = oof::Session::new(&mut conn)?;

                let map_attrs: Vec<_> = map_attrs
                    .into_iter()
                    .map(oof::AttributeName::from)
                    .collect();
                let mut p = oof::Projection::from_patterns(&patterns);
                let mut attrs = p.attribute_map(map_var, &map_attrs);
                attrs.limit = limit;

                let results = oof::query_attribute_map(&attrs, &mut sess)?;

                let jaysons = serde_json::to_string_pretty(&results)?;
                println!("{}", jaysons);

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
                "usage: {} [--db <path>] [<pattern>...] --map <map> [--limit <num>]",
                prog
            );
            eprintln!("       {} [--db <path>] assert", prog);
            eprintln!("       {} [--db <path>] init", prog);
            eprintln!("<pattern> is ...TODO");
            eprintln!("<map>     is ?var :some/attribute [:some/attribute...]");
            std::process::exit(1);
        }
        Ok(cmd) => {
            eprintln!("{:#?}", cmd);
            if let Err(e) = cmd.run() {
                println!("error: {}", e);
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

fn parse_args<'a, I: Iterator<Item = &'a str>>(mut args: I) -> Result<Command<'a>, ArgError<'a>> {
    let mut patterns = vec![];
    let mut db = Option::<&str>::None;
    let mut map = Option::<&str>::None;
    let mut limit = Option::<&str>::None;

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
            "--map" => {
                map = Some(args.next().ok_or(ArgError::NeedsValue(arg))?);
            }
            "--limit" => {
                limit = Some(args.next().ok_or(ArgError::NeedsValue(arg))?);
            }
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

    let map = parse_map(map.unwrap_or("?_ :entity/uuid")).map_err(ArgError::invalid("--map"))?;

    let limit = limit
        .map(|s| {
            s.parse()
                .map_err(|e| anyhow::format_err!("could not parse i64: {}", e))
                .map_err(ArgError::invalid("--limit"))
        })
        .transpose()?
        .unwrap_or_default();

    return Ok(Command::Query {
        path,
        map,
        limit,
        patterns,
    });
}

fn parse_pattern<'a>(s: &'a str) -> anyhow::Result<oof::Pattern<'a, oof::Value>> {
    let mut parts = s.splitn(3, |s: char| s.is_ascii_whitespace());
    match (parts.next(), parts.next(), parts.next()) {
        (Some(e), Some(a), Some(v)) => {
            let entity = parse_variable(e)
                .map(|var| oof::VariableOr::Variable(std::borrow::Cow::from(var)))
                .or_else(|_| {
                    e.parse::<uuid::Uuid>()
                        .map(oof::EntityName::from)
                        .map(oof::VariableOr::Value)
                })?;

            let attribute = parse_variable(a)
                .map(|var| oof::VariableOr::Variable(std::borrow::Cow::from(var)))
                .or_else(|_| {
                    parse_attribute(a)
                        .map(oof::AttributeName::from)
                        .map(oof::VariableOr::Value)
                })?;

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

fn parse_map<'a>(s: &'a str) -> anyhow::Result<(&'a str, Vec<&'a str>)> {
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

fn parse_attribute<'a>(s: &'a str) -> anyhow::Result<&'a str> {
    if !s.starts_with(":") {
        anyhow::bail!("missing ':' prefix for {}", s)
    }
    Ok(&s[1..])
}