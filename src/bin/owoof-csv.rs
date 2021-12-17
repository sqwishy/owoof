//! import one csv into a sqlite database with owoof

use std::borrow::Cow;
use std::error::Error;
use std::path::PathBuf;
use std::{fs, io, iter};

use owoof::{AttributeRef, DontWoof, Optional, ValueRef};

use rusqlite::OpenFlags;

use anyhow::Context;

#[derive(Debug)]
struct Args<'a> {
    db: PathBuf,
    input: Option<PathBuf>,
    mappings: Vec<Mapping<'a>>,
    dry_run: bool,
    limit: usize,
}

#[derive(Debug)]
struct Mapping<'a> {
    attribute: &'a AttributeRef,
    column: Cow<'a, str>,
}

fn do_import<'a>(args: Args<'a>) -> anyhow::Result<()> {
    let input: Box<dyn io::Read> = match args.input {
        Some(path) => Box::new(fs::File::open(path).map(io::BufReader::new)?),
        None => {
            if atty::is(atty::Stream::Stdin) {
                eprintln!("reading csv from stdin (and stdin looks like a tty) good luck!");
            }
            Box::new(io::stdin())
        }
    };

    let mut reader = csv::Reader::from_reader(input);
    let headers = reader.headers()?;
    let take_indices = args
        .mappings
        .iter()
        .map(|mapping| {
            headers
                .iter()
                .position(|h| h == mapping.column)
                .ok_or(mapping)
        })
        .collect::<Result<Vec<usize>, _>>()
        .map_err(|mapping| {
            let headers = headers
                .iter()
                .flat_map(|s| iter::once("\n» ").chain(iter::once(s)))
                .collect::<String>();
            anyhow::anyhow!(
                "failed find column `{}` in headers:{}",
                mapping.column,
                headers
            )
        })?;

    if args.dry_run {
        eprintln!("the following mappings were planned");

        args.mappings.iter().for_each(|mapping| {
            eprintln!("{}\t{}", mapping.attribute, mapping.column);
        });

        eprintln!("but this is a dry run, nothing will be imported");
        return Ok(());
    }

    let mut db = rusqlite::Connection::open_with_flags(
        &args.db,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let tx = db.transaction()?;
    let woof = DontWoof::from(tx);

    let db_attribute = woof.attribute(woof.encode(AttributeRef::from_static(":db/attribute"))?)?;

    let attributes = args
        .mappings
        .iter()
        .map(|m| {
            let ident = woof.encode(m.attribute)?;
            match woof.attribute(ident).optional()? {
                Some(attribute) => Ok(attribute),
                None => woof
                    .fluent_entity()?
                    .assert(db_attribute, ident)
                    .map(owoof::Encoded::<owoof::Entity>::from),
            }
        })
        .collect::<Result<Vec<_>, _>>()
        .context("encode attribute")?;

    let mut records_seen = 0usize;
    let mut limit = (0 != args.limit).then(|| args.limit);

    let mut record = csv::StringRecord::new();
    while Some(0) != limit && reader.read_record(&mut record)? {
        let e = woof.new_entity()?;

        take_indices
            .iter()
            .zip(attributes.iter().cloned())
            .map(|(&idx, a)| {
                let text = record.get(idx).context("no value")?;
                woof.encode(parse_value(text))
                    .and_then(|v| woof.assert(e, a, v).map(drop))
                    .with_context(|| format!("failed to assert {:?}", text))
            })
            .zip(args.mappings.iter())
            .map(|(res, map)| res.with_context(|| format!("for column {:?}", map.column)))
            .collect::<anyhow::Result<()>>()
            .with_context(|| match record.position() {
                Some(p) => format!("on line {}", p.line()),
                None => format!("on line ???"),
            })?;

        records_seen += 1;
        limit.as_mut().map(|l| *l -= 1);
    }

    woof.optimize()?;
    woof.into_tx().commit()?;

    eprintln!("imported {} rows/entities", records_seen);

    Ok(())
}

pub fn parse_value(s: &str) -> ValueRef<'_> {
    Option::<ValueRef>::None
        .or_else(|| s.parse::<owoof::Entity>().map(ValueRef::from).ok())
        .or_else(|| s.try_into().map(ValueRef::Attribute).ok())
        .or_else(|| s.parse::<bool>().map(ValueRef::from).ok())
        .or_else(|| s.parse::<i64>().map(ValueRef::from).ok())
        .or_else(|| s.parse::<f64>().map(ValueRef::from).ok())
        .or_else(|| s.parse::<uuid::Uuid>().map(ValueRef::from).ok())
        .unwrap_or(ValueRef::Text(s))
}

fn main() {
    let args_vec = std::env::args().collect::<Vec<String>>();
    let mut args = args_vec.iter().map(String::as_str);

    let exe = args
        .next()
        .map(|s| s.rsplit('/').next().unwrap_or(s))
        .unwrap_or("owoof-csv");

    match parse_args(args) {
        Err(ArgError::Usage) => usage_and_exit(exe),
        Err(err) => {
            eprintln!("oof! {}", err);
            print_traceback(&err);
            eprintln!("");
            usage_and_exit(exe)
        }
        Ok(args) => {
            if let Err(err) = do_import(args) {
                eprintln!("oof! {}", err);
                print_traceback(err.as_ref());
                std::process::exit(1);
            }
        }
    }
}

fn print_traceback(err: &dyn Error) {
    let mut source = err.source();
    while let Some(err) = source {
        eprintln!("   » {}", err);
        source = err.source();
    }
}

fn usage_and_exit(exe: &str) -> ! {
    eprintln!("usage: {} [options...] <mappings...>", exe);
    eprintln!("");
    eprintln!("[options...] is a sequence of any of the following.");
    eprintln!("\t-n, --dry-run");
    eprintln!(
        "\t--db <{}> (defaults to OWOOF_DB environment variable)",
        default_db_path().display()
    );
    eprintln!("\t-i, --input <input.csv> (defaults to stdin)");
    eprintln!("");
    eprintln!("<mappings...> is a sequence that arguments that map csv headers to attributes.");
    eprintln!("\t':pet/name pet_name'\twill read values in the column pet_name and assert them with the :pet/name attribute");
    eprintln!("\t':pet/name'         \twill defaults the column name to `name`, the part after / with non-alphabet characters replaced with _");
    eprintln!("");
    eprintln!("We try to convert values into an entity, attribute, number, or uuid before giving up and just inserting it as text.");
    std::process::exit(2);
}

fn parse_args<'a, I>(mut args: I) -> Result<Args<'a>, ArgError<'a>>
where
    I: Iterator<Item = &'a str>,
{
    let mut db = Option::<&str>::None;
    let mut input = Option::<&str>::None;
    let mut mappings = Vec::<&str>::default();
    let mut dry_run = false;
    let mut limit = 0usize;

    while let Some(arg) = args.next() {
        match arg {
            "-h" | "--help" => return Err(ArgError::Usage),
            "-n" | "--dry-run" => dry_run = true,
            "-l" | "--limit" => {
                limit = args
                    .next()
                    .ok_or(ArgError::NeedsValue(arg))?
                    .parse()
                    .map_err(ArgError::invalid(arg))?
            }
            "--db" => {
                db.replace(args.next().ok_or(ArgError::NeedsValue(arg))?);
            }
            "-i" | "--input" => {
                input.replace(args.next().ok_or(ArgError::NeedsValue(arg))?);
            }
            "--" => {
                mappings.extend(args);
                break;
            }
            _ if arg.starts_with("-") => return Err(ArgError::Unknown(arg)),
            _ => mappings.push(arg),
        }
    }

    Ok(Args {
        dry_run,
        limit,
        db: db
            .map(|s| {
                s.parse()
                    .context("parse --db")
                    .map_err(ArgError::invalid("--db"))
            })
            .unwrap_or_else(|| Ok(default_db_path()))?,
        input: input
            .map(|s| {
                s.parse()
                    .context("parse input csv")
                    .map_err(ArgError::invalid("--csv"))
            })
            .transpose()?,
        mappings: mappings
            .into_iter()
            .map(|s| parse_mapping(s))
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(ArgError::invalid("<mappings...>"))?,
    })
}

fn parse_mapping<'a>(s: &'a str) -> anyhow::Result<Mapping<'a>> {
    let attribute = s.split_whitespace().next().unwrap_or(s);
    let rest = s[attribute.len()..].trim();

    let attribute: &AttributeRef = attribute.try_into().context("parse attribute")?;

    let column = if rest.is_empty() {
        guess_csv_header_from_attribute(attribute).into()
    } else {
        rest.into()
    };

    Ok(Mapping { attribute, column })
}

fn guess_csv_header_from_attribute(attribute: &AttributeRef) -> String {
    let mut s = attribute
        .tail()
        .chars()
        .skip_while(|c| !c.is_alphabetic())
        .map(|c| if c.is_alphabetic() { c } else { '_' })
        .collect::<String>();
    if s.ends_with('_') {
        s = s.trim_end_matches('_').to_owned()
    }
    s
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
    fn invalid<I: Into<anyhow::Error>>(arg: &'a str) -> impl Fn(I) -> ArgError<'a> {
        move |e| ArgError::Invalid(arg, e.into())
    }
}

fn default_db_path() -> PathBuf {
    std::env::var_os("OWOOF_DB")
        .map(PathBuf::from)
        .unwrap_or("owoof.sqlite".into())
}
