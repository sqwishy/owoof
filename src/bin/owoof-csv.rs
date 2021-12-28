//! import one csv into a sqlite database with owoof

use std::borrow::Cow;
use std::error::Error;
use std::iter;
use std::path::PathBuf;

use owoof::{AttributeRef, DontWoof, Optional};

use rusqlite::OpenFlags;

use anyhow::Context;

#[derive(Debug)]
struct Args<'a> {
    db: PathBuf,
    input: Option<PathBuf>,
    mappings: Vec<ToAttribute<'a>>,
    dry_run: bool,
    limit: usize,
    output: bool,
    csv_delimiter: u8,
}

#[derive(Debug)]
struct ToAttribute<'a> {
    column: Cow<'a, str>,
    attribute: &'a AttributeRef,
}

#[derive(Debug)]
struct ToPosition<'a> {
    attribute: &'a AttributeRef,
    position: usize,
}

fn do_import<'a>(args: Args<'a>) -> anyhow::Result<()> {
    let input = open_check_tty(args.input.as_ref())?;

    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .delimiter(args.csv_delimiter)
        .from_reader(input);
    let headers = reader.headers()?;
    let mut to_positions = lookup_header_indices(headers, args.mappings.as_slice())?;

    if args.dry_run {
        eprintln!("the following mappings were planned");

        for mapping in args.mappings {
            eprintln!("{}\t{}", mapping.attribute, mapping.column);
        }

        eprintln!("but this is a dry run, nothing will be imported");
        return Ok(());
    }

    let id_mapping: Option<ToPosition> = to_positions
        .iter()
        .position(|m| m.attribute == AttributeRef::from_static(":db/id"))
        .map(|i| to_positions.remove(i));

    let mut db = rusqlite::Connection::open_with_flags(
        &args.db,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let woof = DontWoof::new(&mut db)?;

    /* attribute identifiers -> attribute entities */
    let attributes = find_or_assert_attributes(&woof, args.mappings.as_slice())?;

    let mut records_seen = 0usize;
    let mut limit = (0 != args.limit).then(|| args.limit);
    let mut output = if args.output {
        let mut w = csv::WriterBuilder::new()
            .delimiter(args.csv_delimiter)
            .from_writer(io::stdout());
        /* write headers */
        w.write_record(
            &iter::once(":db/id")
                .chain(headers.iter())
                .collect::<csv::StringRecord>(),
        )?;
        Some(w)
    } else {
        None
    };

    let mut record = csv::StringRecord::new();
    while Some(0) != limit && reader.read_record(&mut record)? {
        let e = if let Some(ToPosition { position, .. }) = id_mapping {
            let entity = record
                .get(position)
                .context("no value")?
                .parse::<owoof::Entity>()
                .context("parse entity")?;
            woof.encode(entity)?
        } else {
            woof.new_entity()?
        };

        to_positions
            .iter()
            .zip(attributes.iter().cloned())
            .map(|(to, a): (&ToPosition, _)| {
                let text = record.get(to.position).context("no value")?;
                let value: owoof::Value = todo!();
                woof.encode(value)
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

        if let Some(output) = output.as_mut() {
            output.write_record(
                &iter::once(woof.decode(e)?.to_string().as_str())
                    .chain(record.iter())
                    .collect::<csv::StringRecord>(),
            )?;
        }
    }

    woof.optimize()?;
    woof.into_tx().commit()?;

    eprintln!("imported {} rows/entities", records_seen);

    Ok(())
}

fn lookup_header_indices<'a>(
    headers: &csv::StringRecord,
    mappings: &[ToAttribute<'a>],
) -> anyhow::Result<Vec<ToPosition<'a>>> {
    mappings
        .iter()
        .map(|to| {
            let ToAttribute { attribute, column } = to;
            headers
                .iter()
                .position(|h| h == column)
                .map(|position| ToPosition { attribute, position })
                .ok_or(column)
        })
        .collect::<Result<Vec<ToPosition<'a>>, _>>()
        .map_err(|column| {
            let headers = headers
                .iter()
                .flat_map(|s| iter::once("\n» ").chain(iter::once(s)))
                .collect::<String>();
            anyhow::anyhow!("failed find column `{}` in headers:{}", column, headers)
        })
}

fn find_or_assert_attributes<'a>(
    woof: &DontWoof,
    mappings: &[ToAttribute<'a>],
) -> anyhow::Result<Vec<owoof::Encoded<owoof::Entity>>> {
    let db_attribute = woof.attribute(woof.encode(AttributeRef::from_static(":db/attribute"))?)?;

    mappings
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
        .context("encode attribute")
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
    eprintln!("\t-l, --limit N\timport only N rows, import everything if N is zero");
    eprintln!("\t-n, --dry-run\tcheck csv mappings but don't modify the database");
    eprintln!("\t-o, --output \twrites inserted :db/id to stdout (see below for more detail)");
    eprintln!(
        "\t--db         \t<{}> (defaults to OWOOF_DB environment variable)",
        default_db_path().display()
    );
    eprintln!("\t-i, --input <input.csv> (defaults to stdin)");
    eprintln!("");
    eprintln!("<mappings...> is a sequence that arguments that map csv headers to attributes.");
    eprintln!("\t':pet/name pet_name'\twill read values in the column pet_name and assert them with the :pet/name attribute");
    eprintln!("\t':pet/name'         \twill defaults the column name to `name`, the part after / with non-alphabet characters replaced with _");
    eprintln!("");
    eprintln!("Each row imported is an entity added to the database.  When --output is passed, a copy of the input csv is written to stdout along with a :db/id column that includes the entity id of each row.");
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
    let mut output = false;
    let mut csv_delimiter = ",";

    while let Some(arg) = args.next() {
        match arg {
            "-h" | "--help" => return Err(ArgError::Usage),
            "-n" | "--dry-run" => dry_run = true,
            "-o" | "--output" => output = true,
            "-d" | "--delimiter" => {
                csv_delimiter = args.next().ok_or(ArgError::NeedsValue(arg))?
            }

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
        output,
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
        csv_delimiter: {
            (csv_delimiter.len() == 1)
                .then(|| csv_delimiter.bytes().next().unwrap())
                .context("expected a single byte")
                .map_err(ArgError::invalid("--delimiter"))?
        },
    })
}

fn parse_mapping<'a>(s: &'a str) -> anyhow::Result<ToAttribute<'a>> {
    let attribute = s.split_whitespace().next().unwrap_or(s);
    let rest = s[attribute.len()..].trim();

    let attribute: &AttributeRef = attribute.try_into().context("parse attribute")?;

    let column = if rest.is_empty() {
        guess_csv_header_from_attribute(attribute).into()
    } else {
        rest.into()
    };

    Ok(ToAttribute { attribute, column })
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

use std::{fs, io};

pub fn open_check_tty(input: Option<&PathBuf>) -> io::Result<Box<dyn io::Read>> {
    match input {
        Some(path) => {
            let file = fs::File::open(path)?;
            Ok(Box::new(io::BufReader::new(file)))
        }
        None => {
            if atty::is(atty::Stream::Stdin) {
                eprintln!("reading csv from stdin (and stdin looks like a tty) good luck!");
            }
            Ok(Box::new(io::stdin()))
        }
    }
}
