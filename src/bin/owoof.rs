//! query, assert, or retract triplets

use std::error::Error;
use std::path::PathBuf;

use owoof::disperse::zip_with_keys;
use owoof::driver::just;
use owoof::network::TriplesField;
use owoof::retrieve::{self, Pattern, Variable};
use owoof::{either, sql::PushToQuery, AttributeRef, DontWoof, Ordering, Value, ValueRef};

use anyhow::Context;

const OPEN_RW: rusqlite::OpenFlags =
    rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE.union(rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX);

const OPEN_CREATE: rusqlite::OpenFlags = OPEN_RW.union(rusqlite::OpenFlags::SQLITE_OPEN_CREATE);

// type Object = std::collections::BTreeMap<owoof::Attribute, owoof::Value>;
#[derive(serde::Deserialize)]
struct Object {
    #[serde(rename = ":db/id")]
    id: Option<owoof::Entity>,
    #[serde(flatten)]
    other: std::collections::BTreeMap<owoof::Attribute, owoof::Value>,
}

fn main() {
    let args_vec = std::env::args().collect::<Vec<String>>();
    let mut args = args_vec.iter().map(String::as_str);

    let exe = args
        .next()
        .map(|s| s.rsplit('/').next().unwrap_or(s))
        .unwrap_or("owoof");

    let parsed = parse_args(args).unwrap_or_else(|err| match err {
        ArgError::Usage => usage_and_exit(exe),
        _ => {
            eprintln!("oof! {}", &err);
            print_traceback(&err);
            eprintln!("");
            usage_and_exit(exe)
        }
    });

    match &parsed.mode {
        Mode::Find => do_find(parsed),
        Mode::Init => do_init(parsed),
        Mode::Assert => do_assert(parsed),
        Mode::Retract => do_retract(parsed),
    }
    .unwrap_or_else(|err: anyhow::Error| {
        eprintln!("oof! {}", &err);
        print_traceback(err.as_ref());
        eprintln!("");
        std::process::exit(1);
    });
}

fn print_traceback(err: &dyn Error) {
    let mut source = err.source();
    while let Some(err) = source {
        eprintln!("   » {}", err);
        source = err.source();
    }
}

fn do_find(find: Args) -> anyhow::Result<()> {
    let mut db = rusqlite::Connection::open_with_flags(&find.path, OPEN_RW)?;
    let woof = DontWoof::new(&mut db)?;

    let mut network = retrieve::NamedNetwork::<ValueRef>::default();

    for pattern in find.patterns.iter() {
        network.add_pattern(pattern);
    }

    let mut retreival = vec![];
    let mut dispersal = find
        .show
        .iter()
        .map(|show| -> anyhow::Result<_> {
            let field = network
                .names
                .lookup(&show.variable)
                .with_context(|| anyhow::anyhow!("cannot show `{}`", &show.variable))?;

            /* If there are no attributes, show ?var.
             * If attributes are given, then show the value of
             * those attributes where ?var is the entity. */
            if show.attributes.is_empty() {
                retreival.push(field);

                Ok(either::left(just::<Value>()))
            } else {
                retreival.extend(
                    show.attributes
                        .iter()
                        .map(|&attribute| network.value_for_entity_attribute(field, attribute)),
                );

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
            order_by.extend(show.attributes.iter().map(|&attribute| {
                let field = network.value_for_entity_attribute(field, attribute);
                (field, *ordering)
            }));
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
    Ok(())
}

fn do_init(init: Args) -> anyhow::Result<()> {
    let mut db = rusqlite::Connection::open_with_flags(&init.path, OPEN_CREATE)?;
    let tx = db.transaction()?;
    owoof::create_schema(&tx)?;
    tx.commit()?;
    eprintln!("{}", init.path.display());
    Ok(())
}

fn do_assert(assert: Args) -> anyhow::Result<()> {
    let mut input = open_check_tty(assert.input.as_ref())?;

    let mut db = rusqlite::Connection::open_with_flags(&assert.path, OPEN_RW)?;
    let woof = DontWoof::new(&mut db)?;

    /* TODO this error message is not helpful */
    let stuff: either::Either<Object, Vec<Object>> = serde_json::from_reader(&mut input)?;
    // let stuff: Object = serde_json::from_reader(&mut input)?;
    // let stuff: either::Either<Object, Vec<Object>> = either::left(stuff);

    let mut ident_cache = std::collections::BTreeMap::default();

    let asserted = match stuff {
        either::Left(one) => vec![one].into_iter(),
        either::Right(v) => v.into_iter(),
    }
    .map(|obj| {
        let Object { id, other } = obj;
        let (e, id) = match id {
            Some(id) => (woof.encode(id)?, id),
            None => {
                let e = woof.new_entity()?;
                (e, woof.decode(e)?)
            }
        };

        other
            .into_iter()
            .map(|(ident, v)| {
                let a = match ident_cache.get(&ident).cloned() {
                    Some(a) => a,
                    None => {
                        let encoded = woof.encode(&ident)?;
                        let a = woof
                            .attribute(encoded)
                            .with_context(|| format!("attribute {}", &ident))?;
                        if ident_cache.len() < 256 {
                            ident_cache.insert(ident.clone(), a);
                        }
                        a
                    }
                };
                let v = woof.encode(v)?;
                woof.assert(e, a, v)?;
                Ok(())
            })
            .collect::<anyhow::Result<()>>()
            .map(|()| id)
    })
    .collect::<Result<Vec<owoof::Entity>, _>>()?;

    woof.into_tx().commit().context("commit")?;

    let jaysons = if asserted.len() == 1 {
        serde_json::to_string_pretty(&asserted[0])
    } else {
        serde_json::to_string_pretty(&asserted)
    }
    .context("serialize results")?;

    eprintln!("{}", jaysons);

    Ok(())
}

fn do_retract(retract: Args) -> anyhow::Result<()> {
    let mut input = open_check_tty(retract.input.as_ref())?;

    let mut db = rusqlite::Connection::open_with_flags(&retract.path, OPEN_RW)?;
    let woof = DontWoof::new(&mut db)?;

    let stuff: either::Either<Object, Vec<Object>> = serde_json::from_reader(&mut input)?;

    let mut ident_cache = std::collections::BTreeMap::default();

    match stuff {
        either::Left(one) => vec![one].into_iter(),
        either::Right(v) => v.into_iter(),
    }
    .map(|obj| {
        let Object { id, other } = obj;
        let id = id.context(":db/id is required to retract")?;
        let e = woof.encode(id)?;

        other
            .into_iter()
            .map(|(ident, v)| {
                let a = match ident_cache.get(&ident).cloned() {
                    Some(a) => a,
                    None => {
                        let encoded = woof.encode(&ident)?;
                        let a = woof
                            .attribute(encoded)
                            .with_context(|| format!("attribute {}", &ident))?;
                        if ident_cache.len() < 256 {
                            ident_cache.insert(ident.clone(), a);
                        }
                        a
                    }
                };
                let v = woof.encode(v)?;
                woof.retract(e, a, v)?;
                Ok(())
            })
            .collect::<anyhow::Result<Sum>>()
    })
    .collect::<anyhow::Result<Sum>>()
    .and_then(|n: Sum| {
        // woof.into_tx().commit().context("commit")?;
        eprintln!("{}", n.usize());
        Ok(())
    })
}

fn usage_and_exit(exe: &str) -> ! {
    eprintln!("usage: {} [--db <path>] <pattern>... [--show <show>] [--limit <num>] [--asc <show>] [--desc <show>] [--find|--explain|--explain-plan]", exe);
    eprintln!("       {} [--db <path>] init", exe);
    eprintln!("       {} [--db <path>] assert  [--input <path>]", exe);
    eprintln!("       {} [--db <path>] retract [--input <path>]", exe);
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

struct Args<'a> {
    mode: Mode,
    path: PathBuf,
    input: Option<PathBuf>,
    show: Vec<Show<'a>>,
    patterns: Vec<Pattern<'a, Value>>,
    order: Vec<(Show<'a>, Ordering)>,
    limit: i64,
    explain: bool,
    // explain_plan: bool,
}

enum Mode {
    Find,
    Init,
    Assert,
    Retract,
}

fn parse_args<'a, I>(mut args: I) -> Result<Args<'a>, ArgError<'a>>
where
    I: Iterator<Item = &'a str>,
{
    let mut mode = Mode::Find;
    let mut db = Option::<&str>::None;
    let mut input = Option::<&str>::None;
    let mut limit = Option::<&str>::None;
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
            "--input" => {
                input.replace(args.next().ok_or(ArgError::NeedsValue(arg))?);
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
            "init" => mode = Mode::Init,
            "assert" => mode = Mode::Assert,
            "retract" => mode = Mode::Retract,
            "--" => {
                patterns.extend(args);
                break;
            }
            _ if arg.starts_with("-") => return Err(ArgError::Unknown(arg)),
            _ => patterns.push(arg),
        }
    }

    if matches!(mode, Mode::Find) && patterns.is_empty() {
        return Err(ArgError::Usage); /* ¯\_(ツ)_/¯*/
    }

    Ok(Args {
        mode,
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
        input: db
            .map(|s| {
                s.parse()
                    .context("parse input path")
                    .map_err(ArgError::invalid("--input"))
            })
            .transpose()?,
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

#[derive(Default, Clone, Copy)]
struct Sum(usize);

impl Sum {
    fn usize(self) -> usize {
        self.0
    }
}

impl std::iter::FromIterator<()> for Sum {
    fn from_iter<T: IntoIterator<Item = ()>>(iter: T) -> Self {
        Sum(iter.into_iter().fold(0usize, |sum, _| sum + 1))
    }
}

impl std::iter::FromIterator<Sum> for Sum {
    fn from_iter<T: IntoIterator<Item = Sum>>(iter: T) -> Self {
        Sum(iter.into_iter().fold(0usize, |sum, x| sum + x.usize()))
    }
}

impl std::iter::FromIterator<usize> for Sum {
    fn from_iter<T: IntoIterator<Item = usize>>(iter: T) -> Self {
        Sum(iter.into_iter().fold(0usize, |sum, x| sum + x))
    }
}
