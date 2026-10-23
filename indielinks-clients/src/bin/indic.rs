// Copyright (C) 2025-2026 Michael Herstine <sp1ff@pobox.com>
//
// This file is part of indielinks.
//
// indielinks is free software: you can redistribute it and/or modify it under the terms of the GNU
// General Public License as published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// indielinks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
// even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// General Public License for more details.
//
// You should have received a copy of the GNU General Public License along with indielinks.  If not,
// see <http://www.gnu.org/licenses/>.

//! # indic - The indielinks client
//!
//! [indic](crate) (think "Indie-C") is a general purpose [indielinks] client. It currently supports
//! a few sub-commands around importing and adding links, but I anticipate it growing.
//!
//! [indielinks]: ../indielinksd/index.html

use std::{
    collections::{HashMap, HashSet},
    env::{self, VarError},
    fmt::Debug,
    fs::{self},
    io::ErrorKind,
    num::NonZero,
    path::PathBuf,
    result::Result as StdResult,
};

use clap::{
    builder::NonEmptyStringValueParser, crate_authors, crate_version, parser::ValueSource,
    value_parser, Arg, ArgAction, ArgMatches, Command,
};
use http::HeaderValue;
use nonzero::nonzero;
use secrecy::SecretString;
use serde::Deserialize;
use snafu::{Backtrace, IntoError, OptionExt, ResultExt, Snafu};
use tracing::{debug, Level};
use tracing_subscriber::{util::SubscriberInitExt, EnvFilter, FmtSubscriber};
use url::Url;

use indielinks_shared::{entities::Tagname, known_good, origin::Origin};

use indielinks_clients::{
    add_link::add_link, client::make_indielinks_client, import_onetab::import_onetab,
    import_pinboard::import_pinboard,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Snafu)]
enum Error {
    #[snafu(display("While adding a link, {source}"))]
    AddLink {
        #[snafu(source(from(indielinks_clients::add_link::Error, Box::new)))]
        source: Box<indielinks_clients::add_link::Error>,
    },
    #[snafu(display("The API origin must be specified, either in config or on the command line"))]
    Api,
    #[snafu(display("unable to construct an HTTP client for indielinksd"))]
    Client {
        source: indielinks_clients::client::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("unable to read a configuration file at {}", path.display()))]
    Configuration {
        path: PathBuf,
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("unable to locate a file at {}", path.display()))]
    ConfigurationLocation {
        path: PathBuf,
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("failed to parse your configuration file {}", path.display()))]
    ConfigurationParse {
        path: PathBuf,
        #[snafu(source(from(toml::de::Error, Box::new)))]
        source: Box<toml::de::Error>,
        backtrace: Backtrace,
    },
    #[snafu(display("Exhausted the buffer without matching the predicate"))]
    Eob { backtrace: Backtrace },
    #[snafu(display("failed to read the API key from the environment"))]
    IndicApiKey {
        source: std::env::VarError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to find the initial '[']"))]
    InitialParse,
    #[snafu(display(
        "the location of the indielinksd operational API must be given in either the \
                     configuration file or the command line"
    ))]
    MissingOpsApi { backtrace: Backtrace },
    #[snafu(display(
        "the location of the indielinksd public API must be given in either the \
                     configuration file or the command line"
    ))]
    MissingPublicApi { backtrace: Backtrace },
    #[snafu(display(
        "the token/API key must be given in either the configuration file or INDIO_API_KEY"
    ))]
    MissingToken { backtrace: Backtrace },
    #[snafu(display("No sub-command given; try --help"))]
    NoSubCommand,
    #[snafu(display(
        "Missing token; specify it with the --token option or in your configuration file"
    ))]
    NoToken,
    #[snafu(display("While importing links from OneTab, {source}"))]
    Onetab {
        #[snafu(source(from(indielinks_clients::import_onetab::Error, Box::new)))]
        source: Box<indielinks_clients::import_onetab::Error>,
        backtrace: Backtrace,
    },
    #[snafu(display(
        "The ops API origin must be specified, either in config or on the command line"
    ))]
    Ops,
    #[snafu(display("Failed to find the next ',' or ']'"))]
    Parse,
    #[snafu(display("While importing links from Pinboard, {source}"))]
    Pinboard {
        #[snafu(source(from(indielinks_clients::import_pinboard::Error, Box::new)))]
        source: Box<indielinks_clients::import_pinboard::Error>,
        backtrace: Backtrace,
    },
    #[snafu(display("Premature EOF while deserializing array elements"))]
    PrematureEof { backtrace: Backtrace },
    #[snafu(display("failed to initialize logging"))]
    Subscriber {
        source: tracing_subscriber::util::TryInitError,
        backtrace: Backtrace,
    },
    #[snafu(display("{target_name} is not a known target"))]
    UnknownTarget { target_name: String },
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> StdResult<(), std::fmt::Error> {
        write!(f, "{self}")
    }
}

type Result<T> = StdResult<T, Error>;

const INDIC_API_KEY: &str = "INDIC_API_KEY";

static USER_AGENT: HeaderValue = HeaderValue::from_static(concat!(
    "indic/",
    crate_version!(),
    " ( indielinks@pobox.com )"
));

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                            targets                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// A target is a preconfigured set of options for new posts
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct Target {
    pub replace: bool,
    pub shared: bool,
    #[serde(rename = "toread")]
    pub to_read: bool,
    pub tags: HashSet<Tagname>,
}

impl Default for Target {
    fn default() -> Self {
        Self {
            replace: true,
            shared: false,
            to_read: true,
            tags: HashSet::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         configuration                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

// This is perhaps overly ornate, but I'm trying to incorporate hard-won lessons in how to evolve
// file formats. To begin with I define a series of classes whose only point is to represent
// configuration file formats.

/// The current configuration file format.
///
/// At some point in the future, we'll presumably move to a new representation, call it
/// `ConfigurationRepr2`.
// Not `Serializes` due to the presence of a `SecretString`
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct ConfigurationRepr1 {
    token: Option<SecretString>,
    public_api: Option<Origin>,
    requests_per_minute: Option<NonZero<u32>>,
    targets: HashMap<String, Target>,
}

/// A unified configuration representation.
///
/// This adds a single field to the configuration file format, `version`.
// Caller code remains the same: `from_str::<ConfigurationRepr>(...)` will read a configuration file
// in any format version.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, tag = "version")]
enum ConfigurationRepr {
    #[serde(rename = "1")]
    V1(ConfigurationRepr1),
}

impl Default for ConfigurationRepr {
    fn default() -> Self {
        ConfigurationRepr::V1(ConfigurationRepr1::default())
    }
}

// Finally, we need to combine the contents of the configuration file (if any) with options
// specified on the command line and/or the environment.
#[derive(Clone, Debug)]
struct Configuration {
    token: SecretString,
    public_api: Origin,
    requests_per_minute: NonZero<u32>,
    targets: HashMap<String, Target>,
}

impl Configuration {
    pub fn new(
        from_file: ConfigurationRepr,
        public_api: Option<Origin>,
        token: Option<SecretString>,
    ) -> Result<Self> {
        // Today, `from_file` *must* be "V1"-- version one of our configuration file format. In
        // general, however, it will possibly be of any format version, and we'll have to provide a
        // way to build a `Configuration` instance from each version (and the values provided on the
        // command line and/or environment).
        Ok(match from_file {
            ConfigurationRepr::V1(from_file) => Self {
                token: token.unwrap_or(from_file.token.ok_or(MissingTokenSnafu.build())?),
                public_api: public_api
                    .unwrap_or(from_file.public_api.ok_or(MissingPublicApiSnafu.build())?),
                requests_per_minute: from_file.requests_per_minute.unwrap_or(nonzero!(120u32)),
                targets: from_file.targets,
            },
        })
    }
    pub fn public_api(&self) -> &Origin {
        &self.public_api
    }
    pub fn requests_per_minute(&self) -> NonZero<u32> {
        self.requests_per_minute
    }
    pub fn token(&self) -> &SecretString {
        &self.token
    }
    pub fn target(&self, name: &str) -> Option<&Target> {
        self.targets.get(name)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                     assorted utilities: logging configuration & like that                      //
////////////////////////////////////////////////////////////////////////////////////////////////////

fn parse_command_line() -> ArgMatches {
    Command::new("indic")
        .version(crate_version!())
        .author(crate_authors!())
        .about(color_print::cstr!("The <s>INDI</>elinks <s>C</>lient"))
        .long_about(color_print::cstr!(
            r#"<u>General-purpose client for working with indielinks</>

indic (think "indie-c") is a general-purpose utility for indielinks users.
Currently, it provides a few sub-commands around adding links in various ways,
but it will be built-out as circumstances warrant."#
        ))

        // In the past, I've pulled run-time configuration from command line options, environment
        // variables, and a configuration file (in that order of precedence). I'm not sure that's
        // really worth the effort. For now, I'm just going to put frequently configured items in
        // the command line (perhaps with an environment variable backup) and leave items that are
        // likely to change less frequently in a configuration file.
        .arg(
            dirs::config_dir()
                .into_iter()
                .fold(
                    Arg::new("configuration")
                        .short('c')
                        .long("configuration")
                        .num_args(1)
                        .value_parser(value_parser!(PathBuf))
                        .action(ArgAction::Set)
                        .env("INDIC_CONFIGURATION")
                        .help("The path to your indic configuration file"),
                    // I need something of type T, where T: IntoResettable<clap::builder::OsStr>
                    |arg, dir| arg.default_value(
                        dir.join(".indic.toml").as_mut_os_string().clone())
                )
        )
        .arg(
            Arg::new("public-api")
                .short('A')
                .long("public-api")
                .num_args(1)
                .value_parser(value_parser!(Origin))
                .action(ArgAction::Set)
                .env("INDIC_PUBLIC_API")
                .help(
                    r#"A network location at which the indielinks public API is being served
This option may also be specified in the configuration file."#,
                )
        )
        .arg(
            Arg::new("token")
                .short('t')
                .long("token")
                .num_args(1)
                .value_parser(value_parser!(SecretString))
                .env("INDIC_TOKEN")
                .help("The indielinks API token to be used for authentication")
        )
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .num_args(0)
                .action(ArgAction::SetTrue)
                .help("produce more prolix output"),
        )
        .subcommand(
            Command::new("import-pinboard")
                .about("Import links from Pinboard")
                .long_about(
                    "Import links from Pinboard.

At the time of this writing, the only import format supported is Pinboard JSON. You
can download your Pinboard links in JSON format by following the \"JSON\" link on
the page https://pinboard.in/settings/backup.
",
                )
                .arg(
                    Arg::new("skip")
                        .short('s')
                        .long("skip")
                        .required(false)
                        .num_args(1)
                        .value_parser(value_parser!(usize))
                        .help("Number of bookmarks in the input file to skip"),
                )
                .arg(
                    Arg::new("send")
                        .short('N')
                        .long("send")
                        .required(false)
                        .num_args(1)
                        .value_parser(value_parser!(usize))
                        .help("Number of bookmarks in the input file to send (after --skip'ping)"),
                )
                .arg(
                    Arg::new("chunk-size")
                        .short('c')
                        .long("chunk-size")
                        .required(false)
                        .num_args(1)
                        .value_parser(value_parser!(usize))
                        .help("The maximum number of /posts/add requests to indielinks that may be in-flight at any time"),
                )
                .arg(
                    Arg::new("FILE")
                        .required(true)
                        .value_parser(value_parser!(PathBuf))
                        .index(1) /* Better to be explicit, I think */
                        .help("Pinboard JSON-formatted file containing links to be imported to indielinks"),
                ),
        )
        .subcommand(
            Command::new("import-onetab")
                .about("Import links from OneTab")
                .long_about(
                    "Import links from OneTab.

You can get the OneTab browser plugin at https://www.one-tab.com/.")
                .arg(
                    Arg::new("skip")
                        .short('s')
                        .long("skip")
                        .required(false)
                        .num_args(1)
                        .value_parser(value_parser!(usize))
                        .help("Number of bookmarks in the input file to skip"),
                )
                .arg(
                    Arg::new("send")
                        .short('N')
                        .long("send")
                        .required(false)
                        .num_args(1)
                        .value_parser(value_parser!(usize))
                        .help("Number of bookmarks in the input file to send (after --skip'ping)"),
                )
                .arg(
                    Arg::new("chunk-size")
                        .short('c')
                        .long("chunk-size")
                        .required(false)
                        .num_args(1)
                        .value_parser(value_parser!(usize))
                        .help("The maximum number of /posts/add requests to indielinks that may be in-flight at any time"),
                )
                .arg(Arg::new("private").short('p').long("private")
                     .action(ArgAction::SetTrue)
                     .help("Make all imported links private"))
                .arg(
                    Arg::new("read-later")
                        .short('r')
                        .long("read-later")
                        .action(ArgAction::SetTrue)
                        .help("Set the \"read later\" attribute on all imported links")
                )
                .arg(
                    Arg::new("tag")
                        .short('t')
                        .long("tag")
                        .help("specify a tag to be applied-- may be given more than once")
                        .long_help("Tags may be up to 255 grapheme clusters in length and may not contain commas nor whitespace. Tags may be designated as private by beginning them with a '.'. More than one tag may be given by providing this option more than once (i.e. \"-t a -t b...\").")
                        .action(ArgAction::Append)
                        .value_parser(value_parser!(Tagname))
                )
                .arg(
                    Arg::new("FILE")
                        .required(true)
                        .value_parser(value_parser!(PathBuf))
                        .index(1) /* Better to be explicit, I think */
                        .help("OneTab formatted export file containing links to be imported to indielinks"),
                )
        )
        .subcommand(
            Command::new("add-link")
                .about("Add a link")
                .long_about("Add a single link to indielinks. The URL & title do not need
to be escaped; the implementation will handle that.")
                .arg(
                    Arg::new("replace")
                        .short('P')
                        .long("replace")
                        .num_args(0)
                        .action(ArgAction::SetTrue)
                        .help("Replace the current link, if present")
                )
                .arg(
                    Arg::new("shared")
                        .short('s')
                        .long("shared")
                        .num_args(0)
                        .action(ArgAction::SetTrue)
                        .help("Mark this link as public/shared")
                )
                .arg(
                    Arg::new("tag")
                        .short('t')
                        .long("tag")
                        .help("Specify a tag to be applied to this post-- may be given more than once")
                        .long_help("Tags may be up to 255 grapheme clusters in length and may not contain commas nor whitespace. Tags may be designated as private by beginning them with a '.'. More than one tag may be given by providing this option more than once (i.e. \"-t a -t b...\").")
                        .action(ArgAction::Append)
                        .value_parser(value_parser!(Tagname))
                )
                .arg(
                    Arg::new("target")
                        .short('r')
                        .long("target")
                        .help("pre-configured target for this link")
                        .long_help("Since one will likely re-use many of these options across invocations of this tool, it may be convenient to define them once in the configuration file & afterwards refer to that collection by name; we call such a pre-defined collection a \"target\". ")
                        .value_parser(value_parser!(String))
                        .required(false)
                )
                .arg(
                    Arg::new("title")
                        .short('T')
                        .long("title")
                        .required(true)
                        .num_args(1)
                        .value_parser(NonEmptyStringValueParser::new())
                        .help("Title to be used for this link")
                )
                .arg(
                    Arg::new("to-read")
                        .short('R')
                        .long("to-read")
                        .num_args(0)
                        .action(ArgAction::SetTrue)
                        .help("Mark this link as unread")
                )
                .arg(
                    Arg::new("URL")
                        .required(true)
                        .value_parser(value_parser!(Url))
                        .index(1) /* Better to be explicit, I think */
                        .help("The URL to be added to indielinks"))
        )
        .get_matches()
}

// RN, this duplicates the logic in `indio.rs`-- move up into `lib.rs`?
fn configure_logging(verbose: bool) -> Result<()> {
    FmtSubscriber::builder()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(if verbose { Level::DEBUG } else { Level::INFO }.into())
                .from_env_lossy()
                // I'd *really* like to suppress these two only if they're not mentioned in
                // RUST_LOG, but I can't see how to do that, and the odds that the user's going to
                // want to be interested in these two crates are low.
                .add_directive(known_good!("hyper_util=info".parse()))
                .add_directive(known_good!("tower=info".parse())),
        )
        .with_file(false)
        .with_level(false)
        .with_line_number(false)
        .with_target(false)
        .without_time()
        .with_writer(std::io::stdout)
        .finish()
        .try_init()
        .context(SubscriberSnafu)
}

// All the logic for finding the configuration file, parsing it & combining that with assorted
// command-line arguments & environment variables is in here.
fn parse_configuration(
    configuration_path: PathBuf,
    configuration_path_source: ValueSource,
    public_api: Option<Origin>,
) -> Result<Configuration> {
    // Here's how we resolve configuration: if the user provided a path to a configuration file on
    // the command line, and it's not there, fail (they presumably want to know about that). Else,
    // use the default value; if *that*'s not there, proceed to a default configuration.
    let from_file = match (
        fs::read_to_string(&configuration_path),
        configuration_path_source,
    ) {
        (Ok(text), _) => {
            toml::from_str::<ConfigurationRepr>(&text).context(ConfigurationParseSnafu {
                path: configuration_path,
            })
        }
        (Err(err), ValueSource::DefaultValue) if err.kind() == ErrorKind::NotFound => {
            Ok(ConfigurationRepr::default())
        }
        (Err(err), _) if err.kind() == ErrorKind::NotFound => Err(ConfigurationLocationSnafu {
            path: configuration_path,
        }
        .into_error(err)),
        (Err(err), _) => Err(ConfigurationSnafu {
            path: configuration_path,
        }
        .into_error(err)),
    }?;

    // We don't allow for giving the token/API key on the command line (the secret winds-up in shell
    // history & shows-up in `ps` output, to name just two ways it leaks). Regretfully, I *do* allow
    // it to be given as an environment variable, which is, I suppose, marginally better. If I don't,
    // my only other option is to force the user to place it in their configuration file. Arguably,
    // this is fine: it's protected by filesystem permissions. But still. I'd rather not force it.
    let maybe_api_key = env::var(INDIC_API_KEY)
        .map(Some)
        .or_else(|err| match err {
            VarError::NotPresent => Ok(None),
            err => Err(err),
        })
        .context(IndicApiKeySnafu)?
        .map(Into::into);

    Configuration::new(from_file, public_api, maybe_api_key)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                              main                                              //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::main]
async fn main() -> Result<()> {
    let mut matches = parse_command_line();

    configure_logging(matches.get_flag("verbose"))?;

    debug!("Logging configured.");

    let configuration = parse_configuration(
        known_good!(matches.remove_one::<PathBuf>("configuration")),
        known_good!(matches.value_source("configuration")),
        matches.remove_one("public_api"),
    )?;

    let client = make_indielinks_client(
        USER_AGENT.clone(),
        Some(configuration.token().clone()),
        Some(configuration.requests_per_minute()),
    )
    .context(ClientSnafu)?;

    match matches.remove_subcommand() {
        Some((name, matches)) if name == "import-pinboard" => import_pinboard(
            client,
            configuration.public_api(),
            configuration.token(),
            known_good!(matches.get_one::<PathBuf>("FILE")),
            matches.get_one::<usize>("skip").copied(),
            matches.get_one::<usize>("send").copied(),
            matches.get_one::<usize>("chunk-size").copied(),
        )
        .await
        .context(PinboardSnafu),
        Some((name, matches)) if name == "import-onetab" => import_onetab(
            client,
            configuration.public_api(),
            configuration.token(),
            known_good!(matches.get_one::<PathBuf>("FILE")),
            matches.get_one::<usize>("skip").copied(),
            matches.get_one::<usize>("send").copied(),
            matches.get_one::<usize>("chunk-size").copied(),
            matches.get_flag("read-later"),
            matches.get_flag("private"),
            matches.get_many::<Tagname>("tag"),
        )
        .await
        .context(OnetabSnafu),
        Some((name, mut matches)) if name == "add-link" => {
            // The rules:

            // - if no target given, then the command line arguments `tag`, `replace`, `shared` &
            //   `to-read` are used; they all have default values, so no problems, there.
            // - if a target *is* given, and it's not known, that's an error
            // - if we have a legit target, that forms the default values, but they can be
            //   individually overridden by command line arguments

            let (tags, replace, shared, to_read) = match matches.get_one::<String>("target") {
                Some(target_name) => {
                    let target = configuration
                        .target(target_name)
                        .context(UnknownTargetSnafu {
                            target_name: target_name.clone(),
                        })?;
                    (
                        matches
                            .get_many::<Tagname>("tag")
                            .map(|iter| HashSet::from_iter(iter.cloned()))
                            .unwrap_or(target.tags.clone()),
                        matches
                            .get_one::<bool>("replace")
                            .cloned()
                            .unwrap_or(target.replace),
                        matches
                            .get_one::<bool>("shared")
                            .cloned()
                            .unwrap_or(target.shared),
                        matches
                            .get_one::<bool>("to-read")
                            .cloned()
                            .unwrap_or(target.to_read),
                    )
                }
                None => (
                    matches
                        .get_many::<Tagname>("tag")
                        .map(|iter| HashSet::from_iter(iter.cloned()))
                        .unwrap_or(HashSet::new()),
                    matches.get_flag("replace"),
                    matches.get_flag("shared"),
                    matches.get_flag("to-read"),
                ),
            };

            add_link(
                client,
                configuration.public_api(),
                configuration.token(),
                matches.remove_one::<Url>("URL").unwrap(/* impossible */),
                known_good!(matches.remove_one::<String>("title"))
                    .try_into()
                    .unwrap(/* known good */),
                None, // no notes, as yet
                tags.iter(),
                replace,
                shared,
                to_read,
            )
            .await
            .context(AddLinkSnafu)
        }
        Some(_) => unimplemented!(/* impossible */),
        None => NoSubCommandSnafu.fail(),
    }
}
