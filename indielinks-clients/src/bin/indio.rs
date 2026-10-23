// Copyright (C) 2026 Michael Herstine <sp1ff@pobox.com>
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

//! # indio - The indielinks operator client
//!
//! [indio](crate) (think "INDIelinks Operator") is an [indielinks] client for service operators.
//!
//! [indielinks]: ../indielinksd/index.html
//!
//! This program is separate & distinct form [indic], which is an [indielinks] client intended for
//! indielinks *users*.
//!
//! [indic]: ../indiec/index.html

use std::{
    env::{self, VarError},
    fs,
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
use indielinks_clients::{add_user::add_user, client::make_indielinks_client};
use nonzero::nonzero;
use rpassword::prompt_password;
use secrecy::SecretString;
use serde::Deserialize;
use snafu::{prelude::*, Backtrace, IntoError};
use tracing::{debug, Level};
use tracing_subscriber::{util::SubscriberInitExt, EnvFilter, FmtSubscriber};

use indielinks_shared::{
    api::{Password, SecretPassword},
    entities::{UserEmail, Username},
    known_good,
    origin::Origin,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           Error type                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Snafu)]
#[non_exhaustive]
enum Error {
    #[snafu(display("unable to add user"))]
    AddUser {
        source: indielinks_clients::add_user::Error,
    },
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
    #[snafu(display("failed to read the API key from the environment"))]
    IndioApiKey {
        source: std::env::VarError,
        backtrace: Backtrace,
    },
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
    #[snafu(display("unable to "))]
    Password {
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("failed to initialize logging"))]
    Subscriber {
        source: tracing_subscriber::util::TryInitError,
        backtrace: Backtrace,
    },
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> StdResult<(), std::fmt::Error> {
        write!(f, "{self}")
    }
}

type Result<T> = StdResult<T, Error>;

const INDIO_API_KEY: &str = "INDIO_API_KEY";

static USER_AGENT: HeaderValue = HeaderValue::from_static(concat!(
    "indio/",
    crate_version!(),
    " ( indielinks@pobox.com )"
));

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
// Not `Serialize` due to the presence of a `SecretString`
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct ConfigurationRepr1 {
    token: Option<SecretString>,
    public_api: Option<Origin>,
    ops_api: Option<Origin>,
    requests_per_minute: Option<NonZero<u32>>,
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

impl std::default::Default for ConfigurationRepr {
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
    ops_api: Origin,
    requests_per_minute: NonZero<u32>,
}

impl Configuration {
    pub fn new(
        from_file: ConfigurationRepr,
        ops_api: Option<Origin>,
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
                ops_api: ops_api.unwrap_or(from_file.ops_api.ok_or(MissingOpsApiSnafu.build())?),
                requests_per_minute: from_file.requests_per_minute.unwrap_or(nonzero!(120u32)),
            },
        })
    }
    pub fn ops_api(&self) -> &Origin {
        &self.ops_api
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                     assorted utilities: logging configuration & like that                      //
////////////////////////////////////////////////////////////////////////////////////////////////////

// Setup all command-line options & sub-commands, then parse 'em.
fn parse_command_line() -> ArgMatches {
    Command::new("indio")

        ////////////////////////////////////////////////////////////////////////////////////////////
        // front matter
        ////////////////////////////////////////////////////////////////////////////////////////////
        .version(crate_version!())
        .author(crate_authors!())
        .about(color_print::cstr!("The <s>INDI</>elinks <s>O</>perator client"))
        .long_about(color_print::cstr!(
            r#"<s><u>Service operators' indielinks utility</></>

indio (think "indie-o") is a small command-line utility for operators of the indielinks web service. Sub-commands are listed below. Each one takes a --help flag."#
        ))

        ////////////////////////////////////////////////////////////////////////////////////////////
        // global arguments
        ////////////////////////////////////////////////////////////////////////////////////////////
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
                        .env("INDIO_CONFIGURATION")
                        .help("The path to your indio configuration file"),
                    // I need something of type T, where T: IntoResettable<clap::builder::OsStr>
                    |arg, dir| arg.default_value(
                        dir.join(".indio.toml").as_mut_os_string().clone())
                )
        )
        .arg(
            Arg::new("ops-api")
                .short('O')
                .long("ops-api")
                .num_args(1)
                .value_parser(value_parser!(Origin))
                .action(ArgAction::Set)
                .env("INDIC_OPS_API")
                .help(
                    r#"A network location at which the indielinks private operations API is being served
This option may also be specified in the configuration file."#,
                )
        )
        .arg(
            Arg::new("public-api")
                .short('A')
                .long("public-api")
                .num_args(1)
                .value_parser(value_parser!(Origin))
                .action(ArgAction::Set)
                .env("INDIO_PUBLIC_API")
                .help(
                    r#"A network location at which the indielinks public API is being served
This option may also be specified in the configuration file."#,
                ),
        )
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .num_args(0)
                .action(ArgAction::SetTrue)
                .help("Produce more prolix output")
        )

        ////////////////////////////////////////////////////////////////////////////////////////////
        // sub-commands
        ////////////////////////////////////////////////////////////////////////////////////////////

        .subcommand(
            Command::new("add-user")
                .about("Add a new user to your indielinks instance")
                .long_about(color_print::cstr!(r#"<s><u>Add a new user to your indielinks instance.</></>

Add the user & mint an API key. Note that password validation causes a noticable pause."#))

                    .arg(Arg::new("email")
                         .short('m')
                         .long("email")
                         .required(true)
                         .num_args(1)
                         .value_parser(value_parser!(UserEmail))
                         .action(ArgAction::Set)
                         .help("Specify an email address for this user."))
                    .arg(Arg::new("password")
                         .short('p')
                         .long("password")
                         .required(false)
                         .num_args(1)
                         .value_parser(NonEmptyStringValueParser::new())
                         .action(ArgAction::Set)
                         .help("Specify the password")
                         .long_help("Optionally specify the password for the new user. If not given, the program will interactively prompt for one, so this argument is provided for scripting purposes.")
                    )
                    .arg(Arg::new("username")
                         .short('u')
                         .long("username")
                         .required(true)
                         .num_args(1)
                         .value_parser(value_parser!(Username))
                         .action(ArgAction::Set)
                         .help("New user's name")
                         .long_help("indielinks usernames must be ASCII, may be from five to sixty-four chacacters in length, and must match the regex \"^[a-zA-Z][-_.a-zA-Z0-9]+$\""))
        )
        .subcommand(
            Command::new("purge-raft-logs")
                .about("Purge all Raft logs for the cluster")
                .long_about(color_print::cstr!(r#"<s><u>Purge all Raft logs for the cluster</></>

This sub-command will go straight to the datastore to purge the <u>raft_log</> and <u>raft_meta</u> tables, erasing the clustser state. Consequently, this command should <s>only</s> be run when the cluster is down."#))
        )

        // ...and parse:
        .get_matches()
}

// RN, this duplicates the logic in `indic.rs`-- move up into `lib.rs`?
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
    ops_api: Option<Origin>,
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
    let maybe_api_key = env::var(INDIO_API_KEY)
        .map(Some)
        .or_else(|err| match err {
            VarError::NotPresent => Ok(None),
            err => Err(err),
        })
        .context(IndioApiKeySnafu)?
        .map(Into::into);

    Configuration::new(from_file, ops_api, public_api, maybe_api_key)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                             main()                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::main]
async fn main() -> Result<()> {
    let mut matches = parse_command_line();

    configure_logging(matches.get_flag("verbose"))?;

    debug!("Logging configured.");

    let configuration = parse_configuration(
        known_good!(matches.remove_one::<PathBuf>("configuration")),
        known_good!(matches.value_source("configuration")),
        matches.remove_one("ops_api"),
        matches.remove_one("public_api"),
    )?;

    match matches.remove_subcommand() {
        Some((name, _matches)) if name == "add-user" => {
            unimplemented!()
        }
        Some((name, matches)) if name == "add-user" => {
            let client = make_indielinks_client(
                USER_AGENT.clone(),
                Some(configuration.token().clone()),
                Some(configuration.requests_per_minute()),
            )
            .context(ClientSnafu)?;

            let username = known_good!(matches.get_one::<Username>("username"));
            let password: SecretPassword = match matches.get_one::<String>("password") {
                Some(s) => Box::new(Password(s.clone())).into(),
                None => loop {
                    let first = prompt_password(format!("Enter a password for user {username}: "))
                        .context(PasswordSnafu)?;
                    let second = prompt_password("Repeat password: ").context(PasswordSnafu)?;
                    if first == second {
                        break Box::new(Password(first)).into();
                    }
                },
            };

            let api_key = add_user(
                client,
                configuration.public_api(),
                configuration.ops_api(),
                username,
                &password,
                known_good!(matches.get_one::<UserEmail>("email")),
            )
            .await
            .context(AddUserSnafu)?;

            use secrecy::ExposeSecret;
            println!(
                "Welcome, {username}, to indielinks. Your API key is\n{username}:{}.\nPlease make \
                      a note of it now.",
                api_key.expose_secret()
            );

            Ok(())
        }
        Some(_) => unimplemented!(/* impossible */),
        None => NoSubCommandSnafu.fail(),
    }
}
