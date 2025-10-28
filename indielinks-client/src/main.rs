// Copyright (C) 2025 Michael Herstine <sp1ff@pobox.com>
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

//! # indic
//!
//! [indic](crate) (think "Indie-C") is a general purpose [indielinks] client. It currently supports
//! three sub-commands around importing and adding links, but I anticipate it growing quickly.
//!
//! [indielinks]: ../indielinksd/index.html

use std::{
    collections::{HashMap, HashSet},
    ffi::OsStr,
    fmt::Debug,
    fs::{self},
    io::{self},
    path::PathBuf,
    result::Result as StdResult,
    time::Duration,
};

use bytes::Bytes;
use clap::{
    builder::NonEmptyStringValueParser, crate_authors, crate_version, parser::ValueSource,
    value_parser, Arg, ArgAction, Command,
};
use http::{
    header::{ACCEPT, AUTHORIZATION, USER_AGENT},
    HeaderValue,
};
use rpassword::prompt_password;
use secrecy::SecretString;
use serde::Deserialize;
use snafu::{Backtrace, IntoError, OptionExt, ResultExt, Snafu};
use tap::Pipe;
use tower::{
    buffer::BufferLayer, limit::RateLimitLayer, retry::backoff::MakeBackoff, Service,
    ServiceBuilder,
};
use tower_http::set_header::SetRequestHeaderLayer;
use tracing::Level;
use tracing_subscriber::{fmt, layer::SubscriberExt, EnvFilter, Registry};
use url::Url;

use indielinks_shared::{
    api::{Password, SecretPassword},
    entities::{Tagname, UserEmail, Username},
    origin::Origin,
    service::{Body, ExponentialBackoffPolicy, RateLimit},
};

use indielinks_client::{
    add_link::add_link, add_user::add_user, import_onetab::import_onetab,
    import_pinboard::import_pinboard, service::ReqBody,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Snafu)]
enum Error {
    #[snafu(display("While adding a link, {source}"))]
    AddLink {
        source: indielinks_client::add_link::Error,
    },
    #[snafu(display("While adding a user, {source}"))]
    AddUser {
        source: indielinks_client::add_user::Error,
    },
    #[snafu(display("The API origin must be specified, either in config or on the command line"))]
    Api,
    #[snafu(display("While attempting to read {path:?}, {source}"))]
    BadConfig {
        path: PathBuf,
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While parsing the configuration file, {source}"))]
    Config {
        source: toml::de::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Exhausted the buffer without matching the predicate"))]
    Eob { backtrace: Backtrace },
    #[snafu(display("Failed to find the initial '[']"))]
    InitialParse,
    #[snafu(display("The API key must be specified, either in config or on the command line"))]
    MissingToken,
    #[snafu(display("No sub-command given; try --help"))]
    NoSubCommand,
    #[snafu(display(
        "Missing token; specify it with the --token option or in your configuration file"
    ))]
    NoToken,
    #[snafu(display("While importing links from OneTab, {source}"))]
    Onetab {
        source: indielinks_client::import_onetab::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to find the next ',' or ']'"))]
    Parse,
    #[snafu(display("While prompting for a password, {source}"))]
    Password {
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While importing links from Pinboard, {source}"))]
    Pinboard {
        source: indielinks_client::import_pinboard::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Premature EOF while deserializing array elements"))]
    PrematureEof { backtrace: Backtrace },
    #[snafu(display("Failed to setup the tracing global subscriber: {source}"))]
    Subscriber {
        source: tracing::dispatcher::SetGlobalDefaultError,
        backtrace: Backtrace,
    },
    #[snafu(display("Invalid API key"))]
    Token {
        source: http::header::InvalidHeaderValue,
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

type Result<T> = std::result::Result<T, Error>;

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

/// Current configuration
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigV1 {
    // For this one item, I'll allow it both in config and on the command line
    pub token: Option<SecretString>,
    // alright, well for these *two* items...
    pub api: Option<Origin>,
    /// Rate limit for requests to indielinks
    #[serde(rename = "rate-limit")]
    pub rate_limit: RateLimit,
    pub targets: HashMap<String, Target>,
}

impl ConfigV1 {
    pub fn set_api(self, api: Option<&Origin>) -> Self {
        match api {
            Some(api) => ConfigV1 {
                api: Some(api.clone()),
                ..self
            },
            None => self,
        }
    }
    pub fn set_token(self, token: Option<&SecretString>) -> Self {
        match token {
            Some(token) => ConfigV1 {
                token: Some(token.clone()),
                ..self
            },
            None => self,
        }
    }
    pub fn api(&self) -> Option<&Origin> {
        self.api.as_ref()
    }
    pub fn token(&self) -> Option<&SecretString> {
        self.token.as_ref()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "version", deny_unknown_fields)] // tag "internally"
enum Configuration {
    #[serde(rename = "1")]
    V1(ConfigV1),
}

impl Configuration {
    pub fn rate_limit(&self) -> &RateLimit {
        match self {
            Configuration::V1(config_v1) => &config_v1.rate_limit,
        }
    }
    pub fn set_api(self, api: Option<&Origin>) -> Self {
        match self {
            Configuration::V1(config_v1) => Configuration::V1(config_v1.set_api(api)),
        }
    }
    pub fn set_token(self, token: Option<&SecretString>) -> Self {
        match self {
            Configuration::V1(config_v1) => Configuration::V1(config_v1.set_token(token)),
        }
    }
    pub fn api(&self) -> Option<&Origin> {
        match self {
            Configuration::V1(config_v1) => config_v1.api(),
        }
    }
    pub fn targets(&self) -> &HashMap<String, Target> {
        match self {
            Configuration::V1(config_v1) => &config_v1.targets,
        }
    }
    pub fn token(&self) -> Option<&SecretString> {
        match self {
            Configuration::V1(config_v1) => config_v1.token(),
        }
    }
}

impl Default for Configuration {
    fn default() -> Self {
        Configuration::V1(ConfigV1::default())
    }
}

/// Create an [indielinks] client
///
/// [indielinks]: ../indielinksd/index.html
///
/// Return a [reqwest::Client] wrapped in tower layers that will:
///
/// - set Authorization, User-Agent, and Accept headers on all outgoing requests
/// - rate-limit requests according to the `rate_limit` argument
/// - retry failed requests with exponential backoff
async fn make_indielinks_client(
    user_agent: &str,
    token: Option<SecretString>,
    rate_limit: &RateLimit,
) -> Result<
    impl Service<
            http::Request<ReqBody>,
            Response = http::Response<Bytes>,
            Error = Box<dyn std::error::Error + Send + Sync>,
        > + Clone,
> {
    use indielinks_shared::service::ReqwestServiceLayer;
    use secrecy::ExposeSecret;

    let token = token
        .map(|token| HeaderValue::from_str(&format!("Bearer {}", token.expose_secret())))
        .transpose()
        .context(TokenSnafu)?;
    ServiceBuilder::new()
        .layer(tower::retry::RetryLayer::new(ExponentialBackoffPolicy {
            backoff: tower::retry::backoff::ExponentialBackoffMaker::new(
            // I need to add these to config
            Duration::from_secs(1),
            Duration::from_secs(3),
            10.0,
            tower::util::rng::HasherRng::new(),
        )
        .unwrap(/* known good */)
                .make_backoff(),
            num_attempts: 3,
        }))
        // `RetryLayer` requries that the `Service` it wraps is `Clone`... which `RateLimitLayer` is
        // not. Per https://github.com/tokio-rs/axum/discussions/987#discussioncomment-2678595, we
        // wrap it in a `BufferLayer`. Regrettably, it changes the error type from
        // `proto_reqwest_tower::Error` to `Box<Error + Send + Sync>`
        .layer(BufferLayer::<http::Request<ReqBody>>::new(1024))
        .layer(RateLimitLayer::new(rate_limit.num, rate_limit.duration))
        // Only set the Authorization if we have a token
        .layer(SetRequestHeaderLayer::overriding(AUTHORIZATION, token))
        .layer(SetRequestHeaderLayer::overriding(
            USER_AGENT,
            HeaderValue::from_str(user_agent).unwrap(/* known good*/),
        ))
        .layer(SetRequestHeaderLayer::overriding(
            ACCEPT,
            HeaderValue::from_static("application/json"),
        ))
        // Later: add some instrumentation? For debugging?
        .layer(ReqwestServiceLayer::new(Body))
        .service(reqwest::Client::new())
        .pipe(Ok)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                              main                                              //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::main]
async fn main() -> Result<()> {
    let matches = Command::new("indic")
        .version(crate_version!())
        .author(crate_authors!())
        .about("The INDIelinks Client")
        .long_about(
            "General-purpose client for working with indielinks.

indic (think \"indie-c\") will be a general client for indielinks. Currently, its only
sub-command is 'import', but it will be built-out as circumstances warrant.",
        )
        // In the past, I've pulled run-time configuration from command line options, environment
        // variables, and a configuration file (in that order of precedence). I'm not sure that's
        // really worth the effort. For now, I'm just going to put frequently configured items in
        // the command line (perhaps with an environment variable backup) and leave items that are
        // likely to change less frequently in a configuration file.
        .arg(
            Arg::new("api")
                .short('A')
                .long("api")
                .num_args(1)
                .value_parser(value_parser!(Origin))
                .env("INDIC_API")
                .help("Specify the location of the indielinks API to which you wish to speak")
        )
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .num_args(1)
                .value_parser(value_parser!(PathBuf))
                .default_value(OsStr::new("/home/mgh/.indic.toml"))
                .env("INDIC_CONFIG")
                .help("Specify the path to the configuration file")
        )
        .arg(
            Arg::new("no-default-config")
                .short('C')
                .long("no-default-config")
                .num_args(0)
                .action(ArgAction::SetTrue)
                .help("Don't look for a configuration file at the default location")
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
                        .value_parser(value_parser!(String))
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
        .subcommand(Command::new("add-user")
                    .about("Add a user")
                    .long_about("Add a new user to your indielinks instance.")
                    .arg(Arg::new("username")
                         .short('u')
                         .long("username")
                         .required(true)
                         .num_args(1)
                         .value_parser(value_parser!(Username))
                         .help("New user's name")
                         .long_help("indielinks usernames must be ASCII, may be from five to sixty-four chacacters in length, and must match the regex \"^[a-zA-Z][-_.a-zA-Z0-9]+$\""))
                    .arg(Arg::new("password")
                         .short('p')
                         .long("password")
                         .required(false)
                         .num_args(1)
                         .value_parser(NonEmptyStringValueParser::new())
                         .help("Specify the password")
                         .long_help("Optionally specify the password for the new user. If not given, the program will interactively prompt for one, so this argument is provided for scripting purposes.")
                    )
                    .arg(Arg::new("email")
                         .short('m')
                         .long("email")
                         .required(true)
                         .num_args(1)
                         .value_parser(value_parser!(UserEmail))
                         .help("Specify an email address for this user."))
        )
        .get_matches();

    // Alright-- if we're here, we've parsed our command line arguments. Start by configuring
    // tracing:
    tracing::subscriber::set_global_default(
        Registry::default()
            .with(
                EnvFilter::builder()
                    .with_default_directive(if matches.get_flag("verbose") {
                        Level::DEBUG.into()
                    } else {
                        Level::INFO.into()
                    })
                    .from_env_lossy()
                    // I'd *really* like to suppress these two, unless they're mentioned in
                    // RUST_LOG, but I can't see how to do that, and the odds that the user's going
                    // to want to be interested in these two crates are low.q
                    .add_directive("hyper_util=info".parse().unwrap(/* known good */))
                    .add_directive("tower=info".parse().unwrap(/* known good */)),
            )
            .with(
                fmt::Layer::default()
                    .compact()
                    .without_time()
                    .with_level(false)
                    .with_file(false)
                    .with_line_number(false)
                    .with_target(false),
            ),
    )
    .context(SubscriberSnafu)?;

    // Next-up: configuration
    let mut cfg = if matches.get_flag("no-default-config") {
        Configuration::default()
    } else {
        let path = matches.get_one::<PathBuf>("config").unwrap(/* known good */);
        match fs::read_to_string(path) {
            Ok(config_text) => toml::from_str(&config_text).context(ConfigSnafu)?,
            Err(err) => match (err.kind(), matches.value_source("config")) {
                (io::ErrorKind::NotFound, Some(ValueSource::DefaultValue)) => {
                    Configuration::default()
                }
                _ => {
                    return Err(BadConfigSnafu {
                        path: path.to_path_buf(),
                    }
                    .into_error(err));
                }
            },
        }
    };

    // Patch-up our configuration, if we got any of these on the command line:
    cfg = cfg.set_api(matches.get_one::<Origin>("api"));
    cfg = cfg.set_token(matches.get_one::<SecretString>("token"));

    let client = make_indielinks_client(
        &format!("indic/{} ( sp1ff@pobox.com )", crate_version!()),
        cfg.token().cloned(),
        cfg.rate_limit(),
    )
    .await?;

    match matches.subcommand() {
        Some(("import-pinboard", matches)) => {
            import_pinboard(
                client,
                cfg.api().context(ApiSnafu)?,
                cfg.token().context(MissingTokenSnafu)?,
                matches.get_one::<PathBuf>("FILE").unwrap(/* impossible */),
                matches.get_one::<usize>("skip").copied(),
                matches.get_one::<usize>("send").copied(),
                matches.get_one::<usize>("chunk-size").copied(),
            )
            .await
            .context(PinboardSnafu)
        }
        Some(("import-onetab", matches)) => {
            import_onetab(
                client,
                cfg.api().context(ApiSnafu)?,
                cfg.token().context(MissingTokenSnafu)?,
                matches.get_one::<PathBuf>("FILE").unwrap(/* impossible */),
                matches.get_one::<usize>("skip").copied(),
                matches.get_one::<usize>("send").copied(),
                matches.get_one::<usize>("chunk-size").copied(),
                matches.get_flag("read-later"),
                matches.get_flag("private"),
                matches.get_many::<Tagname>("tag"),
            )
            .await
            .context(OnetabSnafu)
        }
        Some(("add-link", matches)) => {
            // The rules:

            // - if no target given, then the command line arguments `tag`, `replace`, `shared` &
            //   `to-read` are used; they all have default values, so no problems, there.
            // - if a target *is* given, and it's not known, that's an error
            // - if we have a legit target, that forms the default values, but they can be
            //   individually overridden by command line arguments

            let (tags, replace, shared, to_read) = match matches.get_one::<String>("target") {
                Some(target_name) => {
                    let target = cfg.targets().get(target_name).context(UnknownTargetSnafu {
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
                cfg.api().context(ApiSnafu)?,
                cfg.token().context(MissingTokenSnafu)?,
                matches.get_one::<Url>("URL").unwrap(/* impossible */),
                matches.get_one::<String>("title").unwrap(/* impossible */),
                None, // no notes, as yet
                tags.iter(),
                replace,
                shared,
                to_read,
            )
            .await
            .context(AddLinkSnafu)
        }
        Some(("add-user", matches)) => {
            let username = matches.get_one::<Username>("username").unwrap(/* known good */);
            let email = matches.get_one::<UserEmail>("email").unwrap(/* known good */);
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
                cfg.api().context(ApiSnafu)?,
                username,
                &password,
                email,
            )
            .await
            .context(AddUserSnafu)?;

            use secrecy::ExposeSecret;
            println!("Welcome, {username}, to indielinks. Your API key is\n{}.\nPlease make a note of it now.", api_key.expose_secret());

            Ok(())
        }
        Some(_) => unimplemented!(/* impossible */),
        None => NoSubCommandSnafu.fail(),
    }
}
