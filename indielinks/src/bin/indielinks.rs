// Copyright (C) 2024-2025 Michael Herstine <sp1ff@pobox.com>
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

//! # indielinks
//!
//! Bookmarks in the Fediverse.
//!
//! # Introduction
//!
//! indielinks is a federated bookmarking service that supports [ActivityPub].
//!
//! [ActivityPub]: https://www.w3.org/TR/activitypub/#server-to-server-interactions
//!
//! Right now, the library crate has the same name as the binary, meaning that `rustdoc` will
//! ignore the binary create. I should probably rename this file.

use std::{
    collections::HashMap,
    env,
    ffi::{CString, OsString},
    fmt::Display,
    fs::{self, OpenOptions},
    future::IntoFuture,
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
};

use axum::{Router, extract::State, response::IntoResponse, routing::get};
use chrono::Duration;
use clap::{Arg, ArgAction, Command, crate_authors, crate_version, value_parser};
use either::Either;
use http::{HeaderName, HeaderValue};
use lazy_static::lazy_static;
use libc::{
    F_TLOCK, close, dup, exit, fork, getdtablesize, getpid, lockf, open, setsid, umask, write,
};
use opentelemetry::{KeyValue, global};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use secrecy::SecretString;
use serde::Deserialize;
use snafu::{IntoError, prelude::*};
use tap::Pipe;
use tokio::{
    net::TcpListener,
    signal::unix::{SignalKind, signal},
    sync::{Notify, RwLock, mpsc},
};
use tonic::transport::Server as TonicServer;
use tower_http::{
    cors::CorsLayer,
    request_id::{MakeRequestId, PropagateRequestIdLayer, RequestId, SetRequestIdLayer},
    trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer},
};
use tracing::{Level, error, info};
use tracing_subscriber::{
    Layer, Registry,
    filter::EnvFilter,
    fmt::{self, MakeWriter},
    layer::SubscriberExt,
};
use url::Url;

use indielinks_shared::StorUrl;

use indielinks_cache::{
    cache::Cache,
    raft::{CacheNode, Configuration as RaftConfiguration},
    types::NodeId,
};

use indielinks::{
    actor::make_router as make_actor_router,
    background_tasks::{self, Backend as TasksBackend, BackgroundTasks, Context},
    cache::{
        Backend as CacheBackend, FOLLOWER_TO_PUBLIC_INBOX, GrpcClientFactory, GrpcService,
        LogStore, make_router as make_cache_router,
    },
    client::make_client,
    counter_add,
    delicious::make_router as make_delicious_router,
    entities::FollowerId,
    http::Indielinks,
    metrics::{self, Instruments, Sort},
    origin::Origin,
    peppers::Peppers,
    protobuf_interop::protobuf::grpc_service_server::GrpcServiceServer,
    signing_keys::SigningKeys,
    storage::Backend as StorageBackend,
    users::make_router as make_user_router,
    webfinger::webfinger,
};

/// The indielinks application error type
///
/// I'm tentatively opting to build this using [Snafu]. This is because, contra my usual approach
/// of designing a module's error type to be fairly small, offerring a few big "buckets" of failure
/// modes (and attaching context to give more detailed information), at the application level I'm
/// going to provide a fairly rich set of errors in the hopes of helping operators. That means
/// a lot of boilerplate for the hand-authored route, and [Snafu] can reduce that.
///
/// [Snafu]: https://docs.rs/snafu/latest/snafu/index.html
///
/// The reader may wonder: why not [anyhow] or [thiserror]? I don't care for [anyhow]'s approach of
/// collapsing various failure modalities into a single entity because it makes it difficult for
/// callers to decide how to proceed. OTOH, I rather like [thiserror]'s approach; I just swung
/// toward [Snafu] due to its support for adding context, which is a very handy feature IMHO.
///
/// [anyhow]: https://docs.rs/anyhow/latest/anyhow/index.html
/// [thiserror]: https://docs.rs/thiserror/latest/thiserror/index.html
/// [Snafu]: https://docs.rs/snafu/latest/snafu/index.html
///
/// Note that I do not derive the [Debug] trait for this error. This is because `main()` returns
/// `Result<(), Error>`. For this to work, `Result<(), Error>` must implement
/// [std::process::Termination], which has a blanket implementation for `Result<T, E>`, so long as
/// `T` implements `Termination` (which `()` does) and `E` implements `Debug`. Should the `E`
/// variant be returned, the Rust runtime uses the `Debug` implementation to produce an error
/// message on stderr. The derived implementation of `Debug` is not very readable, and in the
/// presence of a backtrace, verbose as well. Therefore, I'm implementating it "by hand", as well as
/// judiciously removing backtraces where they don't provide useful information.
///
/// One might wonder, why not handle the error message ourselves, and use [std::process::exit] to
/// produce a suitable exit code? Well: "no destructors on the current stack or any other thread’s
/// stack will be run. If a clean shutdown is needed it is recommended to only call this function at
/// a known point where there are no more destructors left to run; or, preferably, simply return a
/// type implementing Termination".
#[derive(Snafu)]
pub enum Error {
    #[snafu(display("While serving {asset:?}: {source}"))]
    Asset {
        asset: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("{asset:?} was requested, but not found"))]
    AssetNotFound { asset: PathBuf },
    #[snafu(display("Failed to shut-down background task processing: {source}"))]
    BackgroundShutdown { source: background_tasks::Error },
    #[snafu(display("Failed to setup background task processing: {source}"))]
    BackgroundTasks { source: background_tasks::Error },
    #[snafu(display("Failed to bind to localhost: {source}"))]
    Bind { source: std::io::Error },
    #[snafu(display("Failed to create the CacheNode: {source}"))]
    CacheNode {
        source: indielinks_cache::raft::Error,
    },
    #[snafu(display("Failed to change directory: {source}"))]
    Changedir { source: std::io::Error },
    #[snafu(display("Failed to create an HTTP client: {source}"))]
    Client { source: indielinks::client::Error },
    #[snafu(display("Unable to read configuration file: {source}"))]
    ConfigNotFound {
        pth: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Error parsing configuration file: {source}"))]
    ConfigParse {
        pth: PathBuf,
        source: toml::de::Error,
    },
    #[snafu(display("Couldn't resolve the present working directory: {source}"))]
    CurrentDir { source: std::io::Error },
    #[snafu(display("Failed to connect to DynamoDB: {source}"))]
    Dynamo { source: indielinks::dynamodb::Error },
    #[snafu(display("Failed to parse RUST_LOG: {source}"))]
    EnvFilter {
        source: tracing_subscriber::filter::FromEnvError,
    },
    #[snafu(display("Failed to fork the indielinks process: errno={errno}"))]
    Fork { errno: errno::Errno },
    #[snafu(display("Failed to lock the indielinks lock file: errno={errno}"))]
    LockFile { errno: errno::Errno },
    #[snafu(display("Failed to open the indielinks log file: {source}"))]
    LogFile { source: std::io::Error },
    #[snafu(display("Failed to HUP the logfile: {source}"))]
    LogHup {
        source: tokio::sync::mpsc::error::SendError<PathBuf>,
    },
    #[snafu(display("Failed to open the indielinks lock file: errno={errno}"))]
    OpenLockFile { errno: errno::Errno },
    #[snafu(display("Failed to build the Prometheus exporter: {source}"))]
    PrometheusExporter {
        source: opentelemetry_sdk::metrics::MetricError,
    },
    #[snafu(display("Failed to connect to SycllaDB: {source}"))]
    Syclla { source: indielinks::scylla::Error },
    #[snafu(display("Failed to fork the indielinks process a second time: errno={errno}"))]
    SecondFork { errno: errno::Errno },
    #[snafu(display("Failed to set the tracing subscriber: {source}"))]
    Subscriber {
        source: tracing::subscriber::SetGlobalDefaultError,
    },
    #[snafu(display("Failed to instantiate a Tokio runtime: {source}"))]
    TokioRuntime { source: std::io::Error },
    #[snafu(display("Failed to write the indielinks PID file: errno={errno}"))]
    WritePid { errno: errno::Errno },
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self::Display::fmt(&self, f)
    }
}

type Result<T> = std::result::Result<T, Error>;

type StdResult<T, E> = std::result::Result<T, E>;

static DEFAULT_LOCALSTATEDIR: &str = ".";

/// Logging-related options read from the command line or the environment
struct LogOpts {
    pub daemon: bool,
    pub plain: bool,
    pub level: Level,
}

impl LogOpts {
    fn new(matches: &clap::ArgMatches) -> LogOpts {
        LogOpts {
            daemon: !matches.get_flag("no-daemon"),
            plain: matches.get_flag("plain"),
            level: match (
                matches.get_flag("debug"),
                matches.get_flag("verbose"),
                matches.get_flag("quiet"),
            ) {
                (true, _, _) => Level::TRACE,
                (false, true, _) => Level::DEBUG,
                (false, false, true) => Level::ERROR,
                (_, _, _) => Level::INFO,
            },
        }
    }
}

/// Configuration options read from the CLI (or the environment)
struct CliOpts {
    pub log_opts: LogOpts,
    pub cfg: Option<PathBuf>,
    pub local_statedir: PathBuf,
    pub no_chdir: bool,
}

impl CliOpts {
    fn new(matches: clap::ArgMatches) -> Result<CliOpts> {
        let here = env::current_dir().context(CurrentDirSnafu)?;
        Ok(CliOpts {
            log_opts: LogOpts::new(&matches),
            cfg: matches
                .get_one::<PathBuf>("config")
                .cloned()
                .map(|p| here.join(p)),
            local_statedir: matches
                .get_one::<PathBuf>("local-state")
                .unwrap_or(&PathBuf::from_str(DEFAULT_LOCALSTATEDIR).unwrap())
                .clone(),
            no_chdir: matches.get_flag("no-chdir"),
        })
    }
}

/// Indielinks datastore configuration
///
/// I want to hide the details of the backing datastore from application code to the greatest extent
/// possible; even at the outset of the project, I'm torn between ScyllaDB & DynamoDB. The idea here
/// is that most of indielinks will write to a generic API (albeit one that will likely encode the
/// permitted styles of data access), but that at startup, a particular *implementation* of that API
/// will be chosen, according to configuration. This configuration.
// Nb that we can only deserialize (i.e. not serialize) due to the presence of secrets in the
// struct
#[derive(Clone, Debug, Deserialize)]
pub enum StorageConfig {
    /// Use ScyllaDB/CQL interface
    Scylla {
        /// ScyllaDB credentials, if authentication is to be used
        credentials: Option<(SecretString, SecretString)>,
        /// ScyllaDB hosts; specify as "host:port"
        hosts: Vec<String>,
    },
    /// Use DyanmoDB or Scylla over the Alternator interface
    Dynamo {
        /// AWS credentials: key ID & secret key; you'll pretty-much always need to specify these
        /// when running against DDB, but one could be talking to a local SycllaDB over the
        /// Alternator interface locally and have the cluster be open
        credentials: Option<(SecretString, SecretString)>,
        /// You can find DynamoDB in a few ways. If you're truly talking to DynamoDB in AWS, you can
        /// give a region. You can also specify an URL (like
        /// `https://dynamodb.us-west-2.amazonaws.com`). If you're talking to ScyllaDB over the
        /// Alternator interface, we're going to have to handle load-balancing on the client-side,
        /// so specify more than one.
        #[serde(with = "either::serde_untagged")]
        location: Either<String, Vec<Url>>,
    },
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig::Scylla {
            credentials: None,
            hosts: vec![String::from("localhost:9042")],
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct SigningKeysConfig {
    #[serde(rename = "token-lifetime")]
    token_lifetime: Duration,
    #[serde(rename = "signing-keys")]
    signing_keys: SigningKeys,
}

impl Default for SigningKeysConfig {
    fn default() -> Self {
        SigningKeysConfig {
            token_lifetime: Duration::hours(1),
            signing_keys: SigningKeys::default(),
        }
    }
}

/// Indielinks configuration, version one
#[derive(Clone, Debug, Deserialize)]
struct ConfigV1 {
    /// The [indielinks] log file
    #[serde(rename = "log-file")]
    log_file: PathBuf,
    /// Local address at which to listen for public requests; specify as "address:port". This
    /// is the address to which [indielinks] will bind a listening socket for its public API.
    #[serde(rename = "public-address")]
    public_address: SocketAddr,
    /// Address at which to listen for private requests; specify as "address:port"
    // See note above RE `SocketAddr`.
    #[serde(rename = "private-address")]
    private_address: SocketAddr,
    /// Address at which to listen for Raft-related gRPC messages
    #[serde(rename = "raft-grpc-address")]
    raft_grpc_address: SocketAddr,
    #[serde(rename = "storage-config")]
    storage_config: StorageConfig,
    /// The address at which this [indielinks] instance may be reached from the public internet
    #[serde(rename = "public-origin")]
    public_origin: Origin,
    pepper: Peppers,
    #[serde(rename = "signing-keys")]
    signing_keys: SigningKeysConfig,
    #[serde(rename = "user-agent")]
    user_agent: String,
    #[serde(rename = "collection-page-size")]
    collection_page_size: usize,
    #[serde(rename = "background-tasks")]
    background_tasks: background_tasks::Config,
    #[serde(rename = "raft-config")]
    raft_config: RaftConfiguration,
}

impl ConfigV1 {
    pub fn public_address(&self) -> &SocketAddr {
        &self.public_address
    }
    pub fn private_address(&self) -> &SocketAddr {
        &self.private_address
    }
    pub fn background_tasks(&self) -> &background_tasks::Config {
        &self.background_tasks
    }
}

impl Default for ConfigV1 {
    fn default() -> Self {
        ConfigV1 {
            log_file: PathBuf::from_str("/tmp/indielinks.log").unwrap(/* known good */),
            public_address: "0.0.0.0:20673".parse::<SocketAddr>().unwrap(/* known good */),
            private_address: "127.0.0.1:20674".parse::<SocketAddr>().unwrap(/* known good */),
            raft_grpc_address: "0.0.0.0:20675".parse::<SocketAddr>().unwrap(/* known good */),
            storage_config: StorageConfig::default(),
            public_origin: "http://localhost:20673".parse::<Origin>().unwrap(/* known good */),
            pepper: Peppers::default(),
            signing_keys: SigningKeysConfig::default(),
            user_agent: format!("indielinks/{}; +sp1ff@pobox.com", crate_version!()),
            collection_page_size: 12, // Copied from Mastodon
            background_tasks: background_tasks::Config::default(),
            raft_config: RaftConfiguration::default(),
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "version")] // tag "internally"
enum Configuration {
    #[serde(rename = "1")]
    V1(ConfigV1),
}

/// Parse the indielinks configuration file
fn parse_config(cfg: &Option<PathBuf>) -> Result<ConfigV1> {
    use snafu::IntoError;
    let (pth, defaulted): (PathBuf, bool) = cfg.as_ref().map_or_else(
        || (PathBuf::from_str("/etc/indielinks.toml").unwrap(), true),
        |p| (p.clone(), false),
    );
    match std::fs::read_to_string(&pth) {
        Ok(text) => match toml::from_str::<Configuration>(&text) {
            Ok(cfg) => match cfg {
                Configuration::V1(cfg) => Ok(cfg),
            },
            Err(err) => Err(ConfigParseSnafu { pth }.into_error(err)),
        },
        Err(err) => {
            if defaulted {
                Ok(ConfigV1::default())
            } else {
                Err(ConfigNotFoundSnafu { pth }.into_error(err))
            }
        }
    }
}

/// A tracing-compatible, "reopenable" log file
///
/// I need a thing that implements [MakeWriter] to hand-off to a tracing [Layer]. [MakeWriter], in
/// turn, returns a thing that implements [std::io::Write] that is valid for some lifetime 'a.
///
/// Now, [MakeWriter] is implemented on `Arc<W>` or `Mutex<W>` for any `W` that implements
/// [std::io::Write]. The problem is, `Arc<Mutex<W>>` does *not* implement [MakeWriter], even if `W`
/// implements [std::io::Write].
///
/// I could only see two approaches; hand the [Layer] a reference to the thing and keep a reference
/// for myself, and invoke a method on the thing in response to a `SIGHUP`, or hand the thing off to
/// the [Layer] in toto and use some side-band communications channel to tell it to re-open the file
/// in response to a `SIGHUP`.
///
/// I've for the moment gone with the latter since the former would require me to somehow have the
/// thing implement [std::io::Write] *and* be thread-safe.
struct LogFile {
    fd: Arc<Mutex<std::fs::File>>,
}

impl LogFile {
    /// Open a file at `pth`; return a [LogFile] instance along with the send side of a channel
    /// the caller can use to close & re-open the file.
    pub fn open(pth: &Path) -> StdResult<(LogFile, mpsc::Sender<PathBuf>), std::io::Error> {
        let (tx, rx) = mpsc::channel::<PathBuf>(1);
        let fd = OpenOptions::new()
            .create(true)
            .append(true)
            .open(pth)
            .map(|fd| Arc::new(Mutex::new(fd)))?;
        tokio::spawn(LogFile::rehup(fd.clone(), rx));
        Ok((LogFile { fd }, tx))
    }
    /// Close & re-open the file
    async fn rehup(fd: Arc<Mutex<std::fs::File>>, mut rx: mpsc::Receiver<PathBuf>) {
        while let Some(ref pbuf) = rx.recv().await {
            match OpenOptions::new().create(true).append(true).open(pbuf) {
                Ok(f) => *fd.lock().unwrap() = f,
                Err(err) => error!("Failed to open {:?} ({}).", pbuf, err),
            }
        }
    }
}

pub struct MyMutexGuardWriter<'a>(MutexGuard<'a, std::fs::File>);

impl<'a> MakeWriter<'a> for LogFile {
    type Writer = MyMutexGuardWriter<'a>;
    fn make_writer(&'a self) -> Self::Writer {
        MyMutexGuardWriter(self.fd.lock().expect("lock poisoned"))
    }
}

impl io::Write for MyMutexGuardWriter<'_> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }

    #[inline]
    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        self.0.write_vectored(bufs)
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.0.write_all(buf)
    }

    #[inline]
    fn write_fmt(&mut self, fmt: std::fmt::Arguments<'_>) -> io::Result<()> {
        self.0.write_fmt(fmt)
    }
}

/// Configure indielinks logging
///
/// For now, I'm going to handle logs outside of OTel: if we're running in the foreground (which I
/// anticipate will be the usual case, inside a Docker container), I just want to log to stdout. If
/// we're being run as a daemon, I want to log to file.
///
/// If we're logging to file, return the sender side of a channel that can be used to signal the
/// file to close & re-open itself (in response to a `SIGHUP`, presumably).
///
/// This method can only be invoked once (as it, in turn, calls tracing's [set_global_default]).
fn configure_logging(logopts: &LogOpts, logfile: &Path) -> Result<Option<mpsc::Sender<PathBuf>>> {
    let filter = EnvFilter::builder()
        .with_default_directive(logopts.level.into())
        .from_env()
        .context(EnvFilterSnafu)?;

    // Hmmmm.... I want formatted output, with the following options:
    //
    //               |  -F=false  |  -F=true     |
    //               |------------+--------------|
    // --plain=false |  json,file |  json,stdout |
    // --plain=true  | !json,file | !json,stdout |
    //
    // Thing is, `json()` & `with_writer()` produce `SubscriberBuilder` instances *of
    // different types*. It is for this reason that `Box<dyn Layer<S> + Send + Sync>`
    // implements `Layer`:
    let mut tx = None;
    let formatter: Box<dyn Layer<Registry> + Send + Sync> = if logopts.daemon {
        let (log_file, tx_inner) = LogFile::open(logfile).context(LogFileSnafu)?;
        tx = Some(tx_inner);
        if logopts.plain {
            Box::new(
                fmt::Layer::default()
                    .compact()
                    .with_ansi(false)
                    .with_writer(log_file),
            )
        } else {
            Box::new(fmt::Layer::default().json().with_writer(log_file))
        }
    } else if logopts.plain {
        Box::new(fmt::Layer::default().compact().with_writer(io::stdout))
    } else {
        Box::new(fmt::Layer::default().json().with_writer(io::stdout))
    };

    // Nb. this can only be invoked once (will panic on a second invocation)!
    tracing::subscriber::set_global_default(Registry::default().with(formatter).with(filter))
        .context(SubscriberSnafu)?;

    Ok(tx)
}

async fn otel_middleware(
    State(state): State<Arc<Indielinks>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    // Going to assume that request.path() is a legit, RFC 3986-compliant path, though it could be
    // empty. OTel names must be ASCII and belong to the alphanumeric characters, '_', '.', '-' and
    // '/'. Here, I remove any illegal characters & replace '/' with '.'.
    let stem: String = request
        .uri()
        .path()
        .as_bytes()
        .iter()
        .filter_map(|x| {
            if 47 == *x {
                Some('.')
            } else if (44 < *x && *x < 58) || (64 < *x && *x < 91) || (96 < *x && *x < 123) {
                Some(char::from_u32(*x as u32).unwrap(/* known good */))
            } else {
                None
            }
        })
        .collect();

    let name = format!("http.{}{}", request.method().as_str().to_lowercase(), stem);
    let counter = state.instruments.meter().u64_counter(name).build();
    // Nb. can add attributes like so: &[KeyValue::new("user", user.clone())]
    counter.add(1, &[]);
    next.run(request).await
}

async fn healthcheck() -> &'static str {
    "GOOD"
}

async fn metrics(State(state): State<Arc<Indielinks>>) -> String {
    use prometheus::{Encoder, TextEncoder};

    let encoder = TextEncoder::new();
    let metric_families = state.registry.gather();
    let mut result = Vec::new();
    encoder
        .encode(&metric_families, &mut result)
        .expect("Failed to encode Prom metrics");
    String::from_utf8(result).expect("Failed to encode Prom metrics")
}

lazy_static! {
    static ref CONTENT_TYPES: HashMap<OsString, HeaderValue> = {
        HashMap::from([
            (
                "html".to_owned().into(),
                HeaderValue::from_static("text/html"),
            ),
            (
                "css".to_owned().into(),
                HeaderValue::from_static("text/css"),
            ),
            (
                "js".to_owned().into(),
                HeaderValue::from_static("text/javascript"),
            ),
            (
                "wasm".to_owned().into(),
                HeaderValue::from_static("application/wasm"),
            ),
        ])
    };
    static ref ASSETS: OsString = "assets".to_owned().into();
}

inventory::submit! { metrics::Registration::new("frontend.asset.successes", Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("frontend.asset.404s", Sort::IntegralCounter) }
inventory::submit! { metrics::Registration::new("frontend.asset.failures", Sort::IntegralCounter) }

async fn frontend(
    State(state): State<Arc<Indielinks>>,
    file: Option<axum::extract::Path<PathBuf>>,
) -> axum::response::Response {
    fn frontend1(file: &PathBuf) -> Result<Vec<u8>> {
        fs::read(
            [ASSETS.as_os_str(), file.as_os_str()]
                .iter()
                .collect::<PathBuf>(),
        )
        .map_err(|err| {
            if err.kind() == io::ErrorKind::NotFound {
                Error::AssetNotFound {
                    asset: file.clone(),
                }
            } else {
                AssetSnafu { asset: file }.into_error(err)
            }
        })?
        .pipe(Ok)
    }

    let file = file
        .unwrap_or(axum::extract::Path(PathBuf::from("index.html")))
        .0;

    match frontend1(&file) {
        Ok(body) => {
            let mut rsp = axum::response::Response::builder().status(http::StatusCode::OK);
            if let Some(Some(header_value)) = file.extension().map(|ext| CONTENT_TYPES.get(ext)) {
                rsp = rsp.header(http::header::CONTENT_TYPE, header_value);
            }
            counter_add!(
                state.instruments,
                "frontend.asset.successes",
                1,
                &[KeyValue::new("asset", file.to_string_lossy().into_owned())]
            );
            rsp.status(http::StatusCode::OK).body(body.into()).expect(
                "Failed to construct a response from /fe. This is a bug & should be investigated",
            )
        }
        Err(Error::AssetNotFound { .. }) => {
            counter_add!(
                state.instruments,
                "frontend.asset.404s",
                1,
                &[KeyValue::new("asset", file.to_string_lossy().into_owned())]
            );
            http::StatusCode::NOT_FOUND.into_response()
        }
        Err(err) => {
            error!("{err:?}");
            counter_add!(
                state.instruments,
                "frontend.asset.failures",
                1,
                &[KeyValue::new("asset", file.to_string_lossy().into_owned())]
            );
            http::StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// Counter for generating request IDs; I realize that a u64 gives me a lot less information than a
/// UUID (the traditional type for request IDs), but I judge it to be enough, as well as more easily
/// readable, and a useful guage of how long the server's been up.
#[derive(Clone, Debug, Default)]
struct RequestIdGenerator {
    counter: Arc<AtomicU64>,
}

impl MakeRequestId for RequestIdGenerator {
    fn make_request_id<B>(&mut self, _request: &axum::extract::Request<B>) -> Option<RequestId> {
        self.counter
            .fetch_add(1, Ordering::SeqCst)
            .to_string()
            .pipe(|s| RequestId::new(HeaderValue::from_str(&s).unwrap(/* known good */)))
            .pipe(Some)
    }
}

/// Make the [Router] that will be accessible to the world
fn make_world_router(state: Arc<Indielinks>) -> Router {
    Router::new()
        .route("/healthcheck", get(healthcheck))
        .route("/metrics", get(metrics))
        // It's *really* irritating that I need to specify three separate routes to handle each of
        // these cases, but here we are. At least I only need to implement one handler.
        .route("/fe", get(frontend))
        .route("/fe/", get(frontend))
        .route("/fe/{file}", get(frontend))
        .route(
            "/.well-known/webfinger",
            get(webfinger).layer(CorsLayer::permissive()),
        )
        .merge(make_actor_router(state.clone()))
        .nest("/api/v1", make_delicious_router(state.clone()))
        .nest("/api/v1", make_user_router(state.clone()))
        // Reproducing this diagram from <https://docs.rs/axum/latest/axum/middleware/index.html>
        // because the towwer_http docs
        // <https://docs.rs/tower-http/0.6.2/tower_http/request_id/index.html> are incorrect: we
        // want incoming requests to hit the `SetRequestIdLayer` *first*, so we need to make that
        // the last/outer layer which we apply:
        //
        //                 requests
        //                    |
        //                    v
        // +---------  SetRequestIdLayer      ---------+
        // | +-------      OTEL layer         -------+ |
        // | | +-----      TraceLayer         -----+ | |
        // | | | +--- PropagateRequestIdLayer ---+ | | |
        // | | | |                               | | | |
        // | | | |          handler              | | | |
        // | | | |                               | | | |
        // | | | +--- PropagateRequestIdLayer ---+ | | |
        // | | +-----      TraceLayer         -----+ | |
        // | +-------      OTEL Layer         -------+ |
        // +---------   SetRequestIdLayer     ---------+
        //                    |
        //                    v
        //                responses
        .layer(PropagateRequestIdLayer::new(HeaderName::from_static(
            "x-request-id",
        )))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().include_headers(true))
                .on_response(DefaultOnResponse::new().include_headers(true)),
        )
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            otel_middleware,
        ))
        .layer(SetRequestIdLayer::new(
            HeaderName::from_static("x-request-id"),
            RequestIdGenerator::default(),
        ))
        .with_state(state)
}

/// Make the [Router] that will only be locally accessible
fn make_local_router(state: Arc<Indielinks>) -> Router {
    Router::new()
        .nest("/ops/cache", make_cache_router(state.clone()))
        .layer(TraceLayer::new_for_http())
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            otel_middleware,
        ))
        .with_state(state.clone())
}

pub async fn select_storage(
    config: &StorageConfig,
    node_id: NodeId,
) -> Result<(
    Arc<dyn StorageBackend + Send + Sync>,
    Arc<dyn TasksBackend + Send + Sync>,
    Arc<dyn CacheBackend + Send + Sync>,
)> {
    match config {
        StorageConfig::Scylla { credentials, hosts } => {
            let x = Arc::new(
                indielinks::scylla::Session::new(hosts, credentials, node_id)
                    .await
                    .context(SycllaSnafu)?,
            );
            Ok((x.clone(), x.clone(), x.clone()))
        }
        StorageConfig::Dynamo {
            credentials,
            location,
        } => {
            let x = Arc::new(
                indielinks::dynamodb::Client::new(location, credentials, node_id)
                    .await
                    .context(DynamoSnafu)?,
            );
            Ok((x.clone(), x.clone(), x.clone()))
        }
    }
}

/// Serve `indielinks` API requests
async fn serve(registry: prometheus::Registry, opts: CliOpts) -> Result<()> {
    // Produce a future which can be used to signal graceful shutdown, below.
    async fn shutdown_signal(nfy: Arc<Notify>) {
        nfy.notified().await
    }

    let mut sighup = signal(SignalKind::hangup()).unwrap();
    let mut sigkill = signal(SignalKind::terminate()).unwrap();

    // Failure to parse at this point is fatal; below, we fall back to the last "known-good"
    // configuration & keep going.
    let mut cfg = parse_config(&opts.cfg)?;
    let log_file_hup = configure_logging(&opts.log_opts, &cfg.log_file)?;
    // At this point we have logging-- huzzah!
    info!("indielinks version {} starting.", crate_version!());

    let instruments = Arc::new(Instruments::new("indielinks"));

    // Loop forever, handling SIGHUPs, until asked to terminate:
    loop {
        let client =
            make_client(&cfg.user_agent, instruments.clone(), None).context(ClientSnafu)?;

        // Re-build our database connections each pass, in case configuration values have changed:
        let (storage, tasks, cache) =
            select_storage(&cfg.storage_config, cfg.raft_config.this_node).await?;
        // Setup background task processing. This, too, is subject to configuration. `nosql_tasks`
        // is a task processing implementation backed by our datastore.
        let nosql_tasks = Arc::new(BackgroundTasks::new(tasks));
        // Save a reference to it for use by our web-service:
        let task_sender = nosql_tasks.clone();
        // Setup the context for our tasks
        let context = Context {
            origin: cfg.public_origin.clone(),
            client: client.clone(),
            storage: storage.clone(),
        };
        // Move `nosql_tasks` into a new `Processor`, which lets us shut down background task
        // processing in an orderly manner:
        let task_processor = background_tasks::new(
            nosql_tasks,
            context,
            Some(cfg.background_tasks().clone()),
            instruments.clone(),
        )
        .context(BackgroundTasksSnafu)?;
        let cache_node = CacheNode::<GrpcClientFactory>::new(
            &cfg.raft_config,
            GrpcClientFactory,
            LogStore::new(cache),
        )
        .await
        .context(CacheNodeSnafu)?;
        // Alright-- setup shared state for the web service itself:
        let first_cache = Arc::new(RwLock::new(
            Cache::<GrpcClientFactory, FollowerId, StorUrl>::new(
                FOLLOWER_TO_PUBLIC_INBOX,
                cache_node.clone(),
            ),
        ));

        // This will need to be re-thought as the number (and types) of caches grows, but for now:

        let state = Arc::new(Indielinks {
            origin: cfg.public_origin.clone(),
            registry: registry.clone(),
            storage,
            instruments: instruments.clone(),
            pepper: cfg.pepper.clone(),
            token_lifetime: cfg.signing_keys.token_lifetime,
            signing_keys: cfg.signing_keys.signing_keys.clone(),
            client,
            collection_page_size: cfg.collection_page_size,
            task_sender,
            cache_node: cache_node.clone(),
            first_cache: first_cache.clone(),
        });

        let world_nfy = Arc::new(Notify::new());
        let local_nfy = Arc::new(Notify::new());
        let grpc_nfy = Arc::new(Notify::new());

        let world_server = axum::serve(
            TcpListener::bind(cfg.public_address())
                .await
                .context(BindSnafu)?,
            make_world_router(state.clone()),
        )
        .with_graceful_shutdown(shutdown_signal(world_nfy.clone()));

        let local_server = axum::serve(
            TcpListener::bind(cfg.private_address())
                .await
                .context(BindSnafu)?,
            make_local_router(state.clone()),
        )
        .with_graceful_shutdown(shutdown_signal(local_nfy.clone()));

        let (mut processor_join_handle, processor_shutdown) = task_processor.into_parts();

        let mut world_server = world_server.into_future();
        let mut local_server = local_server.into_future();

        fn log_on_err<T, E>(x: StdResult<T, E>)
        where
            E: std::error::Error + std::fmt::Debug,
        {
            if let Err(err) = x {
                error!("{:?}", err);
            }
        }

        let mut grpc_server = std::pin::pin!(TonicServer::builder().serve_with_shutdown(
            cfg.raft_grpc_address,
            GrpcServiceServer::new(GrpcService::new(cache_node, first_cache)),
            grpc_nfy.notified()
        ));

        tokio::select! {
            // Intentionally not handling these-- the servers *should* never shutdown on their own.
            // That said, if I don't move `world_server` into a Future, it never gets polled.
            _ = &mut world_server => unimplemented!(),
            _ = &mut local_server => unimplemented!(),
            _ = &mut grpc_server => unimplemented!(),
            _ = sighup.recv() => { // Future<Output = Option<()>>
                info!("Received SIGHUP; closing log file & DB connections to re-read configuration.");
                // Signal our axum servers to shut-down...
                world_nfy.notify_one();
                local_nfy.notify_one();
                grpc_nfy.notify_one();
                // & wait for them to complete.
                log_on_err(world_server.await);
                log_on_err(local_server.await);
                // There's not much to be done on failure, nor do we expect a result, but if there
                // _was_ an error of some kind, I'd like to know about it.

                // Cool! Now re-read our
                // configuration:
                cfg = match parse_config(&opts.cfg) {
                    Ok(cfg) => cfg,
                    Err(_) => cfg
                };
                if let Some(ref lfh) = log_file_hup {
                    // We've daemonized and are writing to a log file. Since we're keeping the file
                    // handle open, we'll continue writing to that filesystem entity, even if
                    // someone else (such as, say, `logrotate`) renames it. Such utilities *do*
                    // rename the log file underneath us, they then send a `SIGHUP` to us to signal
                    // us to close & re-open the file (under the same name); this will result in us
                    // writing to the *new* file.
                    lfh.send(cfg.log_file.clone()).await.context(LogHupSnafu)?;
                    info!("Started new log file.");
                }
            }
            _ = sigkill.recv() => { // Future<Output = Option<()>>
                info!("Received SIGKILL; terminating.");
                // That's it-- we're outta here. Signal our axum servers to shut-down...
                world_nfy.notify_one();
                local_nfy.notify_one();
                grpc_nfy.notify_one();
                // wait for our axum servers to complete...
                log_on_err(world_server.await);
                log_on_err(local_server.await);
                // and shut-down our background processor:
                processor_shutdown.notify_one();
                // There's not much to be done on failure here, but if there is a problem, I'd like
                // to at least know:
                match tokio::time::timeout(std::time::Duration::from_secs(5), processor_join_handle)
                    .await {
                        Ok(Err(err)) => error!("Failed to shut-down the event processor: {:?}", err),
                        Err(err) => error!("Failed waiting to shut-down the event processor: {:?}", err),
                        _ => ()
                    };
                break;
            }
            res = &mut processor_join_handle => {
                // This shouldn't happen!
                error!("The background task processor exited early with {:?}; shutting-down.", res);
                // 🤷 OK, well, not much to be done, here, except to signal our axum serverse to shutdown...
                world_nfy.notify_one();
                local_nfy.notify_one();
                // wait for them...
                log_on_err(world_server.await);
                log_on_err(local_server.await);
                // and bail.
                break;
            },
        }; // End tokio::select!.
    } // End loop.

    Ok(())
}

/// Initialize telemetry
///
/// OTel is complex, and IMHO poorly documented. It's not that there isn't documentation, it's
/// more that the extant documentation uses a lot of colloquial terms for very specific purposes
/// and does a poor job of explaining those purposes to the non-initiate. All quotes below are
/// from the Open Telemetry SDK [docs].
///
/// [docs]: https://docs.rs/opentelemetry_sdk/0.22.1/opentelemetry_sdk/index.html
///
/// To produce metrics, e.g., I apparently need a "meter provider", and that is a part of the
/// "SDK". All "meters" produced by a given meter provider will "be associated with the same
/// Resource, have the same Views applied to them, and have their produced metric telemetry
/// passed to the configured MetricReaders."
///
/// OK... a "resource" is "an immutable representation of the entity producing telemetry as
/// attributes," so I guess I want a resource for the `indielinks` service itself, perhaps along
/// with other resources corresponding to different library crates (?)
///
/// A "view" is "used to customize the metrics that are output by the SDK." Apparently, views
/// can be applied post-facto (i.e. after the code is instrumented) to do things like customize
/// which attributes get reported, modify aggregation, or even drop entire metrics.
///
/// A "metric reader" is "the interface used between the SDK and an exporter." Huh.
///
/// Working off the opentelemetry-prometheus sample code, I can hand a Prometheus "exporter"
/// to the Meter Provider, but to get an exporter, I need a "registry":
fn init_telemetry() -> Result<prometheus::Registry> {
    let registry = prometheus::Registry::new();

    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()
        .context(PrometheusExporterSnafu)?;

    global::set_meter_provider(SdkMeterProvider::builder().with_reader(exporter).build());
    Ok(registry)
}

/// Make this process into a daemon
///
/// The first step in daemonizing is to dissassociate this process from it's controlling terminal &
/// make sure it cannot acquire a new one. This, AFAIU, is to disconnect us from any job control
/// associated with that terminal, and in particular to prevent us from being disturbed when & if
/// that terminal is closed (I'm still hazy on the details, but at least the session leader (and
/// perhaps it's descendants) will be sent a `SIGHUP` in that eventuality).
///
/// After that, the rest of the work seems to consist of shedding all the things we (may have)
/// inherited from our creator. Things such as:
///
///   - present working directory
///   - umask
///   - all file descriptors
///     - stdin, stdout & stderr should be closed (we don't know from or to where they may have
///       been redirected), and re-opened to locations appropriate to this daemon
///     - any other file descriptors should be closed; this process can then re-open any that
///       it needs for its work
///
/// In the case of a Tokio program, there's an issue with the interaction between forking & the
/// tokio runtime-- tokio will spin-up a thread pool, and threads do not mix well with `fork()'. The
/// trick is to fork this process *before* starting-up the Tokio runtime.
///
/// The reader may object that this could all be handled by the
/// [daemonize](https://docs.rs/daemonize) crate. I chose not to introduce another dependency just
/// for the sake of a single function, and in any event, I learned a lot about process management
/// while doing so & wound up choosing to do a few things differently.
///
/// References:
///
///   - <http://www.steve.org.uk/Reference/Unix/faq_2.html>
///   - <https://en.wikipedia.org/wiki/SIGHUP>
///   - <http://www.enderunix.org/docs/eng/daemon.php>
fn daemonize(local_statedir: &Path, no_chdir: bool) -> Result<()> {
    use errno::errno;
    use std::os::unix::ffi::OsStringExt;

    unsafe {
        // Removing ourselves from from this process' controlling terminal's job control (if any).
        // Begin by forking; this does a few things:
        //
        // 1. returns control to the shell invoking us, if any
        // 2. guarantees that the child is not a process group leader
        let pid = fork();
        if pid < 0 {
            return ForkSnafu { errno: errno() }.fail();
        } else if pid != 0 {
            // We are the parent process-- exit.
            exit(0);
        }

        // In the last step, we said we wanted to be sure we are not a process group leader. That
        // is because this call will fail if we do. It will create a new session, with us as
        // session (and process) group leader.
        setsid();

        // Since controlling terminals are associated with sessions, we now have no controlling tty
        // (so no job control, no SIGHUP when cleaning up that terminal, &c). We now fork again
        // and let our parent (the session group leader) exit; this means that this process can
        // never regain a controlling tty.
        let pid = fork();
        if pid < 0 {
            return SecondForkSnafu { errno: errno() }.fail();
        } else if pid != 0 {
            // We are the parent process-- exit.
            exit(0);
        }

        // We next change the present working directory to avoid keeping the present one in
        // use. `indielinks`` can run pretty much anywhere, so /tmp is as good a place as any.
        if !no_chdir {
            // A little unhappy about hard-coding that, but if /tmp doesn't exist I expect few
            // things will work.
            std::env::set_current_dir("/tmp").context(ChangedirSnafu)?;
        }

        umask(0);

        // Close all file descriptors ("nuke 'em from orbit-- it's the only way to be sure")...
        let mut i = getdtablesize() - 1;
        while i > -1 {
            close(i);
            i -= 1;
        }
        // and re-open stdin, stdout & stderr all redirected to /dev/null. `i' will be zero, since
        // "The file descriptor returned by a successful call will be the lowest-numbered file
        // descriptor not currently open for the process"...
        i = open(b"/dev/null\0" as *const [u8; 10] as _, libc::O_RDWR);
        // and these two will be 1 & 2 for the same reason.
        dup(i);
        dup(i);

        let pth: PathBuf = [local_statedir.to_str().unwrap(), "run", "indielinks.pid"]
            .iter()
            .collect();
        let pth_c = CString::new(pth.into_os_string().into_vec()).unwrap();
        let mut fd = open(
            pth_c.as_ptr(),
            libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC,
            0o640,
        );
        if -1 == fd {
            // Fallback to pwd
            let pth_c = CString::new("indielinks.pid").unwrap();
            fd = open(
                pth_c.as_ptr(),
                libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC,
                0o640,
            );
            if -1 == fd {
                return OpenLockFileSnafu { errno: errno() }.fail();
            };
        }
        if lockf(fd, F_TLOCK, 0) < 0 {
            return LockFileSnafu { errno: errno() }.fail();
        }

        // "File locks are released as soon as the process holding the locks closes some file
        // descriptor for the file"-- just leave `fd' until this process terminates.
        let pid = getpid();
        let pid_buf = format!("{}", pid).into_bytes();
        let pid_length = pid_buf.len();
        let pid_c = CString::new(pid_buf).unwrap();
        if write(fd, pid_c.as_ptr() as *const libc::c_void, pid_length) < pid_length as isize {
            return WritePidSnafu { errno: errno() }.fail();
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    let opts = CliOpts::new(
        Command::new("indielinks")
            .version(crate_version!())
            .author(crate_authors!())
            .about("Bookmarks in the Fediverse")
            .long_about("`indielinks` is a federated bookmarking service.")
            .arg(
                Arg::new("config")
                    .short('c')
                    .long("config")
                    .num_args(1)
                    .value_parser(value_parser!(PathBuf))
                    .env("INDIELINKS_CONFIG")
                    .help(
                        "path (absolute or relative to the process' current directory) to a \
                       configuration file",
                    ),
            )
            .arg(
                Arg::new("local-state")
                    .short('L')
                    .long("local-state")
                    .num_args(1)
                    .value_parser(value_parser!(PathBuf))
                    .env("INDIELINKS_LOCALSTATEDIR")
                    .help(
                        "path (absolute or relative to the process' current directory) to the \
                           directory in which local state shall be stored (\"/var/run\", e.g.)",
                    ),
            )
            .arg(
                Arg::new("debug")
                    .short('D')
                    .long("debug")
                    .num_args(0)
                    .action(ArgAction::SetTrue)
                    .env("INDIELINKS_DEBUG")
                    .help("produce debug output"),
            )
            .arg(
                Arg::new("no-chdir")
                    .short('C')
                    .long("no-chdir")
                    .num_args(0)
                    .action(ArgAction::SetTrue)
                    .env("INDIELINKS_NO_CHDIR")
                    .help("Do not change directory before daemonizing; ignored if running in foreground")
            )
            .arg(
                Arg::new("no-daemon")
                    .short('F')
                    .long("no-daemon")
                    .num_args(0)
                    .action(ArgAction::SetTrue)
                    .env("INDIELINKS_NO_DAEMON")
                    .help("do not daemonize; remain in foreground"),
            )
            .arg(
                Arg::new("plain")
                    .short('p')
                    .long("plain")
                    .num_args(0)
                    .action(ArgAction::SetTrue)
                    .env("INDIELINKS_PLAIN")
                    .help("log in human-readable format, not JSON/structured logging"),
            )
            .arg(
                Arg::new("quiet")
                    .short('q')
                    .long("quiet")
                    .num_args(0)
                    .action(ArgAction::SetTrue)
                    .env("INDIELINKS_QUIET")
                    .help("produce only error output"),
            )
            .arg(
                Arg::new("verbose")
                    .short('v')
                    .long("verbose")
                    .num_args(0)
                    .action(ArgAction::SetTrue)
                    .env("INDIELINKS_VERBOSE")
                    .help("produce prolix output"),
            )
            .get_matches(),
    )?;
    // There are a number of things that can go wrong in the process of daemonization *after* we've
    // forked this process & lost the terminal to which we could write error messages. For instance:
    //
    //     1) the process doesn't have access to the location at which we're writing the PID file;
    //     this can happen when running it as oneself during development when LOCALSTATEDIR is
    //     configured to, say /usr/local/var
    //
    //     2) the configuration file is given as a relative path; this fails because by the time we
    //     try to open it, we've already cd'd to /tmp
    //
    // If this happens, the child process will simply exit leaving no trace of what went wrong,
    // which is extremely frustrating for operators. Regrettably, my logging implementation in turn
    // depends upon Tokio! Perhaps I can setup a "simple" logging facility for now, to be replaced
    // by the full-blown tracing implementation.
    if opts.log_opts.daemon {
        daemonize(&opts.local_statedir, opts.no_chdir)?;
    }
    // spin-up the Tokio runtime...
    tokio::runtime::Runtime::new()
        .context(TokioRuntimeSnafu)?
        // and start our server:
        .block_on(serve(init_telemetry()?, opts))
}
