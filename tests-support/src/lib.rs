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

//! # [indielinks] Integration Test Support
//!
//! [indielinks]: ../indielinks/index.html
//!
//! ## Introduction
//!
//! The Rust unit & integration testing framework is really oriented toward testing *libraries*, not
//! programs. "Integration tests" are intended to test a library package from the perspective of a
//! consumer of that library (rather than a privileged insider, as with unit tests). These are, of
//! course, common testing needs, and indielinks eagerly makes use of them. However, testing a
//! binary package, or any crate that requires additional services at runtime (a database or a
//! cache, for instance), requires the test author to implement that support on their own.
//!
//! There's no notion of test fixtures, nor even of simple setup & teardown operations that apply to
//! multiple tests. indielinks has a number of cases like this. [Nextest] provides a great number of
//! features above & beyond what `cargo test` offers, but 1) is implemented as a Cargo plugin (i.e.
//! to use it one needs to say `cargo nextest` rather than `cargo test`) and 2) still provides
//! nothing in terms of test fixtures.
//!
//! [Nextest]: https://nexte.st
//!
//! Initially, I just handled each case individually & separately, writing whatever support code I
//! needed. I wrote about the process [here]. As time went on, patterns began to appear, enough to
//! think about factoring out common logic.
//!
//! [here]: https://www.unwoundstack.com/blog/integration-testing-rust-binaries.html
//!
//! T.J. Telan [found] himself in the same boat when building [Fluvio] and lays out an approach that
//! takes more work, but offers limitless opportunities for customization while still fitting into
//! the `cargo test` framework: change-out the default *test harness*. In Cargo.toml, one is free to
//! explicitly define tests, and opt-out of `libtest`, the default testing harness provided by Rust:
//!
//! ```toml
//! [[test]]
//!     name = "delicious"
//!     harness = false
//! ```
//!
//! [found]: https://tjtelan.com/blog/rust-custom-test-harness/
//! [Fluvio]: https://github.com/infinyon/fluvio
//!
//! With this configuration, `cargo test` now expects to find a file named `delicious.rs` in the
//! `tests` subdirectory containing a `main()`. `cargo test` will compile it and execute it, passing
//! any command-line parameters passed to `cargo test` after the `--`. It is expected to exit with
//! status zero if all tests passed, and one else. That's it-- that's the contract.
//!
//! [Advanced Testing in Rust] notes that a compliant implementation could simply ignore the
//! command-line parameters, albeit at the cost of surprising one's users, and suggests the use of
//! [libtest-mimic] to avoid that.
//!
//! [Advanced Testing in Rust]: https://rust-exercises.com/advanced-testing/00_intro/00_welcome.html
//! [libtest-mimic]: https://docs.rs/libtest-mimic/latest/libtest_mimic/index.html
//!
//! This (internal) crate is intended to make writing integration tests in this style much easier by
//! reducing boilerplate as well as providing helpful utilities. It is not a general-purpose
//! integration testing library; rather it is tailored to the indielinks project.
//!
//! ## Ontology
//!
//! There are different aspects of the indielinks project that need this sort of support. The
//! distributed caching facility needs multiple instances of a test binary built on top of the
//! [indielinks-cache] crate, a database, and a client. The background task processing facility
//! needs a database. The webservice itself can be configured in numerous ways, run as a single node
//! or in a cluster, and again needs a database. Each of these aspects will call for its own sort of
//! test fixture and its own set of configuration parameters. In each case, we'll likely want to run
//! a given test case against multiple instantiations of that fixture and that configuration. For
//! instance, having written a test for the indielinks web service, we'd probably want to run it
//! with the web service backed by ScyllaDB and again by DynamoDB. When configured as a single node
//! & when running in a cluster. And so on.
//!
//! We term each of these aspects a _domain_. To each domain corresponds a _fixture_ and a _domain
//! configuration_. By _fixture_ we mean a collection of associated services (like databases) that
//! can perhaps be instantiated in multiple ways (we can provide a database via ScyllaDB or via
//! DynamoDB). Different instances of such a fixture fix those choices of associated service; so the
//! fixture defined by the background task processing domain might have two instances: one for
//! SycllaDB & one for DynamoDB.
//!
//! Likewise, each domain defines a _domain configuration_: the set of all parameters needed
//! to provision any possible instance of that domain's fixture. The domain configuration for
//! background task processing would include configuration allowing the fixture to communicate
//! with either database.
//!
//! Finally, we define for each domain a _backend_. A backend is a type intended to provide various
//! services to integration tests. In the case of background task processing, the backend might
//! contain the transmit side of the channel used to send tasks to the system.
//!
//! We expect one or more integration test binaries for each domain, but not any integration test
//! binary that spans domains.
//!
//! ## Using the Library
//!
//! Each domain is expected to define its own fixture, configuration & test types. Both synchronous
//! and async tests are supported. This crate contains a simple example in it's `trivial`
//! integration test.
//!
//! Prescriptively, in order to use [tests-support](crate), integration test authors will:
//!
//! 1. define their fixture type; implement the [Fixture] trait on it
//! 2. define their test type; implement either [SyncIntegrationTest] or [AsyncIntegrationTest] on it
//! 3. provide for something that implements `Iterator<Item = &'static F>` where `F` is the fixture
//!    type
//! 4. provide for something that implements `IntoIterator<Item = &'static T>` where `T` is the test
//!    type. This iterator will need to be re-used, so [tests-support](crate) also requires that
//!    `IntoIterator::IntoIter` be `Clone`
//! 5. decide how to obtain general, test runner configuration & domain-specific configuration (on
//!    which more below)
//! 6. call either [sync_integration_test] or [async_integration_test] from their `main()` with
//!    a [TestConfiguration] instance
//!
//! ### Finding Configuration
//!
//! [libtest-mimic] offers no way to add new command-line switches, so to enable the user to specify
//! a configuration file, the integration test author will have to fall back on environment
//! variables. [tests-support](crate) provides a few utilities for dealing with this. As a test
//! author, you have two options:
//!
//! 1. if the domain-specific configuration is [Default], then [TestConfiguration] will be, as well.
//!    Call [TestConfiguration::new_or_default()]; it will examine the `INDIELINKS_TEST_CONFIG` and
//!    if it's set, interpret it as a path (relative to the crate root) to a configuration file. If
//!    it's not set, it will return the default configuratoin
//!
//! 2. otherwise, define a "default" location for an expected configuration file, and call
//!    [TestConfiguration::new] with it; the user can still override that via `INDIELINKS_TEST_CONFIG`
//!
//! ## Implementation Notes
//!
//! A perhaps misguided desire to make this library generic across different sorts of projects has
//! led me fairly deeply into Rust's trait system (on several methods, the list of trait bounds is
//! longer than the parameter list; it feels like library development in C++: a lot of work under
//! the hood in order to present a simple API to callers). The code also reflects the time I've
//! recently spent working in functional languages.
//!
//! One particular design decision deserves some discussion. Since we're using [libtest-mimic],
//! at the end of the day, we need to produce from the caller-supplied tests (whatever form they
//! may take) a "runner" that satisfies the trait bound:
//!
//! ```ignore
//! FnOnce() -> Result<(), Failed> + Send + 'static
//! ```
//!
//! In other words, the runner may not contain any references, or if it does, they must have
//! `'static` lifetime. This mean that I had to choose between moving the caller-supplied tests into
//! the runner (infeasible since we need to reuse them), cloning them and moving the clones into the
//! runner (better, but still imposes a non-trivial obligation on caller code), or require `'static`
//! references to the caller-supplied tests. Since at the time of this writing, I'm using the
//! [inventory] crate throughout, which provides iterators yielding static references to the
//! inventoried items, I chose the third option. The other obvious idiom, a static array of
//! test structures, would also produce static references.
//!
//! ### Code Layout
//!
//! How I like to layout these sort of integration tests:
//!
//! ```text
//! +-- project (1)
//! |   |
//! |   +-- tests (2)
//! |
//! +-- tests-project (3)
//!     |
//!     +-- src (4)
//!     |
//!     +-- tests (5)
//!         |
//!         +-- common (6)
//!             |
//!             +-- mod.rs
//! ```
//!
//! 1. `project` is a crate with a binary package you'd like to test. It takes a `[dev-dependency]`
//!    on the `tests-project` crate.
//! 2. `project` integration tests are as per usual: integration tests for the `project` *library* package
//! 3. `tests-project` is the crate we're concerned with here: it exists to house one or more
//!    integration tests for the `project` _binary_ pacakge
//! 4. `test-project/src` contains the "guts" of the test code, along with any utilities specific to
//!    this domain. By "guts" I mean test logic un-connected to this crate's types; just test functions
//!    in the natural vocabulary of this domain
//! 5. `test-project/tests` will contain one or more integration tests that have opted-out of
//!    `libtest` and use this crate instead. Here is where [Fixture]s, [SyncIntegrationTest]s and
//!    [AsyncIntegrationTest]s are defined
//! 6. If there are multiple integration tests, and they need to share code related to this crate,
//!    it can be shared in the usual manner here
//!
//! ### Instantiating Fixtures
//!
//! In hindsight, I probably erred in having the caller supply an iterator yielding `'static F:
//! Fixture`. This means we can't mutate fixtures after they've been instantiated. This is
//! inconvenient if, for example, we'd like the "setup" operation to modify fixture state (say, by
//! caching a copy of the "backend"). It would have perhaps been better to have the entities
//! provided by our caller be factories that know how to instantiate fixtures. This library could
//! then instantiate each and invoke methods taking a receiver of `&mut self`. So far, I can get
//! away with the current arrangement, and I'm not eager to undertake another rewrite just yet.

use std::{
    collections::HashSet,
    env, fs,
    hash::Hash,
    io,
    path::{Path, PathBuf},
    process::ExitCode,
    result::Result as StdResult,
    str::FromStr,
    sync::Arc,
};

use async_trait::async_trait;
use itertools::Itertools;
use libtest_mimic::{Arguments, Conclusion, Failed, Trial};
use serde::{Deserialize, de::DeserializeOwned};
use snafu::{Backtrace, IntoError, ResultExt, Snafu};
use tap::Pipe;
use tokio::runtime::Runtime;
use tracing::{Level, debug, info};
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           Error type                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

// This will almost surely need to be refactored once this crate grows.
#[derive(Debug, Snafu)]
pub enum Error<F>
where
    F: std::fmt::Debug + Fixture, // See below for the definition of `Fixture`
    <<F as Fixture>::Id as FromStr>::Err: std::error::Error + 'static,
{
    #[snafu(display("Failed to parse {pathb:?}: {source}"))]
    De {
        pathb: PathBuf,
        #[snafu(source(from(toml::de::Error, Box::new)))]
        source: Box<toml::de::Error>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to read INDIELINKS_TEST_CONFIG: {source}"))]
    Env {
        source: std::env::VarError,
        backtrace: Backtrace,
    },
    #[snafu(display("While parsing the RUST_LOG environment variable, {source}"))]
    Filter {
        source: tracing_subscriber::filter::FromEnvError,
        backtrace: Backtrace,
    },
    #[snafu(display("A test fixture reports {source}"))]
    Fixture {
        source: <F as Fixture>::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Unable to parse a fixture Id: {source}"))]
    FixtureId {
        source: <<F as Fixture>::Id as FromStr>::Err,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to read {pathb:?}: {source}"))]
    Read {
        pathb: PathBuf,
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("While setting the global tracing subscriber, {source}"))]
    SetGlobalDefault {
        source: tracing::subscriber::SetGlobalDefaultError,
        backtrace: Backtrace,
    },
}

type Result<T, F> = std::result::Result<T, Error<F>>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         public traits                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// The [Fixture] is the fundamental building block for a domain's integration tests. Each instance
/// of the type implementing this trait represents a distinct collection of associated services
/// required to test that domain. All instances must support setup, teardown and the provisioning of
/// backend instances.
///
/// Note that `setup()` is guaranteed to be called before `new_backend()`, in case the "backend" is
/// dependent on something spun-up during `setup()` (say, a database client). Each of `setup()`,
/// `new_backend()`, and `teardown()` will called exactly once, in that order, per execution of the
/// program.
#[async_trait]
pub trait Fixture {
    type Error: std::error::Error + 'static;
    /// Domain-specific services provided to integration tests
    type Backend: Clone + Send + 'static;
    /// The type of configuration salient to this sort of fixture
    type Configuration: Clone + DeserializeOwned + Send + 'static;
    // This will get some trait bounds
    type Id: Eq + Hash + FromStr;
    /// Identify this [Fixture]; if two instances return the same `Id`, they should compare [Eq]
    fn id(&self) -> Self::Id;
    /// A human-readable name for this fixture; it's intended for display purposes,
    /// but will be part of each test's name from the perspective of [libtest-mimic], and
    /// so will play a part in test filtering
    fn get_name(&self) -> String;
    /// Create this domain's "backend"
    async fn new_backend(
        &self,
        config: &Self::Configuration,
    ) -> StdResult<Self::Backend, Self::Error>;
    /// Setup this test fixture
    async fn setup(&self, config: &Self::Configuration) -> StdResult<(), Self::Error>;
    /// Teardown this test fixture
    async fn teardown(&self, config: &Self::Configuration) -> StdResult<(), Self::Error>;
}

/// Once [Fixture] is defined, the next step is to define the test type. This trait defines the
/// operations the test type must support in order to work with [Fixture]s.
///
/// We do not assume that a given test will be run in every instance of its associated [Fixture]
/// type. The `germane()` method allows a given test to indicate which fixtures it will run in. The
/// default implementation returns true for all.
// Test authors will likely find it convenient to write async test functions taking whatever
// parameters are salient. The test type will then be quite simple, extracting the required
// parameters from the domain backend and/or configuration & passing them along. The idea is to make
// it as easy as possible to write const constructors for use with inventorying.
pub trait IntegrationTest {
    /// The sort of [Fixture] this test expects
    type F: Fixture;
    fn germane(&self, _: <Self::F as Fixture>::Id) -> bool {
        true
    }
    /// The name is mostly used for display purposes when reporting test results. However, the tests
    /// will be run in lexicographical order of name, so test authors will perhaps want to name
    /// "smoke" tests, or tests more likely to fail in such a way as to have them sort first.
    fn name(&self) -> String;
}

#[async_trait]
pub trait AsyncIntegrationTest: IntegrationTest {
    async fn run(
        &self,
        config: <Self::F as Fixture>::Configuration,
        backend: <Self::F as Fixture>::Backend,
    ) -> StdResult<(), Failed>;
}

pub trait SyncIntegrationTest: IntegrationTest {
    fn run(
        &self,
        config: <Self::F as Fixture>::Configuration,
        backend: <Self::F as Fixture>::Backend,
    ) -> StdResult<(), Failed>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       test configuration                                       //
////////////////////////////////////////////////////////////////////////////////////////////////////

mod de_level {
    use super::*;

    use serde::Deserializer;
    use std::str::FromStr;

    pub fn deserialize<'de, D>(deserializer: D) -> StdResult<Level, D::Error>
    where
        D: Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        Level::from_str(&text).map_err(|_| {
            serde::de::Error::custom(format!("{text} cannot be interepreted as a log level"))
        })
    }
}

/// Configuration for the test runner itself
///
/// Configuration that applies across all domains belongs here.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestRunnerConfiguration {
    #[serde(deserialize_with = "de_level::deserialize", rename = "log-level")]
    pub log_level: Level,
    pub logging: bool,
    #[serde(rename = "no-setup")]
    pub no_setup: bool,
    #[serde(rename = "no-teardown")]
    pub no_teardown: bool,
    #[serde(rename = "enforce-single-threaded")]
    pub enforce_single_threaded: bool,
}

impl Default for TestRunnerConfiguration {
    fn default() -> Self {
        Self {
            log_level: Level::INFO,
            logging: false,
            no_setup: false,
            no_teardown: false,
            enforce_single_threaded: true,
        }
    }
}

// This is the core execution engine for this library. The signature may look imposing,
// but I've esentially parameterized it over:
// - how to get the list of tests (type parameter `IT`)
// - how to run the tests (R & S)
// - the test type itslef (T)
//
// Type checking aside, here is where we iterate over all tests germane to a single, given
// fixture and execute them.
fn execute_fixture<IT, R, S, T>(
    tests: IT,      // An iterator over our tests; Item = 'static T
    test_runner: R, // How to go from a &'static T to an FnOnce() -> StdResult<(), Failed> + Send + 'static
    args: &Arguments,
    runner_config: &TestRunnerConfiguration,
    domain_config: &<<T as IntegrationTest>::F as Fixture>::Configuration,
    rt: Arc<Runtime>,
    fixture: &<T as IntegrationTest>::F,
) -> StdResult<Conclusion, <<T as IntegrationTest>::F as Fixture>::Error>
where
    IT: Iterator<Item = &'static T>,
    R: Fn(
        <<T as IntegrationTest>::F as Fixture>::Configuration,
        <<T as IntegrationTest>::F as Fixture>::Backend,
        Arc<Runtime>,
        &'static T,
    ) -> S,
    S: FnOnce() -> StdResult<(), Failed> + Send + 'static,
    T: IntegrationTest + 'static,
{
    if !runner_config.no_setup {
        rt.block_on(fixture.setup(domain_config))?;
    }

    let backend = rt.block_on(fixture.new_backend(domain_config))?;

    let conclusion = libtest_mimic::run(
        args,
        tests
            .sorted_by_key(|test| test.name())
            .filter(|test| test.germane(fixture.id()))
            .map(|test| {
                Trial::test(
                    format!("{}/{}", fixture.get_name(), test.name()),
                    test_runner(domain_config.clone(), backend.clone(), rt.clone(), test),
                )
            })
            .collect(),
    );

    // drop the backend here, since it will likely depend on the test fixture. For instance, some of
    // my tests have a backend that owns a ScyllaDB connection, which spews logs when the cluster
    // goes down (!)
    drop(backend);

    if !runner_config.no_teardown {
        rt.block_on(fixture.teardown(domain_config))?;
    }

    Ok(conclusion)
}

// The thing we return must be 'static, so we can either move the test into this function, or demand
// a borrow with 'static lifetime. I chose the latter.
fn run_sync<Test>(
    configuration: <<Test as IntegrationTest>::F as Fixture>::Configuration,
    backend: <<Test as IntegrationTest>::F as Fixture>::Backend,
    _: Arc<Runtime>,
    test: &'static Test,
) -> impl FnOnce() -> StdResult<(), Failed> + Send + 'static
where
    Test: SyncIntegrationTest + Sync,
{
    move || test.run(configuration, backend)
}

// The thing we return must be 'static, so we can either move the test into this function, or demand
// a borrow with 'static lifetime. I chose the latter.
fn run_async<Test>(
    configuration: <<Test as IntegrationTest>::F as Fixture>::Configuration,
    backend: <<Test as IntegrationTest>::F as Fixture>::Backend,
    rt: Arc<Runtime>,
    test: &'static Test,
) -> impl FnOnce() -> StdResult<(), Failed> + Send + 'static
where
    Test: AsyncIntegrationTest + Sync,
{
    let configuration = configuration.clone();
    let backend = backend.clone();
    move || rt.block_on(async { test.run(configuration, backend).await })
}

/// A unified configuration structure for any given integration test.
///
/// This struct contains both the test runner configuration as well as domain-specific
/// configuration. I think it would be nicer to keep them separate, but the contraints of
/// `libtest-mimic` make it difficult to define additional runtime options (such as a configuration
/// file), so I only want the user to have to specify one.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestConfiguration<F: Fixture> {
    pub runner: Option<TestRunnerConfiguration>,
    pub domain: F::Configuration,
}

// It's up to the integration test to somehow obtain configuration. Here we provide a few utilities.
// Some are only available if the domain configuration implements `Default`.
impl<F: Fixture> Default for TestConfiguration<F>
where
    F::Configuration: Default,
{
    fn default() -> Self {
        Self {
            runner: Default::default(),
            domain: Default::default(),
        }
    }
}

impl<F> TestConfiguration<F>
where
    F: std::fmt::Debug + Fixture,
    <<F as Fixture>::Id as FromStr>::Err: std::error::Error,
{
    pub fn new<P: AsRef<Path>>(
        env_var: &str,
        default_config: P,
    ) -> Result<TestConfiguration<F>, F> {
        match env::var(env_var) {
            Ok(s) => Self::from_path(s),
            Err(env::VarError::NotPresent) => Self::from_path(default_config.as_ref()),
            Err(err) => Err(EnvSnafu.into_error(err)),
        }
    }
    fn from_path<P: AsRef<Path>>(pth: P) -> Result<TestConfiguration<F>, F> {
        fs::read_to_string(pth.as_ref())
            .context(ReadSnafu {
                pathb: pth.as_ref().to_path_buf(),
            })?
            .pipe(|text| toml::from_str::<TestConfiguration<F>>(&text))
            .context(DeSnafu {
                pathb: pth.as_ref().to_path_buf(),
            })
    }
}

impl<F> TestConfiguration<F>
where
    F: std::fmt::Debug + Fixture,
    F::Configuration: Default,
    <<F as Fixture>::Id as FromStr>::Err: std::error::Error,
{
    pub fn new_or_default(env_var: &str) -> Result<TestConfiguration<F>, F> {
        match env::var(env_var) {
            Ok(s) => Self::from_path(s),
            Err(env::VarError::NotPresent) => Ok(Default::default()),
            Err(err) => Err(EnvSnafu.into_error(err)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                      public entry points                                       //
////////////////////////////////////////////////////////////////////////////////////////////////////

// Execute an integration test suite across one or more fixtures.
//
// The caller is responsible for obtaining configuration.
//
// This will exit with status zero on test success, 101 on test failure & 1 on error-- I don't
// think this is stricly compliant with the cargo convention, but I like the distinction between
// test failure & program error (Nb that if a test returns `Failed`, that will show-up as a test
// failure).
//
// This is the core routine for this library; it can handle both synchronous & asynchronous tests.
fn integration_test<IF, IT, R, S, F, T>(
    runner_config: Option<TestRunnerConfiguration>,
    domain_config: <F as Fixture>::Configuration,
    fixtures: IF, // `Iterator`
    tests: IT,    // `IntoIterator` with a Clonable `IntoIter` so we can reuse it
    test_runner: R,
) -> Result<ExitCode, F>
where
    IF: Iterator<Item = &'static F>,
    F: Fixture + 'static,
    IT: IntoIterator<Item = &'static T>,
    IT::IntoIter: Clone,
    R: Fn(
        <<T as IntegrationTest>::F as Fixture>::Configuration,
        <<T as IntegrationTest>::F as Fixture>::Backend,
        Arc<Runtime>,
        &'static T,
    ) -> S,
    S: FnOnce() -> StdResult<(), Failed> + Send + 'static,
    T: IntegrationTest<F = F> + 'static,
    F: std::fmt::Debug,
    <<F as Fixture>::Id as FromStr>::Err: std::error::Error,
{
    // Decorating main() with `#[tokio::main]` won't work, because it leaves us with no way to get
    // our hands on that runtime. The solution is to construct the runtime ourselves.
    let rt = Arc::new(Runtime::new().expect("Failed to build a tokio multi-threaded runtime"));

    let runner_config = runner_config.unwrap_or_default();

    // Armed with our configuration, we can configure logging: if logging is requested, we'll log at
    // the configured default level to `stdout`. The filter can be overridden via the (conventional)
    // `RUST_LOG` environment variable.
    if runner_config.logging {
        let filter = EnvFilter::builder()
            .with_default_directive(runner_config.log_level.into())
            .from_env()
            .context(FilterSnafu)?;
        // This is a personal indulgence-- I often run compilations inside Emacs, and while
        // libtest-mimic somehow figures-out that it's not running inside a full-on terminal,
        // tracing-subscriber does not. Recall that `env::var()` will return
        // `Err(VarError::NotPresent)` if the variable is not set.
        let with_ansi = env::var("INSIDE_EMACS").is_err();
        tracing::subscriber::set_global_default(
            Registry::default()
                .with(
                    fmt::Layer::default()
                        .compact()
                        .with_ansi(with_ansi)
                        .with_writer(io::stdout),
                )
                .with(filter),
        )
        .context(SetGlobalDefaultSnafu)?;
    }

    debug!("Logging configured.");

    // I want to provide a quick & easy way for the developer to scope an execution to a fixture or
    // small set of fixtures. We can't extend the set of command-line arguments, so that leaves us
    // environment variables.
    let enabled_fixtures = match env::var("INDIELINKS_TEST_FIXTURES") {
        Ok(s) => Some(s),
        Err(env::VarError::NotPresent) => None,
        Err(err) => {
            return Err(EnvSnafu.into_error(err));
        }
    }
    .map(|s| {
        s.split(',')
            .map(|s| s.parse::<<F as Fixture>::Id>())
            .collect::<StdResult<HashSet<<F as Fixture>::Id>, _>>()
    })
    .transpose()
    .context(FixtureIdSnafu)?;

    let mut args = Arguments::from_args();

    if runner_config.enforce_single_threaded {
        info!("Temporarily overriding --test-threads to 1.");
        args.test_threads = Some(1);
    }

    let tests = tests.into_iter();

    let failed = fixtures
        .filter(|fix| {
            enabled_fixtures
                .as_ref()
                .map(|m| m.contains(&fix.id()))
                .unwrap_or(true)
        })
        .map(|fixture| {
            execute_fixture(
                tests.clone(),
                &test_runner,
                &args,
                &runner_config,
                &domain_config,
                rt.clone(),
                fixture,
            )
        })
        .collect::<StdResult<Vec<Conclusion>, _>>()
        .context(FixtureSnafu)?
        .iter()
        .any(Conclusion::has_failed);

    Ok(if failed {
        ExitCode::from(101)
    } else {
        ExitCode::SUCCESS
    })
}

/// Execute a synchronous integration test
///
/// This will exit with status zero on test success, 101 on test failure & 1 on error-- I don't
/// think this is stricly compliant with the cargo convention, but I like the distinction between
/// test failure & program error (Nb that if a test returns `Failed`, that will show-up as a test
/// failure).
pub fn sync_integration_test<IF, IT, F, T>(
    TestConfiguration { runner, domain }: TestConfiguration<F>,
    fixtures: IF,
    tests: IT,
) -> Result<ExitCode, F>
where
    F: std::fmt::Debug + Fixture + 'static,
    <<F as Fixture>::Id as FromStr>::Err: std::error::Error,
    IF: Iterator<Item = &'static F>,
    IT: IntoIterator<Item = &'static T>,
    T: SyncIntegrationTest<F = F> + Sync + 'static,
    IT::IntoIter: Clone,
{
    integration_test(runner, domain, fixtures, tests, run_sync)
}

/// Execute an asynchronous integration test
///
/// This will exit with status zero on test success, 101 on test failure & 1 on error-- I don't
/// think this is stricly compliant with the cargo convention, but I like the distinction between
/// test failure & program error (Nb that if a test returns `Failed`, that will show-up as a test
/// failure).
pub fn async_integration_test<IF, IT, F, T>(
    TestConfiguration { runner, domain }: TestConfiguration<F>,
    fixtures: IF,
    tests: IT,
) -> Result<ExitCode, F>
where
    F: std::fmt::Debug + Fixture + 'static,
    <<F as Fixture>::Id as FromStr>::Err: std::error::Error,
    IF: Iterator<Item = &'static F>,
    IT: IntoIterator<Item = &'static T>,
    T: AsyncIntegrationTest<F = F> + Sync + 'static,
    IT::IntoIter: Clone,
{
    integration_test(runner, domain, fixtures, tests, run_async)
}
