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

// Let's tighten-up the types for this, a bit.

use std::{borrow::Cow, result::Result as StdResult};

use aws_config::{BehaviorVersion, meta::region::RegionProviderChain};
use aws_sdk_dynamodb::config::Region as AwsSdkRegion;
use either::Either;
use itertools::Itertools;
use secrecy::ExposeSecret;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use snafu::{Backtrace, Snafu};
use url::Url;

use crate::credentials::Credentials;

/// A serializable [Region](aws_sdk_dynamodb::config::Region). Implemented as a newtype around the
/// AWS SDK's [Region] to work around Rust's orphan traits rule
// Not making this sortable since the AWS SDK doesn't
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct Region(pub AwsSdkRegion);

impl Region {
    /// Creates a new `Region` from the given string.
    pub fn new(region: impl Into<Cow<'static, str>>) -> Self {
        Self(AwsSdkRegion::new(region))
    }

    /// Const function that creates a new `Region` from a static str.
    pub const fn from_static(region: &'static str) -> Self {
        Self(AwsSdkRegion::from_static(region))
    }
}

impl std::fmt::Display for Region {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::ops::Deref for Region {
    type Target = AwsSdkRegion;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::convert::AsRef<AwsSdkRegion> for Region {
    fn as_ref(&self) -> &AwsSdkRegion {
        &self.0
    }
}

impl std::convert::AsRef<str> for Region {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl<'de> Deserialize<'de> for Region {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        Ok(Region::new(s))
    }
}

impl Serialize for Region {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.0.as_ref())
    }
}

/// Where to find DynamoDB on the network. Again implemented as a newtype due to orphan traits
/// rules. [Location] is not only serializable, but also parsable by [clap].
// Not making this sortable since `Region` isn't
#[derive(Clone, Debug, Deserialize, Hash, Eq, PartialEq, Serialize)]
pub struct Location(#[serde(with = "either::serde_untagged")] pub Either<Region, Vec<Url>>);

impl std::fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Either::Left(region) => write!(f, "{region}"),
            Either::Right(urls) => write!(
                f,
                "[{}]",
                urls.iter()
                    .map(Url::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

impl std::ops::Deref for Location {
    type Target = Either<Region, Vec<Url>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Region> for Location {
    fn from(value: Region) -> Self {
        Location(Either::Left(value))
    }
}

impl From<Vec<Url>> for Location {
    fn from(value: Vec<Url>) -> Self {
        Location(Either::Right(value))
    }
}

impl std::convert::AsRef<Either<Region, Vec<Url>>> for Location {
    fn as_ref(&self) -> &Either<Region, Vec<Url>> {
        &self.0
    }
}

impl clap::builder::ValueParserFactory for Location {
    type Parser = LocationParser;

    fn value_parser() -> Self::Parser {
        LocationParser
    }
}

#[derive(Clone, Debug)]
pub struct LocationParser;

impl clap::builder::TypedValueParser for LocationParser {
    type Value = Location;

    fn parse_ref(
        &self,
        _cmd: &clap::Command,
        _arg: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> std::result::Result<Self::Value, clap::Error> {
        use clap::error::ErrorKind;
        let vals = value
            .to_str()
            .ok_or(clap::Error::new(ErrorKind::InvalidValue))?
            .split(',')
            .collect::<Vec<&str>>();
        match vals.iter().exactly_one() {
            Ok(s) => Ok(Location(match Url::parse(s) {
                Ok(url) => Either::Right(vec![url]),
                Err(_) => Either::Left(Region::new(s.to_string())),
            })),
            Err(_) => Ok(Location(Either::Right(
                vals.iter()
                    .cloned()
                    .map(Url::parse)
                    .collect::<std::result::Result<Vec<Url>, _>>()
                    .map_err(|_| clap::Error::new(ErrorKind::InvalidValue))?,
            ))),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       client construction                                      //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Errors encountered while constructing a DynamoDB/Alternator client.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("No endpoint URLs specified"))]
    NoEndpoints { backtrace: Backtrace },
}

pub type Result<T> = StdResult<T, Error>;

/// How a DynamoDB client should source its AWS credentials.
#[derive(Clone, Debug)]
enum CredentialsSource {
    /// Use the credentials explicitly supplied by the operator.
    Explicit(aws_sdk_dynamodb::config::Credentials),
    /// Leave the AWS SDK's default credential provider chain in place.
    DefaultChain,
    /// Install throwaway credentials so that request signing succeeds against a local Alternator
    /// (which doesn't verify them).
    Test,
}

/// Decide how a DynamoDB client created for `location` should source its AWS credentials.
fn credentials_source(location: &Location, credentials: Option<&Credentials>) -> CredentialsSource {
    match (&location.0, credentials) {
        (_, Some(Credentials((id, secret)))) => {
            CredentialsSource::Explicit(aws_sdk_dynamodb::config::Credentials::new(
                id.expose_secret(),
                secret.expose_secret(),
                None,
                None,
                "indielinks",
            ))
        }
        (Either::Left(_), None) => CredentialsSource::DefaultChain,
        (Either::Right(_), None) => CredentialsSource::Test,
    }
}

/// Build the AWS SDK config loader from which our DynamoDB client is created.
fn client_config_loader(
    location: &Location,
    credentials: Option<&Credentials>,
) -> Result<aws_config::ConfigLoader> {
    match &location.0 {
        Either::Left(region) => Ok(aws_config::defaults(BehaviorVersion::latest()).region(
            RegionProviderChain::first_try(Some(region.0.clone()))
                .or_default_provider()
                .or_else(AwsSdkRegion::new("us-west-2")),
        )),
        Either::Right(endpoints) => endpoints
            .first()
            .map(|endpoint_url| {
                aws_config::defaults(BehaviorVersion::latest())
                    // We *have* to specify a region for request signing; for a local Alternator the
                    // value doesn't matter, so just pick one.
                    .region("us-west-2")
                    .endpoint_url(endpoint_url.as_str())
            })
            .ok_or(NoEndpointsSnafu {}.build()),
    }
    .map(|loader| match credentials_source(location, credentials) {
        CredentialsSource::Explicit(credentials) => loader.credentials_provider(credentials),
        CredentialsSource::DefaultChain => loader,
        CredentialsSource::Test => loader.test_credentials(),
    })
}

/// Create an AWS SDK DynamoDB client.
pub async fn create_client(
    location: &Location,
    credentials: Option<&Credentials>,
) -> Result<aws_sdk_dynamodb::Client> {
    Ok(aws_sdk_dynamodb::Client::new(
        &client_config_loader(location, credentials)?.load().await,
    ))
}

#[cfg(test)]
mod test {
    use super::*;

    use aws_sdk_dynamodb::config::ProvideCredentials;

    fn region_location() -> Location {
        Location::from(Region::new("us-west-2"))
    }

    fn alternator_location() -> Location {
        Location::from(vec![
            Url::parse("http://localhost:8000").expect("the Alternator URL should parse"),
        ])
    }

    fn explicit_credentials() -> Credentials {
        Credentials(("test-access-key-id".into(), "test-secret-access-key".into()))
    }

    #[test]
    fn regional_location_without_credentials_preserves_default_chain() {
        assert!(matches!(
            credentials_source(&region_location(), None),
            CredentialsSource::DefaultChain
        ));
    }

    #[test]
    fn explicit_credentials_override_wherever_supplied() {
        let credentials = explicit_credentials();
        for location in [region_location(), alternator_location()] {
            match credentials_source(&location, Some(&credentials)) {
                CredentialsSource::Explicit(credentials) => {
                    assert_eq!(credentials.access_key_id(), "test-access-key-id");
                    assert_eq!(credentials.secret_access_key(), "test-secret-access-key");
                }
                source => panic!("explicit credentials should be selected, got {source:?}"),
            }
        }
    }

    #[test]
    fn alternator_without_credentials_uses_test_credentials() {
        assert!(matches!(
            credentials_source(&alternator_location(), None),
            CredentialsSource::Test
        ));
    }

    #[tokio::test]
    async fn regional_loader_resolves_explicit_credentials() {
        let credentials = explicit_credentials();
        let config = client_config_loader(&region_location(), Some(&credentials))
            .expect("the loader should build")
            .load()
            .await;
        let credentials = config
            .credentials_provider()
            .expect("a credentials provider should be installed")
            .provide_credentials()
            .await
            .expect("explicit credentials should resolve");
        assert_eq!(credentials.access_key_id(), "test-access-key-id");
        assert_eq!(credentials.secret_access_key(), "test-secret-access-key");
    }

    #[tokio::test]
    async fn alternator_loader_resolves_test_credentials() {
        let config = client_config_loader(&alternator_location(), None)
            .expect("the loader should build")
            .load()
            .await;
        let credentials = config
            .credentials_provider()
            .expect("a credentials provider should be installed")
            .provide_credentials()
            .await
            .expect("test credentials should resolve");
        assert_eq!(credentials.access_key_id(), "ANOTREAL");
    }
}
