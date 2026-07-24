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

//! # application configuration
//!
//! ## Introduction
//!
//! This module handles application configuration for the `indielinksd` daemon process.

use std::{
    collections::HashSet, net::SocketAddr, num::NonZeroU32, path::PathBuf,
    result::Result as StdResult, str::FromStr,
};

use chrono::Duration;
use clap::crate_version;
use http::HeaderName;
use indielinks_shared::{
    origin::{NetLoc, Origin},
    service::ExponentialBackoffParameters,
};
use nonzero::nonzero;
use secrecy::SecretString;
use serde::Deserialize;
use snafu::{ResultExt, Snafu};
use url::Url;

use indielinks_cache::raft::Configuration as RaftConfiguration;

use crate::{
    background_tasks, dynamodb::Location as DynamoLocation, http::SameSite, peppers::Peppers,
    signing_keys::SigningKeys, util::Credentials,
};

#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum Error {
    #[snafu(display("While reading the header blacklist, {source}"))]
    HeaderBlacklist {
        source: http::header::InvalidHeaderName,
    },
}

pub type Result<T> = StdResult<T, Error>;

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
        credentials: Option<Credentials>,
        /// ScyllaDB hosts; specify as "host:port" (or anything that can be parsed as a [SocketAddr])
        hosts: Vec<SocketAddr>,
        /// Optional address translation
        translations: Option<Vec<(SocketAddr, SocketAddr)>>,
    },
    /// Use DyanmoDB or Scylla over the Alternator interface
    Dynamo {
        /// AWS credentials: key ID & secret key; you'll pretty-much always need to specify these
        /// when running against DDB, but one could be talking to a local SycllaDB over the
        /// Alternator interface locally and have the cluster be open
        credentials: Option<Credentials>,
        /// You can find DynamoDB in a few ways. If you're truly talking to DynamoDB in AWS, you can
        /// give a region. You can also specify an URL (like
        /// `https://dynamodb.us-west-2.amazonaws.com`). If you're talking to ScyllaDB over the
        /// Alternator interface, we're going to have to handle load-balancing on the client-side,
        /// so specify more than one.
        location: DynamoLocation,
    },
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig::Scylla {
            credentials: None,
            hosts: vec!["localhost:9042".parse::<SocketAddr>().unwrap(/* known good */)],
            translations: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct SigningKeysConfig {
    #[serde(rename = "token-lifetime")]
    pub token_lifetime: Duration,
    #[serde(rename = "refresh-token-lifetime")]
    pub refresh_token_lifetime: Duration,
    #[serde(rename = "signing-keys")]
    pub signing_keys: Option<SigningKeys>,
}

impl Default for SigningKeysConfig {
    fn default() -> Self {
        SigningKeysConfig {
            token_lifetime: Duration::minutes(5),
            refresh_token_lifetime: Duration::hours(36),
            signing_keys: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct OtelExportConfig {
    /// Endpoint that will receive metric data in OTLP format
    pub endpoint: Url,
    /// Interval at which metrics will be pushed to `endpoint`; defaults to 60 seconds
    pub interval: Option<std::time::Duration>,
}

/// Rate-limit settings for indielinksd _clients_.
///
/// `per_hour` is the permissible number of requests per hour per network location (host +
/// port). Non-default per-netloc quotas can be defined via `custom`.
#[derive(Clone, Debug, Deserialize)]
pub struct ClientRateLimits {
    #[serde(rename = "per-hour")]
    pub per_hour: NonZeroU32,
    pub custom: Vec<(NetLoc, NonZeroU32)>,
}

impl Default for ClientRateLimits {
    fn default() -> Self {
        Self {
            per_hour: nonzero!(2880u32), // 8/sec
            custom: Default::default(),
        }
    }
}

/// [indielinksd](crate) client configuration
#[derive(Clone, Debug, Deserialize)]
pub struct ClientConfiguration {
    #[serde(rename = "rate-limits")]
    pub rate_limits: ClientRateLimits,
    pub timeout: Duration,
}

impl Default for ClientConfiguration {
    fn default() -> Self {
        Self {
            rate_limits: Default::default(),
            timeout: Duration::seconds(5),
        }
    }
}

/// Headers to elide when logging
// Newtype to perform validation at the site of ingestion; write it down as a simple list
#[derive(Clone, Debug, Deserialize)]
#[serde(try_from = "Vec<String>")]
pub struct HeaderBlacklist(HashSet<HeaderName>);

impl HeaderBlacklist {
    pub fn iter(&self) -> std::collections::hash_set::Iter<'_, HeaderName> {
        self.0.iter()
    }
}

impl TryFrom<Vec<String>> for HeaderBlacklist {
    type Error = Error;

    fn try_from(value: Vec<String>) -> Result<Self> {
        Ok(HeaderBlacklist(
            value
                .into_iter()
                .map(|s| HeaderName::from_bytes(s.as_bytes()))
                .collect::<StdResult<Vec<HeaderName>, _>>()
                .context(HeaderBlacklistSnafu)?
                .into_iter()
                .collect(),
        ))
    }
}

impl From<HeaderBlacklist> for HashSet<HeaderName> {
    fn from(value: HeaderBlacklist) -> Self {
        value.0
    }
}

// I suppose I could pull-in the `cookie` crate... but c'mon: it's a few cookies.

#[derive(Clone, Debug, Deserialize)]
pub struct UsersConfiguration {
    #[serde(rename = "same-site")]
    pub same_site: SameSite,
    #[serde(rename = "secure-cookies")]
    pub secure_cookies: bool,
    #[serde(rename = "allowed-origins")]
    pub allowed_origins: Vec<Origin>,
}

/// Return a configuration suitable for non-same-origin, http
impl Default for UsersConfiguration {
    fn default() -> Self {
        UsersConfiguration {
            same_site: SameSite::None,
            secure_cookies: false,
            allowed_origins: vec![
                Origin::try_from("http://localhost:18080".to_owned()).unwrap(/* known good */),
                Origin::try_from("http://localhost:20676".to_owned()).unwrap(/* known good */),
                Origin::try_from("http://127.0.0.1:18080".to_owned()).unwrap(/* known good */),
                Origin::try_from("http://127.0.0.1:20676".to_owned()).unwrap(/* known good */),
                Origin::try_from("http://localhost:18443".to_owned()).unwrap(/* known good */),
                Origin::try_from("http://127.0.0.1:18443".to_owned()).unwrap(/* known good */),
            ],
        }
    }
}

/// Indielinks configuration, version one
#[derive(Clone, Debug, Deserialize)]
pub struct ConfigV1 {
    /// The [indielinks](crate) log file
    #[serde(rename = "log-file")]
    pub log_file: PathBuf,
    /// OTLP export target; None means don't export
    #[serde(rename = "otlp-export")]
    pub otlp_export: Option<OtelExportConfig>,
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
    pub raft_grpc_address: SocketAddr,
    #[serde(rename = "storage-config")]
    pub storage_config: StorageConfig,
    /// The address at which this [indielinks](crate) instance may be reached from the public internet
    #[serde(rename = "public-origin")]
    pub public_origin: Origin,
    pub pepper: Option<Peppers>,
    #[serde(rename = "signing-keys")]
    pub signing_keys: SigningKeysConfig,
    #[serde(rename = "users-config")]
    pub users_config: UsersConfiguration,
    #[serde(rename = "user-agent")]
    pub user_agent: String,
    #[serde(rename = "client-exponential-backoff")]
    pub client_exponential_backoff: ExponentialBackoffParameters,
    #[serde(rename = "client-configuration")]
    pub client_configuration: ClientConfiguration,
    #[serde(rename = "local-client-configuration")]
    pub local_client_configuration: ClientConfiguration,
    #[serde(rename = "general-purpose-client-configuration")]
    pub general_purpose_client_configuration: ClientConfiguration,
    #[serde(rename = "collection-page-size")]
    pub collection_page_size: usize,
    pub assets: Option<PathBuf>,
    #[serde(rename = "background-tasks")]
    background_tasks: background_tasks::Config,
    #[serde(rename = "raft-config")]
    pub raft_config: RaftConfiguration,
    #[serde(rename = "header-blacklist")]
    pub header_blacklist: Option<HeaderBlacklist>,
    #[serde(rename = "pinboard-token")]
    pub pinboard_token: Option<SecretString>,
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
            otlp_export: None,
            public_address: "0.0.0.0:20679".parse::<SocketAddr>().unwrap(/* known good */),
            private_address: "127.0.0.1:20680".parse::<SocketAddr>().unwrap(/* known good */),
            raft_grpc_address: "0.0.0.0:20681".parse::<SocketAddr>().unwrap(/* known good */),
            storage_config: StorageConfig::default(),
            public_origin: "http://localhost:20679".parse::<Origin>().unwrap(/* known good */),
            pepper: None,
            signing_keys: SigningKeysConfig::default(),
            users_config: UsersConfiguration::default(),
            user_agent: format!("indielinks/{}; +sp1ff@pobox.com", crate_version!()),
            client_exponential_backoff: Default::default(),
            client_configuration: Default::default(),
            local_client_configuration: Default::default(),
            general_purpose_client_configuration: Default::default(),
            collection_page_size: 12, // Copied from Mastodon
            assets: None,
            background_tasks: background_tasks::Config::default(),
            raft_config: RaftConfiguration::default(),
            header_blacklist: None,
            pinboard_token: None,
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "version")] // tag "internally"
pub enum Configuration {
    #[serde(rename = "1")]
    V1(ConfigV1),
}
