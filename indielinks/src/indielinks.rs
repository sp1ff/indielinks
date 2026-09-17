// Copyright (C) 2024-2026 Michael Herstine <sp1ff@pobox.com>
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

//! # [indielinks] state & secrets management
//!
//! [indielinks]: crate
//!
//! ## Secrets
//!
//! [indielinks] provides a simple chained secrets resolver: environment variables take precedence
//! over the configuration file, which either holds the secrets inline or names the AWS SSM
//! Parameter Store parameters from which they are to be fetched.

use std::{
    env::{self, VarError},
    path::PathBuf,
    result::Result as StdResult,
    sync::Arc,
};

use aws_config::load_from_env;
use aws_sdk_ssm::{
    config::http::HttpResponse,
    error::SdkError,
    operation::{
        get_parameter::GetParameterError,
        get_parameters::{GetParametersError, GetParametersOutput},
    },
    types::Parameter,
    Client,
};
use chrono::Duration;
use either::Either;
use nonzero::nonzero;
use opentelemetry_prometheus_text_exporter::PrometheusExporter;
use secrecy::SecretString;
use serde::de::DeserializeOwned;
use snafu::{Backtrace, IntoError, OptionExt, ResultExt, Snafu};
use tap::Pipe;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use indielinks_shared::{
    entities::{SsmParameter, Tagname},
    instance_state::InstanceStateV0,
    origin::Origin,
};

use indielinks_cache::raft::SharedCacheNode;

use crate::{
    ap_resolution::ApResolver,
    background_tasks::BackgroundTasks,
    cache::{GrpcClientFactory, SLOT_TOP_K_TAGS},
    configuration::ConfigV1,
    home_timeline::HomeTimelines,
    http::SameSite,
    outboxes::UserOutboxes,
    peppers::Peppers,
    popular_items::CachedTopK,
    recent_posts_lists::RecentPostsList,
    signing_keys::SigningKeys,
    storage::Backend as StorageBackend,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum Error {
    #[snafu(display("failed to read the environment variable {env_var}"))]
    Env {
        env_var: String,
        source: std::env::VarError,
        backtrace: Backtrace,
    },
    #[snafu(display("failed to deserialize the environment variable {env_var} to a secret"))]
    EnvSerde {
        env_var: String,
        source: serde_json::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("parameter {parameter_name} exists, but its value is empty"))]
    EmptyParameter {
        parameter_name: String,
        backtrace: Backtrace,
    },
    #[snafu(display("the returned parameter representing peppers was empty"))]
    EmptyPeppers { backtrace: Backtrace },
    #[snafu(display("the returned parameter representing signing keys was empty"))]
    EmptySigningKeys { backtrace: Backtrace },
    #[snafu(display("failed to fetch parameter {parameter_name}"))]
    FetchParameter {
        parameter_name: String,
        #[snafu(source(from(SdkError<GetParameterError, HttpResponse>, Box::new)))]
        source: Box<SdkError<GetParameterError, HttpResponse>>,
        backtrace: Backtrace,
    },
    #[snafu(display("failed to fetch parameters {parameter_names:?}"))]
    FetchParameters {
        parameter_names: Vec<String>,
        #[snafu(source(from(SdkError<GetParametersError, HttpResponse>, Box::new)))]
        source: Box<SdkError<GetParametersError, HttpResponse>>,
        backtrace: Backtrace,
    },
    #[snafu(display("the requested parameters {parameter_names:?} are invalid"))]
    InvalidParameters {
        parameter_names: Vec<String>,
        backtrace: Backtrace,
    },
    #[snafu(display("the 'get parameters' call succeeded, but no parameters came back"))]
    MissingParameters { backtrace: Backtrace },
    #[snafu(display(
        "the request for {parameter_name} succeeded, but there is no such parameter"
    ))]
    NoSuchParameter {
        parameter_name: String,
        backtrace: Backtrace,
    },
    #[snafu(display(
        "{parameter_name} was successfully retrieved, but unable to be deserialized"
    ))]
    ParameterSerde {
        parameter_name: String,
        source: serde_json::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("failed to deserialize the peppers SSM Parameter"))]
    PeppersSerde {
        source: serde_json::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("failed to deserialize the signingkeys SSM Parameter"))]
    SigningKeysSerde {
        source: serde_json::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("expected two parameters, but received {count}"))]
    WrongNumberOfParameters { count: usize, backtrace: Backtrace },
}

pub type Result<T> = StdResult<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       application State                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Application state available to all handlers
///
/// We make this available to [axum] handlers through the [State] extractor. The general assumption
/// is that [Indielinks] is immutable. Elements that _are_ mutable, such as caches, will need to
/// provide interior mutability (through a [Mutex] or some such). Note that since every handler
/// shares a common instance of this struct, providing for interior mutability can easily introduce
/// a performance bottleneck.
///
/// [State]: axum::extract::State
pub struct Indielinks {
    pub origin: Origin,
    pub instance_id: Uuid,
    pub instance_state: InstanceStateV0,
    pub storage: Arc<dyn StorageBackend + Send + Sync>,
    pub exporter: PrometheusExporter,
    pub pepper: Peppers,
    pub token_lifetime: Duration,
    pub refresh_token_lifetime: Duration,
    pub signing_keys: SigningKeys,
    pub pinboard_token: Option<SecretString>,
    pub users_same_site: SameSite,
    pub users_secure_cookies: bool,
    pub allowed_origins: Vec<Origin>,
    pub ap_client: crate::client_types::ClientType,
    pub local_client: crate::client_types::ClientType,
    pub general_purpose_client: crate::client_types::ClientType,
    pub collection_page_size: usize,
    pub assets: PathBuf,
    pub task_sender: Arc<BackgroundTasks>,
    pub cache_node: SharedCacheNode<crate::cache::GrpcClientFactory>,
    // Shared, mutable access to the resolver needed, therefore we need an
    // `Arc<thing that can give a mutable borrow>`; `ApResolver`, being a cache, pretty-much always
    // requires a mutable borrow, so I used a `Mutex` instead of an `RwLock`.
    //
    // This is pretty sub-optimal, since it means we're going to be locking access to the resolver
    // for the duration of internet calls, making this a real bottleneck.
    pub ap_resolver: Arc<Mutex<ApResolver>>,
    // Similarly here.
    pub home_timelines: Arc<Mutex<HomeTimelines>>,
    // and here
    pub user_outboxes: Arc<Mutex<UserOutboxes>>,
    // No shared ownership needed for the Recent Posts List, but we do need to guard concurrent access
    pub recent_posts_list: RwLock<RecentPostsList<GrpcClientFactory, GrpcClientFactory>>,
    // ditto here
    pub top_k_tags: RwLock<CachedTopK<Tagname, GrpcClientFactory, GrpcClientFactory>>,
}

impl Indielinks {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        instance_id: Uuid,
        instance_state: InstanceStateV0,
        storage: Arc<dyn StorageBackend + Send + Sync>,
        exporter: PrometheusExporter,
        ap_client: crate::client_types::ClientType,
        local_client: crate::client_types::ClientType,
        general_purpose_client: crate::client_types::ClientType,
        task_sender: Arc<BackgroundTasks>,
        cache_node: SharedCacheNode<GrpcClientFactory>,
        ap_resolver: Arc<Mutex<ApResolver>>,
        home_timelines: Arc<Mutex<HomeTimelines>>,
        user_outboxes: Arc<Mutex<UserOutboxes>>,
        cfg: &ConfigV1,
    ) -> Result<Self> {
        let (pepper, signing_keys) = resolve_secrets(
            "INDIELINKS_PEPPERS",
            &cfg.pepper,
            "INDIELINKS_SIGNING_KEYS",
            &cfg.signing_keys.signing_keys,
        )
        .await?;

        Ok(Self {
            origin: cfg.public_origin.clone(),
            instance_id,
            instance_state,
            storage,
            exporter,
            pepper,
            token_lifetime: cfg.signing_keys.token_lifetime,
            refresh_token_lifetime: cfg.signing_keys.refresh_token_lifetime,
            signing_keys,
            pinboard_token: cfg.pinboard_token.clone(),
            users_same_site: cfg.users_config.same_site.clone(),
            users_secure_cookies: cfg.users_config.secure_cookies,
            allowed_origins: cfg.users_config.allowed_origins.clone(),
            ap_client,
            local_client,
            general_purpose_client,
            collection_page_size: cfg.collection_page_size,
            assets: cfg.assets.clone().unwrap_or(PathBuf::from("assets")),
            task_sender,
            cache_node: cache_node.clone(),
            ap_resolver,
            home_timelines,
            user_outboxes,
            recent_posts_list: RwLock::new(RecentPostsList::new(
                cache_node.clone(),
                nonzero!(256usize),
                GrpcClientFactory,
            )),
            top_k_tags: RwLock::new(CachedTopK::new(
                *SLOT_TOP_K_TAGS,
                GrpcClientFactory,
                cache_node.clone(),
                nonzero!(64usize),
            )),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       Secrets Management                                       //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Where a secret is to be found: either already in hand, or to be fetched from AWS SSM
/// Parameter Store under a parameter name
enum Source<T> {
    Inline(T),
    Ssm(SsmParameter),
}

/// Read the environment variable `env_var`, mapping a missing variable to `None`
fn read_env(env_var: &str) -> Result<Option<String>> {
    match env::var(env_var) {
        Ok(json) => Ok(Some(json)),
        Err(VarError::NotPresent) => Ok(None),
        Err(err) => Err(EnvSnafu { env_var }.into_error(err)),
    }
}

/// Choose the source for a secret: the environment variable, if set, takes precedence over the
/// configuration value
fn select_source<T>(
    env_value: Option<&str>,
    env_var: &str,
    configuration_value: &Either<SsmParameter, T>,
) -> Result<Source<T>>
where
    T: Clone + DeserializeOwned,
{
    match env_value {
        Some(json) => serde_json::from_str(json)
            .map(Source::Inline)
            .context(EnvSerdeSnafu { env_var }),
        None => Ok(match configuration_value {
            Either::Left(parameter) => Source::Ssm(parameter.clone()),
            Either::Right(value) => Source::Inline(value.clone()),
        }),
    }
}

async fn fetch_parameter<T>(client: &Client, parameter_name: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    client
        .get_parameter()
        .name(parameter_name)
        .with_decryption(true)
        .send()
        .await
        .context(FetchParameterSnafu { parameter_name })?
        .parameter
        .context(NoSuchParameterSnafu { parameter_name })?
        .value
        .context(EmptyParameterSnafu { parameter_name })?
        .pipe(|json| serde_json::from_str::<T>(&json))
        .context(ParameterSerdeSnafu { parameter_name })?
        .pipe(Ok)
}

// Take a `GetParametersOutput` & transform it to a pair of `Parameter`s (peppers and signing keys,
// respectively) if possible; otherwise fail.
fn transform_output(output: GetParametersOutput) -> Result<(Parameter, Parameter)> {
    output
        .invalid_parameters
        .map(|parameter_names| {
            if parameter_names.is_empty() {
                Ok(())
            } else {
                Err(InvalidParametersSnafu { parameter_names }.build())
            }
        })
        .transpose()?;

    output
        .parameters
        .map(|parameter_names| match parameter_names.as_slice() {
            [peppers, signing_keys] => Ok((peppers.clone(), signing_keys.clone())),
            _ => Err(WrongNumberOfParametersSnafu {
                count: parameter_names.len(),
            }
            .build()),
        })
        .transpose()?
        .context(MissingParametersSnafu)
}

// Carry out the final deserialization of the peppers & signing keys.
fn deserialize_output(
    peppers: Parameter,
    signing_keys: Parameter,
) -> Result<(Peppers, SigningKeys)> {
    Ok((
        serde_json::from_str::<Peppers>(&peppers.value.context(EmptyPeppersSnafu)?)
            .context(PeppersSerdeSnafu)?,
        serde_json::from_str::<SigningKeys>(&signing_keys.value.context(EmptySigningKeysSnafu)?)
            .context(SigningKeysSerdeSnafu)?,
    ))
}

/// Resolve the peppers & signing keys; prefer the environment variables, followed by the
/// configuration values. Configuration either supplies the secrets inline or names the AWS SSM
/// Parameter Store parameters from which they are to be fetched.
pub async fn resolve_secrets(
    peppers_env_var: &str,
    peppers_config_value: &Either<SsmParameter, Peppers>,
    signing_keys_env_var: &str,
    signing_keys_config_value: &Either<SsmParameter, SigningKeys>,
) -> Result<(Peppers, SigningKeys)> {
    match (
        select_source(
            read_env(peppers_env_var)?.as_deref(),
            peppers_env_var,
            peppers_config_value,
        )?,
        select_source(
            read_env(signing_keys_env_var)?.as_deref(),
            signing_keys_env_var,
            signing_keys_config_value,
        )?,
    ) {
        (Source::Ssm(peppers_name), Source::Ssm(signing_keys_name)) => {
            // Retrieve both from SSM in a single batched call
            Client::new(&load_from_env().await)
                .get_parameters()
                .names(peppers_name.as_ref())
                .names(signing_keys_name.as_ref())
                .with_decryption(true)
                .send()
                .await
                .context(FetchParametersSnafu {
                    parameter_names: vec![&peppers_name, &signing_keys_name]
                        .into_iter()
                        .map(String::from)
                        .collect::<Vec<String>>(),
                })?
                .pipe(transform_output)?
                .pipe(|(peppers, signing_keys)| deserialize_output(peppers, signing_keys))
        }
        (Source::Ssm(peppers_name), Source::Inline(signing_keys)) => {
            // Retrieve the peppers from SSM
            Ok((
                fetch_parameter(&Client::new(&load_from_env().await), peppers_name.as_ref())
                    .await?,
                signing_keys,
            ))
        }
        (Source::Inline(peppers), Source::Ssm(signing_keys_name)) => {
            // Retrieve the signing keys from SSM
            Ok((
                peppers,
                fetch_parameter(
                    &Client::new(&load_from_env().await),
                    signing_keys_name.as_ref(),
                )
                .await?,
            ))
        }
        (Source::Inline(peppers), Source::Inline(signing_keys)) => Ok((peppers, signing_keys)),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn select_source_prefers_the_environment() {
        let json = serde_json::to_string(&Peppers::default())
            .expect("the peppers should serialize to JSON");
        // The environment variable wins over an inline configuration value...
        assert!(matches!(
            select_source::<Peppers>(
                Some(&json),
                "INDIELINKS_PEPPERS",
                &Either::Right(Peppers::default())
            ),
            Ok(Source::Inline(_))
        ));
        // ...and over an SSM parameter name
        assert!(matches!(
            select_source::<Peppers>(
                Some(&json),
                "INDIELINKS_PEPPERS",
                &Either::Left(
                    SsmParameter::new("/indielinks/prod/peppers".to_owned())
                        .expect("the parameter name should be valid")
                )
            ),
            Ok(Source::Inline(_))
        ));
    }

    #[test]
    fn select_source_falls_back_to_configuration() {
        // An inline configuration value is used as-is
        assert!(matches!(
            select_source::<Peppers>(
                None,
                "INDIELINKS_PEPPERS",
                &Either::Right(Peppers::default())
            ),
            Ok(Source::Inline(_))
        ));
        // A parameter name selects an SSM fetch
        let name = SsmParameter::new("/indielinks/prod/peppers".to_owned())
            .expect("the parameter name should be valid");
        assert!(matches!(
            select_source::<Peppers>(
                None,
                "INDIELINKS_PEPPERS",
                &Either::Left(name.clone())
            ),
            Ok(Source::Ssm(parameter)) if parameter == name
        ));
    }

    #[test]
    fn select_source_rejects_malformed_json() {
        assert!(matches!(
            select_source::<Peppers>(
                Some("not json"),
                "INDIELINKS_PEPPERS",
                &Either::Right(Peppers::default())
            ),
            Err(Error::EnvSerde { .. })
        ));
    }
}
