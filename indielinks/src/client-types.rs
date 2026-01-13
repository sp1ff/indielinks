use governor::{clock::DefaultClock, state::keyed::DefaultKeyedStateStore};
use tower::retry::Retry;
use tower_gcra::extractors::KeyedDashmapMiddleware;
use tower_http::set_header::SetRequestHeader;

use indielinks_shared::service::{ExponentialBackoffPolicy, ReqwestService};

use crate::{
    authn::AddSha256DigestIfNotPresent,
    client::{HostExtractor, HostKey, InstrumentedService},
};

// In indielinks-client, I make the client type generic, with a type constraint like:
//
//     impl Service<
//         http::Request<Bytes>,
//         Response = http::Response<Bytes>,
//         Error = Box<dyn std::error::Error + Send + Sync>,
//     > + Clone,
//
// In this crate, however, I find myself naming the type more frequently, so I'm going with a type
// alias. This is also inconvenient, but better than making every type and function that deals with
// our client type generic. Since the types are extremely large, I've moved them off into their own
// module to avoid cluttering the code.
//
// If you need to update this, say due to adding or removing a layer in `make_client()`, just do a
// `cargo build`; the `make_client()` return type won't type-check, but the compiler will write the
// expected type to a text file in the build directory (it will be the second one)-- just copy it
// from there over this one:

pub type GenericClientType<KE, S, C, MW> = SetRequestHeader<
    tower::util::Either<
        SetRequestHeader<
            tower::util::Either<
                AddSha256DigestIfNotPresent<
                    tower::util::Either<
                        SetRequestHeader<
                            SetRequestHeader<
                                Retry<
                                    ExponentialBackoffPolicy,
                                    tower_gcra::keyed::Governor<
                                        InstrumentedService<
                                            ReqwestService<
                                                reqwest::Client,
                                                indielinks_shared::service::Body,
                                            >,
                                        >,
                                        KE,
                                        http::Request<bytes::Bytes>,
                                        S,
                                        C,
                                        MW,
                                    >,
                                >,
                                http::HeaderValue,
                            >,
                            for<'a> fn(
                                &'a http::Request<bytes::Bytes>,
                            )
                                -> std::option::Option<http::HeaderValue>,
                        >,
                        SetRequestHeader<
                            Retry<
                                ExponentialBackoffPolicy,
                                tower_gcra::keyed::Governor<
                                    InstrumentedService<
                                        ReqwestService<
                                            reqwest::Client,
                                            indielinks_shared::service::Body,
                                        >,
                                    >,
                                    KE,
                                    http::Request<bytes::Bytes>,
                                    S,
                                    C,
                                    MW,
                                >,
                            >,
                            http::HeaderValue,
                        >,
                    >,
                >,
                tower::util::Either<
                    SetRequestHeader<
                        SetRequestHeader<
                            Retry<
                                ExponentialBackoffPolicy,
                                tower_gcra::keyed::Governor<
                                    InstrumentedService<
                                        ReqwestService<
                                            reqwest::Client,
                                            indielinks_shared::service::Body,
                                        >,
                                    >,
                                    KE,
                                    http::Request<bytes::Bytes>,
                                    S,
                                    C,
                                    MW,
                                >,
                            >,
                            http::HeaderValue,
                        >,
                        for<'a> fn(
                            &'a http::Request<bytes::Bytes>,
                        )
                            -> std::option::Option<http::HeaderValue>,
                    >,
                    SetRequestHeader<
                        Retry<
                            ExponentialBackoffPolicy,
                            tower_gcra::keyed::Governor<
                                InstrumentedService<
                                    ReqwestService<
                                        reqwest::Client,
                                        indielinks_shared::service::Body,
                                    >,
                                >,
                                KE,
                                http::Request<bytes::Bytes>,
                                S,
                                C,
                                MW,
                            >,
                        >,
                        http::HeaderValue,
                    >,
                >,
            >,
            for<'a> fn(&'a http::Request<bytes::Bytes>) -> std::option::Option<http::HeaderValue>,
        >,
        tower::util::Either<
            AddSha256DigestIfNotPresent<
                tower::util::Either<
                    SetRequestHeader<
                        SetRequestHeader<
                            Retry<
                                ExponentialBackoffPolicy,
                                tower_gcra::keyed::Governor<
                                    InstrumentedService<
                                        ReqwestService<
                                            reqwest::Client,
                                            indielinks_shared::service::Body,
                                        >,
                                    >,
                                    KE,
                                    http::Request<bytes::Bytes>,
                                    S,
                                    C,
                                    MW,
                                >,
                            >,
                            http::HeaderValue,
                        >,
                        for<'a> fn(
                            &'a http::Request<bytes::Bytes>,
                        )
                            -> std::option::Option<http::HeaderValue>,
                    >,
                    SetRequestHeader<
                        Retry<
                            ExponentialBackoffPolicy,
                            tower_gcra::keyed::Governor<
                                InstrumentedService<
                                    ReqwestService<
                                        reqwest::Client,
                                        indielinks_shared::service::Body,
                                    >,
                                >,
                                KE,
                                http::Request<bytes::Bytes>,
                                S,
                                C,
                                MW,
                            >,
                        >,
                        http::HeaderValue,
                    >,
                >,
            >,
            tower::util::Either<
                SetRequestHeader<
                    SetRequestHeader<
                        Retry<
                            ExponentialBackoffPolicy,
                            tower_gcra::keyed::Governor<
                                InstrumentedService<
                                    ReqwestService<
                                        reqwest::Client,
                                        indielinks_shared::service::Body,
                                    >,
                                >,
                                KE,
                                http::Request<bytes::Bytes>,
                                S,
                                C,
                                MW,
                            >,
                        >,
                        http::HeaderValue,
                    >,
                    for<'a> fn(
                        &'a http::Request<bytes::Bytes>,
                    ) -> std::option::Option<http::HeaderValue>,
                >,
                SetRequestHeader<
                    Retry<
                        ExponentialBackoffPolicy,
                        tower_gcra::keyed::Governor<
                            InstrumentedService<
                                ReqwestService<reqwest::Client, indielinks_shared::service::Body>,
                            >,
                            KE,
                            http::Request<bytes::Bytes>,
                            S,
                            C,
                            MW,
                        >,
                    >,
                    http::HeaderValue,
                >,
            >,
        >,
    >,
    for<'a> fn(&'a http::Request<bytes::Bytes>) -> std::option::Option<http::HeaderValue>,
>;

// KE :=> HostExtractor
// C  :=> DefaultClock
// S  :=> DefaultKeyedStateStore<HostKey>
// M  :=> KeyedDashmapMiddleware<HostKey>
pub type ClientType = SetRequestHeader<
    tower::util::Either<
        SetRequestHeader<
            tower::util::Either<
                AddSha256DigestIfNotPresent<
                    tower::util::Either<
                        SetRequestHeader<
                            SetRequestHeader<
                                Retry<
                                    ExponentialBackoffPolicy,
                                    tower_gcra::keyed::Governor<
                                        InstrumentedService<
                                            ReqwestService<
                                                reqwest::Client,
                                                indielinks_shared::service::Body,
                                            >,
                                        >,
                                        HostExtractor,
                                        http::Request<bytes::Bytes>,
                                        DefaultKeyedStateStore<HostKey>,
                                        DefaultClock,
                                        KeyedDashmapMiddleware<HostKey>,
                                    >,
                                >,
                                http::HeaderValue,
                            >,
                            for<'a> fn(
                                &'a http::Request<bytes::Bytes>,
                            )
                                -> std::option::Option<http::HeaderValue>,
                        >,
                        SetRequestHeader<
                            Retry<
                                ExponentialBackoffPolicy,
                                tower_gcra::keyed::Governor<
                                    InstrumentedService<
                                        ReqwestService<
                                            reqwest::Client,
                                            indielinks_shared::service::Body,
                                        >,
                                    >,
                                    HostExtractor,
                                    http::Request<bytes::Bytes>,
                                    DefaultKeyedStateStore<HostKey>,
                                    DefaultClock,
                                    KeyedDashmapMiddleware<HostKey>,
                                >,
                            >,
                            http::HeaderValue,
                        >,
                    >,
                >,
                tower::util::Either<
                    SetRequestHeader<
                        SetRequestHeader<
                            Retry<
                                ExponentialBackoffPolicy,
                                tower_gcra::keyed::Governor<
                                    InstrumentedService<
                                        ReqwestService<
                                            reqwest::Client,
                                            indielinks_shared::service::Body,
                                        >,
                                    >,
                                    HostExtractor,
                                    http::Request<bytes::Bytes>,
                                    DefaultKeyedStateStore<HostKey>,
                                    DefaultClock,
                                    KeyedDashmapMiddleware<HostKey>,
                                >,
                            >,
                            http::HeaderValue,
                        >,
                        for<'a> fn(
                            &'a http::Request<bytes::Bytes>,
                        )
                            -> std::option::Option<http::HeaderValue>,
                    >,
                    SetRequestHeader<
                        Retry<
                            ExponentialBackoffPolicy,
                            tower_gcra::keyed::Governor<
                                InstrumentedService<
                                    ReqwestService<
                                        reqwest::Client,
                                        indielinks_shared::service::Body,
                                    >,
                                >,
                                HostExtractor,
                                http::Request<bytes::Bytes>,
                                DefaultKeyedStateStore<HostKey>,
                                DefaultClock,
                                KeyedDashmapMiddleware<HostKey>,
                            >,
                        >,
                        http::HeaderValue,
                    >,
                >,
            >,
            for<'a> fn(&'a http::Request<bytes::Bytes>) -> std::option::Option<http::HeaderValue>,
        >,
        tower::util::Either<
            AddSha256DigestIfNotPresent<
                tower::util::Either<
                    SetRequestHeader<
                        SetRequestHeader<
                            Retry<
                                ExponentialBackoffPolicy,
                                tower_gcra::keyed::Governor<
                                    InstrumentedService<
                                        ReqwestService<
                                            reqwest::Client,
                                            indielinks_shared::service::Body,
                                        >,
                                    >,
                                    HostExtractor,
                                    http::Request<bytes::Bytes>,
                                    DefaultKeyedStateStore<HostKey>,
                                    DefaultClock,
                                    KeyedDashmapMiddleware<HostKey>,
                                >,
                            >,
                            http::HeaderValue,
                        >,
                        for<'a> fn(
                            &'a http::Request<bytes::Bytes>,
                        )
                            -> std::option::Option<http::HeaderValue>,
                    >,
                    SetRequestHeader<
                        Retry<
                            ExponentialBackoffPolicy,
                            tower_gcra::keyed::Governor<
                                InstrumentedService<
                                    ReqwestService<
                                        reqwest::Client,
                                        indielinks_shared::service::Body,
                                    >,
                                >,
                                HostExtractor,
                                http::Request<bytes::Bytes>,
                                DefaultKeyedStateStore<HostKey>,
                                DefaultClock,
                                KeyedDashmapMiddleware<HostKey>,
                            >,
                        >,
                        http::HeaderValue,
                    >,
                >,
            >,
            tower::util::Either<
                SetRequestHeader<
                    SetRequestHeader<
                        Retry<
                            ExponentialBackoffPolicy,
                            tower_gcra::keyed::Governor<
                                InstrumentedService<
                                    ReqwestService<
                                        reqwest::Client,
                                        indielinks_shared::service::Body,
                                    >,
                                >,
                                HostExtractor,
                                http::Request<bytes::Bytes>,
                                DefaultKeyedStateStore<HostKey>,
                                DefaultClock,
                                KeyedDashmapMiddleware<HostKey>,
                            >,
                        >,
                        http::HeaderValue,
                    >,
                    for<'a> fn(
                        &'a http::Request<bytes::Bytes>,
                    ) -> std::option::Option<http::HeaderValue>,
                >,
                SetRequestHeader<
                    Retry<
                        ExponentialBackoffPolicy,
                        tower_gcra::keyed::Governor<
                            InstrumentedService<
                                ReqwestService<reqwest::Client, indielinks_shared::service::Body>,
                            >,
                            HostExtractor,
                            http::Request<bytes::Bytes>,
                            DefaultKeyedStateStore<HostKey>,
                            DefaultClock,
                            KeyedDashmapMiddleware<HostKey>,
                        >,
                    >,
                    http::HeaderValue,
                >,
            >,
        >,
    >,
    for<'a> fn(&'a http::Request<bytes::Bytes>) -> std::option::Option<http::HeaderValue>,
>;
