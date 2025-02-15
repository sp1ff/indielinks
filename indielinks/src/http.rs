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

use crate::{
    metrics, peppers::Peppers, signing_keys::SigningKeys, storage::Backend as StorageBackend,
};

use axum::Json;
use chrono::Duration;
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        Error Responses                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// A serializable struct for use in HTTP error responses
///
/// This is intended to be used in the [IntoResponse] implementations for whatever error type
/// an axum handler is using.
///
/// [IntoResponse]: https://docs.rs/axum/latest/axum/response/trait.IntoResponse.html
///
/// This may be a violation of the YNGNI! principle, but I'd like to return a JSON body for errors.
/// I can't see a way to enforce the rule that all axum handlers do this, but I can at least
/// setup a standard representation of an error response.
#[derive(Debug, Deserialize, Serialize)]
pub struct ErrorResponseBody {
    pub error: String,
}

impl axum::response::IntoResponse for ErrorResponseBody {
    fn into_response(self) -> axum::response::Response {
        Json(self).into_response()
    }
}

#[allow(type_alias_bounds)]
pub type Result<T: axum::response::IntoResponse> = std::result::Result<T, ErrorResponseBody>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        JWT Signing Keys                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

// Not sure these will stay here; I'm still finding my way. I want to be able to rotate keys "live";
// i.e. add a new signing key to the app config, SIGHUP the program, and from then on all *new* JWTs
// will be signed with the new key, but JWTs signed by the old one will still be honored. Then, when
// all the old JWTs have expired, we can remove the old key from the configuration.

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       Application State                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Application state available to all handlers
// Not sure this is going to stay here.
pub struct Indielinks {
    pub domain: String,
    pub storage: Box<dyn StorageBackend + Send + Sync>,
    pub registry: prometheus::Registry,
    pub instruments: metrics::Instruments,
    pub pepper: Peppers,
    pub token_lifetime: Duration,
    pub signing_keys: SigningKeys,
}
