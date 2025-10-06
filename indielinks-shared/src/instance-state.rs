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

#[cfg(feature = "backend")]
use scylla::{deserialize::value::DeserializeValue, serialize::value::SerializeValue};
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, ResultExt, Snafu};
#[cfg(feature = "backend")]
use tap::Pipe;

use crate::entities::{generate_rsa_keypair, UserPrivateKey, UserPublicKey};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("When generating an RSA keypair, {source}"))]
    Keypair { source: crate::entities::Error },
    #[cfg(feature = "backend")]
    #[snafu(display("Can't deserialize a {typ} from a null frame slice"))]
    NoFrameSlice { typ: String, backtrace: Backtrace },
}

type Result<T> = std::result::Result<T, Error>;

/// Immutable, per instance state. Written at install time
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct InstanceStateV0 {
    /// Public key for the instance actor
    #[serde(rename = "public-key")]
    pub public_key: UserPublicKey,
    /// Private key for the instance actor
    #[serde(rename = "private-key")]
    pub private_key: UserPrivateKey,
}

impl InstanceStateV0 {
    pub fn new() -> Result<InstanceStateV0> {
        let (public_key, private_key) = generate_rsa_keypair().context(KeypairSnafu)?;
        Ok(InstanceStateV0 {
            public_key,
            private_key,
        })
    }
}

#[cfg(feature = "backend")]
impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for InstanceStateV0 {
    fn type_check(
        typ: &scylla::cluster::metadata::ColumnType,
    ) -> std::result::Result<(), scylla::errors::TypeCheckError> {
        use crate::native_type_check;

        native_type_check!(typ, Blob, scylla::errors::TypeCheckError, "InstanceStateV0")
    }

    fn deserialize(
        _: &'metadata scylla::cluster::metadata::ColumnType<'metadata>,
        v: Option<scylla::deserialize::FrameSlice<'frame>>,
    ) -> std::result::Result<Self, scylla::errors::DeserializationError> {
        use crate::entities::mk_de_err;

        v.ok_or(
            NoFrameSliceSnafu {
                typ: "InstanceStateV0".to_owned(),
            }
            .build(),
        )
        .map_err(mk_de_err)?
        .as_slice()
        .pipe(rmp_serde::from_slice::<InstanceStateV0>)
        .map_err(mk_de_err)
    }
}

#[cfg(feature = "backend")]
impl SerializeValue for InstanceStateV0 {
    fn serialize<'b>(
        &self,
        typ: &scylla::cluster::metadata::ColumnType,
        writer: scylla::serialize::writers::CellWriter<'b>,
    ) -> std::result::Result<
        scylla::serialize::writers::WrittenCellProof<'b>,
        scylla::errors::SerializationError,
    > {
        use scylla::errors::SerializationError;

        use crate::{entities::mk_ser_err, native_type_check};

        native_type_check!(typ, Blob, SerializationError, "InstanceStateV0")?;
        let buf = rmp_serde::to_vec(&self).map_err(mk_ser_err)?;
        writer.set_value(buf.as_slice()).map_err(mk_ser_err)
    }
}
