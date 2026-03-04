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

use std::{fmt::Display, ops::Deref, result::Result as StdResult};

#[cfg(feature = "backend")]
use scylla::{
    cluster::metadata::ColumnType,
    deserialize::{FrameSlice, value::DeserializeValue},
    errors::{DeserializationError, SerializationError, TypeCheckError},
    serialize::{
        value::SerializeValue,
        writers::{CellWriter, WrittenCellProof},
    },
};
use serde::{Deserialize, Deserializer, Serialize};
use snafu::Snafu;

use crate::entities::mk_serde_de_err;

#[derive(Debug, Snafu)]
#[snafu(display("Empty string"))]
pub struct Empty;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct NonEmptyString(String);

// Implement `Deserialize` by hand to fail if the serialized value isn't a legit URL
impl<'de> Deserialize<'de> for NonEmptyString {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        NonEmptyString::try_from(s).map_err(mk_serde_de_err::<'de, D>)
    }
}

#[cfg(feature = "backend")]
impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for NonEmptyString {
    fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
        String::type_check(typ)
    }
    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> StdResult<Self, DeserializationError> {
        use crate::entities::mk_de_err;

        NonEmptyString::try_from(<String as DeserializeValue>::deserialize(typ, v)?)
            .map_err(mk_de_err)
    }
}

#[cfg(feature = "backend")]
impl SerializeValue for NonEmptyString {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
        SerializeValue::serialize(&self.0.as_str(), typ, writer)
    }
}

impl TryFrom<String> for NonEmptyString {
    type Error = Empty;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() {
            Err(Empty)
        } else {
            Ok(NonEmptyString(value))
        }
    }
}

impl TryFrom<&str> for NonEmptyString {
    type Error = Empty;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.is_empty() {
            Err(Empty)
        } else {
            Ok(NonEmptyString(value.to_owned()))
        }
    }
}

impl Display for NonEmptyString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Deref for NonEmptyString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl AsRef<str> for NonEmptyString {
    fn as_ref(&self) -> &str {
        self.deref()
    }
}

impl From<NonEmptyString> for String {
    fn from(value: NonEmptyString) -> Self {
        value.0
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn smoke() {
        let a: StdResult<NonEmptyString, Empty> = "foo".to_owned().try_into();
        assert!(a.is_ok());
        let b: StdResult<NonEmptyString, Empty> = "".to_owned().try_into();
        assert!(b.is_err());

        let a = a.unwrap(/* known good */);
        assert_eq!(format!("{a}"), "foo".to_owned());
        assert_eq!("foo", a.as_ref());

        let c: String = a.into();
        assert_eq!(c, "foo".to_owned());
    }
}
