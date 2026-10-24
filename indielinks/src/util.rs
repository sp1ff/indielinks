// Copyright (C) 2025-2026 Michael Herstine <sp1ff@pobox.com>
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

//! # util
//!
//! Much as I loathe catch-all "utility" modules, I truly don't know where these belong. Hopefully,
//! as I build-out the project, this will become more clear.

use std::ops::Deref;

use secrecy::{ExposeSecret, SecretSlice};
use serde::{Deserialize, Deserializer, Serialize};
use serde_bytes::ByteBuf;
use tap::{Conv, Pipe};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           UpToThree                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// An enum expressing zero, one, two or three of a thing.
#[derive(Clone, Debug)]
pub enum UpToThree<T> {
    None,
    One(T),
    Two(T, T),
    Three(T, T, T),
}

#[derive(Debug)]
pub struct NoMoreThanThree {}

impl std::fmt::Display for NoMoreThanThree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NoMoreThanThree")
    }
}

impl std::error::Error for NoMoreThanThree {}

impl<T: Clone> UpToThree<T> {
    pub fn new<U: IntoIterator<Item = T>>(
        iter: U,
    ) -> std::result::Result<UpToThree<T>, NoMoreThanThree> {
        let v = iter.into_iter().collect::<Vec<T>>();
        match v.len() {
            0 => Ok(UpToThree::None),
            1 => Ok(UpToThree::One(v[0].clone())),
            2 => Ok(UpToThree::Two(v[0].clone(), v[1].clone())),
            3 => Ok(UpToThree::Three(v[0].clone(), v[1].clone(), v[2].clone())),
            _ => Err(NoMoreThanThree {}),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                              Key                                               //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// A general-purpose encryption key
///
/// [Key] is a deserializable, secret, slice of byte.
#[derive(Clone, Debug)]
pub struct Key(SecretSlice<u8>);

impl Key {
    pub fn is_empty(&self) -> bool {
        self.0.expose_secret().is_empty()
    }
    pub fn len(&self) -> usize {
        self.0.expose_secret().len()
    }
}

// And let's implement a few convenience traits for `Key`, mostly designed to make it possible to
// use a `Key` wherever one might want to use a `SecretSlice<u8>`.

impl AsRef<SecretSlice<u8>> for Key {
    fn as_ref(&self) -> &SecretSlice<u8> {
        self.deref()
    }
}

// I'm OK implementing `Deref` here, since `Key` really just exists so I can implement `Deserialize`
// on it.
impl Deref for Key {
    type Target = SecretSlice<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// I *think* I can't just derive `Deserialize` because [u8] doesn't implement `DeserializeOwned`
impl<'de> Deserialize<'de> for Key {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        <ByteBuf as serde::Deserialize>::deserialize(deserializer)
            .map_err(|err| <D::Error as serde::de::Error>::custom(format!("{err:?}")))?
            .pipe(|x| x.into_vec())
            .conv::<SecretSlice<u8>>()
            .pipe(Key)
            .pipe(Ok)
    }
}

// It may seem strange to implement `Serialize` for something secret, but it is handy-- for the
// `indielinks-schemas secrets` sub-command, for instace. Caller beware.
impl Serialize for Key {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(self.0.expose_secret())
    }
}

impl From<Vec<u8>> for Key {
    fn from(value: Vec<u8>) -> Self {
        Key(value.into())
    }
}
