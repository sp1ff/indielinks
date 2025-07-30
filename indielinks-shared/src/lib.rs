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

use chrono::{DateTime, NaiveDate, Utc};
use lazy_static::lazy_static;
use regex::Regex;
#[cfg(feature = "backend")]
use scylla::{
    DeserializeRow,
    cluster::metadata::NativeType,
    deserialize::{DeserializationError, FrameSlice, TypeCheckError, value::DeserializeValue},
    frame::response::result::ColumnType,
    serialize::{
        SerializationError,
        value::SerializeValue,
        writers::{CellWriter, WrittenCellProof},
    },
};
use serde::{Deserialize, Deserializer, Serialize};
use snafu::{Backtrace, prelude::*};
#[cfg(feature = "backend")]
use tap::Pipe;
use unicode_segmentation::UnicodeSegmentation;
use url::Url;
use uuid::Uuid;

use std::{collections::HashSet, fmt::Display, ops::Deref, str::FromStr};

type StdResult<T, E> = std::result::Result<T, E>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[cfg(feature = "backend")]
    #[snafu(display("{col_name} expected type {expected:?}; got {actual:?}"))]
    ColumnTypeMismatch {
        col_name: String,
        actual: ColumnType<'static>,
        expected: ColumnType<'static>,
        backtrace: Backtrace,
    },
    #[cfg(feature = "backend")]
    #[snafu(display("Can't deserialize a {typ} from a null frame slice"))]
    NoFrameSlice { typ: String, backtrace: Backtrace },
    #[snafu(display("{text} is not a valid `day` for a post"))]
    PostDay { text: String, backtrace: Backtrace },
    #[snafu(display("{text} is not a valid tag name"))]
    Tagname { text: String, backtrace: Backtrace },
    #[snafu(display("Failed to parse {text} as an URL: {source}"))]
    Url {
        text: String,
        source: url::ParseError,
        backtrace: Backtrace,
    },
}

type Result<T> = std::result::Result<T, Error>;

// We start with a series of newtype structs to both refine native types & to allow me to work
// around Rust's orphaned trait rules and implement traits not defined by this crate
// (`DeserializeValue`, `SerializeValue`) on types not defined in this crate (`PublicKey`,
// `PrivateKey` and so on). It's all boilerplate; nothing terribly complex, but it *is* tedious. I
// wonder if I'm missing some handy crate that provides macros for this...

// This is kind of lame, but I'm in the process of updating the code to version 1.0 of the Scylla
// crate, and I don't want to get sidetracked handling the new ColumnType altogether.
#[cfg(feature = "backend")]
#[macro_export]
macro_rules! native_type_check {
    ($var_name:ident, $native_type:ident, $err_type:ty, $column_name:expr) => {
        ($var_name == &ColumnType::Native(NativeType::$native_type))
            .then_some(())
            .ok_or(<$err_type>::new(
                ColumnTypeMismatchSnafu {
                    col_name: $column_name.to_owned(),
                    actual: $var_name.clone().into_owned(),
                    expected: ColumnType::Native(NativeType::$native_type),
                }
                .build(),
            ))
    };
}

#[cfg(feature = "backend")]
fn mk_de_err(err: impl std::error::Error + Send + Sync + 'static) -> DeserializationError {
    DeserializationError::new(err)
}

#[cfg(feature = "backend")]
fn mk_ser_err(err: impl std::error::Error + Send + Sync + 'static) -> SerializationError {
    SerializationError::new(err)
}

fn mk_serde_de_err<'de, D: serde::Deserializer<'de>>(err: impl std::error::Error) -> D::Error {
    <D::Error as serde::de::Error>::custom(format!("{:?}", err))
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                          Identifiers                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "backend")]
#[macro_export]
macro_rules! __define_id {
    ($type_name:ident) => {
        // Arggghhhh... the derive macro doesn't work with newtype structs.
        impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for $type_name {
            fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
                Uuid::type_check(typ)
            }
            fn deserialize(
                typ: &'metadata ColumnType<'metadata>,
                v: Option<FrameSlice<'frame>>,
            ) -> StdResult<Self, DeserializationError> {
                Ok(Self(<Uuid as DeserializeValue>::deserialize(typ, v)?))
            }
        }
        // Again, the derive macro doesn't work with newtype structs.
        impl SerializeValue for $type_name {
            fn serialize<'b>(
                &self,
                typ: &ColumnType<'_>,
                writer: CellWriter<'b>,
            ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
                SerializeValue::serialize(&self.0, typ, writer)
            }
        }
    };
}

#[cfg(not(feature = "backend"))]
#[macro_export]
macro_rules! __define_id {
    ($type_name:ident) => {};
}

/// # Introduction
///
/// Use this to declare a type intended to be used as an opaque identifier for some other sort of entity.
///
/// # Background
///
/// In a NoSQL world, we can't count on an auto-increment column in our tables to serve as an
/// opaque identifier. It is instead up to the application developer to assign their own ids.
/// Depending on the degree of integration with the database, I've seen implementations that assign
/// ranges of numeric identifiers to distinct nodes in the database (so as to avoid collisions), but
/// by far the most common approach is simply to move to a UUID (I guess this trades space for ease
/// of implementation).
///
/// I suppose I could have just used [Uuid] to represent this (wrapped in a newtype struct to
/// impement the ScyllaDB-related interfaces on it)... but I just couldn't bring myself to use the
/// same type to represent identifiers for users, tags and posts all at the same time.
///
/// This macro will define a newtype struct wrapping [Uuid] implementing the traits [Display],
/// [DeserializeValue] and [SerializeValue]. I thought to use a type alias, but those don't
/// work very well with newtype structs (in particular, you can't access the type's constructor
/// through the alias (not sure why)).
///
/// As an aside, [Display] will format the uuid in the form of an URN with namespace identifier
/// given by the corresponding macro argument.
#[macro_export]
macro_rules! define_id {
    ($type_name:ident, $nid:expr) => {
        #[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
        #[serde(transparent)]
        pub struct $type_name(Uuid);
        impl $type_name {
            pub fn new(s: &str) -> StdResult<$type_name, uuid::Error> {
                Ok($type_name(Uuid::parse_str(s)?))
            }
            // This seems like a landmine; I should probably just store strings in the same way I
            // display them?
            pub fn to_raw_string(&self) -> String {
                format!("{}", self.0.as_simple())
            }
        }
        impl Default for $type_name {
            fn default() -> Self {
                $type_name(Uuid::new_v4())
            }
        }
        impl Display for $type_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                // I'm not thrilled with this-- I'd prefer to represent it as an URN for display
                // purposes. Thing is, this is the format used by `serde-dynamo`, so I want to be
                // consistent with that.
                write!(f, "{}", self.0.as_hyphenated())
            }
        }
        impl FromStr for $type_name {
            type Err = uuid::Error;

            fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
                $type_name::new(s)
            }
        }
        impl AsRef<Uuid> for $type_name {
            fn as_ref(&self) -> &Uuid {
                self.deref()
            }
        }
        impl Deref for $type_name {
            type Target = Uuid;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
        impl From<$type_name> for Uuid {
            fn from(value: $type_name) -> Self {
                value.0
            }
        }
        impl From<&$type_name> for $type_name {
            fn from(value: &$type_name) -> Self {
                $type_name(value.0)
            }
        }
        $crate::__define_id!($type_name);
    };
}

define_id!(UserId, "userid");
define_id!(PostId, "postid");

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                            StorUrl                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Newtype to work around Rust's orphaned traits rule
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct StorUrl(Url);

// Implement `Deserialize` by hand to fail if the serialized value isn't a legit URL
impl<'de> Deserialize<'de> for StorUrl {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        StorUrl::try_from(s).map_err(mk_serde_de_err::<'de, D>)
    }
}

#[cfg(feature = "backend")]
impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for StorUrl {
    fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
        String::type_check(typ)
    }
    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> StdResult<Self, DeserializationError> {
        StorUrl::try_from(<String as DeserializeValue>::deserialize(typ, v)?).map_err(mk_de_err)
    }
}

#[cfg(feature = "backend")]
impl SerializeValue for StorUrl {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
        SerializeValue::serialize(&self.0.as_str(), typ, writer)
    }
}

impl Deref for StorUrl {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl AsRef<str> for StorUrl {
    fn as_ref(&self) -> &str {
        self.deref()
    }
}

impl AsRef<Url> for StorUrl {
    fn as_ref(&self) -> &Url {
        &self.0
    }
}

impl From<StorUrl> for Url {
    fn from(value: StorUrl) -> Self {
        value.0
    }
}

impl From<Url> for StorUrl {
    fn from(value: Url) -> Self {
        Self(value)
    }
}

impl From<&Url> for StorUrl {
    fn from(value: &Url) -> Self {
        Self(value.clone())
    }
}

impl TryFrom<String> for StorUrl {
    type Error = Error;

    fn try_from(s: String) -> std::result::Result<Self, Self::Error> {
        Ok(StorUrl(Url::parse(&s).context(UrlSnafu { text: s })?))
    }
}

impl Display for StorUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                            TagName                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

const MAX_TAGNAME_LENGTH: usize = 255;

fn check_tagname(s: &str) -> bool {
    [
        !s.is_empty(),
        UnicodeSegmentation::graphemes(s, true).count() <= MAX_TAGNAME_LENGTH,
        !s.contains(char::is_whitespace),
        !s.contains(','),
    ]
    .into_iter()
    // Ack! Does Rust really not offer the identity operator?
    .all(|x| x)
}

/// Following Pinboard conventions, tags may be up to 255 characters in length; the Pinboard [docs]
/// say "All entities are encoded as UTF-8. In the length limits below, 'characters' means logical
/// characters rather than bytes." I'm not sure what is meant by "logical characters", but I'm going
/// to use Unicode graphemes. Tags may not include whitespace. AFAICT, del.icio.us tags could
/// contain commas, but Pinboard disallows them, so I will, too.
///
/// [docs]: https://pinboard.in/api/
///
/// Pinboard also provides a feature of "private tags": if the tag begins with an ASCII period, it
/// is considered private. I model that here, although I haven't decided how to deal with it more
/// generally.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct Tagname(String);

impl Tagname {
    /// Correct-by-construction [Tagname] constructor
    pub fn new(text: &str) -> Result<Tagname> {
        check_tagname(text)
            .then_some(Tagname(text.to_string()))
            .ok_or(
                TagnameSnafu {
                    text: text.to_string(),
                }
                .build(),
            )
    }
    pub fn private(&self) -> bool {
        self.0.as_bytes()[0] == b'.' // Index is safe
    }
}

impl AsRef<str> for Tagname {
    fn as_ref(&self) -> &str {
        self.deref()
    }
}

impl Deref for Tagname {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Tagname> for String {
    fn from(value: Tagname) -> Self {
        value.0
    }
}

// Implement `Deserialize` by hand to fail if the serialized value isn't a legit `Tagname`
impl<'de> Deserialize<'de> for Tagname {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        Tagname::try_from(s).map_err(mk_serde_de_err::<'de, D>)
    }
}

#[cfg(feature = "backend")]
impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for Tagname {
    fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
        String::type_check(typ)
    }
    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> StdResult<Self, DeserializationError> {
        Tagname::try_from(<String as DeserializeValue>::deserialize(typ, v)?).map_err(mk_de_err)
    }
}

impl std::fmt::Display for Tagname {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for Tagname {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Tagname::new(s)
    }
}

#[cfg(feature = "backend")]
impl SerializeValue for Tagname {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
        SerializeValue::serialize(&self.0, typ, writer)
    }
}

impl TryFrom<String> for Tagname {
    type Error = Error;

    fn try_from(name: String) -> std::result::Result<Self, Self::Error> {
        if check_tagname(&name) {
            Ok(Tagname(name))
        } else {
            TagnameSnafu { text: name }.fail()
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn tagname() {
        assert!(Tagname::new("").is_err());
        assert!(Tagname::new("我翻开日记，第一行便写着：“今天晚饭以后，我就无缘无故地觉得有些怕人。”我不知道怕在哪里，可是既然怕人，便躲到屋里，不敢见谁。我越想越觉得奇怪，这里的人，同住了这么多年，他们的脸色本来并不怎样可怕，但近来却时时都换了模样似的，连那小孩子也诡秘地望着我。我想，我自己总没有得罪他们罢？因为从前是不怕的。可是一想到他们那样的眼色，心里就慌起来，似乎随时都要露出牙齿，向我扑来 我便心跳得更厉害，走到门口，看看天色还早，却又不敢出门，生怕有人看见我。于是，只好坐下来，怀疑四壁的缝隙里也藏着窥伺的眼睛。我拿起一本书想看看，却又不知翻到哪里好，觉得每一页上，都仿佛写着“吃人”两个字，阴森森地直扑到我眼前，让我毛骨悚然。").is_err());
        assert!(Tagname::new("foo bar").is_err());
        assert!(Tagname::new("foo,bar").is_err());
        assert!(Tagname::new("aws").is_ok());
        assert!(Tagname::new("我不知道怕在哪里").is_ok());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                            PostDay                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

// It might be worth giving the `refined` crate a look: <https://github.com/tomoikey/refined_type>.
// My concern is that in order to implement things like `SerializeValue` & `DeserializeValue`, I'll
// have to wrap the refined type in a newtype. That might be fine, but for now I'd prefer to do that
// in a separate patch.

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct PostDay(String);

lazy_static! {
    static ref POST_DAY: Regex = Regex::new("^[0-9]{4}-[0-9]{2}-[0-9]{2}$").unwrap(/* known good */);
}

fn check_postday(s: &str) -> bool {
    POST_DAY.is_match(s)
}

impl PostDay {
    pub fn new(s: &str) -> Result<PostDay> {
        if check_postday(s) {
            Ok(PostDay(s.to_string()))
        } else {
            Err(PostDaySnafu {
                text: s.to_string(),
            }
            .build())
        }
    }
}

impl AsRef<str> for PostDay {
    fn as_ref(&self) -> &str {
        self.deref()
    }
}

impl Deref for PostDay {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<PostDay> for String {
    fn from(value: PostDay) -> Self {
        value.0
    }
}

// Implement `Deserialize` by hand to fail if the serialized value isn't a legit `PostDay`
impl<'de> Deserialize<'de> for PostDay {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        PostDay::try_from(s).map_err(mk_serde_de_err::<'de, D>)
    }
}

#[cfg(feature = "backend")]
impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for PostDay {
    fn type_check(typ: &ColumnType<'_>) -> StdResult<(), TypeCheckError> {
        native_type_check!(typ, Ascii, TypeCheckError, "PostDay")
    }
    fn deserialize(
        _: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> StdResult<Self, DeserializationError> {
        v.ok_or(
            NoFrameSliceSnafu {
                typ: "PostDay".to_owned(),
            }
            .build(),
        )
        .map_err(mk_de_err)?
        .as_slice()
        .pipe(std::str::from_utf8)
        .map_err(mk_de_err)?
        .pipe(PostDay::new)
        .map_err(mk_de_err)?
        .pipe(Ok)
    }
}

impl std::fmt::Display for PostDay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::convert::From<DateTime<Utc>> for PostDay {
    fn from(value: DateTime<Utc>) -> Self {
        PostDay(value.format("%Y-%m-%d").to_string())
    }
}

impl std::convert::From<&DateTime<Utc>> for PostDay {
    fn from(value: &DateTime<Utc>) -> Self {
        PostDay(value.format("%Y-%m-%d").to_string())
    }
}

impl std::convert::From<NaiveDate> for PostDay {
    fn from(value: NaiveDate) -> Self {
        PostDay(value.format("%Y-%m-%d").to_string())
    }
}

impl std::convert::From<&NaiveDate> for PostDay {
    fn from(value: &NaiveDate) -> Self {
        PostDay(value.format("%Y-%m-%d").to_string())
    }
}

#[cfg(feature = "backend")]
impl SerializeValue for PostDay {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> StdResult<WrittenCellProof<'b>, SerializationError> {
        native_type_check!(typ, Ascii, SerializationError, "PostDay")?;
        self.0
            .to_string()
            .as_bytes()
            .pipe(|x| writer.set_value(x))
            .map_err(mk_ser_err)?
            .pipe(Ok)
    }
}

impl TryFrom<String> for PostDay {
    type Error = Error;

    fn try_from(text: String) -> std::result::Result<Self, Self::Error> {
        if check_postday(&text) {
            Ok(PostDay(text))
        } else {
            PostDaySnafu { text }.fail()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                              Post                                              //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents an indielinks post
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "backend", derive(DeserializeRow))]
pub struct Post {
    url: StorUrl,
    user_id: UserId,
    id: PostId,
    posted: DateTime<Utc>,
    day: PostDay,
    title: String,
    notes: Option<String>,
    tags: HashSet<Tagname>,
    public: bool,
    unread: bool,
}

impl Post {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        url: &StorUrl,
        id: &PostId,
        user_id: &UserId,
        posted: &DateTime<Utc>,
        day: &PostDay,
        title: &str,
        notes: &Option<String>,
        tags: &HashSet<Tagname>,
        public: bool,
        unread: bool,
    ) -> Post {
        Post {
            url: url.clone(),
            user_id: *user_id,
            id: *id,
            posted: *posted,
            day: day.clone(),
            title: title.to_string(),
            notes: notes.clone(),
            tags: tags.clone(),
            public,
            unread,
        }
    }
    pub fn day(&self) -> PostDay {
        self.day.clone()
    }
    pub fn delete_tag(&mut self, tag: &Tagname) {
        self.tags.remove(tag);
    }
    pub fn id(&self) -> PostId {
        self.id
    }
    pub fn notes(&self) -> Option<&str> {
        self.notes.as_deref()
    }
    pub fn posted(&self) -> DateTime<Utc> {
        self.posted
    }
    pub fn rename_tag(&mut self, from: &Tagname, to: &Tagname) {
        if self.tags.remove(from) {
            self.tags.insert(to.clone());
        }
    }
    pub fn tags(&self) -> impl Iterator<Item = &Tagname> {
        self.tags.iter()
    }
    pub fn title(&self) -> &str {
        &self.title
    }
    pub fn url(&self) -> &StorUrl {
        &self.url
    }
    pub fn user_id(&self) -> UserId {
        self.user_id
    }
}
