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

//! # Application Logic
//!
//! ## Introduction
//!
//! I haven't been terribly careful about mixing application logic with HTTP handlers, but I've
//! finally gotten burned: I need to add posts from both the del.icio.us and the bookmarklets APIs.
//! I've refactored the common logic here, and I hope this module will grow into a general-purpose
//! "application logic" repository.

use std::{collections::HashSet, result::Result as StdResult, sync::Arc};

use chrono::{DateTime, Utc};
use snafu::{ResultExt, Snafu};
use tracing::debug;
use url::Url;

use indielinks_shared::{
    entities::{PostId, Tagname},
    nonempty_string::NonEmptyString,
};

use crate::{
    activity_pub::SendCreate,
    background_tasks::{BackgroundTasks, Sender},
    entities::User,
    storage::Backend as StorageBackend,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("While adding the post internally, {source}"))]
    AddPost { source: crate::storage::Error },
    #[snafu(display("While federating this post, {source}"))]
    Federation {
        source: crate::background_tasks::Error,
    },
    #[snafu(display("While updating post times, {source}"))]
    UpdatePostTimes { source: crate::storage::Error },
}

pub type Result<T> = StdResult<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           public API                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Add an indielinks [Post](indielinks_shared::entities::Post)
#[allow(clippy::too_many_arguments)]
pub async fn add_post(
    user: &User,
    url: &Url,
    title: &NonEmptyString,
    dt: Option<&DateTime<Utc>>,
    notes: Option<&NonEmptyString>,
    tags: &HashSet<Tagname>,
    replace: bool,
    shared: bool,
    to_read: bool,
    storage: &(dyn StorageBackend + Send + Sync),
    sender: Arc<BackgroundTasks>,
) -> Result<bool> {
    let postid = PostId::default();
    let now = Utc::now();
    let dt = dt.unwrap_or(&now);
    let added = storage
        .add_post(
            user,
            // At one time, I wondered whether we should resolve defaults here, or in the storage
            // backend. It turns out to be better to handle it at the API level.
            replace, url, &postid, title, dt, notes, shared, to_read, tags,
        )
        .await
        .context(AddPostSnafu)?;
    if added {
        storage
            .update_user_post_times(user, dt)
            .await
            .context(UpdatePostTimesSnafu)?;
        if shared {
            debug!(
                "Scheduling Post {} for communication to all federated servers",
                postid
            );
            sender
                .send(SendCreate::new(&postid, dt))
                .await
                .context(FederationSnafu)?;
        }
    }
    Ok(added)
}
