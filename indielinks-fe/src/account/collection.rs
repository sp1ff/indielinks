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

#![cfg(target_arch = "wasm32")]

//! # Follower/Following collection presentation
//!
//! Shared loading, pagination, error, empty, and list presentation for the ActivityPub
//! `following` and `followers` collections.

use gloo_net::http::Request;
use leptos::{either::Either, prelude::*};
use serde::Deserialize;
use snafu::{ResultExt, prelude::*};
use url::Url;

use indielinks_shared::api::UserProfile;

use crate::{
    components::feedback::{EmptyState, ErrorState, LoadingState},
    http::{error_for_status1, send_with_retry_no_body},
    types::Api,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Snafu)]
enum Error {
    #[snafu(display("Failed to build the collection URL"))]
    BuildUrl,
    #[snafu(display("While fetching the collection, {source}"))]
    Http { source: crate::http::Error },
    #[snafu(display("While reading the collection response, {source}"))]
    Json {
        #[snafu(source(from(gloo_net::Error, std::sync::Arc::new)))]
        source: std::sync::Arc<gloo_net::Error>,
    },
    #[snafu(display("The collection response was malformed"))]
    Malformed,
}

type Result<T> = std::result::Result<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                    ActivityPub collection types                                //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// An ActivityPub ordered collection header.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Collection {
    total_items: u32,
    first: Option<String>,
}

/// A single page of an ActivityPub ordered collection.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CollectionPage {
    ordered_items: Option<Vec<String>>,
    next: Option<String>,
}

/// Which account collection is being presented.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CollectionKind {
    Following,
    Followers,
}

impl CollectionKind {
    fn endpoint(self) -> &'static str {
        match self {
            Self::Following => "following",
            Self::Followers => "followers",
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        collection loading                                      //
////////////////////////////////////////////////////////////////////////////////////////////////////

async fn load_profile(api: &str) -> Result<UserProfile> {
    let rsp = send_with_retry_no_body(|| Request::get(&format!("{api}/api/v1/users/profile")))
        .await
        .and_then(error_for_status1)
        .context(HttpSnafu)?;
    rsp.json::<UserProfile>().await.context(JsonSnafu)
}

async fn load_collection(api: String, kind: CollectionKind) -> Result<Vec<String>> {
    let profile = load_profile(&api).await?;

    let mut url = Url::parse(&api).map_err(|_| BuildUrlSnafu.build())?;
    url.path_segments_mut()
        .map_err(|_| BuildUrlSnafu.build())?
        .push("users")
        .push(&profile.username)
        .push(kind.endpoint());

    let collection: Collection = send_with_retry_no_body(|| Request::get(url.as_str()))
        .await
        .and_then(error_for_status1)
        .context(HttpSnafu)?
        .json()
        .await
        .context(JsonSnafu)?;

    if collection.total_items == 0 {
        return Ok(Vec::new());
    }

    let first = collection.first.ok_or_else(|| MalformedSnafu.build())?;
    let mut items = Vec::new();
    let mut next = Some(first);

    while let Some(page_url) = next {
        let page: CollectionPage = send_with_retry_no_body(|| Request::get(&page_url))
            .await
            .and_then(error_for_status1)
            .context(HttpSnafu)?
            .json()
            .await
            .context(JsonSnafu)?;

        let page_items = page.ordered_items.ok_or_else(|| MalformedSnafu.build())?;
        items.extend(page_items);
        next = page.next;
    }

    Ok(items)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       collection component                                     //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Present a paginated ActivityPub actor collection.
#[component]
pub fn CollectionView(
    title: &'static str,
    description: &'static str,
    error_title: &'static str,
    empty_title: &'static str,
    empty_message: &'static str,
    kind: CollectionKind,
    rerender: ArcTrigger,
    #[prop(optional)] children: Option<Children>,
) -> impl IntoView {
    let api = expect_context::<Api>().0;
    let collection = LocalResource::new({
        let rerender = rerender.clone();
        move || {
            let api = api.clone();
            let rerender = rerender.clone();
            async move {
                rerender.track();
                load_collection(api, kind).await
            }
        }
    });

    view! {
        <section aria-labelledby="collection-heading" class="account-section">
            <h2 class="account-section__heading" id="collection-heading">
                {title}
            </h2>
            <p class="account-section__help">{description}</p>
            {children.map(|children| children())}
            <ErrorBoundary fallback=move |errors| view! {
                <ErrorState
                    title=error_title
                    errors
                    retry=Callback::new({
                        let rerender = rerender.clone();
                        move |()| rerender.notify()
                    })
                />
            }>
                <Transition fallback=move || view! {
                    <LoadingState label="Loading…" />
                }>
                    {move || -> Result<_> {
                        let items = collection.get().transpose()?;
                        Ok(items.map(|items| {
                            if items.is_empty() {
                                Either::Left(view! {
                                    <EmptyState title=empty_title message=empty_message />
                                })
                            } else {
                                Either::Right(view! {
                                    <ul class="actor-list" role="list">
                                        {items.into_iter().map(|actor| {
                                            let href = actor.clone();
                                            view! {
                                                <li class="actor-list__item">
                                                    <a
                                                        class="actor-list__link"
                                                        href=href
                                                        target="_blank"
                                                        rel="noreferrer"
                                                    >
                                                        {actor}
                                                    </a>
                                                </li>
                                            }
                                        }).collect_view()}
                                    </ul>
                                })
                            }
                        }))
                    }}
                </Transition>
            </ErrorBoundary>
        </section>
    }
}
