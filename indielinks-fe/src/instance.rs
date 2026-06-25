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

use std::{result::Result as StdResult, sync::Arc};

use gloo_net::http::Request;
use leptos::{either::EitherOf3, prelude::*};
use nonempty_collections::vector::NEVec;
use nonzero::nonzero;
use snafu::prelude::*;
use tap::Pipe;
use thaw::{Icon, InfoLabel, InfoLabelInfo, Spinner};

use indielinks_shared::{
    api::{
        ClusterStatsResponse, RecentPostsRequest, RecentPostsResponse, TopKTagsRequest,
        TopKTagsResponse,
    },
    entities::{Post, Tagname},
};

use crate::{
    http::error_for_status1,
    types::{Api, Token},
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Snafu)]
enum Error {
    #[snafu(display("While sending an HTTP request, {source}"))]
    Http {
        #[snafu(source(from(gloo_net::Error, Arc::new)))]
        source: Arc<gloo_net::Error>,
    },
    #[snafu(display("While deserializing the recent posts response, {source}"))]
    Posts {
        #[snafu(source(from(gloo_net::Error, Arc::new)))]
        source: Arc<gloo_net::Error>,
    },
    #[snafu(display("While deserializing the cluster stats response, {source}"))]
    Stats {
        #[snafu(source(from(gloo_net::Error, Arc::new)))]
        source: Arc<gloo_net::Error>,
    },
    #[snafu(display("Failed HTTP call: {source}"))]
    Status { source: crate::http::Error },
    #[snafu(display("While deserializing the top-k tags response, {source}"))]
    Tags {
        #[snafu(source(from(gloo_net::Error, Arc::new)))]
        source: Arc<gloo_net::Error>,
    },
}

type Result<T> = StdResult<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                     Top-K Tags Navigation                                      //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// A very simple component rendering a "refresh" option at the top of the popular tags list
#[component]
fn TopKTagsNav(refresh: ArcTrigger) -> impl IntoView {
    view! {
        <div class="mx-auto flex items-center gap-2 p-2">
            <span class="text-lg">"Most Popular Tags "</span>
            <Icon icon=icondata::IoRefresh class="text-gray-400"
                  on_click=move |_| {
                      refresh.notify()
                  }
            />
        </div>
    }
    //
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        Top-K Tags List                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
fn TopKTagsList(tags: NEVec<(Tagname, f64)>) -> impl IntoView {
    tags.into_iter()
        .map(|(tag, score)| {
            view! {
                <div class="flex gap-2 p-1">
                    <span>{ format!("{tag}") }</span>
                    <span>{ format!("{score:.2}")}</span>
                </div>
            }
        })
        .collect::<Vec<_>>()
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           Top-K Tags                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

async fn load_tags(api: &str) -> Result<Option<NEVec<(Tagname, f64)>>> {
    Request::post(&format!("{api}/api/v1/users/top-k-tags"))
        .json(&TopKTagsRequest {
            num_items: Some(nonzero!(32usize)),
        })
        .context(HttpSnafu)?
        .send()
        .await
        .context(HttpSnafu)?
        .pipe(error_for_status1)
        .context(StatusSnafu)?
        .json::<TopKTagsResponse>()
        .await
        .context(TagsSnafu)
        .map(|response| NEVec::try_from_vec(response.tags))
}

// I'd like to factor this out into its own component, but start here for now.
#[component]
pub fn TopKTags() -> impl IntoView {
    let api = expect_context::<Api>().0;

    let token = expect_context::<Token>();

    // Setup a mechanism by which we can force this view to be re-rendered. A signal won't really do
    // it because this summarizes state after other operations outside our awareness. "A trigger is
    // a data-less signal with the sole purpose of notifying other reactive code of a change."
    let refresh = ArcTrigger::new();

    // Setup a local resource yielding a `Result<Option<NEVec<(Tagname, f64)>>>`. I can never keep
    // track of whether I want a `Resource` or an `Action`. According to the book, "Actions and
    // resources seem similar, but they represent fundamentally different things. If you’re trying
    // to load data by running an async function, either once or when some other value changes, you
    // probably want to use a resource. If you’re trying to occasionally run an async function in
    // response to something like a user clicking a button, you probably want to use an Action."

    // Seems like this is a resource:
    let tags = LocalResource::new({
        let refresh = refresh.clone();
        move || {
            let api = api.clone();
            refresh.track();
            async move { load_tags(&api).await }
        }
    });

    view! {
        // In the event of an unrecoverable error in any of our child components, we'll end-up
        // here, rendering a little "Oops!" label on which the usewr can click to get more
        // information. I used this idiom on the user's page, as well.
        <ErrorBoundary
            fallback=|errors| view! {
                <InfoLabel>
                    <InfoLabelInfo slot>
                        <ul>
                        { move || errors
                          .get()
                          .into_iter()
                          .map(|(_, err)| view!{ <li>{err.to_string()}</li>})
                          .collect::<Vec<_>>() }
                        </ul>
                    </InfoLabelInfo>
                    "Ooops!"
                </InfoLabel>
            } >
            <Transition fallback=move || view! { <Spinner /> }>
            {
                // The body of the `Transition` is a lambda yielding a `Result`; that means we can
                // use the `?` sigil naturally below in cases where we want to invoke our
                // <ErrorBoundary> fallback, above.
                move || -> Result<_> {
                    // `.get()` yields an *option* wrapped around the actual return value; in our
                    // case, we'll get an `Option<Result<Option<NEVec<(Tagname, f64)>>>>`. This lets
                    // us model the resource having not yet resolved. Now, because we're inside a
                    // <Transition>, we *know* it will never return the `None` case, but for
                    // starters, let's handle the error case:
                    let tags = tags.get().transpose()?; // `Option<Option<NEVec<...>>>` Now,
                    // technically, this view will return a `Result<Option<Either<...>>>`. However,
                    // again, we know we'll never have the `None` variant returned (because we're in
                    // a <Transition>), so just work "inside" the `Option` via `.map()` to avoid
                    // having to explicitly handle that case (with something inelegant like an
                    // `unimplemented()` or something):
                    Ok(tags.map(|maybe_tags| {
                        match maybe_tags {
                            Some(tags) => EitherOf3::A(view! {
                                <TopKTagsNav refresh=refresh.clone() />
                                <TopKTagsList tags=tags/>
                            }),
                            None => match token.get() {
                                Some(_) => EitherOf3::B(view! {
                                    <div class="mx-auto max-w-md m-8 text-gray-600 p-2">
                                        <p>"This instance doesn't have any tags, yet. Click "<a href="/a" class="text-blue-600 underline hover:text-blue-800 visited:text-purple-600">"here"</a>" to start adding some."</p>
                                    </div>
                                }),
                                None => EitherOf3::C(view! {
                                    <div class="mx-auto max-w-md m-8 text-gray-600 p-2">
                                        <p>"This instance doesn't have any tags, yet. "<a href="/s" class="text-blue-600 underline hover:text-blue-800 visited:text-purple-600">"Sign-in"</a>" to start adding some."</p>
                                    </div>
                                })
                            }
                        }
                    }))
                }
            }
            </Transition >
        </ErrorBoundary>
    }
} // TopKTags

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                    Recent Posts Navigation                                     //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// A very simple component rendering a "refresh" option at the top of the recent posts list
#[component]
fn RecentPostsNav(refresh: ArcTrigger) -> impl IntoView {
    view! {
        <div class="mx-auto flex items-center gap-2 p-2">
            <span class="text-lg">"Most Recent Public Posts "</span>
            <Icon icon=icondata::IoRefresh class="text-gray-400"
                  on_click=move |_| {
                      refresh.notify()
                  }
            />
        </div>
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       Recent Posts List                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
fn RecentPostsList(posts: NEVec<Post>) -> impl IntoView {
    posts
        .into_iter()
        // Really need to factor this (along with the logic in `home.rs` out into `components`)...
        .map(|post| {
            let url = post.url().clone();
            let title = post.title().to_owned();
            let posted = post.posted().format("%Y-%m-%d %H:%M:%S").to_string();

            view!{
                <div class="p-1">
                    // The link itself (larger, more prominent)
                    <div class="text-lg">
                        <a href={ url.to_string() } class="text-blue-600 underline hover:text-blue-800"> { title }</a>
                    </div>
                    // The post time & tags (smaller, gray text)
                    <div class="flex">
                    <div class="flex-[0 0 auto] text-gray-400"> { posted } </div>
                    <div class="flex px-2 gap-1">
                    {
                        post
                            .tags()
                            .cloned()
                            .map(|tag| view! {
                                <span>{ format!("{tag}") }</span>
                            })
                            .collect::<Vec<_>>()
                    }
                    </div>
                </div>
            </div>
        }})
        .collect::<Vec<_>>()
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       Recent Posts List                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

async fn load_posts(api: &str) -> Result<Option<NEVec<Post>>> {
    Request::post(&format!("{api}/api/v1/users/recent-posts"))
        .json(&RecentPostsRequest {
            token: None,
            page_size: Some(nonzero!(32usize)),
        })
        .context(HttpSnafu)?
        .send()
        .await
        .context(HttpSnafu)?
        .pipe(error_for_status1)
        .context(StatusSnafu)?
        .json::<RecentPostsResponse>()
        .await
        .context(PostsSnafu)
        .map(|maybe_page| match maybe_page {
            Some(response) => Some(response.page),
            None => None,
        })
}

#[component]
pub fn RecentPosts() -> impl IntoView {
    let api = expect_context::<Api>().0;

    let token = expect_context::<Token>();

    // Setup a mechanism by which we can force this view to be re-rendered. A signal won't really do
    // it because this summarizes state after other operations outside our awareness. "A trigger is
    // a data-less signal with the sole purpose of notifying other reactive code of a change."
    let refresh = ArcTrigger::new();

    // Setup a local resource yielding a `Result<Option<RecentPostsPage>>>`. I can never keep
    // track of whether I want a `Resource` or an `Action`. According to the book, "Actions and
    // resources seem similar, but they represent fundamentally different things. If you’re trying
    // to load data by running an async function, either once or when some other value changes, you
    // probably want to use a resource. If you’re trying to occasionally run an async function in
    // response to something like a user clicking a button, you probably want to use an Action."

    // Seems like this is a resource:
    let posts = LocalResource::new({
        let refresh = refresh.clone();
        move || {
            let api = api.clone();
            refresh.track();
            async move { load_posts(&api).await }
        }
    });

    view! {
        // In the event of an unrecoverable error in any of our child components, we'll end-up
        // here, rendering a little "Oops!" label on which the usewr can click to get more
        // information. I used this idiom on the user's page, as well.
        <ErrorBoundary
            fallback=|errors| view! {
                <InfoLabel>
                    <InfoLabelInfo slot>
                        <ul>
                        { move || errors
                          .get()
                          .into_iter()
                          .map(|(_, err)| view!{ <li>{err.to_string()}</li>})
                          .collect::<Vec<_>>() }
                        </ul>
                    </InfoLabelInfo>
                    "Ooops!"
                </InfoLabel>
            } >
            <Transition fallback=move || view! { <Spinner /> }>
            {
                // The body of the `Transition` is a lambda yielding a `Result`; that means we can
                // use the `?` sigil naturally below in cases where we want to invoke our
                // <ErrorBoundary> fallback, above.
                move || -> Result<_> {
                    // `.get()` yields an *option* wrapped around the actual return value; in our
                    // case, we'll get an `Option<Result<Option<RecentPostsPage>>>>`. This lets
                    // us model the resource having not yet resolved. Now, because we're inside a
                    // <Transition>, we *know* it will never return the `None` case, but for
                    // starters, let's handle the error case:
                    let posts = posts.get().transpose()?; // `Option<Option<RecentPostsPage>>` Now,
                    // technically, this view will return a `Result<Option<Either<...>>>`. However,
                    // again, we know we'll never have the `None` variant returned (because we're in
                    // a <Transition>), so just work "inside" the `Option` via `.map()` to avoid
                    // having to explicitly handle that case (with something inelegant like an
                    // `unimplemented()` or something):
                    Ok(posts.map(|maybe_posts| {
                        match maybe_posts {
                            Some(posts) => EitherOf3::A(view! {
                                <RecentPostsNav refresh=refresh.clone() />
                                <RecentPostsList posts/>
                            }),
                            None => match token.get() {
                                Some(_) => EitherOf3::B(view! {
                                    <div class="mx-auto max-w-md m-8 text-gray-600 p-2">
                                        <p>"This instance doesn't have any posts, yet. Click "<a href="/a" class="text-blue-600 underline hover:text-blue-800 visited:text-purple-600">"here"</a>" to start adding some."</p>
                                    </div>
                                }),
                                None => EitherOf3::C(view! {
                                    <div class="mx-auto max-w-md m-8 text-gray-600 p-2">
                                        <p>"This instance doesn't have any posts, yet. "<a href="/s" class="text-blue-600 underline hover:text-blue-800 visited:text-purple-600">"Sign-in"</a>" to start adding some."</p>
                                    </div>
                                })
                            }
                        }
                    }))
                }
            }
            </Transition >
        </ErrorBoundary>
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         Cluster Stats                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

async fn load_stats(api: &str) -> Result<ClusterStatsResponse> {
    Request::get(&format!("{api}/api/v1/users/cluster-stats"))
        .send()
        .await
        .context(HttpSnafu)?
        .pipe(error_for_status1)
        .context(StatusSnafu)?
        .json::<ClusterStatsResponse>()
        .await
        .context(StatsSnafu)
}

#[component]
fn ClusterStats() -> impl IntoView {
    let api = expect_context::<Api>().0;
    // Seems like this is a resource:
    let stats = LocalResource::new({
        move || {
            let api = api.clone();
            async move { load_stats(&api).await }
        }
    });
    view! {
        <ErrorBoundary
            fallback=|errors| view! {
                <InfoLabel>
                    <InfoLabelInfo slot>
                        <ul>
                        { move || errors
                          .get()
                          .into_iter()
                          .map(|(_, err)| view!{ <li>{err.to_string()}</li>})
                          .collect::<Vec<_>>() }
                        </ul>
                    </InfoLabelInfo>
                    "Ooops!"
                </InfoLabel>
            } >
            <Transition fallback=move || view! { <Spinner /> }>
            {
                move || -> Result<_> {
                    let stats = stats.get().transpose()?;
                    Ok(stats.map(|stats| {
                        let verbiage = match (stats.raft_initialized, stats.raft_leader) {
                            (Some(initialized), Some(leader)) => {
                                format!("This instance has {} users & {} posts. The raft was initialized {}, the leader is {}, and the raft term is {}.", stats.num_users, stats.num_posts, initialized, leader, stats.raft_term)
                            },
                            (Some(initialized), None) => {
                                format!("This instance has {} users & {} posts. The raft was initialized {}, and the raft term is {}.", stats.num_users, stats.num_posts, initialized, stats.raft_term)
                            },
                            (None, Some(leader)) => {
                                // Pretty-sure this can't happen, but 🤷
                                format!("This instance has {} users & {} posts. The raft leader is {}, and the raft term is {}.", stats.num_users, stats.num_posts, leader, stats.raft_term)
                            },
                            (None, None) => {
                                format!("This instance has {} users & {} posts. The raft has not yet been initialized", stats.num_users, stats.num_posts)
                            },
                        };
                        view! {
                            <div class="text-gray-400">
                                { verbiage }
                            </div>
                        }
                    }))
                }
            }
            </Transition >
        </ErrorBoundary>
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                     The instance verbiage                                      //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
fn Verbiage() -> impl IntoView {
    let api = expect_context::<Api>().0;
    // Seems like this is a resource:
    let stats = LocalResource::new({
        move || {
            let api = api.clone();
            async move { load_stats(&api).await }
        }
    });
    view! {
        <ErrorBoundary
            fallback=|errors| view! {
                <InfoLabel>
                    <InfoLabelInfo slot>
                        <ul>
                        { move || errors
                          .get()
                          .into_iter()
                          .map(|(_, err)| view!{ <li>{err.to_string()}</li>})
                          .collect::<Vec<_>>() }
                        </ul>
                    </InfoLabelInfo>
                    "Ooops!"
                </InfoLabel>
            } >
            <Transition fallback=move || view! { <Spinner /> }>
            {
                move || -> Result<_> {
                    let stats = stats.get().transpose()?;
                    Ok(stats.map(|stats| {
                        view! {
                            <div class="text-gray-600">
                                "This is "{ format!("{}", stats.origin) }", an indielinks instance. Think of it as del.icio.us on the fediverse. Contact "<a href="mailto:sp1ff@pobox.com">"sp1ff@pobox.com"</a>" for an account!"
                            </div>
                        }
                    }))
                }
            }
            </Transition >
        </ErrorBoundary>
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                               The "instance", or "popular" page                                //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// indielinks "instance" page
// This is going to be pretty simple, for now: a rolling list of recent (public) posts, the most
// popular tags, and some basic stats. On top, some welcoming verbiage & contact information. In
// the future, this would be an ideal place for the local and/or federated feeds.
#[component]
pub fn Instance() -> impl IntoView {
    view! {
        // I think I'm going to allow each component to handle its own errors; why give-up rendering
        // the entire page when only one component has a problem?
        <div class="grid grid-rows-[auto_1fr_auto] h-screen gap-2 p-4  text-gray-600">
            <div>
              <Verbiage />
            </div>
          <div class="flex justify-between">
            <div class="min-w-64 border border-solid border-sky-100 overflow-y-auto">
                <RecentPosts />
            </div>
            <div class="min-w-48 border border-solid border-sky-100 overflow-y-auto">
                <TopKTags />
            </div>
          </div>
          <div class="border border-solid border-sky-100">
            <ClusterStats />
          </div>
        </div>
    }
}
