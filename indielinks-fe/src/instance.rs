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

//! # The public instance page
//!
//! Popular introduces an indielinks instance and presents its recent public links, ranked tags,
//! and service statistics. Each independently useful data region owns its resource and recovery.

use std::{result::Result as StdResult, sync::Arc};

use gloo_net::http::Request;
use leptos::{either::Either, prelude::*};
use nonempty_collections::vector::NEVec;
use nonzero::nonzero;
use snafu::prelude::*;
use tap::Pipe;
use thaw::Icon;
use url::Url;

use indielinks_shared::{
    api::{
        ClusterStatsResponse, RecentPostsRequest, RecentPostsResponse, TopKTagsRequest,
        TopKTagsResponse,
    },
    entities::{Post, StorUrl, Tagname},
};

use crate::{
    components::feedback::{EmptyAction, EmptyState, ErrorState, LoadingState},
    http::error_for_status1,
    types::{Api, Base, Token},
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Snafu)]
enum Error {
    #[snafu(display("While sending an HTTP request"))]
    Http {
        #[snafu(source(from(gloo_net::Error, Arc::new)))]
        source: Arc<gloo_net::Error>,
    },
    #[snafu(display("While deserializing the recent links response"))]
    Posts {
        #[snafu(source(from(gloo_net::Error, Arc::new)))]
        source: Arc<gloo_net::Error>,
    },
    #[snafu(display("While deserializing the instance statistics"))]
    Stats {
        #[snafu(source(from(gloo_net::Error, Arc::new)))]
        source: Arc<gloo_net::Error>,
    },
    #[snafu(display("The server returned an unsuccessful response"))]
    Status { source: crate::http::Error },
    #[snafu(display("While deserializing the popular tags response"))]
    Tags {
        #[snafu(source(from(gloo_net::Error, Arc::new)))]
        source: Arc<gloo_net::Error>,
    },
}

type Result<T> = StdResult<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         Shared helpers                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

fn host_label(url: &StorUrl) -> String {
    let url: &Url = url.as_ref();
    url.host_str()
        .map(str::to_owned)
        .unwrap_or_else(|| url.scheme().to_owned())
}

fn empty_action(token: Token, base: &str) -> EmptyAction {
    if token.get().is_some() {
        EmptyAction::Link {
            href: format!("{base}/a"),
            label: "Add link",
        }
    } else {
        EmptyAction::Link {
            href: format!("{base}/s"),
            label: "Sign in",
        }
    }
}

#[component]
fn RefreshButton(
    label: &'static str,
    loading: Signal<bool>,
    callback: Callback<()>,
) -> impl IntoView {
    view! {
        <button
            aria-label=label
            aria-busy=move || loading.get().to_string()
            class="popular-panel__refresh"
            disabled=move || loading.get()
            type="button"
            on:click=move |_| callback.run(())
        >
            <span aria-hidden="true"><Icon icon=icondata::IoRefresh /></span>
            <span>{move || if loading.get() { "Refreshing…" } else { "Refresh" }}</span>
        </button>
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                      Recent public links                                       //
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
        .map(|page| page.map(|page| page.page))
}

#[component]
fn PublicLink(post: Post) -> impl IntoView {
    let url = post.url().clone();
    let host = host_label(&url);
    let datetime = post.posted().to_rfc3339();
    let posted = post.posted().format("%Y-%m-%d %H:%M UTC").to_string();
    let notes = post.notes().map(str::to_owned);
    let mut tags = post.tags().cloned().collect::<Vec<_>>();
    tags.sort();
    let tag_list = (!tags.is_empty()).then(|| {
        view! {
            <ul aria-label="Tags" class="public-link__tags" role="list">
                {tags.into_iter().map(|tag| view! {
                    <li class="public-link__tag">{tag.to_string()}</li>
                }).collect_view()}
            </ul>
        }
    });

    view! {
        <article class="public-link">
            <h3 class="public-link__title">
                <a href=url.to_string()>{post.title().to_owned()}</a>
            </h3>
            <div class="public-link__metadata">
                <span class="public-link__host">{host}</span>
                <span aria-hidden="true">"·"</span>
                <time datetime=datetime>{posted}</time>
            </div>
            {notes.map(|notes| view! { <p class="public-link__notes">{notes}</p> })}
            {tag_list}
        </article>
    }
}

#[component]
fn RecentPosts() -> impl IntoView {
    let api = expect_context::<Api>().0;
    let base = expect_context::<Base>().0;
    let token = expect_context::<Token>();
    let refresh = ArcTrigger::new();
    let loading = RwSignal::new(true);
    let posts = LocalResource::new({
        let refresh = refresh.clone();
        move || {
            let api = api.clone();
            refresh.track();
            async move {
                let result = load_posts(&api).await;
                loading.set(false);
                result
            }
        }
    });
    let refresh_callback = Callback::new({
        let refresh = refresh.clone();
        move |()| {
            if !loading.get_untracked() {
                loading.set(true);
                refresh.notify();
            }
        }
    });

    view! {
        <section aria-labelledby="recent-public-links-heading" class="content-panel popular-panel">
            <header class="popular-panel__header">
                <h2 class="content-panel__heading" id="recent-public-links-heading">
                    "Recent public links"
                </h2>
                <RefreshButton
                    label="Refresh recent public links"
                    loading=loading.into()
                    callback=refresh_callback
                />
            </header>
            <div class="content-panel__body">
                <ErrorBoundary fallback={
                    let refresh = refresh.clone();
                    move |errors| view! {
                        <ErrorState
                            title="Recent links could not be loaded"
                            errors
                            retry=Callback::new({
                                let refresh = refresh.clone();
                                move |()| {
                                    loading.set(true);
                                    refresh.notify();
                                }
                            })
                        />
                    }
                }>
                    <Transition fallback=move || view! {
                        <LoadingState label="Loading recent links…" />
                    }>
                        {move || -> Result<_> {
                            let response = posts.get().transpose()?;
                            Ok(response.map(|posts| match posts {
                                Some(posts) => Either::Left(view! {
                                    <ol class="public-links" role="list">
                                        {posts.into_iter().map(|post| view! {
                                            <li class="public-links__item"><PublicLink post /></li>
                                        }).collect_view()}
                                    </ol>
                                }),
                                None => Either::Right(view! {
                                    <EmptyState
                                        title="No public links yet"
                                        message="Be the first to add a link to this instance."
                                        action=empty_action(token, &base)
                                    />
                                }),
                            }))
                        }}
                    </Transition>
                </ErrorBoundary>
            </div>
        </section>
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         Popular tags                                           //
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

#[component]
fn PopularTags() -> impl IntoView {
    let api = expect_context::<Api>().0;
    let base = expect_context::<Base>().0;
    let token = expect_context::<Token>();
    let refresh = ArcTrigger::new();
    let loading = RwSignal::new(true);
    let tags = LocalResource::new({
        let refresh = refresh.clone();
        move || {
            let api = api.clone();
            refresh.track();
            async move {
                let result = load_tags(&api).await;
                loading.set(false);
                result
            }
        }
    });
    let refresh_callback = Callback::new({
        let refresh = refresh.clone();
        move |()| {
            if !loading.get_untracked() {
                loading.set(true);
                refresh.notify();
            }
        }
    });

    view! {
        <section aria-labelledby="popular-tags-heading" class="content-panel popular-panel">
            <header class="popular-panel__header">
                <h2 class="content-panel__heading" id="popular-tags-heading">"Popular tags"</h2>
                <RefreshButton
                    label="Refresh popular tags"
                    loading=loading.into()
                    callback=refresh_callback
                />
            </header>
            <div class="content-panel__body">
                <ErrorBoundary fallback={
                    let refresh = refresh.clone();
                    move |errors| view! {
                        <ErrorState
                            title="Popular tags could not be loaded"
                            errors
                            retry=Callback::new({
                                let refresh = refresh.clone();
                                move |()| {
                                    loading.set(true);
                                    refresh.notify();
                                }
                            })
                        />
                    }
                }>
                    <Transition fallback=move || view! {
                        <LoadingState label="Loading popular tags…" />
                    }>
                        {move || -> Result<_> {
                            let response = tags.get().transpose()?;
                            Ok(response.map(|tags| match tags {
                                Some(tags) => Either::Left(view! {
                                    <ol class="popular-tags" role="list">
                                        {tags.into_iter().enumerate().map(|(index, (tag, score))| {
                                            let score_label = format!("score {score:.2}");
                                            view! {
                                                <li class="popular-tags__item">
                                                    <span aria-hidden="true" class="popular-tags__rank">
                                                        {index + 1}
                                                    </span>
                                                    <span class="popular-tags__name">{tag.to_string()}</span>
                                                    <span aria-label=score_label class="popular-tags__score">
                                                        {format!("{score:.2}")}
                                                    </span>
                                                </li>
                                            }
                                        }).collect_view()}
                                    </ol>
                                }),
                                None => Either::Right(view! {
                                    <EmptyState
                                        title="No popular tags yet"
                                        message="Tags will appear as people save links."
                                        action=empty_action(token, &base)
                                    />
                                }),
                            }))
                        }}
                    </Transition>
                </ErrorBoundary>
            </div>
        </section>
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                      Instance information                                      //
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
fn InstanceDetails() -> impl IntoView {
    let api = expect_context::<Api>().0;
    let refresh = ArcTrigger::new();
    let stats = LocalResource::new({
        let refresh = refresh.clone();
        move || {
            let api = api.clone();
            refresh.track();
            async move { load_stats(&api).await }
        }
    });

    view! {
        <div class="instance-details">
            <ErrorBoundary fallback={
                let refresh = refresh.clone();
                move |errors| view! {
                    <section class="content-panel instance-layout__details-error">
                        <ErrorState
                            title="Instance details could not be loaded"
                            errors
                            retry=Callback::new({
                                let refresh = refresh.clone();
                                move |()| refresh.notify()
                            })
                        />
                    </section>
                }
            }>
                <Transition fallback=move || view! {
                    <section class="content-panel instance-layout__details-loading">
                        <LoadingState label="Loading instance details…" />
                    </section>
                }>
                    {move || -> Result<_> {
                        Ok(stats.get().transpose()?.map(|stats| {
                            let initialized = stats
                                .raft_initialized
                                .map(|value| value.to_rfc3339())
                                .unwrap_or_else(|| "not initialized".to_owned());
                            let leader = stats
                                .raft_leader
                                .map(|value| value.to_string())
                                .unwrap_or_else(|| "none".to_owned());
                            view! {
                                <section
                                    aria-labelledby="instance-statistics-heading"
                                    class="content-panel instance-layout__statistics"
                                >
                                    <h2 id="instance-statistics-heading">"About this instance"</h2>
                                    <p class="instance-statistics__origin">{stats.origin.to_string()}</p>
                                    <dl class="instance-statistics">
                                        <div>
                                            <dt>"Users"</dt>
                                            <dd>{stats.num_users}</dd>
                                        </div>
                                        <div>
                                            <dt>"Saved links"</dt>
                                            <dd>{stats.num_posts}</dd>
                                        </div>
                                    </dl>
                                    <details class="instance-service-details">
                                        <summary>"Service details"</summary>
                                        <dl>
                                            <div><dt>"Raft initialized"</dt><dd>{initialized}</dd></div>
                                            <div><dt>"Raft leader"</dt><dd>{leader}</dd></div>
                                            <div><dt>"Raft term"</dt><dd>{stats.raft_term}</dd></div>
                                        </dl>
                                    </details>
                                </section>
                            }
                        }))
                    }}
                </Transition>
            </ErrorBoundary>
        </div>
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         Popular page                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Render the public instance page.
#[component]
pub fn Instance() -> impl IntoView {
    view! {
        <div class="instance-layout">
            <section class="instance-layout__introduction">
                <p class="instance-layout__eyebrow">"Welcome to indielinks"</p>
                <h1>"Popular"</h1>
                <p class="instance-layout__lead">
                    "Del.icio.us on the Fediverse: save, tag, and discover useful links."
                </p>
                <p class="instance-layout__contact">
                    "Want an account? "
                    <a href="mailto:sp1ff@pobox.com?subject=indielinks%20account%20request">
                        "Contact the administrator"
                    </a>
                    "."
                </p>
            </section>
            <InstanceDetails />
            <div class="instance-layout__panels">
                <RecentPosts />
                <PopularTags />
            </div>
        </div>
    }
}
