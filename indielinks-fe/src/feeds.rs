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

//! # indielinks-fe Feeds
//!
//! This is the top-level page for displaying the user's assorted ActivityPub "feeds".

// It's also the place where I'm trying to up-level my Leptos coding a bit. The problem I'm having
// is that a Leptos component combines state, logic, and presentation. A sufficiently complex
// component that combines all three can quickly become un-readable (and un-manageable, un-testable,
// and so on).
//
// I'm going to try a few things to address this problem:
//
// 1. begin thinking of my UI as smaller, composable components (avoiding "God components")
//
// 2. for each component, respect the "three layer model"
//    <https://martinfowler.com/bliki/PresentationDomainDataLayering.html>: state, logic &
//    presentation (alternatively <https://www.russ.dev/posts/frontend-architecture/>, view, model,
//    data access). The latter two are often implemented via what are regrettably named "hooks":
//    factory functions that create all the reactive plumbing & return the tuple or struct to the
//    caller (presumably the component, which will incorporate them into the JSX or view! handling
//    presentation)
//
// 3. for each component, strive to produce what Fowler calls a "headless component": an
//    independently testable unit of code that models state & logic without presentation
//    <https://martinfowler.com/articles/headless-component.html>

use std::{cmp::PartialEq, collections::VecDeque, hash::Hash, result::Result as StdResult};

use gloo_net::http::Request;
use leptos::{
    either::{Either, EitherOf3},
    prelude::*,
    tachys::view::keyed::SerializableKey,
};
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use tracing::debug;
use url::Url;

use indielinks_shared::api::{
    FeedPost, LikeRequest, TimelineInitialPage, TimelineInitialRsp, TimelineReq, TimelineSincePage,
    TimelineSinceRsp, TimelineToken,
};

use crate::{
    components::dropdown::{
        Dropdown, DropdownMenuItem, DropdownMenuItems, DropdownTrigger, use_dropdown,
    },
    http::send_with_retry,
    types::{Api, Token},
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("While deserializing the initial timeline, {source}"))]
    Load {
        source: gloo_net::Error,
        backtrace: Backtrace,
    },
    Refresh,
    #[snafu(display("While sending an HTTP request, {source}"))]
    Request {
        source: gloo_net::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Got response status {status}"))]
    Status {
        status: u16,
        backtrace: Backtrace,
    },
    #[snafu(display("While deserializing an update to the timeline, {source}"))]
    UpdateSince {
        source: gloo_net::Error,
        backtrace: Backtrace,
    },
}

pub type Result<T> = StdResult<T, Error>;

fn error_for_status(rsp: gloo_net::http::Response) -> Result<gloo_net::http::Response> {
    let status = rsp.status();
    if status >= 200 && status < 300 {
        Ok(rsp)
    } else {
        Err(StatusSnafu { status }.build())
    }
}

impl From<gloo_net::Error> for Error {
    fn from(_value: gloo_net::Error) -> Error {
        Error::Refresh
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

// The top navigation widget for a timeline
//
// This component manages a small div at the top of a timeline for either reloading or updating the
// timeline contents. If the timeline is initially empty, it functions as a "refresh" button (i.e.
// clicking on it will trigger a reload). If the timeline is live, it functions as an "update"
// button (i.e. just load new timeline items). In both cases, on click, it will turn from a
// clickable button to a plain text string that says "loading...", and on load it will revert to the
// appropriate button.
#[component]
fn TimelineTopNav<O, T>(
    token: RwSignal<Option<TimelineToken>>,
    update: Action<TimelineToken, O>,
    reload: LocalResource<T>,
) -> impl IntoView
where
    O: Send + Sync + 'static,
    T: Clone + 'static,
{
    let on_update = move |token: TimelineToken| {
        update.dispatch(token);
    };

    let on_reload = move |_| {
        reload.refetch();
    };

    let action_pending = Memo::new(move |_| reload.get().is_none() || update.pending().get());

    view! {
        <div class="">
        {
            move || {
                if action_pending.get() {
                    EitherOf3::A(view!{<span class="timeline-top-nav">"loading..."</span>})
                } else {
                    if let Some(token) = token.get() {
                        EitherOf3::B(view!{<button class="timeline-top-nav" on:click=move |_| on_update(token.clone())>"update"</button>})
                    } else {
                        EitherOf3::C(view!{<button class="timeline-top-nav" on:click=on_reload>"reload"</button>})
                    }
                }
            }
        }
        </div>
    }
}

// The feed itself
//
// This componenet manages a stack of `FeedPost`s in a timeline. Since I can reuse for for the home,
// local & federated timelines, it seemed worth it to factor it out.
#[component]
fn Timeline<IF, I, KF, K>(each: IF, key: KF) -> impl IntoView
where
    IF: Fn() -> I + Send + 'static,
    I: IntoIterator<Item = FeedPost> + Send + 'static,
    KF: Fn(&FeedPost) -> K + Send + Clone + 'static,
    K: Eq + Hash + SerializableKey + 'static,
{
    let open_menu = use_dropdown::<MenuId>();

    view! {
        <For each=each key=key
            children=move |post: FeedPost| {
                view!{<ViewPost post=post.clone() open_menu/>}}>
        </For>
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum DropdownSort {
    Share,
    Miscellaneous,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MenuId {
    url: Url,
    sort: DropdownSort,
}

#[component]
fn ViewPost(post: FeedPost, open_menu: RwSignal<Option<MenuId>>) -> impl IntoView {
    let api = expect_context::<Api>().0;
    let token = expect_context::<Token>();

    let (replying, set_replying) = signal::<bool>(false);

    let send_like = Action::new_local(move |(id, actor): &(Url, Url)| {
        let api = api.clone();
        let token = token.clone();
        let id2 = id.clone();
        let id = id.clone();
        let actor = actor.clone();
        let send_like = move || {
            let api = api.clone();
            let token = token.clone();
            let id = id.clone();
            let actor = actor.clone();
            async move {
                // Regrettably, at this time, this lambda has to return a `gloo_net::Error`.
                let token = token.get().ok_or(gloo_net::Error::GlooError(
                    "Missing token; this is a bug.".to_owned(),
                ))?;
                Request::post(&format!("{api}/api/v1/users/like"))
                    .header("Authorization", &format!("Bearer {token}"))
                    .json(&LikeRequest {
                        id: id.clone(),
                        actor: actor.clone(),
                    })
                    .map_err(|err| gloo_net::Error::GlooError(format!("{err}")))?
                    .send()
                    .await
            }
        };
        async move {
            match send_with_retry(send_like)
                .await
                .context(RequestSnafu)
                .and_then(error_for_status)
            {
                Ok(_) => debug!("Liked post {id2}"),
                Err(err) => debug!("While liking post {id2}, {err}."),
            }
        }
    });

    let share_menu_id = MenuId {
        url: post.id.clone(),
        sort: DropdownSort::Share,
    };
    let misc_menu_id = MenuId {
        url: post.id.clone(),
        sort: DropdownSort::Miscellaneous,
    };

    let post_id = post.id.clone();
    let post_actor = post.actor.clone();
    view! {
        <div class="feed-item" style="display:flex; flex-direction: column; gap: 0.5rem;">
            <div class="feed-item-content" style="text-align: center;" inner_html=post.content></div>
            {
                // let post_id = post_id.clone();
                // let post_actor = post_actor.clone();
                move || {
                    let post_id = post_id.clone();
                    let post_actor = post_actor.clone();
                    let share_menu_id = share_menu_id.clone();
                    let misc_menu_id = misc_menu_id.clone();
                    if replying.get() {
                        Either::Left(view!{
                            <div class="feed-item-reply">
                                <div style="flex: 1 1 0; min-height 0; display: flex; flex-direction: column;">
                                    <textarea style="flex: 1 1 0; resize: none; width: 100%;"></textarea>
                                </div>
                                <div class="feed-item-reply-actions">
                                    <button on:click=move |_| set_replying.set(false)>"send"</button>
                                    <button on:click=move |_| set_replying.set(false)>"cancel"</button>
                                </div>
                            </div>
                        })
                    } else {
                        Either::Right(
                            view! {
                                <div class="feed-item-actions">
                                    <button on:click=move |_| {send_like.dispatch((post_id.clone(), post_actor.clone()));}>"like"</button>
                                    " "
                                    <Dropdown open_menu>
                                        <DropdownTrigger text="share".to_string() menu_id=share_menu_id.clone() />
                                        <DropdownMenuItems menu_id=share_menu_id.clone()>
                                            <DropdownMenuItem
                                                text="share".to_string()
                                                handler=Callback::new(|()| debug!("Share selected"))/>
                                            <DropdownMenuItem
                                                text="quote".to_string()
                                                handler=Callback::new(|()| debug!("Quote selected"))/>
                                        </DropdownMenuItems>
                                    </Dropdown>
                                    " "
                                    <button on:click=move |_| set_replying.set(true)>"reply"</button>
                                    " "
                                    <Dropdown open_menu>
                                        <DropdownTrigger text="more".to_string() menu_id=misc_menu_id.clone() />
                                        <DropdownMenuItems menu_id=misc_menu_id.clone()>
                                            <DropdownMenuItem
                                                text="copy lnk".to_string()
                                                handler=Callback::new(|()| debug!("Copy link selected"))/>
                                        </DropdownMenuItems>
                                    </Dropdown>
                                </div>
                            }
                        )
                    }
                }
            }
        </div>
    }
}

#[component]
fn TimelineBottomNav() -> impl IntoView {
    view! {}
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                    The Home Feed Component                                     //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Title panel for the Home timeline
///
/// Could just be flat text, but might want to add a context menu at some point.
// Definitely a candidate for being factored-out: just text and perhaps a sub-component.
#[component]
fn HomeTimelineTitle() -> impl IntoView {
    view! {
        <div class="feed-title">
            <span>"Home"</span>
        </div>
    }
}

// My first hook... not sure how reusable this is going to be, but at least it will help me seperate
// state + logic from presentation.

struct HomeTimelineHook {
    posts: ReadSignal<VecDeque<FeedPost>>,
    set_posts: WriteSignal<VecDeque<FeedPost>>,
    since: RwSignal<Option<TimelineToken>>,
    before: RwSignal<Option<TimelineToken>>,
    // This is a little weird; you'd expect the `LocalResource` to be parameterized by the type of
    // thing it loads, but we're updating `set_posts` under the hood, instead.
    load: LocalResource<()>,
    update: Action<TimelineToken, ()>,
}

// Expects the Api & Token to be in context.
async fn load_home() -> Result<TimelineInitialRsp> {
    // Not sure this is the best way to handle this, but even if I arrange the code so as to pass
    // the textual token down, how to handle refresh on expiry? At a minimum, we'd need shared,
    // mutable ownership.
    let api = expect_context::<Api>().0;
    let token = expect_context::<Token>();

    let send_request = move || {
        let api = api.clone();
        let token = token.clone();
        async move {
            // Regrettably, at this time, this lambda has to return a `gloo_net::Error`.
            let token = token.get().ok_or(gloo_net::Error::GlooError(
                "Missing token; this is a bug.".to_owned(),
            ))?;
            Request::post(&format!("{api}/api/v1/users/timeline"))
                .header("Authorization", &format!("Bearer {token}"))
                .json(&TimelineReq::Initial { max_posts: None })
                .map_err(|err| gloo_net::Error::GlooError(format!("{err}")))?
                .send()
                .await
        }
    };

    send_with_retry(send_request)
        .await
        .context(RequestSnafu)
        .and_then(error_for_status)?
        .json::<TimelineInitialRsp>()
        .await
        .context(LoadSnafu)
}

async fn update_home_since(since: TimelineToken) -> Result<TimelineSinceRsp> {
    // Not sure this is the best way to handle this, but even if I arrange the code so as to pass
    // the textual token down, how to handle refresh on expiry? At a minimum, we'd need shared,
    // mutable ownership.
    let api = expect_context::<Api>().0;
    let token = expect_context::<Token>();

    let send_request = move || {
        let api = api.clone();
        let token = token.clone();
        let since = since.clone();
        async move {
            // Regrettably, at this time, this lambda has to return a `gloo_net::Error`.
            let token = token.get().ok_or(gloo_net::Error::GlooError(
                "Missing token; this is a bug.".to_owned(),
            ))?;
            Request::post(&format!("{api}/api/v1/users/timeline"))
                .header("Authorization", &format!("Bearer {token}"))
                .json(&TimelineReq::Since {
                    since: since.clone(),
                    max_posts: None,
                })
                .map_err(|err| gloo_net::Error::GlooError(format!("{err}")))?
                .send()
                .await
        }
    };

    send_with_retry(send_request)
        .await
        .context(RequestSnafu)
        .and_then(error_for_status)?
        .json::<TimelineSinceRsp>()
        .await
        .context(UpdateSinceSnafu)
}

fn use_home_timeline(_api: String, _token: RwSignal<Option<String>>) -> HomeTimelineHook {
    let (posts, set_posts) = signal(VecDeque::new());
    let since_s = RwSignal::<Option<TimelineToken>>::new(None);
    let before_s = RwSignal::<Option<TimelineToken>>::new(None);
    let load = LocalResource::new(
        // This has to be a type that implements `Fn()` and returns a future with static lifetime.
        move || async move {
            debug!("Fetching the user's home timeline.");
            // This is lame-- handle the error case
            if let Ok(Some(TimelineInitialPage {
                posts,
                since,
                before,
            })) = load_home().await
            {
                set_posts.update(|x| {
                    *x = posts.into_iter().collect();
                });
                since_s.update(|a| *a = Some(since));
                before_s.update(|b| *b = Some(before));
            } else {
                debug!("This user appears to have no posts in their home timeline!");
            }
        },
    );
    let update = Action::new_local(move |since: &TimelineToken| {
        let since = since.clone();
        async move {
            debug!("Updating the timeline.");
            // This is lame-- handle the error case
            if let Ok(Some(TimelineSincePage { posts, since })) = update_home_since(since).await {
                // This is really irritating, but `VecDeque ` doesn't have a ""bulk prepend""
                set_posts.update(|x /*: &mut VecDeque<FeedPost>*/| {
                    posts.into_iter().rev().for_each(|post| x.push_front(post));
                });
                since_s.update(|b| *b = Some(since));
            } else {
            }
        }
    });

    HomeTimelineHook {
        posts,
        set_posts,
        since: since_s,
        before: before_s,
        load,
        update,
    }
}

#[component]
fn HomeTimeline(set_mode: WriteSignal<Option<Url>>) -> impl IntoView {
    let api = expect_context::<Api>().0;
    let token = expect_context::<Token>();

    let HomeTimelineHook {
        posts,
        set_posts,
        since,
        before,
        load,
        update,
    } = use_home_timeline(api, token);

    view! {
        <Await future=load.into_future() let:_unit>
            <div class="feed">
                <HomeTimelineTitle />
                <TimelineTopNav token=since update reload=load/>
                <Timeline each=move || posts.get() key=|post| post.id.clone()/>
                <TimelineBottomNav />
            </div>
        </Await>
    }
}

#[component]
fn HomeThread() -> impl IntoView {
    view! {
        <div class="feed">
            <span>"Home thread"</span>
        </div>
    }
}

/// # The "Home" feed component
///
/// ## Introduction
///
/// Conventionally, ActivityPub apps offer a few different feeds to their users. The "Home" feed
/// simply consists of all the posts by people the user follows, together with replies to & shares
/// of their own posts (I'm probably missing something; I've never come across a formal defintion).
/// The [indielinks] API exposes this timeline at the `/user/timeline` endpoint. [HomeFeed] is a
/// Leptos component for rendering the output.
// I might want to generalize this at some later point in time; it seems that the same component,
// suitably parameterized, ought to be able to handle the home, local & federated timelines, and
// perhaps even a notifications feed (i.e. follows, likes, & so forth). That said, I'm just now
// turning my hand to writing reusable Leptos components, and I'll probably start with something
// more basic, so for now let's keep this purpose-built.
#[component]
fn HomeFeed() -> impl IntoView {
    // We can display the Home feed in one of two "modes": the default, which is just a list of
    // posts, or "thread", in which the user has clicked on a post and we're now showing the thread
    // containing that post (not implemented as of yet). This is a signal which we'll pass down
    // to our sub-components allowing them to place us into "threaded" mode.
    let (mode, set_mode) = signal::<Option<Url>>(None);

    view! {
        // There will be errors that make it impossible to render the feed at all (can't reach the
        // API, for instance)
        <ErrorBoundary
            fallback=|_errors| view!{<p>"Ooops!"</p>}>
            // We can display either as a straightforward timeline, or as a "threaded view" when
            // someone clicks on a post
            {
                move || {
                    if mode.get().is_some() {
                        Either::Left(view!{<HomeThread />})
                    } else {
                        Either::Right(view!{<HomeTimeline set_mode/>})
                    }
                }
            }
        </ErrorBoundary>
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                      The Feeds Component                                       //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// The indielinks' user "feeds" page
#[component]
pub fn Feeds() -> impl IntoView {
    let _api = expect_context::<Api>().0;

    // I'm still confused on how & when these `View` constructing functions are invoked, but it
    // seems that this function won't be invoked until after sign-in, so we can extract the token &
    // provide it to all our subordinate components via context rather than prop drilling.
    let _token = expect_context::<Token>()
        .get_untracked()
        .expect("No token; this is a bug & should be reported.");

    view! {
        <div class="feeds-layout" style="display: flex; height: 100vh;">
          <div class="feeds-sidebar" style="display: flex;">
            <span>"profile, search box & preferences to go here!"</span>
          </div>
          <div class="feeds-content">
            // <div class="feed">
            //   <span>"Home"</span>
            // </div>
            <HomeFeed />
            <div class="feed">
              <span>"Local"</span>
            </div>
            <div class="feed">
              <span>"Federated"</span>
            </div>
            <div class="feed">
              <span>"Notifications"</span>
            </div>
          </div>
        </div>
    }
}
