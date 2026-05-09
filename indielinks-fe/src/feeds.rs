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
//! Components for displaying the user's home feed of ActivityPub activities.

use std::{collections::VecDeque, result::Result as StdResult, sync::Arc};

use gloo_net::http::Request;
use leptos::{either::Either, prelude::*};
use nonempty_collections::NEVec;
use snafu::prelude::*;
use tap::Pipe;
use thaw::{
    Button, ButtonAppearance, Icon, InfoLabel, InfoLabelInfo, Spinner, Toast, ToastBody,
    ToastIntent, ToastOptions, ToastTitle, ToasterInjection,
};
use tracing::{error, info};

use indielinks_shared::api::{
    FeedPost, TimelineBeforePage, TimelineBeforeRsp, TimelineInitialPage, TimelineInitialRsp,
    TimelineReq, TimelineSincePage, TimelineSinceRsp, TimelineToken,
};

use crate::{
    components::{
        dropdown::use_dropdown,
        post::{MenuId, Post},
    },
    http::{error_for_status1, send_with_retry},
    types::Api,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Snafu)]
pub enum Error {
    #[snafu(display("While sending an HTTP request, {source}"))]
    Http { source: crate::http::Error },
    #[snafu(display("While deserializing the initial timeline request, {source}"))]
    InitialResponseDe {
        #[snafu(source(from(gloo_net::Error, Arc::new)))]
        source: Arc<gloo_net::Error>,
    },
    #[snafu(display("While serializing the initial timeline request, {source}"))]
    TimelineRequest {
        #[snafu(source(from(gloo_net::Error, Arc::new)))]
        source: Arc<gloo_net::Error>,
    },
    #[snafu(display("While deserializing an update to the timeline, {source}"))]
    UpdateBefore {
        #[snafu(source(from(gloo_net::Error, Arc::new)))]
        source: Arc<gloo_net::Error>,
    },
    #[snafu(display("While deserializing an update to the timeline, {source}"))]
    UpdateSince {
        #[snafu(source(from(gloo_net::Error, Arc::new)))]
        source: Arc<gloo_net::Error>,
    },
}

type Result<T> = StdResult<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                             TopNav                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Asynchronously load feed items newer than `since`. Expects `Api` to be available in the context.
async fn update_home_since(since: TimelineToken) -> Result<TimelineSinceRsp> {
    // Not sure this is the best way to handle this.
    let api = expect_context::<Api>().0;

    send_with_retry(
        move || Request::post(&format!("{api}/api/v1/users/timeline")),
        TimelineReq::Since {
            since,
            max_posts: None,
        },
    )
    .await
    .context(HttpSnafu)?
    .pipe(error_for_status1)
    .context(HttpSnafu)?
    .json::<TimelineSinceRsp>()
    .await
    .context(UpdateSinceSnafu)
}

/// Top navigation panel for the home feed
#[component]
fn TopNav(since: TimelineToken, posts_s: RwSignal<VecDeque<FeedPost>>) -> Result<impl IntoView> {
    let since_s = RwSignal::new(since);

    let update = Action::new_local(move |_: &()| {
        async move {
            match update_home_since(since_s.get_untracked()).await {
                Ok(Some(TimelineSincePage { posts, since })) => {
                    since_s.set(since);
                    // This is really irritating, but `VecDeque ` doesn't have a ""bulk prepend""
                    // (at least on stable)
                    posts_s.update(|x /*: &mut VecDeque<FeedPost>*/| {
                        posts.into_iter().rev().for_each(|post| x.push_front(post));
                    });
                    Ok(())
                }
                Ok(None) => {
                    info!("No new feed items");
                    Ok(())
                }
                Err(err) => {
                    error!("While updating the home timeline, {err}");
                    Err(err)
                }
            }
        }
    });

    let toaster = ToasterInjection::expect_context();

    Effect::new(move |_| {
        if let Some(Err(err)) = update.value().get() {
            toaster.dispatch_toast(
                move || {
                    view! {
                        <Toast>
                            <ToastTitle>"New posts"</ToastTitle>
                            <ToastBody>{format!("{err}")} </ToastBody>
                            </Toast>
                    }
                },
                ToastOptions::default().with_intent(ToastIntent::Error),
            )
        }
    });

    Ok(view! {
        <div class="mx-auto flex">
            <div class="mx-auto flex items-center">
                <Button
                    class="!font-normal !text-gray-600"
                    appearance=ButtonAppearance::Transparent
                    on_click=move |_| { update.dispatch(());} >
                    "new posts"
                </Button>
            </div>
        </div>
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           BottomNav                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Asynchronously load feed items older than `before`. Expects `Api` to be available in the context.
async fn update_home_before(before: TimelineToken) -> Result<TimelineBeforeRsp> {
    // Not sure this is the best way to handle this.
    let api = expect_context::<Api>().0;

    send_with_retry(
        move || Request::post(&format!("{api}/api/v1/users/timeline")),
        TimelineReq::Before {
            before,
            max_posts: None,
        },
    )
    .await
    .context(HttpSnafu)?
    .pipe(error_for_status1)
    .context(HttpSnafu)?
    .json::<TimelineBeforeRsp>()
    .await
    .context(UpdateBeforeSnafu)
}

/// Bottom navigation panel on the user's home feed
#[component]
fn BottomNav(
    before: TimelineToken,
    posts_s: RwSignal<VecDeque<FeedPost>>,
) -> Result<impl IntoView> {
    let before_s = RwSignal::new(before);

    let update = Action::new_local(move |_: &()| {
        async move {
            match update_home_before(before_s.get_untracked()).await {
                Ok(Some(TimelineBeforePage { posts, before })) => {
                    before_s.set(before);
                    // This is really irritating, but `VecDeque ` doesn't have a ""bulk prepend""
                    // (at least on stable)
                    posts_s.update(|x /*: &mut VecDeque<FeedPost>*/| {
                        posts.into_iter().for_each(|post| x.push_back(post));
                    });
                    Ok(())
                }
                Ok(None) => {
                    info!("No new feed items");
                    Ok(())
                }
                Err(err) => {
                    error!("While updating the home timeline, {err}");
                    Err(err)
                }
            }
        }
    });

    let toaster = ToasterInjection::expect_context();

    Effect::new(move |_| {
        if let Some(Err(err)) = update.value().get() {
            toaster.dispatch_toast(
                move || {
                    view! {
                        <Toast>
                            <ToastTitle>"Older posts"</ToastTitle>
                            <ToastBody>{format!("{err}")} </ToastBody>
                            </Toast>
                    }
                },
                ToastOptions::default().with_intent(ToastIntent::Error),
            )
        }
    });

    Ok(view! {
        <div class="mx-auto flex pt-[8px]">
            <div class="mx-auto flex items-center">
                <Button
                    class="!font-normal !text-gray-600"
                    appearance=ButtonAppearance::Transparent
                    on_click=move |_| { update.dispatch(());} >
                    "older posts"
                </Button>
            </div>
        </div>
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                            ItemFeed                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// A component rendering the user's home feed of items, exclusive of navigation widgets
#[component]
pub fn ItemFeed(
    posts: NEVec<FeedPost>,
    since: TimelineToken,
    before: TimelineToken,
) -> Result<impl IntoView> {
    let open_menu = use_dropdown::<MenuId>();
    let posts_s = RwSignal::<VecDeque<FeedPost>>::new(VecDeque::from_iter(posts.into_iter()));

    Ok(view! {
        <TopNav since posts_s />
        <For each=move || posts_s.get()
             key=|post| post.id.clone()
             children=move |post: FeedPost| {
                 view! { <Post post open_menu /> }
             } >
        </For>
        <BottomNav before posts_s />
    })
}

/// Initialize the home timeline
async fn initial_load(api: String) -> Result<TimelineInitialRsp> {
    send_with_retry(
        move || Request::post(&format!("{api}/api/v1/users/timeline")),
        TimelineReq::Initial { max_posts: None },
    )
    .await
    .context(HttpSnafu)?
    .pipe(error_for_status1)
    .context(HttpSnafu)?
    .json::<TimelineInitialRsp>()
    .await
    .context(InitialResponseDeSnafu)
}

/// This is the root "item feed" component
///
/// This handles errors in subordinate components, & showing a spinner until the initial load is
/// complete. When that initial load resolves, it will instantiate the [ItemFeed] component with
/// that initial list of items.
///
/// Assumes that `Api` is provided via context.
// *Still* finding my way, here. I initially fought with the system, trying to use `<Suspense>` to
// handle that initial load, and then drive reactivity off the deque of items. Finally, I decided to
// split the responsibilities into two components. Still. Coding speculatively, here.
#[component]
pub fn ItemFeedOuter() -> impl IntoView {
    let api = expect_context::<Api>().0;

    // Setup a local resource yielding a `Result<Option<TimelineInitialPage>>>>`. In the event that
    // it comes back `None` (meaning that the user has no posts in their timeline, likely because
    // they're not following anyone, yet), we want to have a means by which we can *force* a reload
    // of this resource. "A trigger is a data-less signal with the sole purpose of notifying other
    // reactive code of a change."
    let rerender = ArcTrigger::new();

    // This will reactively track the trigger, re-running and yielding a new `Result` every time it
    // is fired. Any code that reactively tracks this resource's `Result` with then also be re-run.
    let posts = LocalResource::new({
        let api = api.clone();
        let rerender = rerender.clone();
        move || {
            let api = api.clone();
            let rerender = rerender.clone();
            async move {
                rerender.track();
                initial_load(api).await
            }
        }
    });

    view! {
        <ErrorBoundary
            // In the event of an unrecoverable error in any of our child components, we'll end-up
            // here, rendering a little "Oops!" label on which the usewr can click to get more
            // information. This mirrors the fallback for the saved links feed.
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
            <Transition fallback=move || view! { <Spinner /> } >
            {
                // let on_click=on_click.clone();
                // The trick here is to provide a lambda returning a `Result`; that way, we can
                // use the `?` sigil in the natural way, and, on failure, our "fallback" will be
                // triggered.
                move || -> Result<_> {
                    // We also need to reference the local resource in order to get the
                    // `<Transition>` to fire:
                    let initial_response: Option<TimelineInitialRsp> = posts.get().transpose()?;
                    // The trick here is that, although `initial_response` is an `Option<...>`,
                    // and therefore the `.map()` invocation below will results in an
                    // `Option<Result<impl IntoView>>`, we *know* we'll never hit the `None`
                    // case, due to the fact that we're in a `<Transition>` component. So this
                    // idiom avoids any ungainly `match`ing (or whatever) on the `Option` while
                    // still avoiding a blank screen.
                    //
                    // Now, I don't entirely understand how, but the `Err` variant returned by
                    // `ItemFeed` is somehow *also* propagated back up to our fallback.

                    let on_click = {
                        let rerender = rerender.clone();
                        move |_| rerender.notify()
                    };

                    Ok(initial_response.map(|initial_response| {
                        match initial_response {
                            Some(TimelineInitialPage { posts, since, before }) => {
                                Either::Left(view! { <ItemFeed posts since before /> })
                            },
                            None => Either::Right(view! {
                                <div class="mx-auto max-w-md m-8 p-8 text-gray-600">
                                    <p>"You don't have any posts in your home timeline, yet. You can start by following "<a href="https://indieweb.social/@sp1ff" class="text-blue-600 underline hover:text-blue-800 visited:text-purple-600">me</a>" on Mastodon. I'll be adding a \"find people to follow\" page soon."
                                    <Icon icon=icondata::IoReloadOutline
                                          class="text-gray-800 m-2 cursor-pointer"
                                          on_click=on_click />
                                    </p>
                                </div>
                            }),
                        }
                    }))
                    // So, the upshot of all this is that we'll get a spinner during the initial
                    // load (only), we can drive reactive re-rendering off a deque of posts in
                    // the `ItemFeed` component, *and* we can naturally use the `?` sigil to
                    // trigger the fallback view above. Nice!
                }
            }
            </Transition>
        </ErrorBoundary>
    }
}
