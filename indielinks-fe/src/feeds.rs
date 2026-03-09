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

// I'm resurrecting this from last autumn's abortive attempt. I'm going to be "coding out loud" in
// comments as I try to recapture my place.

use std::collections::VecDeque;

use gloo_net::http::Request;
use leptos::{either::Either, prelude::*};
use nonempty_collections::NEVec;
use thaw::Scrollbar;
use tracing::{debug, error};

use indielinks_shared::api::{
    FeedPost, TimelineBeforePage, TimelineBeforeRsp, TimelineInitialPage, TimelineInitialRsp,
    TimelineReq, TimelineSincePage, TimelineSinceRsp, TimelineToken,
};

use crate::{
    http::{send_with_retry, string_for_status},
    types::{Api, Token},
};

// I'd really rather return a `Result`, here.
async fn load_home(api: String, token: String) -> Option<TimelineInitialRsp> {
    // `gloo_net::http::Request` isn't Clone (!), so I can't build the request once and send it over
    // & over.
    send_with_retry(|| {
        Request::post(&format!("{api}/api/v1/users/timeline"))
        .header("Authorization", &format!("Bearer {token}"))
        .json(&TimelineReq::Initial { max_posts: None })
        .unwrap(/* TODO(sp1ff): error */) // See? how to handle this?
        .send()
    })
    .await
    .map_err(|err| err.to_string())
    .and_then(string_for_status)
    .map_err(|err| {
        error!("Requesting the initial chunk of posts from the users's timeline: {err:?}")
    })
    .ok()?
    .json::<TimelineInitialRsp>()
    .await
    .map_err(|err| error!("Deserializing the initial chunk of the user's timeline; {err:?}"))
    .ok()
}

// Again, why not return a `Result`?
async fn call_before(
    api: String,
    token: String,
    before: TimelineToken,
) -> Option<TimelineBeforeRsp> {
    // `gloo_net::http::Request` isn't Clone (!), so I can't build the request once and send it over
    // & over.
    send_with_retry(|| {
        Request::post(&format!("{api}/api/v1/users/timeline"))
        .header("Authorization", &format!("Bearer {token}"))
        .json(&TimelineReq::Before { before: before.clone(), max_posts: None })
        .unwrap(/* TODO(sp1ff): error */) // This could be handled, if I returned a `Result`
        .send()
    })
    .await
    .map_err(|err| err.to_string())
    .and_then(string_for_status)
    .map_err(|err| error!("Requesting the next page of posts from the users's timeline: {err:?}"))
    .ok()?
    .json::<TimelineBeforeRsp>()
    .await
    .map_err(|err| error!("Deserializing the next page of the user's timeline; {err:?}"))
    .ok()
}

// And again-- why not a `Result`?
async fn call_since(api: String, token: String, after: TimelineToken) -> Option<TimelineSinceRsp> {
    // `gloo_net::http::Request` isn't Clone (!), so I can't build the request once and send it over
    // & over.
    send_with_retry(|| {
        Request::post(&format!("{api}/api/v1/users/timeline"))
        .header("Authorization", &format!("Bearer {token}"))
        .json(&TimelineReq::Since { since: after.clone(), max_posts: None })
        .unwrap(/* TODO(sp1ff): error */) // Aaaaarrrrrghhh
        .send()
    })
    .await
    .map_err(|err| err.to_string())
    .and_then(string_for_status)
    .map_err(|err| error!("Requesting recent posts from the users's timeline: {err:?}"))
    .ok()?
    .json::<TimelineSinceRsp>()
    .await
    .map_err(|err| error!("Deserializing recent posts in the user's timeline; {err:?}"))
    .ok()
}

fn Post(post: FeedPost) -> impl IntoView {
    view! {
        <div class="feed-item" style="display:flex; text-align: center;">
          <div inner_html=post.content></div>
        </div>
    }
}

/// The indielinks' user "feeds" page
#[component]
pub fn Feeds() -> impl IntoView {
    let api = use_context::<Api>()
        .expect("Failed to rerieve the API net location")
        .0;

    // I'm still confused on how & when these `View` constructing functions are invoked, but it
    // seems that this function won't be invoked until after sign-in, so we can extract the token &
    // provide it to all our subordinate components via context rather than prop drilling.
    let token = use_context::<Token>()
        .expect("No token Cell!?")
        .get_untracked()
        .expect("No token!?");

    // RN, I'm structuring this along the lines of the example in the Leptos Book, section 3.4
    // "Iteration" <https://book.leptos.dev/view/04_iteration.html>, under the code sample for
    // "Dynamic Rendering with the <For/> Component": we make the deque of posts reactive (tho not
    // each individual post). Then, whenever the deque changes, the view will be re-rendered.

    let (posts, set_posts) = signal(VecDeque::new());
    let (after, set_after) = signal::<Option<TimelineToken>>(None);
    let (before, set_before) = signal::<Option<TimelineToken>>(None);

    // Signal indicating whether or not we've exhausted the Home timeline
    let (exhausted, set_exhausted) = signal(false);

    // OK-- a `LocalResource` is just a wrapper for a future that can be used synchronously &
    // reactively: if `x` is a `LocalResource`, `x.get()` will return Option<T> (where T is the
    // future's result type). `None` indicates that the future has not yet resolved. If the future's
    // implementation accesses a signal, it becomes reactive & dependent on that signal (i.e. the
    // function will be run again if the signal changes).
    //
    // Auuughhhh... I finally gave up looking for a way around this after reading this very nice
    // write-up which confirmed my fears:
    // <https://academy.fpblock.com/blog/captures-closures-async/>. We're going to need `api` &
    // `token`, below, so we have to clone 'em here so we can move the clones into the closure:
    let api_clone = api.clone();
    let token_clone = token.clone();
    // Fetch newer posts
    let update_label = RwSignal::new("update".to_owned());
    // Fetch older posts
    let more_label = RwSignal::new("more".to_owned());
    let home_data = LocalResource::new(
        // This has to be a type that implements `Fn()` and returns a future with static lifetime.
        move || {
            // This closure owns `api_clone` & `token_clone`, but in order to produce a `Future`
            // with 'static lifetime, we're going to need to move them into the async block, below,
            // so we need a second clone of each (so that this closure can be run repeatedly):
            let api_clone = api_clone.clone();
            let token_clone = token_clone.clone();
            async move {
                debug!("Fetching the user's home timeline.");
                // if let Some((first_posts, after, before)) =
                if let Some(Some(TimelineInitialPage {
                    posts,
                    since,
                    before,
                })) = load_home(api_clone.clone(), token_clone.clone()).await
                {
                    set_posts.update(|x| {
                        *x = posts.into_iter().collect();
                    });
                    set_after.update(|a| *a = Some(since));
                    set_before.update(|b| *b = Some(before));
                    update_label.set("update".to_owned());
                } else {
                    debug!("This user appears to have no posts in their home timeline!");
                    update_label.set("refresh".to_owned());
                    set_exhausted.set(true);
                }
            }
        },
    );

    let api_clone = api.clone();
    let token_clone = token.clone();
    // Fetch older posts
    let next_page = Action::new_local(move |_: &()| {
        debug!("next_page Action dispatched.");
        let api = api_clone.clone();
        let token = token_clone.clone();
        async move {
            let page_token = before.get();
            if let Some(Some(TimelineBeforePage { posts, before })) = call_before(
                api.clone(),
                token.clone(),
                page_token.expect("Missing page token"),
            )
            .await
            {
                debug!("next_page Action loaded {} additional posts", posts.len());
                set_posts.update(|x| x.extend(posts));
                set_before.update(|b| *b = Some(before));
            } else {
                debug!("next_page Action loaded no additional posts-- exhausted!");
                set_exhausted.set(true);
            }
        }
    });

    // Fetch newer posts. We have the same problem here, but since we don't need `api` & `token` any
    // more
    let since = Action::new_local(move |_: &()| {
        debug!("since Action dispatched.");
        let api = api.clone();
        let token = token.clone();
        async move {
            let page_token = after.get();
            if let Some(Some(TimelineSincePage { posts, since })) = call_since(
                api.clone(),
                token.clone(),
                page_token.expect("Missing page token"),
            )
            .await
            {
                // This is really irritating, but `VecDeque ` doesn't have a ""bulk prepend""
                set_posts.update(|x /*: &mut VecDeque<FeedPost>*/| {
                    posts.into_iter().rev().for_each(|post| x.push_front(post));
                });
                set_after.update(|b| *b = Some(since));
            }
        }
    });

    // Add a little logic to toggle the navigation labels while running async requests:
    Effect::new(move |_| {
        if since.value().get().is_some() {
            if before.get().is_none() {
                update_label.set("refresh".to_owned());
            } else {
                update_label.set("update".to_owned());
            }
        }
    });
    Effect::new(move |_| {
        if next_page.value().get().is_some() {
            more_label.set("more".to_owned());
        }
    });

    view! {
        <div class="feeds-layout" style="display: flex; height: 100vh;">
          <div class="feeds-sidebar" style="display: flex;">
            <span>"profile, search box & preferences to go here!"</span>
          </div>
          <div class="feeds-content">
            <div class="feed">

              // I'm finding it difficult to work with the "let binding", here. Thing is, you'll get
              // a *reference* to the data, which is fine if you want to copy ints or strings out of
              // it and into this View, but not if you want to keep a reference around.
              <Await future=home_data.into_future() let:_data>
                // No scroll thumb (?)
                <Scrollbar>

                  <div class="feed-nav">
                    <a href="#" on:click=move |_| {
                      update_label.set("Loading...".to_owned());
                      if before.get().is_none() {
                          home_data.refetch();
                      } else {
                          since.dispatch(());
                      }
                      }>
                      {move ||  update_label.get() }
                    </a>
                  </div>

                  <For each=move || posts.get()
                       key=|post| post.id.clone()
                       // let:post
                       children=move |post| {
                           Post(post)
                       }
                  >
                  </For>

                  <div class="feed-nav"> {
                    move || {
                      if exhausted.get() {
                          Either::Left(view!{<p>"(no more)"</p>})
                      } else {
                        Either::Right(view!{
                            <a href="#" on:click=move |_| {
                                more_label.set("Loading...".to_owned());
                                next_page.dispatch(());
                            }>
                            { more_label.get() }
                            </a>
                        })
                      }
                    }
                  }
                  </div>

                </Scrollbar>
              </Await>

            </div>
            <div class="feed">
            </div>
            <div class="feed">
            </div>
            <div class="feed">
            </div>
          </div>
        </div>
    }
}
