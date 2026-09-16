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

//! # A component for viewing a single post
//!
//! Displaying a given [FeedPost] turns out to be fairly complicated when taking into account likes,
//! shares, replies &c, as well as the fact that you can "drill down" into any given post to see the
//! conversation. This module hosts a top-level component, [Post] that handles all of this.
//!
//! While these have been factored-out into their own module, I've made little-to-no effort to make
//! these truly generic components: they're styled directly here, they show toast on errors, &c.

use std::{cmp::PartialEq, result::Result as StdResult, sync::Arc};

use gloo_net::http::Request;
use leptos::{either::Either, html, prelude::*};
use snafu::{ResultExt, Snafu};
use thaw::{Icon, ToastIntent, ToasterInjection};
use tracing::{debug, error};
use url::Url;

use indielinks_shared::api::{
    FeedPost, LikeRequest, ReplyRequest, ThreadContextRequest, ThreadContextResponse,
};

use crate::{
    components::feedback::{EmptyState, LoadingState, show_toast},
    http::send_with_retry,
    types::Api,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module Error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

// I've made this type `Clone` by wrapping source errors in `Arc`s. I'm not sure I like this, but
// being `Clone` allows me to propagate errors out of `Action`s & `Resource`s. Two other
// possibilities:
//
// 1. stringify the error at the "reactive boundry"; that is, bubble-up the strongly-typed error
//    until you're ready to return from an `Actoin` or `Resource`, then convert from `Result<T>` to
//    `StdResult<T, String>`.
//
// 2. define a parallel (and `Clone`) error type to be used in the UI; implement `From<Error>` for
//    it; I think I like this one, but I want to get more experience in *handling* errors in the UI,
//    first.
#[derive(Clone, Debug, Snafu)]
#[non_exhaustive]
pub enum Error {
    #[snafu(display("While deserializing the initial timeline, {source}"))]
    Load {
        #[snafu(source(from(gloo_net::Error, Arc::new)))]
        source: Arc<gloo_net::Error>,
    },
    Refresh,
    #[snafu(display("While sending an HTTP request, {source}"))]
    Request {
        #[snafu(source(from(gloo_net::Error, Arc::new)))]
        source: Arc<gloo_net::Error>,
    },
    #[snafu(display("While sending an HTTP request, {source}"))]
    Request1 {
        source: crate::http::Error,
    },
    #[snafu(display("Got response status {status}"))]
    Status {
        status: u16,
    },
}

pub type Result<T> = StdResult<T, Error>;

// This is duplicated from `feeds.rs`; I'm going to refactor as a part of a general re-think of my
// HTTP handling.
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

fn pop_toast(toaster: ToasterInjection, intent: ToastIntent, title: String, message: String) {
    show_toast(toaster, intent, title, message);
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                        federated post                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

// It's a pity to represent both the post & actor using `Url`-- could I use newtypes to distinguish
// between the two? I have a task to look at this more generally on the backend.
fn use_replying(post_id: Url, actor_id: Url) -> Action<String, Result<()>> {
    // This seems awfully complex, but the closure we pass to the `Action` constructor must
    // implement `Fn`; i.e. all of its captures must either be `Copy` or moved into the closure.

    // That wouldn't be so bad, except this closure needs *another* closure (that it will pass to
    // `send_with_retry()`), and *that* one must also be `Fn`!
    Action::new_local(move |text: &String| {
        // Now, both `post_id` and `actor_id`, being referenced inside this block, have bee *moved*
        // here. I would have thought that we could just move them again into the next closure, but
        // the if I do that, without keeping copies here, the borrow checker complains. So:
        let post_id = post_id.clone();
        let actor_id = actor_id.clone();
        let text = text.clone();
        async move {
            send_with_retry(
                move || {
                    let api = expect_context::<Api>().0;
                    debug!("POSTing to {api}/api/v1/users/reply");
                    Request::post(&format!("{api}/api/v1/users/reply"))
                },
                ReplyRequest {
                    // taking care to clone them before using (so that this block can
                    // be invoked again, if needed).
                    id: post_id.clone(),
                    actor: actor_id.clone(),
                    text: text.clone(),
                },
            )
            .await
            .context(Request1Snafu)
            .and_then(error_for_status)
            .map(|_| ())
        }
    })
}

#[component]
fn ReplyComposer(
    // I tried making this borrows, but the compiler insisted that "this function's return type
    // contains a borrowed value, but there is no value for it to be borrowed from"... which makes
    // no sense to me. It has nothing to do with the `Action` returned from `use_replying()`-- the
    // error was induced *just by making `post_id` (or `actor_id`) be passed by reference*, even if
    // I never called `use_replying()`-- there must be something in the view! macro doing this.
    post_id: Url,
    actor_id: Url,
    reply_element: NodeRef<html::Textarea>,
    set_replying: WriteSignal<bool>,
    #[prop(optional_no_strip)] rerender: Option<ArcTrigger>,
) -> impl IntoView {
    let send_reply = use_replying(post_id, actor_id);

    let toaster = ToasterInjection::expect_context();

    Effect::new(move |_| {
        if let Some(element) = reply_element.get() {
            let _ = element.focus();
        }
    });

    Effect::new(move |_| {
        // We're moving `rerender` into this closure, which is fine-- it's not used anywhere else.
        // However, because it's an `Option<ArcTrigger>`, I'm going to invoke it below via
        // `Option::map()` which consumes the argument. So. We move `rerender` into the closure,
        // then consume a *clone* on every invocation.
        let rerender = rerender.clone();
        match send_reply.value().get() {
            Some(Err(err)) => pop_toast(
                toaster,
                ToastIntent::Error,
                "Reply".into_owned(),
                format!("{err}"),
            ),
            Some(Ok(_)) => {
                // use_context::<ArcTrigger>().map(|trigger| trigger.notify());
                rerender.map(|trigger| trigger.notify());
                // Important! Take care not to set this signal until we're all done, here-- this
                // will actually tear-down this scope (killing this `Effect`, among other things)
                set_replying.set(false);
            }
            None => (),
        }
    });

    view! {
        <form
            aria-label="Reply to this post"
            class="federated-post__reply"
            on:submit=move |event| {
                event.prevent_default();
                let text = reply_element.get().map(|element| element.value()).unwrap_or_default();
                send_reply.dispatch(text);
            }
        >
            <label class="federated-post__reply-field">
                <span class="sr-only">"Your reply"</span>
                <textarea
                    autofocus=true
                    class="federated-post__reply-input"
                    node_ref=reply_element
                    placeholder="Your reply..."
                    rows="4"
                ></textarea>
            </label>
            <div class="federated-post__reply-actions">
                <button
                    aria-busy=move || send_reply.pending().get().to_string()
                    class="federated-post__reply-submit"
                    disabled=move || send_reply.pending().get()
                    type="submit"
                >
                    <span aria-hidden="true" class="federated-post__action-icon">
                        <Icon icon=icondata::BsSend />
                    </span>
                    {move || if send_reply.pending().get() { "sending…" } else { "send reply" }}
                </button>
                <button
                    class="federated-post__reply-cancel"
                    disabled=move || send_reply.pending().get()
                    on:click=move |_| set_replying.set(false)
                    type="button"
                >
                    "cancel"
                </button>
            </div>
        </form>
    }
}

fn use_favorite(post_id: Url, actor_id: Url) -> Action<(), Result<()>> {
    Action::new_local(move |_: &()| {
        // Both `post_id` and `actor_id`, being referenced inside this block, have bee *moved*
        // here. I would have thought that we could just move them again into the next closure, but
        // the if I do that, without keeping copies here, the borrow checker complains. So:
        let post_id = post_id.clone();
        let actor_id = actor_id.clone();
        async move {
            send_with_retry(
                move || {
                    let api = expect_context::<Api>().0;
                    Request::post(&format!("{api}/api/v1/users/like"))
                },
                LikeRequest {
                    id: post_id.clone(),
                    actor: actor_id.clone(),
                },
            )
            .await
            .context(Request1Snafu)
            .and_then(error_for_status)
            .map(|_| ())
        }
    })
}

#[component]
fn PostActions(
    post_id: Url,
    actor_id: Url,
    actor_label: String,
    conversation_button: NodeRef<html::Button>,
    on_conversation: Option<Callback<()>>,
    reply_button: NodeRef<html::Button>,
    set_replying: WriteSignal<bool>,
) -> impl IntoView {
    let send_favorite = use_favorite(post_id, actor_id);

    let toaster = ToasterInjection::expect_context();

    Effect::new(move |_| {
        if let Some(Err(err)) = send_favorite.value().get() {
            pop_toast(
                toaster,
                ToastIntent::Error,
                "Favorite".into_owned(),
                format!("{err}"),
            )
        }
    });

    let action_label = format!("Actions for {actor_label}");

    view! {
        <div aria-label=action_label class="federated-post__actions" role="group">
            <button
                aria-busy=move || send_favorite.pending().get().to_string()
                aria-label="Favorite"
                class="federated-post__action"
                disabled=move || send_favorite.pending().get()
                on:click=move |_| {
                    send_favorite.dispatch(());
                }
                type="button"
            >
                <span aria-hidden="true" class="federated-post__action-icon">
                    <Icon icon=icondata::AiStarOutlined />
                </span>
                <span class="federated-post__action-label">"favorite"</span>
            </button>
            <button
                aria-label="Reply"
                class="federated-post__action"
                node_ref=reply_button
                on:click=move |_| set_replying.set(true)
                type="button"
            >
                <span aria-hidden="true" class="federated-post__action-icon">
                    <Icon icon=icondata::BsReply />
                </span>
                <span class="federated-post__action-label">"reply"</span>
            </button>
            {match on_conversation {
                Some(on_conversation) => Either::Left(view! {
                    <button
                        aria-label="View conversation"
                        class="federated-post__action"
                        node_ref=conversation_button
                        on:click=move |_| on_conversation.run(())
                        type="button"
                    >
                        <span aria-hidden="true" class="federated-post__action-icon">
                            <Icon icon=icondata::FiMessageCircle />
                        </span>
                        <span class="federated-post__action-label">"conversation"</span>
                    </button>
                }),
                None => Either::Right(view! {
                    <span class="federated-post__current-label">"current post"</span>
                }),
            }}
        </div>
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
enum PostContext {
    #[default]
    Feed,
    Parent,
    Current,
    Child,
}

impl PostContext {
    const fn article_class(self) -> &'static str {
        match self {
            Self::Feed => "federated-post",
            Self::Parent => "federated-post federated-post--parent",
            Self::Current => "federated-post federated-post--current",
            Self::Child => "federated-post federated-post--child",
        }
    }

    const fn in_conversation(self) -> bool {
        !matches!(self, Self::Feed)
    }
}

#[derive(Clone, Debug)]
struct ActorLabel {
    short: String,
    host: String,
    url: String,
}

fn actor_label(actor: &Url) -> ActorLabel {
    let url = actor.to_string();
    let host = actor
        .host_str()
        .map(str::to_owned)
        .unwrap_or_else(|| actor.scheme().to_owned());
    let identifier = actor
        .path_segments()
        .and_then(|segments| segments.filter(|segment| !segment.is_empty()).next_back())
        .filter(|segment| !segment.is_empty())
        .unwrap_or(&host);
    let short = if identifier.starts_with('@') {
        identifier.to_owned()
    } else {
        format!("@{identifier}")
    };

    ActorLabel { short, host, url }
}

#[component]
fn ActorIdentity(actor: Url, nested: bool) -> impl IntoView {
    let actor = actor_label(&actor);
    let heading = if nested {
        Either::Left(view! {
            <h4 class="federated-post__actor-heading">
                <a class="federated-post__actor-link" href=actor.url.clone() title=actor.url.clone()>
                    <span class="federated-post__actor-name">{actor.short.clone()}</span>
                    <span class="federated-post__actor-host">{actor.host.clone()}</span>
                </a>
            </h4>
        })
    } else {
        Either::Right(view! {
            <h3 class="federated-post__actor-heading">
                <a class="federated-post__actor-link" href=actor.url.clone() title=actor.url.clone()>
                    <span class="federated-post__actor-name">{actor.short.clone()}</span>
                    <span class="federated-post__actor-host">{actor.host.clone()}</span>
                </a>
            </h3>
        })
    };

    view! {
        <div class="federated-post__actor">
            <span aria-hidden="true" class="federated-post__avatar">
                <Icon icon=icondata::FiUser />
            </span>
            {heading}
        </div>
    }
}

#[component]
fn PostHeader(post: FeedPost, context: PostContext) -> impl IntoView {
    let datetime = post.published.to_rfc3339();
    let published = post.published.format("%Y-%m-%d %H:%M UTC").to_string();

    view! {
        <header class="federated-post__header">
            <ActorIdentity actor=post.actor nested=context.in_conversation() />
            <a class="federated-post__permalink" href=post.id.to_string()>
                <time datetime=datetime>{published}</time>
            </a>
        </header>
        {post.in_reply_to.is_some().then(|| view! {
            <div class="federated-post__reply-context">"reply"</div>
        })}
    }
}

#[component]
fn PostContent(content: String) -> impl IntoView {
    view! { <div class="federated-post__content" inner_html=content></div> }
}

#[component]
fn FederatedPost(
    post: FeedPost,
    #[prop(default = PostContext::Feed)] context: PostContext,
    conversation_button: NodeRef<html::Button>,
    #[prop(optional)] on_conversation: Option<Callback<()>>,
    #[prop(optional_no_strip)] rerender: Option<ArcTrigger>,
) -> impl IntoView {
    let (replying, set_replying) = signal(false);
    let reply_button = NodeRef::<html::Button>::new();
    let reply_element = NodeRef::<html::Textarea>::new();
    let post_id = post.id.clone();
    let post_actor = post.actor.clone();
    let actor_label = actor_label(&post.actor).short;
    let content = post.content.clone();

    Effect::new(move |was_replying: Option<bool>| {
        let is_replying = replying.get();
        if was_replying == Some(true)
            && !is_replying
            && let Some(button) = reply_button.get()
        {
            let _ = button.focus();
        }
        is_replying
    });

    view! {
        <article class=context.article_class()>
            <PostHeader post=post.clone() context />
            <div class="federated-post__main">
                <PostContent content />
                {move || {
                    if replying.get() {
                        Either::Left(view! {
                            <ReplyComposer
                                post_id=post_id.clone()
                                actor_id=post_actor.clone()
                                reply_element
                                set_replying
                                rerender=rerender.clone()
                            />
                        })
                    } else {
                        Either::Right(view! {
                            <PostActions
                                post_id=post_id.clone()
                                actor_id=post_actor.clone()
                                actor_label=actor_label.clone()
                                conversation_button
                                on_conversation
                                reply_button
                                set_replying
                            />
                        })
                    }
                }}
            </div>
        </article>
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                          Conversation                                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Creates a reactive stack of `ThreadContextResponse` values.
///
/// This is a Leptos hook in the SolidJS sense: a plain function that allocates
/// reactive state and returns it to the caller.  The component that calls it
/// owns the stack for its lifetime; nothing outside the component can observe
/// or mutate it.
///
/// The stack is a `Vec` used in the traditional sense — push to navigate
/// forward, pop to go back.  The *current* conversation node is always the
/// last element.  We start with an empty vec; the component pushes the initial
/// response once the Action resolves.
fn use_conversation_stack() -> RwSignal<Vec<ThreadContextResponse>> {
    RwSignal::new(vec![])
}

async fn get_context(url: Url) -> Result<ThreadContextResponse> {
    send_with_retry(
        move || {
            let api = expect_context::<Api>().0;
            Request::post(&format!("{api}/api/v1/users/context"))
        },
        ThreadContextRequest { ap_id: url },
    )
    .await
    .context(Request1Snafu)
    .and_then(error_for_status)?
    .json::<ThreadContextResponse>()
    .await
    .context(LoadSnafu)
}

/// Renders a conversation thread centred on the post at the top of the stack.
///
/// # Props
///
/// - `show` — the parent's `WriteSignal<bool>`.  Set to `false` when the user
///   presses the back arrow while on the root node, returning to the button.
/// - `initial_url` — URL of the first post to display. Dispatched to the
///   context-loading action when the component first mounts.
#[component]
fn Conversation(
    show: WriteSignal<bool>,
    initial_url: Url,
    #[prop(optional_no_strip)] rerender: Option<ArcTrigger>,
) -> impl IntoView {
    let action: Action<Url, Result<ThreadContextResponse>> = Action::new_local(move |url: &Url| {
        let url = url.clone();
        async move { get_context(url.clone()).await }
    });
    let stack = use_conversation_stack();

    // Kick off the initial data load.  The Leptos component function runs only
    // once per instance, so this dispatch happens exactly once.
    action.dispatch(initial_url);

    let toaster = ToasterInjection::expect_context();

    // Whenever the Action resolves — from the initial load or any subsequent
    // navigation dispatch — push the new response onto the stack.
    Effect::new(move |_| {
        // So this seems a bit dodgy to me... what if we have multiple invocations of `action` in
        // flight simultaneously?
        if let Some(response) = action.value().get() {
            match response {
                Ok(ctx) => {
                    debug!("Got a new context of {ctx:?}");
                    stack.update(|s| s.push(ctx));
                }
                Err(err) => {
                    error!("Failed to retrieve thread context: {err:#?}");
                    pop_toast(
                        toaster,
                        ToastIntent::Error,
                        "Conversation".into_owned(),
                        format!("{err}"),
                    )
                }
            }
        }
    });

    // Back-arrow handler.
    // When the stack has exactly one entry (the root), popping it would leave
    // an empty stack with nothing to render, so we dismiss the component
    // instead by flipping the parent's boolean signal back to false.
    let on_back = move |_| {
        // `get_untracked` avoids creating a reactive dependency inside an
        // event handler where we don't want re-subscription side effects.
        if stack.get_untracked().len() <= 1 {
            stack.update(|s| {
                s.pop();
            });
            show.set(false);
        } else {
            stack.update(|s| {
                s.pop();
            });
        }
    };

    let back_button = NodeRef::<html::Button>::new();
    Effect::new(move |_| {
        if let Some(button) = back_button.get() {
            let _ = button.focus();
        }
    });

    view! {
        <section
            aria-busy=move || action.pending().get().to_string()
            aria-label="Conversation"
            class="conversation"
        >
            <header class="conversation__header">
                <button
                    class="conversation__back"
                    node_ref=back_button
                    on:click=on_back
                    type="button"
                >
                    <span aria-hidden="true" class="conversation__back-icon">
                        <Icon icon=icondata::BiArrowBackRegular />
                    </span>
                    "back"
                </button>
                <h3 class="conversation__heading">"conversation"</h3>
            </header>

            {move || {
                match stack.get().last().cloned() {
                    None if action.pending().get() => view! {
                        <LoadingState label="Loading conversation…" />
                    }.into_any(),
                    None => view! {
                        <EmptyState
                            title="Conversation unavailable"
                            message="Return to the timeline and try opening it again."
                        />
                    }.into_any(),
                    Some(ctx) => {
                        let parent = ctx.parent.map(|post| {
                            let url = post.id.clone();
                            let on_conversation = Callback::new(move |()| {
                                action.dispatch(url.clone());
                            });
                            view! {
                                <li class="conversation__item conversation__item--parent">
                                    <div class="conversation__relation">"parent"</div>
                                    <FederatedPost
                                        post
                                        context=PostContext::Parent
                                        conversation_button=NodeRef::new()
                                        on_conversation
                                        rerender=rerender.clone()
                                    />
                                </li>
                            }
                        });
                        let children = ctx.children.into_iter().map(|post| {
                            let url = post.id.clone();
                            let on_conversation = Callback::new(move |()| {
                                action.dispatch(url.clone());
                            });
                            view! {
                                <li class="conversation__item conversation__item--child">
                                    <div class="conversation__relation">"reply"</div>
                                    <FederatedPost
                                        post
                                        context=PostContext::Child
                                        conversation_button=NodeRef::new()
                                        on_conversation
                                        rerender=rerender.clone()
                                    />
                                </li>
                            }
                        }).collect_view();

                        view! {
                            <ol class="conversation__thread" role="list">
                                {parent}
                                <li class="conversation__item conversation__item--current">
                                    <FederatedPost
                                        post=ctx.post
                                        context=PostContext::Current
                                        conversation_button=NodeRef::new()
                                        rerender=rerender.clone()
                                    />
                                </li>
                                {children}
                            </ol>
                        }.into_any()
                    },
                }
            }}
        </section>
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                              Post                                              //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
pub fn Post(
    post: FeedPost,
    #[prop(optional_no_strip)] rerender: Option<ArcTrigger>,
) -> impl IntoView {
    let (show_conversation, set_show_conversation) = signal(false);
    let conversation_button = NodeRef::<html::Button>::new();
    let on_conversation = Callback::new(move |()| set_show_conversation.set(true));

    Effect::new(move |was_showing: Option<bool>| {
        let showing = show_conversation.get();
        if was_showing == Some(true)
            && !showing
            && let Some(button) = conversation_button.get()
        {
            let _ = button.focus();
        }
        showing
    });

    view! {
        <Show
            when=move || show_conversation.get()
            fallback={
                let post = post.clone();
                let rerender = rerender.clone();
                move || view! {
                    <FederatedPost
                        post=post.clone()
                        conversation_button
                        on_conversation
                        rerender=rerender.clone()
                    />
                }
            }>
            <Conversation
                show=set_show_conversation
                initial_url=post.clone().id
                rerender=rerender.clone()
            />
        </Show>
    }
}
