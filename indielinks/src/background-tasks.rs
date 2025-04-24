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

//! # Background Task Processing
//!
//! Interestingly, [axum] makes no provision for compute outside the context of handling HTTP
//! requests, and there doesn't appear to be a "go to" Rust implementation of a task queue. I'm
//! perhaps succumbing to my tendency to "just code it up", but this module provides an async
//! background persistent task queue for [indielinks].
//!
//! [indielinks]: crate
//!
//! # Design
//!
//! The goal is to allow [indielinks] request handlers to spawn background tasks off the "hot path"
//! of serving that request. For instance, when accepting an ActivityPub [Follow] request, the
//! receiver must send an [Accept] back to the would-be follower as a second HTTP request/response
//! pair; rather than engage in that HTTP request/response exchange in the [Follow] request handler,
//! adding a second round-trip to the handler's latency and introducing numerous additional points
//! of failure, it would make more sense to just accept the [Follow], return the HTTP response to
//! the would-be follower, and arrange for some kind of background worker to send the [Accept]
//! "nearline"; as soon as possible, best-effort. Not only would this make for a simpler request
//! handler, but it also opens up the possibility of retries (with backoff) without making for an
//! overly-complex and slow [Follow] handler: the background worker could retry for days
//! potentially, making it reslient to network partitions or outages on the part of the follower.
//!
//! [Follow]: https://www.w3.org/TR/activitystreams-vocabulary/#dfn-follow
//! [Accept]: https://www.w3.org/TR/activitystreams-vocabulary/#dfn-accept
//!
//! One could of course just use [tokio::spawn], but I'd rather not accept the lack of durability
//! entailed in that solution: if the program crashes or is halted after [tokio::spawn] returns, but
//! before the task completes, the task will be "lost" ("There is no guarantee that a spawned task
//! will execute to completion. When a runtime is shutdown, all outstanding tasks are dropped,
//! regardless of the lifecycle of that task."). At the other end of the spectrum, I could add a
//! full-blown [Raft] implementation to indielinks, perhaps allowing for (unstarted) tasks to "move"
//! around a cluster and blocking shutdown of any node with in-progress tasks until they either
//! complete or time-out, but I'm so-far resisting the temptation 😂
//!
//! Instead, I'm landing in between by (ab)using the data store to hold serialized tasks while a
//! single background task (in each process) picks them up from that queue & executes them. This
//! preserves indlielinks' ability to run in a cluster by pushing the synchronization down to the
//! datastore. Handlers> can "send" tasks to the system; once that operation returns successfully,
//! they can proceed with the guarantee that the task has been persisted and hence won't be "lost"
//! should indielinks shut-down before it's executed. The task will use DynamoDB & Scylla primitives
//! to atomically update persisted tasks with a "lease" indicating that no one else should pick-up
//! that task for a specified amount of time. In the worst case scenario, should an indielinks
//! instance pick-up a task & fail before completion, eventually other instances will see the lease
//! as expired, pick-up the task & retry.
//!
//! [Raft]: https://en.wikipedia.org/wiki/Raft_(algorithm)
//!
//! It's a rough & ready solution, and I'm sure a good distributed systems dev could find edge cases
//! that aren't handled well (or at all), but at the time of this writing I believe it to be the sweet
//! spot between "just spawn a task" and "write a Raft implementation": 85% of the benefits & 25% of
//! the work of the ideal solution.
//!
//! Furthermore, it's turned into an interesting little design problem: I'm increasingly impressed
//! with how Rust's type system allows me to express _just_ what I want, and no more.

use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc, task::Poll, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use pin_project::pin_project;
use rmp_serde::to_vec;
use scylla::DeserializeRow;
use serde::{Deserialize, Serialize};
use snafu::{prelude::*, Backtrace, IntoError};
use tokio::{
    sync::Notify,
    task::{Id, JoinError, JoinHandle, JoinSet},
};
use uuid::Uuid;

use crate::{
    counter_add, gauge_setu,
    metrics::{self, Instruments, Sort},
    origin::Origin,
    storage::Backend as StorageBackend,
};

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                       module error type                                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Snafu)]
pub enum Error {
    // Generic error variant trait implementations can use
    #[snafu(display("{source}"))]
    Background {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to mark a ask complete: {source}"))]
    Completion {
        #[snafu(source(from(Error, Box::new)))]
        source: Box<Error>,
    },
    #[snafu(display("Failed to deserialize a task: {source}"))]
    De {
        source: rmp_serde::decode::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("{uuid} is not a recognized task"))]
    Id { uuid: Uuid },
    #[snafu(display("Task processing failed to run to completion: {source}"))]
    Join {
        source: tokio::task::JoinError,
        backtrace: Backtrace,
    },
    #[snafu(display("Timemout shutting-down the task processor: {source}"))]
    ShutdownTimeout {
        source: tokio::time::error::Elapsed,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to pick-up a new task: {source}"))]
    Take {
        #[snafu(source(from(Error, Box::new)))]
        source: Box<Error>,
    },
    #[snafu(display("Tried to remove an unknown TaskId"))]
    TaskId { backtrace: Backtrace },
    #[snafu(display("Failed to serialize a task to messagepack: {source}"))]
    TaskSer {
        source: rmp_serde::encode::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to wait for in-flight tasks: {source}"))]
    Timeout { source: tokio::time::error::Elapsed },
}

impl Error {
    pub fn new(err: impl std::error::Error + Send + Sync + 'static) -> Error {
        Error::Background {
            source: Box::new(err),
            backtrace: Backtrace::capture(),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                             tasks                                              //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// Trait defining a "task" for our purposes.
///
/// This is intentionally as general as possible: this system can handle any task that is [Send],
/// and that convert itself into an async function yielding a `Result<()>`. Persuant to the last
/// point, note especially that the `exec()` method consumes the task!
// This trait *must* be object-safe in order to allow `process()` (below) to handle tasks in a
// generic way.
#[async_trait]
// The generic type parameter has to be at the trait level; putting it in `exec()` would make
// the trait non-object-safe.
pub trait Task<C>: Send {
    /// Consume this task by converting it into a `Future` yielding a `Result<()>`.
    // It's a small pity, I suppose, that I've made the receiver a `Box<Self>`, but there's really
    // not much choice given that this will be invoked by `process()` (below) which *must* deal with
    // tasks in a generic way.
    async fn exec(self: Box<Self>, context: C) -> Result<()>;
    fn timeout(&self) -> Option<Duration>;
}

/// A [Task] that can return a per-type "tag"; this is useful for deserialization.
pub trait TaggedTask<C>: Task<C> {
    type Tag;
    fn get_tag() -> Self::Tag;
}

/// Trait defining the ability to collect, or "send" [Task]s.
///
/// [Task]: crate::background_tasks::Task
///
/// This trait is generic over the [Task] type (rather than making the `send()` method generic) so
/// that implementors can express additional constraints on the types of [Task]s they can send.
#[async_trait]
pub trait Sender<C, T: Task<C>> {
    async fn send(&self, task: T) -> Result<()>;
}

/// Trait defining the ability to harvest, or "receive" [Task]s generically.
///
/// A [Receiver] needs to be able to move [Task] trait objects out of the collection or backend,
/// along with a "cookie" or "handle" identifying that task, and then, at a later time, mark them as
/// complete.
#[async_trait]
pub trait Receiver<C> {
    type TaskId: Send + 'static;
    async fn mark_complete(&self, cookie: Self::TaskId) -> Result<()>;
    async fn take_task(&self) -> Result<Option<(Box<dyn Task<C>>, Self::TaskId)>>;
}

/// Blanket implementation for [Arc]s; if `T` is a [Receiver], then so is `Arc<T>`.
#[async_trait]
impl<C, T: Receiver<C> + Send + Sync> Receiver<C> for Arc<T> {
    type TaskId = T::TaskId;
    async fn mark_complete(&self, cookie: Self::TaskId) -> Result<()> {
        self.as_ref().mark_complete(cookie).await
    }
    async fn take_task(&self) -> Result<Option<(Box<dyn Task<C>>, Self::TaskId)>> {
        self.as_ref().take_task().await
    }
}

/// [Processor] is the type managing the ongoing processing of background tasks. It has a single
/// method, `shutdown()` which will consume the instance & resolve to the result of the processing
/// process (`Result<()>`).
// `Processor` need not be cheaply clonable; will likely be held in one place & then dropped to
// signal that it should shut down.
#[pin_project]
pub struct Processor {
    // This               👇 must match the return type of `process()`
    #[pin]
    processor: JoinHandle<Result<()>>,
    shutdown: Arc<Notify>,
}

impl Future for Processor {
    type Output = std::result::Result<Result<()>, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.processor.poll(cx)
    }
}

impl Processor {
    /// Consume the instance & return the result of processing background tasks
    ///
    /// This method will signal the processing task to shutdown, and wait for time `timeout` for the
    /// task to exit.
    pub async fn shutdown(self, timeout: Duration) -> Result<()> {
        self.shutdown.notify_one();
        tokio::time::timeout(timeout, self.processor)
            .await
            .context(ShutdownTimeoutSnafu)?
            .context(JoinSnafu)?
    }
    /// Split the instance back into it's parts
    ///
    /// This is convenient when waiting on the processor along with other futures (in a
    /// `tokio::select!` invocation, e.g.)
    pub fn into_parts(self) -> (JoinHandle<Result<()>>, Arc<Notify>) {
        (self.processor, self.shutdown)
    }
}

/// Configuration parameters for processing background tasks
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// Timeout that will be used for any task that doesn't define its own
    #[serde(rename = "default-timeout")]
    pub default_timeout: Duration,
    /// The maximum number of tasks to drive concurrently
    #[serde(rename = "max-concurrent-tasks")]
    pub max_concurrent_tasks: usize,
    /// Amount of time to sleep when we have no tasks in process
    #[serde(rename = "sleep-duration")]
    pub sleep_duration: Duration,
    /// Amount of time to wait for in-flight tasks on shutdown
    #[serde(rename = "shutdown-timeout")]
    pub shutdown_timeout: Duration,
    /// Maximum amount of time to drive in-flight tasks without attempting to pick-up new tasks
    #[serde(rename = "pickup-timeout")]
    pub pickup_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            default_timeout: Duration::from_secs(5),
            max_concurrent_tasks: 16,
            sleep_duration: Duration::from_secs(1),
            shutdown_timeout: Duration::from_millis(500),
            pickup_timeout: Duration::from_millis(1000),
        }
    }
}

inventory::submit! { metrics::Registration::new("background.processor.tasks.completed", Sort::IntegralCounter) }

inventory::submit! { metrics::Registration::new("background.processor.tasks.inflight", Sort::IntegralGauge) }

/// Process background tasks. `receiver` is a [Receiver] from which we can draw tasks. `config`
/// holds configuration parameters for the algorithm. `shutdown` is a [Notify] instance the caller
/// can use to signal this function to exit.
async fn process<C: Clone + 'static, R: Receiver<C>>(
    receiver: R,
    context: C,
    config: Config,
    shutdown: Arc<Notify>,
    instruments: Arc<Instruments>,
) -> Result<()> {
    // The basic outline of this logic is to maintain a `JoinSet` of currently running tasks,
    let mut tasks: HashMap<Id, R::TaskId> = HashMap::new();
    // with that, we can setup our `JoinSet`:
    let mut futures = JoinSet::new();
    // The overall structure here is an infinite loop; so long as...
    let mut done = false;
    // `done` is not true, loop:
    while !done {
        // so long as we don't have too much on our plate, try 'n grab another task:
        if futures.len() < config.max_concurrent_tasks {
            if let Some((task, cookie)) = receiver.take_task().await.context(TakeSnafu)? {
                let id = futures
                    .spawn(tokio::time::timeout(
                        task.timeout().unwrap_or(config.default_timeout),
                        task.exec(context.clone()),
                    ))
                    .id();
                tasks.insert(id, cookie);
            }
        }

        gauge_setu!(
            instruments,
            "background.processor.tasks.inflight",
            futures.len() as u64,
            &[]
        );

        if !futures.is_empty() {
            // We've got at least one task; drive 'em all forward, while waiting on our shutdown
            // notification:
            tokio::select! {
                result = futures.join_next_with_id() => {
                    match result {
                        Some(Ok((id, _))) => {
                            // The task has completed succesfully (and been consumed in the
                            // process); now all that remains is to mark it complete.
                            let cookie = tasks.remove(&id).context(TaskIdSnafu)?;
                            receiver.mark_complete(cookie).await.context(CompletionSnafu)?;
                            counter_add!(instruments, "background.processor.tasks.completed", 1, &[]);
                        },
                        Some(Err(err)) => {
                            return Err(JoinSnafu.into_error(err));
                        },
                        None => unimplemented!(), // Precluded by `.is_empty()`, above.
                    }
                },
                // If `futures` has a single task, and that task is long-running, we can get "stuck"
                // in this `select!` statement, driving that task forward, while other tasks pile-up
                // in the queue. By stopping periodically, we can pick-up new tasks.
                _ = tokio::time::sleep(config.pickup_timeout) => (),
                _ = shutdown.notified()=> {
                    done = true;
                }
            }
        } else {
            // We have no tasks; hang out a bit before attempting to pick-up a task, while remaining
            // mindful of our shutdown notification:
            tokio::select! {
                _ = tokio::time::sleep(config.sleep_duration) => (), // Loop around & try again
                _ = shutdown.notified() => {
                    // Abandon `task` and exit!
                    done = true;
                }
            }
        }
    } // End processing loop.

    // Give any in-flight tasks a chance to complete:
    tokio::time::timeout(config.shutdown_timeout, futures.join_all())
        .await
        .context(TimeoutSnafu)?;

    Ok(())
}

/// Create a new [Processor] given a [Receiver].
pub fn new<C: Clone + Send + 'static, R: Receiver<C> + Send + 'static>(
    receiver: R,
    context: C,
    config: Option<Config>,
    instruments: Arc<Instruments>,
) -> std::result::Result<Processor, Error> {
    let shutdown = Arc::new(Notify::new());
    let processor = tokio::spawn(process(
        receiver,
        context,
        config.unwrap_or_default(),
        shutdown.clone(),
        instruments,
    ));
    Ok(Processor {
        processor,
        shutdown,
    })
}

// Let's pressure-test this by mocking-up some implementations of the traits defined so far &
// driving `process()`:
#[cfg(test)]
mod mock {

    use std::{collections::HashSet, ops::DerefMut, sync::Mutex};

    use super::*;

    #[derive(Clone, Debug, Serialize)]
    struct SleepTask {
        pub duration: Duration,
    }

    #[async_trait]
    impl Task<()> for SleepTask {
        async fn exec(self: Box<Self>, _: ()) -> Result<()> {
            Ok(tokio::time::sleep(self.duration).await)
        }
        fn timeout(&self) -> Option<Duration> {
            Some(Duration::from_secs(10))
        }
    }

    struct InMemory {
        pub tasks: Mutex<HashMap<Uuid, Box<dyn Task<()>>>>,
        pub checkouts: Mutex<HashSet<Uuid>>,
    }

    #[async_trait]
    impl Receiver<()> for InMemory {
        type TaskId = Uuid;
        async fn mark_complete(&self, cookie: Self::TaskId) -> Result<()> {
            self.checkouts.lock().unwrap().remove(&cookie);
            // Should check for error, here-- what if `cookie` hasn't been checked-out?
            Ok(())
        }
        async fn take_task(&self) -> Result<Option<(Box<dyn Task<()>>, Self::TaskId)>> {
            let mut m = self.tasks.lock().unwrap();
            let key = { m.keys().next().cloned() };
            match key {
                Some(key) => {
                    let task = m.deref_mut().remove(&key).unwrap();
                    self.checkouts.lock().unwrap().insert(key.clone());
                    Ok(Some((task, key)))
                }
                None => Ok(None),
            }
        }
    }

    // Exercise the bare bones of the system
    #[tokio::test]
    async fn bare_bones() {
        let mut m: HashMap<<InMemory as Receiver<()>>::TaskId, Box<dyn Task<()>>> = HashMap::new();
        m.insert(
            Uuid::new_v4(),
            Box::new(SleepTask {
                duration: Duration::from_millis(250),
            }),
        );
        let backend = InMemory {
            tasks: Mutex::new(m),
            checkouts: Mutex::new(HashSet::new()),
        };
        let shutdown = Arc::new(Notify::new());

        // Process will run forever, so spawn it...
        let handle = tokio::task::spawn(process(
            backend,
            (),
            Config::default(),
            shutdown.clone(),
            Arc::new(Instruments::new("indielinks")),
        ));
        // give it ample time to run...
        tokio::time::sleep(Duration::from_secs(1)).await;
        // signal it to shutdown...
        shutdown.notify_one();
        let _ = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[async_trait]
    impl<T: Task<()> + 'static> Sender<(), T> for InMemory {
        async fn send(&self, task: T) -> Result<()> {
            self.tasks
                .lock()
                .unwrap()
                .insert(Uuid::new_v4(), Box::new(task));
            Ok(())
        }
    }

    // Exercise Sender & Receiver
    #[tokio::test]
    async fn send_and_receive() {
        let sender = Arc::new(InMemory {
            tasks: Mutex::new(HashMap::new()),
            checkouts: Mutex::new(HashSet::new()),
        });
        let receiver = sender.clone();
        let processor = new(
            receiver,
            (),
            Some(Config {
                // Be careful to choose this slightly longer than the longest task, below, in case
                // that task has just gotten started when the shutdown signal arrives.
                shutdown_timeout: Duration::from_millis(800),
                ..Default::default()
            }),
            Arc::new(Instruments::new("indielinks")),
        )
        .unwrap();

        sender
            .send(SleepTask {
                duration: Duration::from_millis(250),
            })
            .await
            .unwrap();
        sender
            .send(SleepTask {
                duration: Duration::from_millis(500),
            })
            .await
            .unwrap();
        sender
            .send(SleepTask {
                duration: Duration::from_millis(350),
            })
            .await
            .unwrap();
        sender
            .send(SleepTask {
                duration: Duration::from_millis(750),
            })
            .await
            .unwrap();

        let result = processor.shutdown(Duration::from_secs(5)).await;
        eprintln!("send_and_receive result: {:#?}", result);

        assert!(result.is_ok());
    }
}

#[derive(Clone)]
pub struct Context {
    pub origin: Origin,
    pub client: reqwest_middleware::ClientWithMiddleware,
    pub storage: Arc<dyn StorageBackend + Send + Sync>,
}

/// In order to "register" a background task type, you need to assign a tag (just using a [Uuid] for
/// now), and a function that "knows" how to deserialize a [MessagePack] serialization of a task of
/// this type.
///
/// [MessagePack]: https://msgpack.org/
// I don't want to maintain a centralized registry or enumeration of all the sorts of background
// tasks indielinks might want to carry out, so I'll reach for Tolnay's [inventory] crate once
// again.
pub struct BackgroundTask {
    pub id: Uuid,
    // clippy complains about this type definition & suggests factoring-out type definitions; I
    // might do that, once I'm clear that this is what I want.
    #[allow(clippy::type_complexity)]
    pub de: fn(&[u8]) -> Result<Box<dyn Task<Context>>>,
}

inventory::collect!(BackgroundTask);

/// Object-safe trait abstracting over ScyllaDB versus DynamoDB/Alternator for operations required
/// by [BackgroundTasks]
// Let's define an object-safe trait defining a "backend" for `BackgroundTasks` (below). Must be
// object safe.
#[async_trait]
pub trait Backend {
    async fn write_task(&self, tag: &Uuid, buf: &[u8]) -> Result<()>;
    // type tag, task id, messagepack-- should probably intro a newtype
    async fn lease_task(&self) -> Result<Option<(Uuid, Uuid, Vec<u8>)>>;
    async fn close_task(&self, uuid: &Uuid) -> Result<()>;
}

/// The [indielinks] background task processor.
///
/// [indielinks]: crate
pub struct BackgroundTasks {
    storage: Arc<dyn Backend + Send + Sync>,
}

impl BackgroundTasks {
    pub fn new(storage: Arc<dyn Backend + Send + Sync>) -> BackgroundTasks {
        BackgroundTasks { storage }
    }
}

#[async_trait]
impl<T> Sender<Context, T> for BackgroundTasks
where
    T: TaggedTask<Context, Tag = Uuid> + Serialize + 'static,
{
    /// Task can be serialized; serialize to MessagePack, then write to a dedicated table
    async fn send(&self, task: T) -> Result<()> {
        let tag = T::get_tag();
        let buf = to_vec(&task).context(TaskSerSnafu)?;
        self.storage.write_task(&tag, &buf).await
    }
}

#[async_trait]
impl Receiver<Context> for BackgroundTasks {
    type TaskId = Uuid;
    async fn mark_complete(&self, cookie: Self::TaskId) -> Result<()> {
        self.storage.close_task(&cookie).await
    }
    async fn take_task(&self) -> Result<Option<(Box<dyn Task<Context>>, Self::TaskId)>> {
        match self.storage.lease_task().await {
            Ok(opt) => match opt {
                Some((tag, id, buf)) => {
                    match inventory::iter::<BackgroundTask>().find(|t| t.id == tag) {
                        Some(t) => Ok(Some(((t.de)(&buf).unwrap(), id))),
                        None => IdSnafu { uuid: id }.fail(),
                    }
                }
                None => Ok(None),
            },
            Err(err) => Err(err),
        }
    }
}

// Convenience: a task as represented in the indielinks backends
#[derive(Clone, Debug, Deserialize, DeserializeRow, Eq, PartialEq, Serialize)]
pub struct FlatTask {
    pub id: Uuid,
    pub created: DateTime<Utc>,
    pub task: Vec<u8>,
    pub tag: Uuid,
    pub lease_expires: DateTime<Utc>,
    pub done: bool,
}

#[cfg(test)]
mod test {
    use super::*;

    use serde::Deserialize;

    #[derive(Clone, Debug, Deserialize, Serialize)]
    struct SleepTask {
        pub sleep: Duration,
    }

    #[async_trait]
    impl Task<Context> for SleepTask {
        async fn exec(self: Box<Self>, _context: Context) -> Result<()> {
            tokio::time::sleep(self.sleep).await;
            Ok(())
        }
        fn timeout(&self) -> Option<Duration> {
            None
        }
    }

    const SLEEP_TASK: Uuid = Uuid::from_fields(
        0xaa27434b,
        0x838b,
        0x43f3,
        &[0xb6, 0xca, 0x43, 0xe6, 0x63, 0x0f, 0xf8, 0x6a],
    );

    impl TaggedTask<Context> for SleepTask {
        type Tag = Uuid;
        fn get_tag() -> Self::Tag {
            SLEEP_TASK
        }
    }

    inventory::submit! {
        BackgroundTask {
            id: SLEEP_TASK,
            de: |buf| { Ok(Box::new(rmp_serde::from_slice::<SleepTask>(buf).context(DeSnafu)?)) }
        }
    }

    #[tokio::test]
    async fn simple() {
        assert!(inventory::iter::<BackgroundTask>()
            .find(|t| t.id == SLEEP_TASK)
            .is_some());
    }
}
