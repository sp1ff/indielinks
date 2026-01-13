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
// You should have received a copy of the GNU General Public License along with mpdpopm.  If not,
// see <http://www.gnu.org/licenses/>.

//! Integration tests for background task processing.

use governor::{Quota, RateLimiter};
use indielinks::{
    background_tasks::{
        self, Backend as TasksBackend, BackgroundTask, BackgroundTasks, Config, Context, Sender,
        TaggedTask, Task,
    },
    client::{make_client, HostExtractor},
    storage::Backend as StorageBackend,
};

use indielinks_shared::origin::Origin;

use async_trait::async_trait;
use libtest_mimic::Failed;
use nonzero::nonzero;
use serde::{Deserialize, Serialize};
use tower_gcra::extractors::KeyedDashmapMiddleware;
use tracing::debug;
use uuid::Uuid;

use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

static DATA1: AtomicBool = AtomicBool::new(false);

// Trivial background task
#[derive(Debug, Deserialize, Serialize)]
struct SleepTask1 {
    sleep: Duration,
}

impl SleepTask1 {
    pub fn new(sleep: Duration) -> SleepTask1 {
        SleepTask1 { sleep }
    }
}

#[async_trait]
impl Task<Context> for SleepTask1 {
    async fn exec(
        self: Box<Self>,
        _context: Context,
    ) -> std::result::Result<(), indielinks::background_tasks::Error> {
        debug!("SleepTask1 executing...");
        tokio::time::sleep(self.sleep).await;
        DATA1.store(true, Ordering::SeqCst);
        debug!("SleepTask1 executing...done.");
        Ok(())
    }
    fn timeout(&self) -> Option<Duration> {
        Some(self.sleep * 2)
    }
}

// 4965b08e-f72a-47a7-b9fa-a717bad5272a
const SLEEP_TASK_1: Uuid = Uuid::from_fields(
    0x4965b08e,
    0xf72a,
    0x47a7,
    &[0xb9, 0xfa, 0xa7, 0x17, 0xba, 0xd5, 0x27, 0x2a],
);

impl TaggedTask<Context> for SleepTask1 {
    type Tag = Uuid;
    fn get_tag() -> Self::Tag {
        SLEEP_TASK_1
    }
}

inventory::submit! {
    BackgroundTask {
        id: SLEEP_TASK_1,
        de: |buf| { Ok(Box::new(rmp_serde::from_slice::<SleepTask1>(buf).unwrap())) }
    }
}

static DATA2: AtomicUsize = AtomicUsize::new(0);

// Slightly different task so I can exercise "sending" multiple task types
#[derive(Debug, Deserialize, Serialize)]
struct SleepTask2 {
    pub sleep: Duration,
    pub on_done: usize,
}

impl SleepTask2 {
    pub fn new(sleep: Duration, on_done: usize) -> SleepTask2 {
        SleepTask2 { sleep, on_done }
    }
}

#[async_trait]
impl Task<Context> for SleepTask2 {
    async fn exec(
        self: Box<Self>,
        _context: Context,
    ) -> std::result::Result<(), indielinks::background_tasks::Error> {
        debug!("SleepTask2 executing...");
        tokio::time::sleep(self.sleep).await;
        DATA2.store(self.on_done, Ordering::SeqCst);
        debug!("SleepTask2 executing...done.");
        Ok(())
    }
    fn timeout(&self) -> Option<Duration> {
        Some(self.sleep * 2)
    }
}

// 8ff6555e-ea7c-49e9-b1f8-8962cded0e8e
const SLEEP_TASK_2: Uuid = Uuid::from_fields(
    0x8ff6555e,
    0xea7c,
    0x49e9,
    &[0xb1, 0xf8, 0x89, 0x62, 0xcd, 0xed, 0x0e, 0x8e],
);

impl TaggedTask<Context> for SleepTask2 {
    type Tag = Uuid;
    fn get_tag() -> Self::Tag {
        SLEEP_TASK_2
    }
}

inventory::submit! {
    BackgroundTask {
        id: SLEEP_TASK_2,
        de: |buf| { Ok(Box::new(rmp_serde::from_slice::<SleepTask2>(buf).unwrap())) }
    }
}

pub async fn first_background(
    origin: Origin,
    backend: Arc<dyn TasksBackend + Send + Sync>,
    storage: Arc<dyn StorageBackend + Send + Sync>,
) -> Result<(), Failed> {
    // Ok: we have a `TasksBackend` instance. We'll use this to construct a `BackgroundTasks`
    // instance. This can both send & process tasks via `backend`.
    let tasks = Arc::new(BackgroundTasks::new(backend));
    // Now we'll create a "sender" backed by `tasks`; I don't specialize it yet, since the `Sender`
    // trait is parameterized by task type. We'll give *this* to the indielinks state, and
    // specialize at send time:
    let sender = tasks.clone();

    // `processor` is now the thing which we can control; we don't need `tasks` any more, so just
    // move it into the processor:
    let processor = background_tasks::new(
        tasks,
        Context {
            origin,
            storage,
            ap_client: make_client("user-agent", true, HostExtractor, RateLimiter::keyed(Quota::per_second(nonzero!(16u32))).use_middleware(KeyedDashmapMiddleware::from(vec![])), &Default::default()).unwrap(/* known good */),
            local_client: make_client("user-agent", false, HostExtractor, RateLimiter::keyed(Quota::per_second(nonzero!(32u32))).use_middleware(KeyedDashmapMiddleware::from(vec![])), &Default::default()).unwrap(/* known good */),
            general_purpose_client: make_client("user-agent", false, HostExtractor, RateLimiter::keyed(Quota::per_second(nonzero!(4u32))).use_middleware(KeyedDashmapMiddleware::from(vec![])), &Default::default()).unwrap(/* known good */),
        },
        Some(Config {
            shutdown_timeout: Duration::from_secs(30),
            ..Default::default()
        }),
    )
    .unwrap();

    // So. "Send" one of each of our tasks to the processor; IRL this would happen in indielinks
    // request handlers. Notice that we can send different task types through the same reference.
    debug!("Sending tasks...");
    let task1 = SleepTask1::new(Duration::from_millis(250));
    sender.send(task1).await?;
    let task2 = SleepTask2::new(Duration::from_millis(750), 11);
    sender.send(task2).await?;
    debug!("Sending tasks...done.");

    // Even if the processor tried & failed to get a lease before we queued up these two tasks, the
    // default "retry" period is 1s. Let's just sleep for twice that in order to give the Tokio
    // runtime sufficient time to run both tasks, sequentially, on a single core:
    debug!("Tasks sent off; sleeping...");
    tokio::time::sleep(Duration::from_millis(2000)).await;
    debug!("Tasks sent off; sleeping...done.");

    debug!("Shutting down...");
    processor.shutdown(Duration::from_secs(2)).await?;
    debug!("Shutting down...done.");

    assert!(DATA1.load(Ordering::SeqCst));
    assert_eq!(DATA2.load(Ordering::SeqCst), 11);

    Ok(())
}
