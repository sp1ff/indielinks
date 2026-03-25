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

//! # metrics-tsk
//!
//! A background task for pulling metrics

use std::sync::Arc;

use indielinks_cache::cache::Cache;
use tokio::sync::Notify;
use tracing::{info, trace};
use url::Url;

use crate::{
    acct::Account,
    ap_entities::{Actor, Note},
    cache::GrpcClientFactory,
    define_metric,
};

define_metric! { "ap-resolution.actors.count", actors_count, Sort::IntegralGauge }
define_metric! { "ap-resolution.notes.count", notes_count, Sort::IntegralGauge }
define_metric! { "ap-resolution.handles.count", handles_count, Sort::IntegralGauge }

pub async fn produce_metrics(
    shutdown: Arc<Notify>,
    actors: Arc<Cache<GrpcClientFactory, Url, Actor>>,
    notes: Arc<Cache<GrpcClientFactory, Url, Note>>,
    handles: Arc<Cache<GrpcClientFactory, Account, Actor>>,
) {
    let mut done = false;

    info!("Beginning metrics task.");
    while !done {
        actors_count.record(actors.count() as u64, &[]);
        notes_count.record(notes.count() as u64, &[]);
        handles_count.record(handles.count() as u64, &[]);

        trace!("Produced counts for AP resolution; sleeping for 60 seconds.");

        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(60)) => (),
            _ = shutdown.notified()=> {
                done = true;
            }
        }
    }
    info!("Ending metrics task.");
}
