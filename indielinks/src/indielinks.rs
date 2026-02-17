// Copyright (C) 2024-2025 Michael Herstine <sp1ff@pobox.com>
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

use std::{path::PathBuf, sync::Arc};

use chrono::Duration;
use lru::LruCache;
use opentelemetry_prometheus_text_exporter::PrometheusExporter;
use tokio::sync::Mutex;
use uuid::Uuid;

use indielinks_shared::{entities::UserId, instance_state::InstanceStateV0, origin::Origin};

use indielinks_cache::raft::CacheNode;

use crate::{
    ap_resolution::ApResolver, background_tasks::BackgroundTasks, home_timeline::Timeline,
    http::SameSite, peppers::Peppers, signing_keys::SigningKeys,
    storage::Backend as StorageBackend,
};

pub type HomeTimelines = LruCache<UserId, Timeline>;

/// Application state available to all handlers
pub struct Indielinks {
    pub origin: Origin,
    pub instance_id: Uuid,
    pub instance_state: InstanceStateV0,
    pub storage: Arc<dyn StorageBackend + Send + Sync>,
    pub exporter: PrometheusExporter,
    pub pepper: Peppers,
    pub token_lifetime: Duration,
    pub refresh_token_lifetime: Duration,
    pub signing_keys: SigningKeys,
    pub users_same_site: SameSite,
    pub users_secure_cookies: bool,
    pub allowed_origins: Vec<Origin>,
    pub ap_client: crate::client_types::ClientType,
    pub local_client: crate::client_types::ClientType,
    pub general_purpose_client: crate::client_types::ClientType,
    pub collection_page_size: usize,
    pub assets: PathBuf,
    pub task_sender: Arc<BackgroundTasks>,
    pub cache_node: CacheNode<crate::cache::GrpcClientFactory>,
    // Shared, mutable access to the resolver needed, therefore we need an
    // `Arc<thing that can give a mutable borrow>`; `ApResolver`, being a cache, pretty-much always
    // requires a mutable borrow, so I used a `Mutex` instead of an `RwLock`.
    //
    // This is pretty sub-optimal, since it means we're going to be locking access to the resolver
    // for the duration of internet calls, making this a real bottleneck.
    pub ap_resolver: Arc<Mutex<ApResolver>>,
    // Similarly here.
    pub home_timelines: Arc<Mutex<HomeTimelines>>,
}
