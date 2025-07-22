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

//! Integration tests for indielinks-cache.

use std::sync::Arc;

use libtest_mimic::Failed;

use indielinks_cache::{raft::StorageError, types::NodeId};

use indielinks::cache::{Backend, LogStore};
use tracing::error;

struct Dropper {
    // backend: Arc<RwLock<dyn Backend + Send + Sync>>,
    backend: Arc<dyn Backend + Send + Sync>,
}

impl Drop for Dropper {
    fn drop(&mut self) {
        let backend = self.backend.clone();
        let result = tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current().block_on(async move {
                backend./*read().await.*/drop_all_rows().await
            })
        });
        if result.is_err() {
            error!("Failed to cleanup Raft storage: {result:#?}");
            panic!();
        }
    }
}

struct Builder {
    // backend: Arc<RwLock<dyn Backend + Send + Sync>>,
    backend: Arc<dyn Backend + Send + Sync>,
}

impl Builder {
    // pub fn new(backend: Arc<RwLock<dyn Backend + Send + Sync>>) -> Builder {
    pub fn new(backend: Arc<dyn Backend + Send + Sync>) -> Builder {
        Builder { backend }
    }
}

impl indielinks_cache::raft::test::StoreBuilder<LogStore, Dropper> for Builder {
    async fn build(&self) -> Result<(Dropper, LogStore), StorageError<NodeId>> {
        Ok((
            Dropper {
                backend: self.backend.clone(),
            },
            LogStore::new(self.backend.clone()),
        ))
    }
}

/// Execute the [openraft] test suite against the [indielinks](crate) log store implementation.
///
/// [openraft]: https://docs.rs/openraft/latest/openraft/index.html
// pub fn openraft_test_suite(backend: Arc<RwLock<dyn Backend + Send + Sync>>) -> Result<(), Failed> {
pub fn openraft_test_suite(backend: Arc<dyn Backend + Send + Sync>) -> Result<(), Failed> {
    let result = indielinks_cache::raft::test::test_storage(Builder::new(backend));
    if let Err(ref err) = result {
        error!("{err:#?}");
    }
    assert!(result.is_ok());
    Ok(())
}
