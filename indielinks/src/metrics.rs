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

//! # indielinks metrics
//!
//! # Introduction
//!
//! Indlielinks uses [OpenTelemetry] to collect & export metrics. From the perspective of
//! [OpenTelemetry] users such as indielinks, the API is not terribly convenient to use. The actual
//! counters, gauges & histograms are called "instruments" in OTel, and we are advised to "Re-use
//! Instruments: Instruments are designed for reuse. Avoid creating new instruments repeatedly."
//! Fine, but where to keep them? I'd prefer not to litter my state type with hundreds of fields of
//! type `Counter<u64>`, `Gauge<f64>` and so on. My first thought was to build a map from metric
//! identifier to instrument, Rust, sadly lacking dependent types, makes that difficult. Yes,
//! since there are likely only a few types of instruments that will be used, one could use an enum,
//! but that would require caller code do jump through hoops like:
//!
//! [OpenTelemetry]: https://docs.rs/opentelemetry/latest/opentelemetry/index.html
//!
//! ```ignore
//! state.instruments.get("metric-name").unwrap().add(1, &[tag...]);
//! // panic if "metric-name" doesn't name 👆 a counter...
//! ```
//!
//! And even if we had dependent types (eliminating the need for the `unwrap()`), there's still the
//! footgun of two different instruments accidentally using the same metric name, barring an
//! inconvenient centralized list.
//!
//! This module implements a solution to these problems.
//!
//! # metrics
//!
//! This module uses David Tolnay's [inventory] crate to work around the need for a centralized
//! list. Declare a metric with the `define_metric!` macro:
//!
//! ```ignore
//! define_metric! { "background.processor.tasks.completed", background_processor_tasks_completed, Sort::IntegralCounter }
//! ```
//!
//! This will declare a `Counter<u64>` instance named "background.processor.tasks.completed" in
//! `background_processor_tasks_completed` (using the `lazy_static!` macro).
//!
//! Then, at program startup, invoke [check_metric_names] to verify that there are no duplicate metric names.

use std::collections::{HashMap, hash_map::Entry};

/// Instrument type
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Sort {
    /// Corresponds to `Counter<u64>`
    IntegralCounter,
    /// Corresponds to `Gauge<f64>`
    FloatGauge,
    /// `Gauge<u64>`
    IntegralGauge,
    // more later?
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Registration {
    pub name: &'static str,
}

inventory::collect!(Registration);

#[macro_export]
macro_rules! define_metric {
    ($name:expr, $id:ident, Sort::IntegralCounter) => {
        inventory::submit! { $crate::metrics::Registration{ name: $name } }
        lazy_static::lazy_static! {
              static ref $id: opentelemetry::metrics::Counter<u64> = {
                  opentelemetry::global::meter("indielinks").u64_counter($name).build()
            };
        }
    };
    ($name:expr, $id:ident, Sort::FloatGauge) => {
        inventory::submit! { $crate::metrics::Registration{ name: $name } }
        lazy_static::lazy_static! {
              static ref $id: opentelemetry::metrics::Gauge<f64> = {
                  opentelemetry::global::meter("indielinks").f64_gauge($name).build()
            };
        }
    };
    ($name:expr, $id:ident, Sort::IntegralGauge) => {
        inventory::submit! { $crate::metrics::Registration{ name: $name } }
        lazy_static::lazy_static! {
              static ref $id: opentelemetry::metrics::Gauge<u64> = {
                  opentelemetry::global::meter("indielinks").u64_gauge($name).build()
            };
        }
    };
}

/// Check for duplicate metric names; call this once at program startup
pub fn check_metric_names() {
    // The "entry" interface on `HashSet` is nightly-only (at the time of this writing), so:
    let mut pool = HashMap::<&'static str, ()>::new();
    IntoIterator::into_iter(inventory::iter::<Registration>).for_each(|reg| {
        match pool.entry(reg.name) {
            Entry::Occupied(_occupied_entry) => {
                panic!("The metric name {} was used more than once!", reg.name)
            }
            Entry::Vacant(vacant_entry) => *vacant_entry.insert(()),
        }
    });
}
