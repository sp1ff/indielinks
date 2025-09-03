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
//! [OpenTelemetry] user such as Indielinks, the API is not terribly convenient to use. The actual
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
//! This module exports a solution to these problems.
//!
//! # metrics
//!
//! This module uses David Tolnay's [inventory] crate to work around the need for a centralized
//! list. Create an [Instruments] instance once and attach it to the application state object:
//!
//!
//! ```ignore
//! Indielinks {
//!     //...
//!     instruments: Intruments::new("indielinks"),
//! }
//! ```
//!
//! and at the metric collection site:
//!
//! ```ignore
//! inventory::submit!(metrics::Registration::new("delicious.authorizations.success"), Counter);
//! // ...
//! async fn do_thing() {
//!     // ...
//!     metrics::counter_add!(state.instruments, "delicious.authorizations.success", 1);
//! }
//! ```
//!
//! The `submit!` invocation will "register" the metric name & the macro `counter_add!` will handle
//! the lookup and unwrapping of the actual instrument at runtime. The [Instruments] constructor
//! will check for any name clashes & "pre-build" all the instruments.
//!
//! One aspect of this design with which I'm uncomfortable is the use of `panic!` to indicate
//! failure to lookup a metric by name, or incorrect typing of an instrument (e.g. calling
//! `counter_add!` on a metric that actually names a gauge). On the one hand, these are logic errors
//! that would be compile-time errors with a richer type systems, and the convention seems to be to
//! panic in these instances (as opposed to using a `Result` on, say, invalid input). On the other
//! hand, a bad metric name in a little-used code path seems like a ticking time bomb to me.

use std::collections::{HashMap, HashSet, hash_map::Entry};

use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Gauge},
};

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

/// The type of thing being inventories
///
/// Register a metric by name & type using
///
/// ```ignore
/// inventory::submit!{metrics::Registration::new("auth.success", Sort::IntegralCounter)}
/// ```
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Registration {
    name: &'static str,
    sort: Sort,
}

impl Registration {
    pub const fn new(name: &'static str, sort: Sort) -> Registration {
        Registration { name, sort }
    }
    pub fn name(&self) -> String {
        self.name.to_string()
    }
    pub fn sort(&self) -> Sort {
        self.sort
    }
}

inventory::collect!(Registration);

pub fn check_metric_registrations() {
    let mut names: HashSet<String> = HashSet::new();
    IntoIterator::into_iter(inventory::iter::<Registration>).for_each(|reg| {
        if names.contains(&reg.name()) {
            panic!();
        }
        names.insert(reg.name());
    });
}

enum Instrument {
    CounterU64(Counter<u64>),
    GaugeF64(Gauge<f64>),
    GaugeU64(Gauge<u64>),
}

/// Container for OTel instruments
pub struct Instruments {
    meter: opentelemetry::metrics::Meter,
    map: HashMap<String, Instrument>,
}

impl Instruments {
    pub fn new(prefix: &'static str) -> Instruments {
        let mut m: HashMap<String, Instrument> = HashMap::new();
        let meter = global::meter(prefix);
        // "Pre-creating" all the registered instruments risks building things that may never be
        // used, but carries the benefit of making `add` and `record` *not* require a `&mut self`,
        // meaning that we can still old an instance of this type in an Arc.
        IntoIterator::into_iter(inventory::iter::<Registration>).for_each(|reg| {
            let name = reg.name();
            match m.entry(reg.name()) {
                Entry::Occupied(_occupied_entry) => {
                    panic!("The metric name {} was used twice", name)
                }
                Entry::Vacant(vacant_entry) => {
                    vacant_entry.insert(match reg.sort() {
                        Sort::IntegralCounter => {
                            Instrument::CounterU64(meter.u64_counter(name).build())
                        }
                        Sort::FloatGauge => Instrument::GaugeF64(meter.f64_gauge(name).build()),
                        Sort::IntegralGauge => Instrument::GaugeU64(meter.u64_gauge(name).build()),
                    });
                }
            }
        });

        Instruments { meter, map: m }
    }
    pub fn meter(&self) -> &opentelemetry::metrics::Meter {
        &self.meter
    }
    // panics if `name` doesn't name a counter
    pub fn add(&self, name: &str, count: u64, attributes: &[KeyValue]) {
        if let Some(Instrument::CounterU64(c)) = self.map.get(name) {
            c.add(count, attributes);
        } else {
            panic!("{} does not name a counter", name);
        }
    }
    // This seems really lame, but I'm in the middle of something at the moment and don't want to
    // dig into it
    pub fn recordf(&self, name: &str, value: f64, attributes: &[KeyValue]) {
        if let Some(Instrument::GaugeF64(g)) = self.map.get(name) {
            g.record(value, attributes);
        } else {
            panic!("{} does not name a gauge", name);
        }
    }
    pub fn recordu(&self, name: &str, value: u64, attributes: &[KeyValue]) {
        if let Some(Instrument::GaugeU64(g)) = self.map.get(name) {
            g.record(value, attributes);
        } else {
            panic!("{} does not name a gauge", name);
        }
    }
}

#[macro_export]
macro_rules! counter_add {
    ($instr:expr, $name:expr, $count:expr, $attrs:expr) => {
        $instr.add($name, $count, $attrs);
    };
}

#[macro_export]
macro_rules! gauge_setu {
    ($instr:expr, $name:expr, $value:expr, $attrs:expr) => {
        $instr.recordu($name, $value, $attrs);
    };
}
