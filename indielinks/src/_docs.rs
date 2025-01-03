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
// You should have received a copy of the GNU General Public License along with mpdpopm.  If not,
// see <http://www.gnu.org/licenses/>.

//! # indielinks
//!
//! General (i.e. not documenting a particular struct or a method) documentation goes here. It's
//! really just a grab bag at this point; I'll polish it after collecting some content.
//!
//! ## The Data Store
//!
//! Some might be surprised at my choice of ScyllaDB and DynamoDB. I admit, I didn't think it
//! through very deeply; it's just that in 2024 I think we're past the point where a relational
//! database should be the default answer when you need to write-down some state. A NoSQL
//! wide-column store is pretty-much my default solution, and it's only when & if I discover during
//! design that I truly can't wriggle-out of joins, or referential integrity, or full-blow ACID
//! transactions that I change to a RDBMS.
//!
//! indielink's data isn't particularly relational in nature, so DynamoDB was my "go to" answer.
//! That said, a design goal for me is to enable indielinks to be run in a wide variety of
//! configurations. If you want to run it as a daemon on bare metal in your house, I'll support
//! that. If you want to run a containerized cluster in the cloud, I'll support that, too.
//! I added ScyllaDB to support the local case: you can install & run ScyllaDB locally.
//! The choice of ScyllaDB was also motivated by their "Alternator" interface, which allows
//! me to talk to it via the DynamoDB API.
