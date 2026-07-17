# Copyright (C) 2026 Michael Herstine <sp1ff@pobox.com>
#
# This file is part of indielinks.
#
# indielinks is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# indielinks is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with
# indielinks. If not, see <http://www.gnu.org/licenses/>.
#
#
# This file contains source code in the Nickel <https://nickel-lang.org/>
# language. Nickel is a gradually-typed configuration generation language. For
# more information on how Nickel is used in this project, see
# `indielinks/src/_docs.rs`. The assorted TOML configuration files are generated
# in the salient `build.rs` files.
#
# This particular Nickel file contains code for producing configuration files
# fo the `background-task-tests` integration test.
let runner-record = import "runner.ncl" in
let mk-scylla-backend = import "test-scylla-backend.ncl" in
let mk-alternator-backend = import "test-alternator-backend.ncl" in
fun stack => {
    runner = runner-record,
    domain = {
        scylla-env-file = stack.scylla-env-file,
        origin = {
            scheme = "http",
            port = stack.public-port,
            host = { RegName = "indiemark.local" }
        },
        scylla = mk-scylla-backend stack,
        dynamo = mk-alternator-backend stack
    }
}

