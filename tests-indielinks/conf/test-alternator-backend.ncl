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
# Regrettably, the tests use a slightly different structure to express native
# DynamoDB configuration, so we can't re-use `../conf/alternator-backend`; we
# have to use this, instead.
let backend = fun stack => {
    location = ["http://127.0.0.1:%{stack.scylla-alternator-port-1}",
                "http://127.0.0.1:%{stack.scylla-alternator-port-2}",
                "http://127.0.0.1:%{stack.scylla-alternator-port-3}"
               ],
} in
backend

