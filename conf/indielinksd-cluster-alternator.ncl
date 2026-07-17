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
# This particular Nickel file contains the code for generating `indielinksd`
# configuration files for a cluster of instances, using the Alternator/DynamoDB
# backend.
let common = import "indielinksd-common.ncl" in
let mk-backend = import "alternator-backend.ncl" in
let mk-front-matter = import "indielinksd-front-matter-clustered.ncl" in
let mk-node = fun stack i => common & mk-front-matter stack i & {
    public-origin = {
        scheme = "http",
        port = stack.haproxy-port,
        host = {
            RegName = "indiemark.local"
        }
    },
    storage-config = mk-backend stack,
}
in
fun stack => std.array.generate (mk-node stack) 5
