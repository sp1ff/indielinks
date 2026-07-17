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
# This particular Nickel file defines "front matter" for indielinksd
# configuration files when running in a clustered confugration.
# TODO(sp1ff): move this on to the Stack type?
let front-matter = fun stack i =>
{
    # TODO(sp1ff): I'd like this to be `/tmp/indielinks` (i.e. no 'd'),
    # like in the single-node variants, but my integration tests expect the 'd';
    # well, cache-tests does, at least.
    log-file = "%{stack.cache-state-dir-prefix}%{i}/indielinksd.log",
    public-address = "0.0.0.0:%{stack.public-port + 3*i}",
    private-address = "127.0.0.1:%{stack.private-port + 3*i}",
    raft-grpc-address = "0.0.0.0:%{stack.grpc-port + 3*i}",
    raft-config = {
        this-node = i
    }
} in
front-matter
