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
# This particular Nickel file defines "front matter" for indielinksd configuration
# files. TODO(sp1ff): move this on to the Stack type?
let front-matter = fun stack =>
{
    log-file = stack.log-file,
    public-address = "0.0.0.0:%{stack.public-port}",
    private-address = "127.0.0.1:%{stack.private-port}",
    raft-grpc-address = "0.0.0.0:%{stack.grpc-port}",
    raft-config = {
        this-node = 0
    }
} in
front-matter

