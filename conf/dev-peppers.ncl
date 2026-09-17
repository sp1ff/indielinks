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
# This particular Nickel file contains the *development-only* peppers in use
# (for peppering user passwords), shared among the generated development-stack
# indielinksd configuration files as well as `smoke-tests` configuration
# files. It is merged into development configurations by `indielinks/build.rs`;
# production deployments reference their secrets by SSM parameter name instead.
{
    peppers = {
        "pepper-ver:20250213" = [
            178, 145, 72, 52, 77, 95, 211, 126, 89, 113, 87, 145, 221, 0, 50, 146,
            98, 233, 25, 119, 109, 174, 75, 91, 106, 171, 0, 103, 14, 140, 244, 54
        ]
    }
}
