/* Copyright (C) 2026 Michael Herstine <sp1ff@pobox.com>

   This file is part of indielinks.

   indielinks is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published
   by the Free Software Foundation, either version 3 of the License,
   or (at your option) any later version.

   indielinks is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
   General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with indielinks. If not, see <http://www.gnu.org/licenses/>.

 */

use indielinks;

/* Basically undoing the changes in 1.cql & starting from scratch. */

drop table if exists post_likes;
drop table if exists post_replies;
drop table if exists post_shares;

drop table if exists likes;
drop table if exists replies;
drop table if exists shares;

create table if not exists likes_replies_shares (
  user_id uuid,
  posted timestamp,
  id uuid,
  /* Is this a like, a reply, or a share? */
  sort tinyint,
  /* The URL of the thing that we are liking, sharing or replying to. */
  in_reply_to ascii,
  visibility tinyint,
  content text,
  /* Partition by user_id, order by descending posted */
  PRIMARY KEY (user_id, posted, id)
) WITH CLUSTERING ORDER BY (posted desc);

create table if not exists incoming_likes_replies_shares (
  user_id uuid,
  received timesetamp,
  ap_id ascii,
  /* Is this a like, a reply, or a share? */
  sort tinyint,
  /* The URL of the indielinks post that is being liked, shared, or replied to. */
  in_reply_to ascii,
  visibility tinyint,
  content text,
  PRIMARY KEY (user_id, received, ap_id)
) WITH CLUSTERING ORDER BY (received desc);
