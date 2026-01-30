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


   This CQL (Cassandra Query Language) file will initialize a ScyllaDB
   database with the indielinks schema, version zero.

   */

use indielinks;

/* Incoming likes, shares & replies. I first thought to just add
   likes, shares & replies to the `posts` table, but then it occurred
   to me that a particularly popular post could have hundreds or
   thousands of these, so I decided to break them out into their own
   tables.

   I also considered adding a "sort" column, and just throwing 'em all
   into a single table, but that would make the app do the
   partitioning in the application code. This seems slightly more
   convenient, despite the repetitious nature of the table
   definitions. */

create table if not exists post_likes (
  post_id uuid,
  like_url ascii,
  created timestamp,
  visibility tinyint,
  PRIMARY KEY ((post_id, reply_id), created)
);

create table if not exists post_replies (
  post_id uuid,
  reply_url ascii,
  created timestamp,
  visibility tinyint,
  PRIMARY KEY ((post_id, reply_id), created)
);

create table if not exists post_shares (
  post_id uuid,
  share_url ascii,
  created timestamp,
  visibility tinyint,
  PRIMARY KEY ((post_id, reply_id), created)
);

/* I'm going to need to query this:

   - when forming a user's outbox
     + will query by userid, sort by time
   - when rendering a post
     + will query by userid & ap_id
*/

drop table if exists likes;

create table if not exists likes (
  user_id uuid,
  created timestamp,
  api_id ascii, /* The thing being liked */
  visbility tinyint,
  PRIMARY KEY (user_id, created)
);

create index if not exists on likes((user_id), ap_id);

drop table if exists replies;

create table if not exists replies (
  user_id uuid,
  created timestamp,
  api_id ascii, /* The thing being liked */
  visbility tinyint,
  content text,
  PRIMARY KEY (user_id, created)
);

create index if not exists on replies((user_id), ap_id);

drop table if exists shares;

create table if not exists shares (
  user_id uuid,
  created timestamp,
  api_id ascii, /* The thing being liked */
  visbility tinyint,
  quote text,
  PRIMARY KEY (user_id, created)
);

create index if not exists on replies((user_id), ap_id);

drop materialized view if exists activity_pub_posts_by_posted;

drop table if exists activity_pub_posts;

