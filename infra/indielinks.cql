/* CQL (Cassandra Query Language) file for charging a local database.

   This is intended, along with `compose.local.yaml` for local
   indielinks development. I.e. the assumption is that the dev will
   stand-up a local ScyllaDB cluster using `docker compose`, then use
   this script to create the keyspace & tables required by indielinks.

   I'll write a second script to load some initial data. */

create keyspace indielinks
  with replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 3}
  and tablets = {'enabled': false}
  ;

use indielinks;

/* Paritition key is the user *ID* (not the username). Should give
   high cardinality, smooth distribution and uniform growth of each
   partition. No clustering key (at the moment I have no range queries
   over users).

  The last two attributes are required for two different reasons. The
  second is needed to service the /posts/recent endpoint; the post day
  is part of the "posts" table primary key, so without this, I'd have
  to start with today, then query for yesterday, and so on, looking
  for the most recent post.

  first_update exists to help me put a lower bound on a search of a
  user's complete history. */
create table users (
  id uuid PRIMARY KEY,
  username ascii,
  discoverable boolean,
  display_name text,
  summary text,
  pub_key_pem ascii,
  priv_key_pem ascii,
  api_key blob,
  first_update timestamp,
  last_update timestamp
);

create index users_by_username on users (username);

/* All of my queries for tags will include the user ID, so I want to
   incorporate that into the partition key to prevent
   full-table/cluster scans.

   The "id" column may seem redundant, but it's important to tracking
   tags across deletions! Imagine a user creates a tag "foo" and tags
   many posts with it. Then, they delete tag "foo", and re-add. All
   the old posts, in the absence of a different id field, will think
   they're still tagged by the *old* "foo, but the counts will reflect
   only the new "foo"! Better, I think, to treat them as two different
   tags that happen to have the same name/label.

   That said, I've used `name` in the primary key, not `id`. This is
   because otherwise it was impossible to atomically update the tag
   cloud when adding posts: I could, with the help of a secondary
   index, query for any tags with a given name for a given user, but
   if none came back, I couldn't atomically *create* the new tag!
   Using `name` in the primary key let's me "create-or-update"
   atomically. */
create table tags (
  id uuid,
  user_id uuid,
  name text,
  count int,
  PRIMARY KEY (user_id, name)
);

create index tags_by_id on tags (id);

/* All my queries are by user ID, so I need to incorporate that into
   the partition key to prevent full-table/cluster scans. That gives
   me high cardinality, but not even distribution: some users will
   inevitably upload more posts than others, sometimes dramatically
   more. In an attempt to address that, and to prevent partitions from
   growing without bound, I'm using a technique I've learned called
   "bucketing"; I'm adding a column corresponding to just the day of
   the posted timestamp, and adding that column to the partition
   key.

   While this will bound the size of any one partition, it doesn't
   completely eliminate the hot partition issue. There are some other
   options, such as making the buckets even smaller, or adding some
   other element to the partition key (perhaps a hash on the URL?),
   but I decided I was overthinking this. Besides, enough of the
   del.icio.us API works in terms of days to make me think that this
   is what they were doing.

   The downside is that I now have to include the day in every query!
   Still, enough of the API endpoints include that day (implicitly or
   explicitly) that that's not so much of a chore. In the worst case,
   if I have to walk the complete collection of a user's posts, I have
   the first_update & last_update columns in the user channel to
   bound my search. */
create table posts (
  id uuid,
  url ascii,
  user_id uuid,
  posted timestamp,
  day ascii,
  title text,
  notes text,
  tags set<uuid>,
  public boolean,
  unread boolean,
  PRIMARY KEY ((user_id, day), id)
);

create materialized view posts_by_user_and_url as
    select * from posts where user_id is not null and url is not null and id is not null and day is not null
    primary key ((user_id, url), id, day);

