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
  id bigint PRIMARY KEY,
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

create index on users (username);

/* All of my queries for tags will include the user ID, so I want to
   incorporate that into the partition key to prevent
   full-table/cluster scans. Using the tag id as a clustering key is a
   bit of an experiment. I have to include it somewhere in order to
   have the primary key reflect identity in this table, but it also
   ought to allow me to grab all of a user's keys in one fell swoop.
   */
create table tags (
  id bigint,
  user_id bigint,
  name text,
  count int,
  PRIMARY KEY (user_id, id)
);

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
  id bigint,
  url ascii,
  user_id bigint,
  posted timestamp,
  day ascii,
  title text,
  notes text,
  tags set<bigint>,
  public boolean,
  unread boolean,
  PRIMARY KEY ((user_id, day), id)
) with clustering order by (id desc);
