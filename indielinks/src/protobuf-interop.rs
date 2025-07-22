// Copyright (C) 2025 Michael Herstine <sp1ff@pobox.com>
//
// This file is part of indielinks.
//
// indielinks is free software: you can redistribute it and/or modify it under the terms of the GNU
// General Public License as published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// indielinks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
// even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// General Public License for more details.
//
// You should have received a copy of the GNU General Public License along with indielinks.  If not,
// see <http://www.gnu.org/licenses/>.

//! Interoperability for protocol buffer-generated types
//!
//! One downside to using gRPC for intra-cluster communications is that one has to implement
//! conversions to & from the prost-generated types by hand. The openraft sample, despite its small
//! size, factors this implementation out into numerous files, For now, I'm hoping to do it here, in
//! this module.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    net::SocketAddr,
    num::NonZero,
};

use indielinks_cache::{
    network::{AppendEntriesRequest, AppendEntriesResponse},
    types::{CacheId, ClusterNode, NodeId, Request, TypeConfig},
};
use openraft::{Entry, LogId, Vote};
use serde::Serialize;
use snafu::*;

pub mod protobuf {
    tonic::include_proto!("indielinkspb");
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Deserialization error: {source}"))]
    De {
        source: rmp_serde::decode::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Missing LeaderId"))]
    LeaderId { backtrace: Backtrace },
    #[snafu(display("Missing LogId"))]
    LogId { backtrace: Backtrace },
    #[snafu(display("Missing Membership"))]
    Membership { backtrace: Backtrace },
    #[snafu(display("Missing Metadata"))]
    Meta { backtrace: Backtrace },
    #[snafu(display("Missing Nodes"))]
    Nodes { backtrace: Backtrace },
    #[snafu(display("Missing Payload"))]
    Payload { backtrace: Backtrace },
    #[snafu(display("Serialization error: {source}"))]
    Ser {
        source: rmp_serde::encode::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to parse {text} to a  SocketAddr: {source}"))]
    SocketAddr {
        text: String,
        source: std::net::AddrParseError,
        backtrace: Backtrace,
    },
    #[snafu(display("Missing Vote"))]
    Vote { backtrace: Backtrace },
    #[snafu(display("The number of virtual nodes was encoded as zero"))]
    ZeroVirtual { backtrace: Backtrace },
}

pub type Result<T> = std::result::Result<T, Error>;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                       openraft::LeaderId<NodeId> <=> protobuf::LeaderId                        //
////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<openraft::LeaderId<NodeId>> for protobuf::LeaderId {
    fn from(value: openraft::LeaderId<NodeId>) -> protobuf::LeaderId {
        protobuf::LeaderId {
            term: value.term,
            node_id: value.node_id,
        }
    }
}

impl From<protobuf::LeaderId> for openraft::LeaderId<NodeId> {
    fn from(value: protobuf::LeaderId) -> Self {
        openraft::LeaderId::<NodeId> {
            term: value.term,
            node_id: value.node_id,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                          openraft::LogId<NodeId> <=> protobuf::LogId                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<LogId<NodeId>> for protobuf::LogId {
    fn from(value: LogId<NodeId>) -> protobuf::LogId {
        protobuf::LogId {
            leader_id: Some(value.leader_id.into()),
            index: value.index,
        }
    }
}

impl TryFrom<protobuf::LogId> for LogId<NodeId> {
    type Error = Error;
    fn try_from(value: protobuf::LogId) -> Result<LogId<NodeId>> {
        Ok(LogId {
            leader_id: value.leader_id.ok_or(LeaderIdSnafu.build())?.into(),
            index: value.index,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           NodeIdSet                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<BTreeSet<NodeId>> for protobuf::NodeIdSet {
    fn from(value: BTreeSet<NodeId>) -> protobuf::NodeIdSet {
        protobuf::NodeIdSet {
            node_ids: HashMap::from_iter(value.into_iter().map(|n| (n, ()))),
        }
    }
}

impl From<protobuf::NodeIdSet> for BTreeSet<NodeId> {
    fn from(value: protobuf::NodeIdSet) -> BTreeSet<NodeId> {
        value.node_ids.into_keys().collect()
    }
}

impl From<Vec<NodeId>> for protobuf::NodeIdSet {
    fn from(value: Vec<NodeId>) -> protobuf::NodeIdSet {
        protobuf::NodeIdSet {
            node_ids: HashMap::from_iter(value.into_iter().map(|n| (n, ()))),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                             NodeId                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<protobuf::NodeIdSet> for Vec<NodeId> {
    fn from(value: protobuf::NodeIdSet) -> Vec<NodeId> {
        value.node_ids.into_keys().collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                              Node                                              //
////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<ClusterNode> for protobuf::Node {
    fn from(value: ClusterNode) -> protobuf::Node {
        protobuf::Node {
            addr: value.addr.to_string(),
        }
    }
}

impl TryFrom<protobuf::Node> for ClusterNode {
    type Error = Error;
    fn try_from(value: protobuf::Node) -> Result<ClusterNode> {
        Ok(ClusterNode {
            addr: value
                .addr
                .parse::<SocketAddr>()
                .context(SocketAddrSnafu { text: value.addr })?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//               openraft::Membership<NodeId, ClusterNode> <=> protobuf::Membership               //
////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<openraft::Membership<NodeId, ClusterNode>> for protobuf::Membership {
    fn from(value: openraft::Membership<NodeId, ClusterNode>) -> protobuf::Membership {
        protobuf::Membership {
            configs: value
                .get_joint_config()
                .iter()
                // Argghhh... can't see a way to *move* the joint config out of `membership`.
                .cloned()
                .map(|s| s.into())
                .collect(),
            nodes: value.nodes().map(|(i, n)| (*i, n.clone().into())).collect(),
        }
    }
}

impl TryFrom<protobuf::Membership> for openraft::Membership<NodeId, ClusterNode> {
    type Error = Error;
    fn try_from(value: protobuf::Membership) -> Result<openraft::Membership<NodeId, ClusterNode>> {
        Ok(openraft::Membership::<NodeId, ClusterNode>::new(
            // I need a Vec<BTreeSet<NodeId>>
            value.configs.into_iter().map(|s| s.into()).collect(),
            BTreeMap::from_iter(
                value
                    .nodes
                    .into_iter()
                    .map(|(i, n)| n.try_into().map(|n| (i, n)))
                    .collect::<Result<Vec<(NodeId, ClusterNode)>>>()?,
            ),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//         openraft::StoredMembership<NodeId, ClusterNode> <=> protobuf::StoredMembership         //
////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<openraft::StoredMembership<NodeId, ClusterNode>> for protobuf::StoredMembership {
    fn from(value: openraft::StoredMembership<NodeId, ClusterNode>) -> protobuf::StoredMembership {
        protobuf::StoredMembership {
            log_id: (*value.log_id()).map(|id| id.into()),
            membership: Some(value.membership().clone().into()),
        }
    }
}

impl TryFrom<protobuf::StoredMembership> for openraft::StoredMembership<NodeId, ClusterNode> {
    type Error = Error;
    fn try_from(
        value: protobuf::StoredMembership,
    ) -> Result<openraft::StoredMembership<NodeId, ClusterNode>> {
        Ok(openraft::StoredMembership::<NodeId, ClusterNode>::new(
            value.log_id.map(|id| id.try_into()).transpose()?,
            value
                .membership
                .ok_or(MembershipSnafu.build())?
                .try_into()?,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                            Payload                                             //
////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<openraft::EntryPayload<TypeConfig>> for protobuf::entry::Payload {
    fn from(value: openraft::EntryPayload<TypeConfig>) -> protobuf::entry::Payload {
        use openraft::EntryPayload;
        match value {
            EntryPayload::Blank => protobuf::entry::Payload::Empty(()),
            EntryPayload::Normal(Request::Init { nodes, num_virtual }) => {
                protobuf::entry::Payload::Normal(protobuf::HashRingRequest {
                    payload: Some(protobuf::hash_ring_request::Payload::Init(
                        protobuf::HashRingInit {
                            nodes: Some(nodes.into()),
                            num_virtual: num_virtual.get() as u64,
                        },
                    )),
                })
            }
            EntryPayload::Normal(Request::InsertNodes { nodes }) => {
                protobuf::entry::Payload::Normal(protobuf::HashRingRequest {
                    payload: Some(protobuf::hash_ring_request::Payload::Insert(
                        protobuf::HashRingInsert {
                            nodes: Some(nodes.into()),
                        },
                    )),
                })
            }
            EntryPayload::Normal(Request::RemoveNodes { nodes }) => {
                protobuf::entry::Payload::Normal(protobuf::HashRingRequest {
                    payload: Some(protobuf::hash_ring_request::Payload::Remove(
                        protobuf::HashRingRemove {
                            nodes: Some(nodes.into()),
                        },
                    )),
                })
            }
            EntryPayload::Membership(membership) => {
                protobuf::entry::Payload::Membership(membership.into())
            }
        }
    }
}

impl TryFrom<protobuf::entry::Payload> for openraft::EntryPayload<TypeConfig> {
    type Error = Error;
    fn try_from(value: protobuf::entry::Payload) -> Result<openraft::EntryPayload<TypeConfig>> {
        Ok(match value {
            protobuf::entry::Payload::Empty(_) => openraft::entry::payload::EntryPayload::Blank,
            protobuf::entry::Payload::Normal(req) => {
                openraft::entry::payload::EntryPayload::Normal(
                    match req.payload.ok_or(PayloadSnafu.build())? {
                        protobuf::hash_ring_request::Payload::Init(hash_ring_init) => {
                            Request::Init {
                                nodes: hash_ring_init.nodes.ok_or(NodesSnafu.build())?.into(),
                                num_virtual: NonZero::new(hash_ring_init.num_virtual as usize)
                                    .context(ZeroVirtualSnafu)?,
                            }
                        }
                        protobuf::hash_ring_request::Payload::Insert(hash_ring_insert) => {
                            Request::InsertNodes {
                                nodes: hash_ring_insert.nodes.ok_or(NodesSnafu.build())?.into(),
                            }
                        }
                        protobuf::hash_ring_request::Payload::Remove(hash_ring_remove) => {
                            Request::RemoveNodes {
                                nodes: hash_ring_remove.nodes.ok_or(NodesSnafu.build())?.into(),
                            }
                        }
                    },
                )
            }
            protobuf::entry::Payload::Membership(membership) => {
                openraft::entry::payload::EntryPayload::<TypeConfig>::Membership(
                    membership.try_into()?,
                )
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                             Entry<TypeConfig> <=> protobuf::Entry                              //
////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<Entry<TypeConfig>> for protobuf::Entry {
    fn from(value: Entry<TypeConfig>) -> protobuf::Entry {
        protobuf::Entry {
            log_id: Some(value.log_id.into()),
            payload: Some(value.payload.into()),
        }
    }
}

impl TryFrom<protobuf::Entry> for Entry<TypeConfig> {
    type Error = Error;
    fn try_from(value: protobuf::Entry) -> Result<Entry<TypeConfig>> {
        Ok(Entry {
            log_id: value.log_id.ok_or(LogIdSnafu.build())?.try_into()?,
            payload: value.payload.ok_or(PayloadSnafu.build())?.try_into()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                 Vote<NodeId> <=> protobuf::Vote                                //
////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<Vote<NodeId>> for protobuf::Vote {
    fn from(value: Vote<NodeId>) -> protobuf::Vote {
        protobuf::Vote {
            leader_id: Some(value.leader_id.into()),
            committed: value.committed,
        }
    }
}

impl TryFrom<protobuf::Vote> for Vote<NodeId> {
    type Error = Error;
    fn try_from(value: protobuf::Vote) -> Result<Vote<NodeId>> {
        Ok(Vote::<NodeId> {
            leader_id: value.leader_id.ok_or(LeaderIdSnafu.build())?.into(),
            committed: value.committed,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//              AppendEntriesRequest<TypeConfig> <=> protobuf::AppendEntriesRequest               //
////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<AppendEntriesRequest<TypeConfig>> for protobuf::AppendEntriesRequest {
    fn from(value: AppendEntriesRequest<TypeConfig>) -> protobuf::AppendEntriesRequest {
        protobuf::AppendEntriesRequest {
            vote: Some(value.vote.into()),
            prev_log_id: value.prev_log_id.map(|x| x.into()),
            entries: value.entries.into_iter().map(|x| x.into()).collect(),
            leader_commit: value.leader_commit.map(|x| x.into()),
        }
    }
}

impl TryFrom<protobuf::AppendEntriesRequest> for AppendEntriesRequest<TypeConfig> {
    type Error = Error;
    fn try_from(value: protobuf::AppendEntriesRequest) -> Result<AppendEntriesRequest<TypeConfig>> {
        Ok(AppendEntriesRequest::<TypeConfig> {
            vote: value.vote.ok_or(VoteSnafu.build())?.try_into()?,
            prev_log_id: value.prev_log_id.map(|id| id.try_into()).transpose()?,
            entries: value
                .entries
                .into_iter()
                .map(|e| e.try_into())
                .collect::<Result<Vec<Entry<TypeConfig>>>>()?,
            leader_commit: value.leader_commit.map(|id| id.try_into()).transpose()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                     AppendEntriesResponse                                      //
////////////////////////////////////////////////////////////////////////////////////////////////////

impl TryFrom<openraft::raft::AppendEntriesResponse<NodeId>> for protobuf::AppendEntriesResponse {
    type Error = Error;
    fn try_from(
        value: openraft::raft::AppendEntriesResponse<NodeId>,
    ) -> Result<protobuf::AppendEntriesResponse> {
        Ok(protobuf::AppendEntriesResponse {
            payload: Some(match value {
                AppendEntriesResponse::Success => {
                    protobuf::append_entries_response::Payload::Success(())
                }
                AppendEntriesResponse::PartialSuccess(log_id) => {
                    protobuf::append_entries_response::Payload::PartialSuccess(
                        protobuf::MaybeLogId {
                            log_id: log_id.map(|id| id.into()),
                        },
                    )
                }
                AppendEntriesResponse::Conflict => {
                    protobuf::append_entries_response::Payload::Conflict(())
                }
                AppendEntriesResponse::HigherVote(vote) => {
                    protobuf::append_entries_response::Payload::HigherVote(vote.into())
                }
            }),
        })
    }
}

impl TryFrom<protobuf::AppendEntriesResponse> for openraft::raft::AppendEntriesResponse<NodeId> {
    type Error = Error;
    fn try_from(
        value: protobuf::AppendEntriesResponse,
    ) -> Result<openraft::raft::AppendEntriesResponse<NodeId>> {
        Ok(match value.payload.ok_or(PayloadSnafu.build())? {
            protobuf::append_entries_response::Payload::Success(_) => {
                openraft::raft::AppendEntriesResponse::Success
            }
            protobuf::append_entries_response::Payload::PartialSuccess(log_id) => {
                openraft::raft::AppendEntriesResponse::PartialSuccess(
                    log_id.log_id.map(|id| id.try_into()).transpose()?,
                )
            }
            protobuf::append_entries_response::Payload::Conflict(_) => {
                openraft::raft::AppendEntriesResponse::Conflict
            }
            protobuf::append_entries_response::Payload::HigherVote(vote) => {
                openraft::raft::AppendEntriesResponse::HigherVote(vote.try_into()?)
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                  SnapshotMeta<NodeId, ClusterNode> <=> protobuf::SnapshotMeta                  //
////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<openraft::storage::SnapshotMeta<NodeId, ClusterNode>> for protobuf::SnapshotMeta {
    fn from(value: openraft::storage::SnapshotMeta<NodeId, ClusterNode>) -> protobuf::SnapshotMeta {
        protobuf::SnapshotMeta {
            last_log_id: value.last_log_id.map(|id| id.into()),
            last_membership: Some(protobuf::StoredMembership {
                log_id: (*value.last_membership.log_id()).map(|id| id.into()),
                membership: Some(value.last_membership.membership().clone().into()),
            }),
            snapshot_id: value.snapshot_id,
        }
    }
}

impl TryFrom<protobuf::SnapshotMeta> for openraft::storage::SnapshotMeta<NodeId, ClusterNode> {
    type Error = Error;
    fn try_from(
        value: protobuf::SnapshotMeta,
    ) -> Result<openraft::storage::SnapshotMeta<NodeId, ClusterNode>> {
        Ok(openraft::storage::SnapshotMeta::<NodeId, ClusterNode> {
            last_log_id: value.last_log_id.map(|id| id.try_into()).transpose()?,
            last_membership: value
                .last_membership
                .ok_or(MembershipSnafu.build())?
                .try_into()?,
            snapshot_id: value.snapshot_id,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//            InstallSnapshotRequest<TypeConfig> <=> protobuf::InstallSnapshotRequest             //
////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<openraft::raft::InstallSnapshotRequest<TypeConfig>> for protobuf::InstallSnapshotRequest {
    fn from(
        value: openraft::raft::InstallSnapshotRequest<TypeConfig>,
    ) -> protobuf::InstallSnapshotRequest {
        protobuf::InstallSnapshotRequest {
            vote: Some(value.vote.into()),
            meta: Some(value.meta.into()),
            offset: value.offset,
            data: value.data,
            done: value.done,
        }
    }
}

impl TryFrom<protobuf::InstallSnapshotRequest>
    for openraft::raft::InstallSnapshotRequest<TypeConfig>
{
    type Error = Error;
    fn try_from(
        value: protobuf::InstallSnapshotRequest,
    ) -> Result<openraft::raft::InstallSnapshotRequest<TypeConfig>> {
        Ok(openraft::raft::InstallSnapshotRequest::<TypeConfig> {
            vote: value.vote.ok_or(VoteSnafu.build())?.try_into()?,
            meta: value.meta.ok_or(MetaSnafu.build())?.try_into()?,
            offset: value.offset,
            data: value.data,
            done: value.done,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//             InstallSnapshotResponse<NodeId> <=> protobuf::InstallSnapshotResponse              //
////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<openraft::raft::InstallSnapshotResponse<NodeId>> for protobuf::InstallSnapshotResponse {
    fn from(
        value: openraft::raft::InstallSnapshotResponse<NodeId>,
    ) -> protobuf::InstallSnapshotResponse {
        protobuf::InstallSnapshotResponse {
            vote: Some(value.vote.into()),
        }
    }
}

impl TryFrom<protobuf::InstallSnapshotResponse>
    for openraft::raft::InstallSnapshotResponse<NodeId>
{
    type Error = Error;
    fn try_from(
        value: protobuf::InstallSnapshotResponse,
    ) -> Result<openraft::raft::InstallSnapshotResponse<NodeId>> {
        Ok(openraft::raft::InstallSnapshotResponse::<NodeId> {
            vote: value.vote.ok_or(VoteSnafu.build())?.try_into()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                         VoteRequest<NodeId> <=> protobuf::VoteRequest                          //
////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<openraft::raft::VoteRequest<NodeId>> for protobuf::VoteRequest {
    fn from(value: openraft::raft::VoteRequest<NodeId>) -> protobuf::VoteRequest {
        protobuf::VoteRequest {
            vote: Some(value.vote.into()),
            last_log_id: value.last_log_id.map(|id| id.into()),
        }
    }
}

impl TryFrom<protobuf::VoteRequest> for openraft::raft::VoteRequest<NodeId> {
    type Error = Error;
    fn try_from(value: protobuf::VoteRequest) -> Result<openraft::raft::VoteRequest<NodeId>> {
        Ok(openraft::raft::VoteRequest::<NodeId> {
            vote: value.vote.ok_or(VoteSnafu.build())?.try_into()?,
            last_log_id: value.last_log_id.map(|id| id.try_into()).transpose()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                        VoteResponse<NodeId> <=> protobuf::VoteResponse                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<openraft::raft::VoteResponse<NodeId>> for protobuf::VoteResponse {
    fn from(value: openraft::raft::VoteResponse<NodeId>) -> protobuf::VoteResponse {
        protobuf::VoteResponse {
            vote: Some(value.vote.into()),
            vote_granted: value.vote_granted,
            last_log_id: value.last_log_id.map(|id| id.into()),
        }
    }
}

impl TryFrom<protobuf::VoteResponse> for openraft::raft::VoteResponse<NodeId> {
    type Error = Error;
    fn try_from(value: protobuf::VoteResponse) -> Result<openraft::raft::VoteResponse<NodeId>> {
        Ok(openraft::raft::VoteResponse::<NodeId> {
            vote: value.vote.ok_or(VoteSnafu.build())?.try_into()?,
            vote_granted: value.vote_granted,
            last_log_id: value.last_log_id.map(|id| id.try_into()).transpose()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                         Cache Requests                                         //
////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn try_into_cache_insert_request<K: Serialize, V: Serialize>(
    cache: CacheId,
    key: impl Into<K>,
    value: impl Into<V>,
) -> Result<protobuf::CacheInsertRequest> {
    Ok(protobuf::CacheInsertRequest {
        cache_id: cache,
        key: rmp_serde::to_vec(&key.into()).context(SerSnafu)?,
        value: rmp_serde::to_vec(&value.into()).context(SerSnafu)?,
    })
}

pub fn try_into_cache_lookup_request<K: Serialize>(
    cache_id: CacheId,
    key: impl Into<K>,
) -> Result<protobuf::CacheLookupRequest> {
    Ok(protobuf::CacheLookupRequest {
        cache_id,
        key: rmp_serde::to_vec(&key.into()).context(SerSnafu)?,
    })
}
