use bytes::Bytes;
use shard_stream_core::Reservation;

use crate::{AtomicGroupIdentity, ProducerIdentity};

/// Transport-neutral immutable batch passed to a durability extension.
///
/// This is an extension contract, not an interbroker wire format. A composing
/// distribution owns framing, authentication, admission, and transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaAppend {
    pub reservation: Reservation,
    pub producer: Option<ProducerIdentity>,
    pub atomic_group: Option<AtomicGroupIdentity>,
    /// Opaque context owned by the injected replication implementation.
    pub extension_context: Option<Bytes>,
    pub payload_digest: [u8; 16],
    pub payload: Bytes,
}
