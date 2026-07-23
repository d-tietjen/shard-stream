//! Stream-native leader leases and write fencing.
//!
//! Blossom is deliberately kept behind [`BlossomLeaseConsensus`]. A concrete
//! adapter owns network transport, validator signatures, validator-generation
//! transitions, and trusted epoch-chain verification. This crate verifies that
//! the finalized certificate returned by that adapter is for the exact
//! canonical lease claim, links it to the previous finalized lease, persists
//! it before activation, and fails writes closed when the lease is absent,
//! stale, or expired.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use shard_stream_core::TopicPartition;

const CLAIM_DOMAIN: &[u8] = b"shard-stream.blossom.lease-claim.v1";
const GROUP_DOMAIN: &[u8] = b"shard-stream.blossom.lease-group.v1";
const STATE_MAGIC: &[u8; 8] = b"SSLEASE1";
const STATE_VERSION: u16 = 1;
const STATE_CHECKSUM_BYTES: usize = 32;
const MAX_STATE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClusterId(pub u128);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u128);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseClaim {
    pub cluster_id: ClusterId,
    pub topic_partition: TopicPartition,
    pub leader_id: NodeId,
    pub leader_epoch: u64,
    pub expires_at_unix_ms: u64,
    pub previous_epoch_hash: [u8; 32],
}

impl LeaseClaim {
    #[must_use]
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(CLAIM_DOMAIN.len() + 16 + 16 + 4 + 16 + 8 + 8 + 32);
        bytes.extend_from_slice(CLAIM_DOMAIN);
        bytes.extend_from_slice(&self.cluster_id.0.to_le_bytes());
        bytes.extend_from_slice(&self.topic_partition.topic_id.get().to_le_bytes());
        bytes.extend_from_slice(&self.topic_partition.partition_id.get().to_le_bytes());
        bytes.extend_from_slice(&self.leader_id.0.to_le_bytes());
        bytes.extend_from_slice(&self.leader_epoch.to_le_bytes());
        bytes.extend_from_slice(&self.expires_at_unix_ms.to_le_bytes());
        bytes.extend_from_slice(&self.previous_epoch_hash);
        bytes
    }

    #[must_use]
    pub fn digest(&self) -> [u8; 32] {
        *blake3::hash(&self.canonical_bytes()).as_bytes()
    }

    #[must_use]
    pub fn group_id(&self) -> [u8; 32] {
        lease_group_id(self.cluster_id, self.topic_partition)
    }
}

#[must_use]
pub fn lease_group_id(cluster_id: ClusterId, topic_partition: TopicPartition) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(GROUP_DOMAIN);
    hasher.update(&cluster_id.0.to_le_bytes());
    hasher.update(&topic_partition.topic_id.get().to_le_bytes());
    hasher.update(&topic_partition.partition_id.get().to_le_bytes());
    *hasher.finalize().as_bytes()
}

/// Proof that an exact lease claim was included in a finalized Blossom epoch.
///
/// The concrete adapter must verify validator signatures, an exact trusted
/// validator generation, the contiguous epoch hash chain, and claim inclusion
/// before constructing this value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlossomLeaseCertificate {
    pub group_id: [u8; 32],
    pub epoch_nonce: u64,
    pub epoch_hash: [u8; 32],
    pub claim_digest: [u8; 32],
    pub validator_generation: u64,
}

pub trait BlossomLeaseConsensus: Send + Sync {
    fn commit_lease(
        &self,
        group_id: [u8; 32],
        claim_payload: &[u8],
        deadline: Duration,
    ) -> ControlResult<BlossomLeaseCertificate>;
}

pub trait WriteFence: Send + Sync {
    fn check_write(&self, topic_partition: TopicPartition, leader_epoch: u64) -> ControlResult<()>;
}

#[derive(Debug, Clone)]
pub struct LeaseCoordinatorConfig {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub state_file: PathBuf,
    pub consensus_deadline: Duration,
    pub max_lease_duration: Duration,
    pub max_leases: usize,
}

impl LeaseCoordinatorConfig {
    fn validate(&self) -> ControlResult<()> {
        if self.consensus_deadline.is_zero()
            || self.max_lease_duration.is_zero()
            || self.max_leases == 0
        {
            return Err(ControlError::InvalidConfig(
                "consensus deadline, maximum lease duration, and lease bound must be nonzero"
                    .into(),
            ));
        }
        if self.max_leases > 1_000_000 {
            return Err(ControlError::InvalidConfig(
                "maximum lease count exceeds the hard safety bound".into(),
            ));
        }
        if self.state_file.as_os_str().is_empty() {
            return Err(ControlError::InvalidConfig(
                "lease state file cannot be empty".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedLease {
    pub claim: LeaseClaim,
    pub certificate: BlossomLeaseCertificate,
}

pub struct LeaseCoordinator {
    config: LeaseCoordinatorConfig,
    consensus: Arc<dyn BlossomLeaseConsensus>,
    leases: Mutex<BTreeMap<TopicPartition, FinalizedLease>>,
}

impl fmt::Debug for LeaseCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LeaseCoordinator")
            .field("config", &self.config)
            .field("lease_count", &lock(&self.leases).len())
            .finish_non_exhaustive()
    }
}

impl LeaseCoordinator {
    pub fn open(
        config: LeaseCoordinatorConfig,
        consensus: Arc<dyn BlossomLeaseConsensus>,
    ) -> ControlResult<Self> {
        config.validate()?;
        let leases = load_state(&config)?;
        Ok(Self {
            config,
            consensus,
            leases: Mutex::new(leases),
        })
    }

    /// Acquire a new fenced leadership epoch or renew the current local epoch.
    ///
    /// A different leader must use a strictly larger `leader_epoch`. A renewal
    /// may keep the same epoch but still creates a new finalized, hash-linked
    /// lease certificate.
    pub fn acquire_or_renew(
        &self,
        topic_partition: TopicPartition,
        leader_epoch: u64,
        expires_at_unix_ms: u64,
    ) -> ControlResult<FinalizedLease> {
        if leader_epoch == 0 {
            return Err(ControlError::InvalidClaim(
                "leader epoch must be nonzero".into(),
            ));
        }
        let now = unix_time_ms()?;
        let max_duration_ms = duration_ms(self.config.max_lease_duration)?;
        if expires_at_unix_ms <= now || expires_at_unix_ms.saturating_sub(now) > max_duration_ms {
            return Err(ControlError::InvalidClaim(
                "lease expiry is elapsed or exceeds the configured maximum".into(),
            ));
        }

        let previous = lock(&self.leases).get(&topic_partition).cloned();
        validate_epoch_transition(previous.as_ref(), self.config.node_id, leader_epoch)?;
        let previous_epoch_hash = previous
            .as_ref()
            .map_or([0; 32], |lease| lease.certificate.epoch_hash);
        let claim = LeaseClaim {
            cluster_id: self.config.cluster_id,
            topic_partition,
            leader_id: self.config.node_id,
            leader_epoch,
            expires_at_unix_ms,
            previous_epoch_hash,
        };
        let certificate = self.consensus.commit_lease(
            claim.group_id(),
            &claim.canonical_bytes(),
            self.config.consensus_deadline,
        )?;
        validate_certificate(&claim, previous.as_ref(), &certificate)?;
        let finalized = FinalizedLease { claim, certificate };

        let mut leases = lock(&self.leases);
        if leases.get(&topic_partition) != previous.as_ref() {
            return Err(ControlError::ConcurrentLeaseChange);
        }
        if previous.is_none() && leases.len() >= self.config.max_leases {
            return Err(ControlError::LeaseLimitReached);
        }
        let mut next = leases.clone();
        next.insert(topic_partition, finalized.clone());
        persist_state(&self.config, &next)?;
        *leases = next;
        Ok(finalized)
    }

    #[must_use]
    pub fn lease(&self, topic_partition: TopicPartition) -> Option<FinalizedLease> {
        lock(&self.leases).get(&topic_partition).cloned()
    }
}

impl WriteFence for LeaseCoordinator {
    fn check_write(&self, topic_partition: TopicPartition, leader_epoch: u64) -> ControlResult<()> {
        let leases = lock(&self.leases);
        let lease = leases
            .get(&topic_partition)
            .ok_or(ControlError::NoLease(topic_partition))?;
        if lease.claim.leader_id != self.config.node_id || lease.claim.leader_epoch != leader_epoch
        {
            return Err(ControlError::Fenced {
                expected_epoch: lease.claim.leader_epoch,
                received_epoch: leader_epoch,
            });
        }
        if lease.claim.expires_at_unix_ms <= unix_time_ms()? {
            return Err(ControlError::LeaseExpired);
        }
        Ok(())
    }
}

fn validate_epoch_transition(
    previous: Option<&FinalizedLease>,
    local_node: NodeId,
    proposed_epoch: u64,
) -> ControlResult<()> {
    let Some(previous) = previous else {
        return Ok(());
    };
    if previous.claim.leader_id == local_node {
        if proposed_epoch < previous.claim.leader_epoch {
            return Err(ControlError::InvalidClaim(
                "lease renewal cannot decrease the leader epoch".into(),
            ));
        }
    } else if proposed_epoch <= previous.claim.leader_epoch {
        return Err(ControlError::InvalidClaim(
            "a new leader must increase the leader epoch".into(),
        ));
    }
    Ok(())
}

fn validate_certificate(
    claim: &LeaseClaim,
    previous: Option<&FinalizedLease>,
    certificate: &BlossomLeaseCertificate,
) -> ControlResult<()> {
    if certificate.group_id != claim.group_id()
        || certificate.claim_digest != claim.digest()
        || certificate.epoch_nonce == 0
        || certificate.epoch_hash == [0; 32]
        || certificate.validator_generation == 0
    {
        return Err(ControlError::InvalidCertificate(
            "certificate does not finalize the exact lease claim".into(),
        ));
    }
    if let Some(previous) = previous {
        if claim.previous_epoch_hash != previous.certificate.epoch_hash
            || certificate.epoch_nonce <= previous.certificate.epoch_nonce
        {
            return Err(ControlError::InvalidCertificate(
                "certificate is not a monotonic continuation of the previous lease".into(),
            ));
        }
    } else if claim.previous_epoch_hash != [0; 32] {
        return Err(ControlError::InvalidCertificate(
            "first lease has a nonzero predecessor".into(),
        ));
    }
    Ok(())
}

fn persist_state(
    config: &LeaseCoordinatorConfig,
    leases: &BTreeMap<TopicPartition, FinalizedLease>,
) -> ControlResult<()> {
    let parent = config.state_file.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let mut bytes = Vec::with_capacity(64 + leases.len().saturating_mul(220));
    bytes.extend_from_slice(STATE_MAGIC);
    push_u16(&mut bytes, STATE_VERSION);
    push_u128(&mut bytes, config.cluster_id.0);
    push_u128(&mut bytes, config.node_id.0);
    push_u32(
        &mut bytes,
        u32::try_from(leases.len())
            .map_err(|_| ControlError::State("too many leases to persist".into()))?,
    );
    for lease in leases.values() {
        encode_lease(&mut bytes, lease);
    }
    bytes.extend_from_slice(blake3::hash(&bytes).as_bytes());
    if bytes.len() as u64 > MAX_STATE_BYTES {
        return Err(ControlError::State(
            "lease state exceeds the hard byte limit".into(),
        ));
    }

    let temporary = temporary_path(&config.state_file);
    let write_result = write_state_file(&temporary, &bytes);
    if let Err(error) = write_result {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    fs::rename(&temporary, &config.state_file)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn write_state_file(path: &Path, bytes: &[u8]) -> ControlResult<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn load_state(
    config: &LeaseCoordinatorConfig,
) -> ControlResult<BTreeMap<TopicPartition, FinalizedLease>> {
    let mut file = match File::open(&config.state_file) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(BTreeMap::new());
        }
        Err(error) => return Err(error.into()),
    };
    if file.metadata()?.len() > MAX_STATE_BYTES {
        return Err(ControlError::State(
            "lease state exceeds the hard byte limit".into(),
        ));
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    if bytes.len() < STATE_MAGIC.len() + 2 + 16 + 16 + 4 + STATE_CHECKSUM_BYTES {
        return Err(ControlError::State("lease state is truncated".into()));
    }
    let body_len = bytes.len() - STATE_CHECKSUM_BYTES;
    let (body, checksum) = bytes.split_at(body_len);
    if blake3::hash(body).as_bytes() != checksum {
        return Err(ControlError::State(
            "lease state BLAKE3 checksum mismatch".into(),
        ));
    }
    let mut decoder = Decoder::new(body);
    if decoder.take(STATE_MAGIC.len())? != STATE_MAGIC {
        return Err(ControlError::State("lease state magic mismatch".into()));
    }
    if decoder.u16()? != STATE_VERSION {
        return Err(ControlError::State(
            "unsupported lease state version".into(),
        ));
    }
    if ClusterId(decoder.u128()?) != config.cluster_id || NodeId(decoder.u128()?) != config.node_id
    {
        return Err(ControlError::State(
            "lease state belongs to a different cluster or node".into(),
        ));
    }
    let count = usize::try_from(decoder.u32()?)
        .map_err(|_| ControlError::State("lease count overflow".into()))?;
    if count > config.max_leases {
        return Err(ControlError::State(
            "persisted lease count exceeds the configured bound".into(),
        ));
    }
    let mut leases = BTreeMap::new();
    for _ in 0..count {
        let lease = decode_lease(&mut decoder, config.cluster_id)?;
        validate_certificate(&lease.claim, None, &lease.certificate).or_else(|error| {
            // On disk, a nonzero predecessor is expected after the first term.
            if lease.claim.previous_epoch_hash != [0; 32]
                && lease.certificate.group_id == lease.claim.group_id()
                && lease.certificate.claim_digest == lease.claim.digest()
                && lease.certificate.epoch_nonce > 0
                && lease.certificate.epoch_hash != [0; 32]
                && lease.certificate.validator_generation > 0
            {
                Ok(())
            } else {
                Err(error)
            }
        })?;
        if leases.insert(lease.claim.topic_partition, lease).is_some() {
            return Err(ControlError::State(
                "lease state contains a duplicate partition".into(),
            ));
        }
    }
    if !decoder.is_empty() {
        return Err(ControlError::State("lease state has trailing data".into()));
    }
    Ok(leases)
}

fn encode_lease(bytes: &mut Vec<u8>, lease: &FinalizedLease) {
    let claim = &lease.claim;
    push_u128(bytes, claim.topic_partition.topic_id.get());
    push_u32(bytes, claim.topic_partition.partition_id.get());
    push_u128(bytes, claim.leader_id.0);
    push_u64(bytes, claim.leader_epoch);
    push_u64(bytes, claim.expires_at_unix_ms);
    bytes.extend_from_slice(&claim.previous_epoch_hash);
    bytes.extend_from_slice(&lease.certificate.group_id);
    push_u64(bytes, lease.certificate.epoch_nonce);
    bytes.extend_from_slice(&lease.certificate.epoch_hash);
    bytes.extend_from_slice(&lease.certificate.claim_digest);
    push_u64(bytes, lease.certificate.validator_generation);
}

fn decode_lease(decoder: &mut Decoder<'_>, cluster_id: ClusterId) -> ControlResult<FinalizedLease> {
    let topic_partition = TopicPartition::new(
        shard_stream_core::TopicId::new(decoder.u128()?),
        shard_stream_core::LogicalPartitionId::new(decoder.u32()?),
    );
    let claim = LeaseClaim {
        cluster_id,
        topic_partition,
        leader_id: NodeId(decoder.u128()?),
        leader_epoch: decoder.u64()?,
        expires_at_unix_ms: decoder.u64()?,
        previous_epoch_hash: decoder.array()?,
    };
    let certificate = BlossomLeaseCertificate {
        group_id: decoder.array()?,
        epoch_nonce: decoder.u64()?,
        epoch_hash: decoder.array()?,
        claim_digest: decoder.array()?,
        validator_generation: decoder.u64()?,
    };
    Ok(FinalizedLease { claim, certificate })
}

fn temporary_path(path: &Path) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("leases");
    path.with_file_name(format!(".{name}.tmp-{}-{nonce}", std::process::id()))
}

fn unix_time_ms() -> ControlResult<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ControlError::Clock)?;
    u64::try_from(duration.as_millis()).map_err(|_| ControlError::Clock)
}

fn duration_ms(duration: Duration) -> ControlResult<u64> {
    u64::try_from(duration.as_millis())
        .map_err(|_| ControlError::InvalidConfig("maximum lease duration is too large".into()))
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn push_u16(bytes: &mut Vec<u8>, value: u16) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_u128(bytes: &mut Vec<u8>, value: u128) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

struct Decoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> Decoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn take(&mut self, count: usize) -> ControlResult<&'a [u8]> {
        let end = self
            .cursor
            .checked_add(count)
            .ok_or_else(|| ControlError::State("lease state offset overflow".into()))?;
        let value = self
            .bytes
            .get(self.cursor..end)
            .ok_or_else(|| ControlError::State("lease state is truncated".into()))?;
        self.cursor = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> ControlResult<[u8; N]> {
        self.take(N)?
            .try_into()
            .map_err(|_| ControlError::State("lease state array is truncated".into()))
    }

    fn u16(&mut self) -> ControlResult<u16> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    fn u32(&mut self) -> ControlResult<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> ControlResult<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn u128(&mut self) -> ControlResult<u128> {
        Ok(u128::from_le_bytes(self.array()?))
    }

    const fn is_empty(&self) -> bool {
        self.cursor == self.bytes.len()
    }
}

pub type ControlResult<T> = Result<T, ControlError>;

#[derive(Debug)]
pub enum ControlError {
    InvalidConfig(String),
    InvalidClaim(String),
    InvalidCertificate(String),
    NoLease(TopicPartition),
    Fenced {
        expected_epoch: u64,
        received_epoch: u64,
    },
    LeaseExpired,
    LeaseLimitReached,
    ConcurrentLeaseChange,
    Clock,
    State(String),
    Io(std::io::Error),
    Consensus(String),
}

impl fmt::Display for ControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(formatter, "invalid control config: {message}"),
            Self::InvalidClaim(message) => write!(formatter, "invalid lease claim: {message}"),
            Self::InvalidCertificate(message) => {
                write!(formatter, "invalid Blossom lease certificate: {message}")
            }
            Self::NoLease(partition) => write!(
                formatter,
                "no finalized lease for {}/{}",
                partition.topic_id, partition.partition_id
            ),
            Self::Fenced {
                expected_epoch,
                received_epoch,
            } => write!(
                formatter,
                "writer is fenced: current epoch {expected_epoch}, received {received_epoch}"
            ),
            Self::LeaseExpired => formatter.write_str("writer lease has expired"),
            Self::LeaseLimitReached => formatter.write_str("lease state capacity reached"),
            Self::ConcurrentLeaseChange => {
                formatter.write_str("lease changed while finality was pending")
            }
            Self::Clock => formatter.write_str("system clock is before or exceeds Unix time"),
            Self::State(message) => write!(formatter, "invalid lease state: {message}"),
            Self::Io(error) => error.fmt(formatter),
            Self::Consensus(message) => write!(formatter, "Blossom consensus failed: {message}"),
        }
    }
}

impl Error for ControlError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ControlError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use shard_stream_core::{LogicalPartitionId, TopicId};

    use super::*;

    static NEXT_TEMP: AtomicU64 = AtomicU64::new(1);

    struct TestConsensus {
        next_nonce: AtomicU64,
    }

    impl TestConsensus {
        fn new() -> Self {
            Self {
                next_nonce: AtomicU64::new(1),
            }
        }
    }

    impl BlossomLeaseConsensus for TestConsensus {
        fn commit_lease(
            &self,
            group_id: [u8; 32],
            claim_payload: &[u8],
            _deadline: Duration,
        ) -> ControlResult<BlossomLeaseCertificate> {
            let nonce = self.next_nonce.fetch_add(1, Ordering::SeqCst);
            let mut epoch = blake3::Hasher::new();
            epoch.update(b"shard-stream.test-finalized-epoch.v1");
            epoch.update(&nonce.to_le_bytes());
            epoch.update(claim_payload);
            Ok(BlossomLeaseCertificate {
                group_id,
                epoch_nonce: nonce,
                epoch_hash: *epoch.finalize().as_bytes(),
                claim_digest: *blake3::hash(claim_payload).as_bytes(),
                validator_generation: 1,
            })
        }
    }

    struct TempState(PathBuf);

    impl TempState {
        fn new() -> Self {
            let nonce = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
            let directory = std::env::temp_dir().join(format!(
                "shard-stream-control-{}-{nonce}",
                std::process::id()
            ));
            fs::create_dir_all(&directory).expect("temp directory");
            Self(directory.join("leases.state"))
        }
    }

    impl Drop for TempState {
        fn drop(&mut self) {
            if let Some(parent) = self.0.parent() {
                let _ = fs::remove_dir_all(parent);
            }
        }
    }

    fn config(path: PathBuf) -> LeaseCoordinatorConfig {
        LeaseCoordinatorConfig {
            cluster_id: ClusterId(7),
            node_id: NodeId(9),
            state_file: path,
            consensus_deadline: Duration::from_secs(1),
            max_lease_duration: Duration::from_secs(60),
            max_leases: 16,
        }
    }

    fn partition() -> TopicPartition {
        TopicPartition::new(TopicId::new(42), LogicalPartitionId::new(3))
    }

    fn future_expiry() -> u64 {
        unix_time_ms().expect("time") + 30_000
    }

    #[test]
    fn finalized_lease_fences_wrong_epochs_and_survives_restart() {
        let state = TempState::new();
        let coordinator =
            LeaseCoordinator::open(config(state.0.clone()), Arc::new(TestConsensus::new()))
                .expect("coordinator");
        let first = coordinator
            .acquire_or_renew(partition(), 4, future_expiry())
            .expect("lease");
        assert_eq!(first.claim.digest(), first.certificate.claim_digest);
        coordinator.check_write(partition(), 4).expect("write");
        assert!(matches!(
            coordinator.check_write(partition(), 3),
            Err(ControlError::Fenced { .. })
        ));
        drop(coordinator);

        let recovered =
            LeaseCoordinator::open(config(state.0.clone()), Arc::new(TestConsensus::new()))
                .expect("recovered");
        recovered
            .check_write(partition(), 4)
            .expect("recovered write");
        assert_eq!(
            recovered.lease(partition()).expect("lease").certificate,
            first.certificate
        );
    }

    #[test]
    fn renewals_are_hash_linked_and_monotonic() {
        let state = TempState::new();
        let coordinator =
            LeaseCoordinator::open(config(state.0.clone()), Arc::new(TestConsensus::new()))
                .expect("coordinator");
        let first = coordinator
            .acquire_or_renew(partition(), 2, future_expiry())
            .expect("first");
        let renewed = coordinator
            .acquire_or_renew(partition(), 2, future_expiry())
            .expect("renewed");
        assert_eq!(
            renewed.claim.previous_epoch_hash,
            first.certificate.epoch_hash
        );
        assert!(renewed.certificate.epoch_nonce > first.certificate.epoch_nonce);
        assert!(matches!(
            coordinator.acquire_or_renew(partition(), 1, future_expiry()),
            Err(ControlError::InvalidClaim(_))
        ));
    }

    #[test]
    fn corrupt_persisted_state_fails_closed() {
        let state = TempState::new();
        let coordinator =
            LeaseCoordinator::open(config(state.0.clone()), Arc::new(TestConsensus::new()))
                .expect("coordinator");
        coordinator
            .acquire_or_renew(partition(), 1, future_expiry())
            .expect("lease");
        drop(coordinator);

        let mut bytes = fs::read(&state.0).expect("state");
        bytes[20] ^= 0x80;
        fs::write(&state.0, bytes).expect("corrupt");
        assert!(matches!(
            LeaseCoordinator::open(config(state.0.clone()), Arc::new(TestConsensus::new())),
            Err(ControlError::State(_))
        ));
    }

    #[test]
    fn canonical_claim_is_stable_and_group_is_partition_scoped() {
        let claim = LeaseClaim {
            cluster_id: ClusterId(1),
            topic_partition: partition(),
            leader_id: NodeId(2),
            leader_epoch: 3,
            expires_at_unix_ms: 4,
            previous_epoch_hash: [5; 32],
        };
        assert_eq!(
            claim.digest(),
            *blake3::hash(&claim.canonical_bytes()).as_bytes()
        );
        let other = TopicPartition::new(TopicId::new(42), LogicalPartitionId::new(4));
        assert_ne!(claim.group_id(), lease_group_id(claim.cluster_id, other));
    }
}
