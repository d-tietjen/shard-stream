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
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use shard_stream_core::TopicPartition;

const CLAIM_DOMAIN: &[u8] = b"shard-stream.blossom.lease-claim.v1";
const GROUP_DOMAIN: &[u8] = b"shard-stream.blossom.lease-group.v1";
const STATE_MAGIC: &[u8; 8] = b"SSLEASE1";
const STATE_VERSION: u16 = 1;
const STATE_CHECKSUM_BYTES: usize = 32;
const MAX_STATE_BYTES: u64 = 64 * 1024 * 1024;
const LEASE_QUEUE_SLOTS: usize = 4_096;
const PERSISTENCE_QUEUE_SLOTS: usize = 256;
const MAX_LEASE_WORKERS: usize = 32;

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
    /// Return the latest lease after verifying the trusted Blossom epoch chain
    /// and exact validator generation. Returning stale state is an error.
    fn latest_lease(
        &self,
        group_id: [u8; 32],
        deadline: Duration,
    ) -> ControlResult<Option<FinalizedLease>>;

    fn commit_lease(
        &self,
        group_id: [u8; 32],
        claim: &LeaseClaim,
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
    config: Arc<LeaseCoordinatorConfig>,
    workers: Vec<SyncSender<LeaseCommand>>,
    worker_threads: Vec<JoinHandle<()>>,
    persistence: Option<PersistenceOwner>,
    lease_count: Arc<AtomicUsize>,
}

impl fmt::Debug for LeaseCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LeaseCoordinator")
            .field("config", &self.config)
            .field("worker_count", &self.workers.len())
            .field("lease_count", &self.lease_count.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

enum LeaseCommand {
    AcquireOrRenew {
        topic_partition: TopicPartition,
        leader_epoch: u64,
        expires_at_unix_ms: u64,
        response: SyncSender<ControlResult<FinalizedLease>>,
    },
    Lease {
        topic_partition: TopicPartition,
        response: SyncSender<Option<FinalizedLease>>,
    },
    Refresh {
        topic_partition: TopicPartition,
        response: SyncSender<ControlResult<Option<FinalizedLease>>>,
    },
    CheckWrite {
        topic_partition: TopicPartition,
        leader_epoch: u64,
        response: SyncSender<ControlResult<()>>,
    },
}

enum PersistenceCommand {
    Replace {
        topic_partition: TopicPartition,
        expected: Option<FinalizedLease>,
        finalized: FinalizedLease,
        response: SyncSender<ControlResult<()>>,
    },
}

#[derive(Clone)]
struct PersistenceHandle {
    sender: SyncSender<PersistenceCommand>,
}

impl PersistenceHandle {
    fn replace(
        &self,
        topic_partition: TopicPartition,
        expected: Option<FinalizedLease>,
        finalized: FinalizedLease,
    ) -> ControlResult<()> {
        let (response, receiver) = sync_channel(1);
        self.sender
            .send(PersistenceCommand::Replace {
                topic_partition,
                expected,
                finalized,
                response,
            })
            .map_err(|_| ControlError::CoordinatorStopped("lease persistence"))?;
        receive_control(receiver, "lease persistence")
    }
}

struct PersistenceOwner {
    sender: Option<SyncSender<PersistenceCommand>>,
    thread: Option<JoinHandle<()>>,
}

impl PersistenceOwner {
    fn spawn(
        config: Arc<LeaseCoordinatorConfig>,
        leases: BTreeMap<TopicPartition, FinalizedLease>,
        lease_count: Arc<AtomicUsize>,
    ) -> ControlResult<Self> {
        let (sender, receiver) = sync_channel(PERSISTENCE_QUEUE_SLOTS);
        let thread = thread::Builder::new()
            .name("shard-stream-lease-persistence".into())
            .spawn(move || run_persistence(config, leases, lease_count, receiver))
            .map_err(ControlError::Io)?;
        Ok(Self {
            sender: Some(sender),
            thread: Some(thread),
        })
    }

    fn handle(&self) -> PersistenceHandle {
        PersistenceHandle {
            sender: self
                .sender
                .as_ref()
                .expect("persistence owner is active")
                .clone(),
        }
    }
}

impl Drop for PersistenceOwner {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct LeaseWorkerState {
    config: Arc<LeaseCoordinatorConfig>,
    consensus: Arc<dyn BlossomLeaseConsensus>,
    leases: BTreeMap<TopicPartition, FinalizedLease>,
    persistence: PersistenceHandle,
}

impl LeaseCoordinator {
    pub fn open(
        config: LeaseCoordinatorConfig,
        consensus: Arc<dyn BlossomLeaseConsensus>,
    ) -> ControlResult<Self> {
        config.validate()?;
        let leases = load_state(&config)?;
        let lease_count = Arc::new(AtomicUsize::new(leases.len()));
        let config = Arc::new(config);
        let persistence = PersistenceOwner::spawn(
            Arc::clone(&config),
            leases.clone(),
            Arc::clone(&lease_count),
        )?;
        let worker_count = std::thread::available_parallelism()
            .map_or(1, usize::from)
            .min(MAX_LEASE_WORKERS)
            .min(config.max_leases)
            .max(1);
        let mut worker_leases = (0..worker_count)
            .map(|_| BTreeMap::new())
            .collect::<Vec<_>>();
        for (topic_partition, lease) in leases {
            let index = lease_worker_index(topic_partition, worker_count);
            worker_leases[index].insert(topic_partition, lease);
        }
        let mut workers = Vec::with_capacity(worker_count);
        let mut worker_threads = Vec::with_capacity(worker_count);
        for (index, leases) in worker_leases.into_iter().enumerate() {
            let (sender, receiver) = sync_channel(LEASE_QUEUE_SLOTS);
            let state = LeaseWorkerState {
                config: Arc::clone(&config),
                consensus: Arc::clone(&consensus),
                leases,
                persistence: persistence.handle(),
            };
            let thread = thread::Builder::new()
                .name(format!("shard-stream-lease-{index}"))
                .spawn(move || run_lease_worker(state, receiver))
                .map_err(ControlError::Io)?;
            workers.push(sender);
            worker_threads.push(thread);
        }
        Ok(Self {
            config,
            workers,
            worker_threads,
            persistence: Some(persistence),
            lease_count,
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
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            LeaseCommand::AcquireOrRenew {
                topic_partition,
                leader_epoch,
                expires_at_unix_ms,
                response,
            },
        )?;
        receive_control(receiver, "lease coordinator")
    }

    #[must_use]
    pub fn lease(&self, topic_partition: TopicPartition) -> Option<FinalizedLease> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            LeaseCommand::Lease {
                topic_partition,
                response,
            },
        )
        .ok()?;
        receiver.recv().ok().flatten()
    }

    /// Refresh a partition fence from the latest fully verified Blossom state.
    ///
    /// HA deployments call this from the lease-renewal/control loop so a
    /// former leader learns a newer fencing token before its local lease
    /// expires. Data replicas must independently reject lower epochs.
    pub fn refresh(
        &self,
        topic_partition: TopicPartition,
    ) -> ControlResult<Option<FinalizedLease>> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            LeaseCommand::Refresh {
                topic_partition,
                response,
            },
        )?;
        receive_control(receiver, "lease coordinator")
    }

    fn send(&self, topic_partition: TopicPartition, command: LeaseCommand) -> ControlResult<()> {
        let index = lease_worker_index(topic_partition, self.workers.len());
        self.workers[index]
            .send(command)
            .map_err(|_| ControlError::CoordinatorStopped("lease coordinator"))
    }
}

impl WriteFence for LeaseCoordinator {
    fn check_write(&self, topic_partition: TopicPartition, leader_epoch: u64) -> ControlResult<()> {
        let (response, receiver) = sync_channel(1);
        self.send(
            topic_partition,
            LeaseCommand::CheckWrite {
                topic_partition,
                leader_epoch,
                response,
            },
        )?;
        receive_control(receiver, "lease coordinator")
    }
}

impl Drop for LeaseCoordinator {
    fn drop(&mut self) {
        self.workers.clear();
        for thread in self.worker_threads.drain(..) {
            let _ = thread.join();
        }
        self.persistence.take();
    }
}

fn run_lease_worker(mut state: LeaseWorkerState, receiver: Receiver<LeaseCommand>) {
    while let Ok(command) = receiver.recv() {
        match command {
            LeaseCommand::AcquireOrRenew {
                topic_partition,
                leader_epoch,
                expires_at_unix_ms,
                response,
            } => {
                let result = acquire_or_renew_owned(
                    &mut state,
                    topic_partition,
                    leader_epoch,
                    expires_at_unix_ms,
                );
                let _ = response.send(result);
            }
            LeaseCommand::Lease {
                topic_partition,
                response,
            } => {
                let _ = response.send(state.leases.get(&topic_partition).cloned());
            }
            LeaseCommand::Refresh {
                topic_partition,
                response,
            } => {
                let result = refresh_owned(&mut state, topic_partition);
                let _ = response.send(result);
            }
            LeaseCommand::CheckWrite {
                topic_partition,
                leader_epoch,
                response,
            } => {
                let result = check_write_owned(&state, topic_partition, leader_epoch);
                let _ = response.send(result);
            }
        }
    }
}

fn acquire_or_renew_owned(
    state: &mut LeaseWorkerState,
    topic_partition: TopicPartition,
    leader_epoch: u64,
    expires_at_unix_ms: u64,
) -> ControlResult<FinalizedLease> {
    refresh_owned(state, topic_partition)?;
    let previous = state.leases.get(&topic_partition).cloned();
    validate_epoch_transition(previous.as_ref(), state.config.node_id, leader_epoch)?;
    let previous_epoch_hash = previous
        .as_ref()
        .map_or([0; 32], |lease| lease.certificate.epoch_hash);
    let claim = LeaseClaim {
        cluster_id: state.config.cluster_id,
        topic_partition,
        leader_id: state.config.node_id,
        leader_epoch,
        expires_at_unix_ms,
        previous_epoch_hash,
    };
    let certificate =
        state
            .consensus
            .commit_lease(claim.group_id(), &claim, state.config.consensus_deadline)?;
    validate_certificate(&claim, previous.as_ref(), &certificate)?;
    let finalized = FinalizedLease { claim, certificate };
    state
        .persistence
        .replace(topic_partition, previous, finalized.clone())?;
    state.leases.insert(topic_partition, finalized.clone());
    Ok(finalized)
}

fn refresh_owned(
    state: &mut LeaseWorkerState,
    topic_partition: TopicPartition,
) -> ControlResult<Option<FinalizedLease>> {
    let group_id = lease_group_id(state.config.cluster_id, topic_partition);
    let observed = state
        .consensus
        .latest_lease(group_id, state.config.consensus_deadline)?;
    let Some(observed) = observed else {
        return Ok(state.leases.get(&topic_partition).cloned());
    };
    if observed.claim.cluster_id != state.config.cluster_id
        || observed.claim.topic_partition != topic_partition
        || observed.certificate.group_id != group_id
    {
        return Err(ControlError::InvalidCertificate(
            "latest Blossom lease belongs to a different group".into(),
        ));
    }
    validate_certificate_shape(&observed.claim, &observed.certificate)?;

    let previous = state.leases.get(&topic_partition).cloned();
    if let Some(current) = &previous {
        if observed.certificate.epoch_nonce < current.certificate.epoch_nonce {
            return Err(ControlError::InvalidCertificate(
                "Blossom adapter returned stale lease state".into(),
            ));
        }
        if observed.certificate.epoch_nonce == current.certificate.epoch_nonce {
            if &observed != current {
                return Err(ControlError::InvalidCertificate(
                    "same Blossom epoch resolved to different lease state".into(),
                ));
            }
            return Ok(Some(current.clone()));
        }
    }
    state
        .persistence
        .replace(topic_partition, previous, observed.clone())?;
    state.leases.insert(topic_partition, observed.clone());
    Ok(Some(observed))
}

fn check_write_owned(
    state: &LeaseWorkerState,
    topic_partition: TopicPartition,
    leader_epoch: u64,
) -> ControlResult<()> {
    let lease = state
        .leases
        .get(&topic_partition)
        .ok_or(ControlError::NoLease(topic_partition))?;
    if lease.claim.leader_id != state.config.node_id || lease.claim.leader_epoch != leader_epoch {
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

fn run_persistence(
    config: Arc<LeaseCoordinatorConfig>,
    mut leases: BTreeMap<TopicPartition, FinalizedLease>,
    lease_count: Arc<AtomicUsize>,
    receiver: Receiver<PersistenceCommand>,
) {
    while let Ok(command) = receiver.recv() {
        match command {
            PersistenceCommand::Replace {
                topic_partition,
                expected,
                finalized,
                response,
            } => {
                let result = replace_persisted_lease(
                    &config,
                    &mut leases,
                    topic_partition,
                    expected.as_ref(),
                    finalized,
                );
                if result.is_ok() {
                    lease_count.store(leases.len(), Ordering::Relaxed);
                }
                let _ = response.send(result);
            }
        }
    }
}

fn replace_persisted_lease(
    config: &LeaseCoordinatorConfig,
    leases: &mut BTreeMap<TopicPartition, FinalizedLease>,
    topic_partition: TopicPartition,
    expected: Option<&FinalizedLease>,
    finalized: FinalizedLease,
) -> ControlResult<()> {
    if leases.get(&topic_partition) != expected {
        return Err(ControlError::ConcurrentLeaseChange);
    }
    if expected.is_none() && leases.len() >= config.max_leases {
        return Err(ControlError::LeaseLimitReached);
    }
    let mut next = leases.clone();
    next.insert(topic_partition, finalized);
    persist_state(config, &next)?;
    *leases = next;
    Ok(())
}

fn receive_control<T>(
    receiver: Receiver<ControlResult<T>>,
    component: &'static str,
) -> ControlResult<T> {
    receiver
        .recv()
        .map_err(|_| ControlError::CoordinatorStopped(component))?
}

fn lease_worker_index(topic_partition: TopicPartition, worker_count: usize) -> usize {
    let topic = topic_partition.topic_id.get();
    let folded =
        (topic as u64) ^ ((topic >> 64) as u64) ^ u64::from(topic_partition.partition_id.get());
    splitmix64(folded) as usize % worker_count
}

const fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
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
    validate_certificate_shape(claim, certificate)?;
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

fn validate_certificate_shape(
    claim: &LeaseClaim,
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
        validate_certificate_shape(&lease.claim, &lease.certificate)?;
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
    CoordinatorStopped(&'static str),
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
            Self::CoordinatorStopped(component) => {
                write!(formatter, "{component} stopped unexpectedly")
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

    enum TestConsensusCommand {
        Latest {
            group_id: [u8; 32],
            response: SyncSender<Option<FinalizedLease>>,
        },
        Commit {
            group_id: [u8; 32],
            claim: LeaseClaim,
            response: SyncSender<ControlResult<BlossomLeaseCertificate>>,
        },
    }

    struct TestConsensus {
        sender: Option<SyncSender<TestConsensusCommand>>,
        thread: Option<JoinHandle<()>>,
    }

    impl TestConsensus {
        fn new() -> Self {
            let (sender, receiver) = sync_channel(64);
            let thread = thread::spawn(move || run_test_consensus(receiver));
            Self {
                sender: Some(sender),
                thread: Some(thread),
            }
        }
    }

    impl Drop for TestConsensus {
        fn drop(&mut self) {
            self.sender.take();
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    impl BlossomLeaseConsensus for TestConsensus {
        fn latest_lease(
            &self,
            group_id: [u8; 32],
            _deadline: Duration,
        ) -> ControlResult<Option<FinalizedLease>> {
            let (response, receiver) = sync_channel(1);
            self.sender
                .as_ref()
                .expect("test consensus is active")
                .send(TestConsensusCommand::Latest { group_id, response })
                .map_err(|_| ControlError::Consensus("test consensus stopped".into()))?;
            receiver
                .recv()
                .map_err(|_| ControlError::Consensus("test consensus stopped".into()))
        }

        fn commit_lease(
            &self,
            group_id: [u8; 32],
            claim: &LeaseClaim,
            _deadline: Duration,
        ) -> ControlResult<BlossomLeaseCertificate> {
            let (response, receiver) = sync_channel(1);
            self.sender
                .as_ref()
                .expect("test consensus is active")
                .send(TestConsensusCommand::Commit {
                    group_id,
                    claim: claim.clone(),
                    response,
                })
                .map_err(|_| ControlError::Consensus("test consensus stopped".into()))?;
            receiver
                .recv()
                .map_err(|_| ControlError::Consensus("test consensus stopped".into()))?
        }
    }

    fn run_test_consensus(receiver: Receiver<TestConsensusCommand>) {
        let mut next_nonce = 1u64;
        let mut leases = BTreeMap::new();
        while let Ok(command) = receiver.recv() {
            match command {
                TestConsensusCommand::Latest { group_id, response } => {
                    let _ = response.send(leases.get(&group_id).cloned());
                }
                TestConsensusCommand::Commit {
                    group_id,
                    claim,
                    response,
                } => {
                    let result = commit_test_lease(&mut leases, &mut next_nonce, group_id, &claim);
                    let _ = response.send(result);
                }
            }
        }
    }

    fn commit_test_lease(
        leases: &mut BTreeMap<[u8; 32], FinalizedLease>,
        next_nonce: &mut u64,
        group_id: [u8; 32],
        claim: &LeaseClaim,
    ) -> ControlResult<BlossomLeaseCertificate> {
        let previous = leases.get(&group_id).cloned();
        let expected_previous = previous
            .as_ref()
            .map_or([0; 32], |lease| lease.certificate.epoch_hash);
        if claim.previous_epoch_hash != expected_previous {
            return Err(ControlError::Consensus(
                "claim does not extend the finalized lease".into(),
            ));
        }
        let nonce = *next_nonce;
        *next_nonce = next_nonce
            .checked_add(1)
            .ok_or_else(|| ControlError::Consensus("test epoch exhausted".into()))?;
        let mut epoch = blake3::Hasher::new();
        epoch.update(b"shard-stream.test-finalized-epoch.v1");
        epoch.update(&nonce.to_le_bytes());
        epoch.update(&claim.canonical_bytes());
        let certificate = BlossomLeaseCertificate {
            group_id,
            epoch_nonce: nonce,
            epoch_hash: *epoch.finalize().as_bytes(),
            claim_digest: claim.digest(),
            validator_generation: 1,
        };
        leases.insert(
            group_id,
            FinalizedLease {
                claim: claim.clone(),
                certificate: certificate.clone(),
            },
        );
        Ok(certificate)
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
    fn a_new_node_imports_latest_finality_and_fences_the_old_node() {
        let first_state = TempState::new();
        let second_state = TempState::new();
        let consensus: Arc<dyn BlossomLeaseConsensus> = Arc::new(TestConsensus::new());
        let first = LeaseCoordinator::open(config(first_state.0.clone()), Arc::clone(&consensus))
            .expect("first coordinator");
        first
            .acquire_or_renew(partition(), 1, future_expiry())
            .expect("first lease");

        let mut second_config = config(second_state.0.clone());
        second_config.node_id = NodeId(10);
        let second = LeaseCoordinator::open(second_config, Arc::clone(&consensus)).expect("second");
        second
            .acquire_or_renew(partition(), 2, future_expiry())
            .expect("transferred lease");
        second.check_write(partition(), 2).expect("new leader");

        first.refresh(partition()).expect("refresh old leader");
        assert!(matches!(
            first.check_write(partition(), 1),
            Err(ControlError::Fenced { .. })
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
