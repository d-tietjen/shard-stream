//! Real source WAL and guarded receiver coordination, not TLS or HA authority.
//! The bounded neutral fence supplies test context; production sender/issuer,
//! current remote lease and historical replay permission remain host contracts.

use super::*;
use crate::{DurableApplyConfig, DurableApplyQueue};
use shard_stream_core::{PhysicalWriteGuard, WriteFenceError};

#[derive(Debug, Default)]
struct ReplicaAdmission {
    inner: FixturePendingAdmission,
    pause_before_physical: Mutex<Option<Arc<ProposalPause>>>,
}

impl PendingWriteAdmission for ReplicaAdmission {
    fn validate_before_reserve(&self) -> Result<(), WriteFenceError> {
        self.inner.validate_before_reserve()
    }

    fn try_admit_physical(
        &self,
        reservation: &Reservation,
    ) -> Result<Box<dyn PhysicalWriteGuard>, WriteFenceError> {
        let pause = self
            .pause_before_physical
            .lock()
            .expect("finite pause")
            .clone();
        if let Some(pause) = pause {
            pause.entered.store(true, Ordering::Release);
            let mut released = pause.released.lock().expect("physical release");
            while !*released {
                released = pause.changed.wait(released).expect("physical barrier");
            }
        }
        self.inner.try_admit_physical(reservation)
    }
}

#[derive(Debug)]
struct ReplicaFence(Arc<ReplicaAdmission>);

impl WriteFence for ReplicaFence {
    fn check_write(&self, partition: TopicPartition, epoch: u64) -> Result<(), WriteFenceError> {
        ExactEpochFence.check_write(partition, epoch)
    }

    fn requires_physical_admission(&self) -> bool {
        true
    }

    fn prepare_replica_write(
        &self,
        partition: TopicPartition,
        epoch: u64,
        source: &ReplicaSourceIdentity,
    ) -> Result<Option<Arc<dyn PendingWriteAdmission>>, WriteFenceError> {
        self.check_write(partition, epoch)?;
        if source != &fixture_source() {
            return Err(WriteFenceError::new(
                "neutral fixture current source mismatch",
            ));
        }
        Ok(Some(self.0.clone()))
    }
}

fn fixture_source() -> ReplicaSourceIdentity {
    ReplicaSourceIdentity {
        broker_id: shard_stream_core::BrokerId::new(2),
        incarnation: 1,
        credential_fingerprint: [3; 32],
    }
}

fn topic() -> TopicConfig {
    TopicConfig {
        topic_id: TopicId::new(1),
        partitions: 1,
        shards: None,
    }
}

fn partition() -> TopicPartition {
    TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0))
}

/// Obtain every replica field from actual checksummed leader WAL, including
/// unresolved atomic chunks, without constructing synthetic reservations.
fn source_batch(path: &std::path::Path, atomic: bool) -> ReplicaAppend {
    let leader = StreamEngine::open_with_fence(config(path), Arc::new(ExactEpochFence))
        .expect("actual source engine");
    leader.create_topic(topic()).expect("actual source topic");
    let mut request = append_request(
        1,
        b"exact remote producer and atomic bytes",
        Some(ProducerIdentity {
            producer_id: 41,
            epoch: 1,
            first_sequence: 0,
            event_id: None,
        }),
    );
    request.leader_epoch = Some(7);
    if atomic {
        request.atomic_group = Some(AtomicGroupIdentity {
            transaction_id: 91,
            chunk_sequence: 0,
        });
    }
    leader
        .append(request)
        .expect("actual durable source append");
    let stored = leader
        .fetch_stored_internal(fetch_request(0), true)
        .expect("checksummed source WAL")
        .pop()
        .expect("one actual source batch");
    replica_from_stored(stored)
}

fn replica_from_stored(stored: StoredBatch) -> ReplicaAppend {
    ReplicaAppend {
        reservation: stored.reservation,
        producer: stored.producer.map(|producer| ProducerIdentity {
            producer_id: producer.producer_id,
            epoch: producer.epoch,
            first_sequence: producer.first_sequence,
            event_id: producer.event_id,
        }),
        atomic_group: stored.atomic_group.map(|group| AtomicGroupIdentity {
            transaction_id: group.transaction_id,
            chunk_sequence: group.chunk_sequence,
        }),
        extension_context: None,
        payload_digest: stored
            .producer
            .map(|producer| producer.payload_digest)
            .or_else(|| stored.atomic_group.map(|group| group.payload_digest))
            .unwrap_or([0; 16]),
        payload: stored.payload,
    }
}

fn assert_receipt_coordinates(
    actual: AppendResponse,
    expected: AppendResponse,
    stable: LogicalOffset,
) {
    assert_eq!(
        (
            actual.request_id,
            actual.batch_id,
            actual.record_id,
            actual.first_offset,
            actual.last_offset,
            actual.placement
        ),
        (
            expected.request_id,
            expected.batch_id,
            expected.record_id,
            expected.first_offset,
            expected.last_offset,
            expected.placement
        )
    );
    // The existing response watermark is conservative live state, not identity.
    assert!(actual.visible_through <= stable);
}

fn new_receiver(path: &std::path::Path, admission: &Arc<ReplicaAdmission>) -> StreamEngine {
    let receiver =
        StreamEngine::open_with_fence(config(path), Arc::new(ReplicaFence(admission.clone())))
            .expect("actual guarded receiver");
    receiver
        .create_topic(topic())
        .expect("actual receiver topic");
    receiver
}

fn prepare(
    receiver: &StreamEngine,
    batch: &ReplicaAppend,
) -> (PreparedReplica, Arc<dyn PendingWriteAdmission>) {
    match receiver
        .plan_replica_append_from_peer(batch, Some(&fixture_source()))
        .expect("pure exact remote preparation")
    {
        ReplicaAppendPlan::Prepared {
            prepared,
            admission,
        } => (*prepared, admission),
        _ => panic!("fresh remote input has no existing receipt or receiver reservation"),
    }
}

fn assert_unallocated(
    receiver: &StreamEngine,
    before: PartitionWatermarks,
    epochs: PartitionEpochs,
    atomic: bool,
) {
    assert_eq!(
        receiver
            .watermarks(partition())
            .expect("actual receiver frontier"),
        before
    );
    assert_eq!(
        receiver
            .partition_epochs(partition())
            .expect("actual receiver epochs"),
        epochs
    );
    assert!(
        receiver
            .producer_progress(partition(), 41)
            .expect("actual producer cursor")
            .is_none()
    );
    if atomic {
        assert!(matches!(
            receiver.inspect_atomic_group(partition(), 91),
            Err(EngineError::UnknownAtomicGroup(91))
        ));
    }
    assert!(
        receiver
            .fetch_stored_internal(fetch_request(0), true)
            .expect("actual receiver WAL")
            .is_empty()
    );
}

#[test]
fn guarded_replica_refusal_preserves_source_coordinates_and_later_real_sink_replay() {
    for atomic in [false, true] {
        let directory = TempDir::new("guarded-replica-refusal-replay");
        let batch = source_batch(&directory.0.join("leader"), atomic);
        let admission = Arc::new(ReplicaAdmission::default());
        let control = Arc::new(RecordingSinkControl::default());
        let limits = config(&directory.0.join("receiver"));
        let receiver = Arc::new(
            StreamEngine::open_with_all_components_and_durable_sink(
                limits.clone(),
                Some(Arc::new(ReplicaFence(admission.clone()))),
                None,
                None,
                Some(recording_sink_config(
                    &control,
                    DurableSinkOptions::default(),
                )),
            )
            .expect("actual guarded receiver with real checkpointed derivative"),
        );
        receiver.create_topic(topic()).expect("receiver topic");
        let queue = DurableApplyQueue::spawn(
            receiver.clone(),
            DurableApplyConfig {
                worker_count: 1,
                queue_slots_per_worker: 4,
                ..DurableApplyConfig::default()
            },
        )
        .expect("actual bounded receiver queue");
        let before = receiver.watermarks(partition()).expect("initial frontier");
        let epochs = receiver
            .partition_epochs(partition())
            .expect("initial epochs");
        let checkpoint = receiver
            .durable_sink_checkpoint(partition())
            .expect("actual initial checkpoint");
        for early in [true, false] {
            admission
                .inner
                .refuse_before_reserve
                .store(early, Ordering::Release);
            admission
                .inner
                .refuse_physical
                .store(!early, Ordering::Release);
            assert!(matches!(
                queue.append_from_peer(batch.clone(), fixture_source()),
                Err(EngineError::Fenced(_))
            ));
            assert_unallocated(&receiver, before, epochs, atomic);
            assert_eq!(
                receiver
                    .durable_sink_checkpoint(partition())
                    .expect("refused derivative"),
                checkpoint
            );
        }
        admission
            .inner
            .refuse_physical
            .store(false, Ordering::Release);
        let response = queue
            .append_from_peer(batch.clone(), fixture_source())
            .expect("same exact remote input remains eligible");
        assert_eq!(response.placement, batch.reservation.placement);
        assert_eq!(response.first_offset, batch.reservation.first_offset);
        let wal = receiver
            .fetch_stored_internal(fetch_request(0), true)
            .expect("actual receiver WAL");
        assert_eq!(wal.len(), 1);
        assert_eq!(wal[0].reservation, batch.reservation);
        assert_eq!(wal[0].payload, batch.payload);
        let admitted = admission.inner.physical_admissions.load(Ordering::Acquire);
        assert_receipt_coordinates(
            queue
                .append_from_peer(batch.clone(), fixture_source())
                .expect("exact immutable retry"),
            response,
            receiver
                .watermarks(partition())
                .expect("actual stable frontier")
                .last_stable_offset,
        );
        assert_eq!(
            admission.inner.physical_admissions.load(Ordering::Acquire),
            admitted
        );
        assert_eq!(
            receiver
                .durable_sink_checkpoint(partition())
                .expect("replica is index-silent"),
            checkpoint
        );
        receiver
            .resume_durable_sink(&[partition()])
            .expect("actual supported retained-WAL replay");
        wait_for_test(|| {
            receiver
                .durable_sink_checkpoint(partition())
                .expect("real checkpoint")
                .is_some_and(|checkpoint| checkpoint.next_offset == LogicalOffset::new(1))
        });
        let indexed = control.state.lock().expect("actual derivative state");
        assert_eq!(indexed.appends.len(), 1);
        assert_eq!(indexed.appends[0].reservation, batch.reservation);
        assert_eq!(indexed.appends[0].payload, batch.payload);
        drop(indexed);
        drop(queue);
        drop(receiver);
        let reopened = StreamEngine::open_with_durable_sink(
            limits,
            recording_sink_config(&control, DurableSinkOptions::default()),
        )
        .expect("actual cold receiver recovery");
        assert_eq!(
            reopened
                .fetch_stored_internal(fetch_request(0), true)
                .expect("checksummed recovered WAL"),
            wal
        );
        assert_eq!(
            reopened
                .durable_sink_checkpoint(partition())
                .expect("recovered derivative"),
            Some(DurableSinkCheckpoint {
                topic_partition: partition(),
                next_offset: LogicalOffset::new(1),
                next_placement_sequence: PlacementSequence::new(1)
            })
        );
    }
}

#[test]
fn guarded_replica_queued_revocation_and_caller_cancel_change_no_receiver_state() {
    for cancel in [false, true] {
        let directory = TempDir::new("guarded-replica-queued-refusal");
        let batch = source_batch(&directory.0.join("leader"), true);
        let admission = Arc::new(ReplicaAdmission::default());
        let pause = Arc::new(ProposalPause::default());
        *admission
            .pause_before_physical
            .lock()
            .expect("finite pause installation") = Some(pause.clone());
        let receiver = new_receiver(&directory.0.join("receiver"), &admission);
        // Release the actual worker before receiver Drop, including assertion unwind.
        let release = ReleaseProposal(pause.clone());
        let before = receiver.watermarks(partition()).expect("initial frontier");
        let epochs = receiver
            .partition_epochs(partition())
            .expect("initial epochs");
        let (prepared, ticket_admission) = prepare(&receiver, &batch);
        let mut ticket = Some(
            receiver
                .shard_for_lane(batch.reservation.placement.virtual_lane_id)
                .expect("actual receiver shard")
                .begin_prepared_replica(
                    prepared,
                    batch.payload.clone(),
                    ticket_admission,
                    receiver._replication_progress.clone(),
                )
                .expect("actual charged receiver queue"),
        );
        wait_for_test(|| pause.entered.load(Ordering::Acquire));
        assert_eq!(
            receiver.watermarks(partition()).expect("queued frontier"),
            before
        );
        assert_eq!(
            receiver
                .partition_epochs(partition())
                .expect("queued epoch"),
            epochs
        );
        if cancel {
            drop(ticket.take());
        } else {
            admission
                .inner
                .refuse_physical
                .store(true, Ordering::Release);
        }
        drop(release);
        if let Some(ticket) = ticket {
            assert!(matches!(
                ticket.wait(),
                ProposedAppendOutcome::Unallocated(EngineError::Fenced(_))
            ));
        }
        // Fetch runs on the same physical queue after the barrier has settled.
        assert_unallocated(&receiver, before, epochs, true);
        assert_eq!(admission.inner.active.load(Ordering::Acquire), 0);
        admission
            .pause_before_physical
            .lock()
            .expect("remove settled pause")
            .take();
        admission
            .inner
            .refuse_physical
            .store(false, Ordering::Release);
        let plan = receiver
            .plan_replica_append_from_peer(&batch, Some(&fixture_source()))
            .expect("fresh same-input preparation");
        assert_eq!(
            receiver
                .execute_replica_append(batch.clone(), plan)
                .expect("later actual progress")
                .placement,
            batch.reservation.placement
        );
        assert_eq!(
            receiver
                .producer_progress(partition(), 41)
                .expect("unpoisoned producer")
                .expect("real producer")
                .next_sequence,
            1
        );
    }
}

#[test]
fn guarded_replica_compare_conflict_changes_no_epoch_producer_or_atomic_state() {
    let directory = TempDir::new("guarded-replica-compare-conflict");
    let batch = source_batch(&directory.0.join("leader"), true);
    let admission = Arc::new(ReplicaAdmission::default());
    let receiver = new_receiver(&directory.0.join("receiver"), &admission);
    let before = receiver.watermarks(partition()).expect("initial frontier");
    let epochs = receiver
        .partition_epochs(partition())
        .expect("initial epochs");
    let (prepared, ticket_admission) = prepare(&receiver, &batch);
    receiver
        .update_topic_replication_policy(
            TopicId::new(1),
            1,
            PartitionReplicationPolicy {
                policy_generation: 2,
                replication_factor: 1,
                min_in_sync_replicas: 1,
            },
        )
        .expect("genuine competing coordinator policy mutation");
    let ticket = receiver
        .shard_for_lane(batch.reservation.placement.virtual_lane_id)
        .expect("receiver shard")
        .begin_prepared_replica(
            prepared,
            batch.payload.clone(),
            ticket_admission,
            receiver._replication_progress.clone(),
        )
        .expect("actual charged stale input");
    assert!(matches!(
        ticket.wait(),
        ProposedAppendOutcome::Unallocated(EngineError::Sequencer(
            SequencerError::ReservationProposalChanged
        ))
    ));
    assert_unallocated(&receiver, before, epochs, true);
    assert_eq!(admission.inner.active.load(Ordering::Acquire), 0);
    let plan = receiver
        .plan_replica_append_from_peer(&batch, Some(&fixture_source()))
        .expect("fresh same-input snapshot");
    receiver
        .execute_replica_append(batch.clone(), plan)
        .expect("later fitting remote commit");
    assert_eq!(
        receiver
            .fetch_stored_internal(fetch_request(0), true)
            .expect("actual receiver data")[0]
            .reservation,
        batch.reservation
    );
}

#[test]
fn guarded_replica_duplicate_requires_current_source_and_exact_received_identity() {
    let directory = TempDir::new("guarded-replica-duplicate");
    let batch = source_batch(&directory.0.join("leader"), false);
    let admission = Arc::new(ReplicaAdmission::default());
    let receiver = Arc::new(new_receiver(&directory.0.join("receiver"), &admission));
    let queue = DurableApplyQueue::spawn(
        receiver.clone(),
        DurableApplyConfig {
            worker_count: 1,
            queue_slots_per_worker: 4,
            ..DurableApplyConfig::default()
        },
    )
    .expect("actual receiver queue");
    let response = queue
        .append_from_peer(batch.clone(), fixture_source())
        .expect("actual first delivery");
    let leader = StreamEngine::open_with_fence(
        config(&directory.0.join("leader")),
        Arc::new(ExactEpochFence),
    )
    .expect("reopen genuine source and its producer cursor");
    let mut next = append_request(
        2,
        b"next actual remote batch",
        Some(ProducerIdentity {
            producer_id: 41,
            epoch: 1,
            first_sequence: 1,
            event_id: None,
        }),
    );
    next.leader_epoch = Some(7);
    leader.append(next).expect("actual later source WAL");
    let next = replica_from_stored(
        leader
            .fetch_stored(fetch_request(1))
            .expect("checksummed later source WAL")
            .pop()
            .expect("one later actual batch"),
    );
    let progressed = queue
        .append_from_peer(next.clone(), fixture_source())
        .expect("ordering cursor follows actual first commit");
    assert_eq!(progressed.first_offset, LogicalOffset::new(1));
    assert_eq!(progressed.placement, next.reservation.placement);
    let before = receiver.watermarks(partition()).expect("durable frontier");
    let admissions = admission.inner.physical_admissions.load(Ordering::Acquire);
    let mut wrong_source = fixture_source();
    wrong_source.incarnation += 1;
    assert!(matches!(
        queue.append_from_peer(batch.clone(), wrong_source),
        Err(EngineError::Fenced(_))
    ));
    let mut changed = batch.clone();
    changed.payload_digest = [8; 16];
    assert!(matches!(
        queue.append_from_peer(changed, fixture_source()),
        Err(EngineError::CorruptState(_))
    ));
    assert_receipt_coordinates(
        queue
            .append_from_peer(batch.clone(), fixture_source())
            .expect("current-source exact retry"),
        response,
        before.last_stable_offset,
    );
    assert_eq!(
        admission.inner.physical_admissions.load(Ordering::Acquire),
        admissions
    );
    assert_eq!(
        receiver
            .watermarks(partition())
            .expect("unchanged retry frontier"),
        before
    );
    assert_eq!(
        receiver
            .fetch_stored(fetch_request(0))
            .expect("actual immutable WAL")
            .len(),
        2
    );
}
