//! Strict delegates for the actual native durable-sink replay algorithm.

use super::*;

struct StrictSink<'a>(&'a mut Scope);

impl StrictSink<'_> {
    fn cache(
        &mut self,
        operation: NativeResumeSinkCacheOperation,
    ) -> Result<(), NativeResumeFailure> {
        self.0.work.reserve_outcomes(1)?;
        let bytes =
            size_of::<NativeResumeObservation<()>>() + size_of::<NativeResumeSinkCacheOperation>();
        self.0.work.reserve_values(bytes, 0, 2, bytes)?;
        self.0.work.check()?;
        let dispatcher = self
            .0
            .owner
            .durable_sink
            .as_ref()
            .ok_or(NativeResumeFailure::MissingSink)?;
        let observed = dispatcher.resume_cache(operation, &self.0.work);
        let accepted = observed.result.is_ok() && observed.refusal.is_none();
        // Retain the actual cache operation before any later clock observation.
        self.0.outcomes.push(NativeResumeOutcome::SinkCache {
            operation,
            observed,
        });
        self.0.work.check()?;
        if accepted {
            Ok(())
        } else {
            Err(NativeResumeFailure::Callback)
        }
    }
}

impl SinkReplay for StrictSink<'_> {
    type Error = NativeResumeFailure;

    fn before_partition(&mut self) -> Result<(), Self::Error> {
        self.0.work.charge(Debit {
            partitions: 1,
            ..Debit::default()
        })?;
        // These three diagnostics contain only bounded native numeric displays.
        let text = "checkpoint offset  for / is behind retained log start ".len()
            + "checkpoint offset  for / is ahead of durable log end ".len()
            + "durable sink recovery found no batch at offset  for /".len();
        let numbers = 3
            * (2 * (u64::MAX.ilog10() as usize + 1)
                + (u128::MAX.ilog10() as usize + 1)
                + (u32::MAX.ilog10() as usize + 1));
        let diagnostic = add(text, numbers)?;
        let values = size_of::<HashMap<DurableAtomicGroup, DurableAtomicDecision>>()
            + size_of::<Vec<DurableAtomicGroup>>();
        self.0.work.reserve_values(
            add(values, diagnostic)?,
            diagnostic,
            5,
            add(values, diagnostic)?,
        )?;
        Ok(())
    }

    fn before_batch(&mut self) -> Result<(), Self::Error> {
        self.0.work.charge(Debit {
            batches: 1,
            ..Debit::default()
        })?;
        // DurableSinkCheckpoint::after can format this actual batch identity
        // and four u64 sequence/offset displays, or either exhaustion message.
        let diagnostic =
            "append  at sequence  and offset  does not follow checkpoint sequence  and offset "
                .len()
                + (u128::MAX.ilog10() as usize + 1)
                + 4 * (u64::MAX.ilog10() as usize + 1)
                + "durable sink placement sequence exhausted".len()
                + "durable sink logical offset exhausted".len();
        self.0
            .work
            .reserve_values(diagnostic, diagnostic, 3, diagnostic)?;
        Ok(())
    }

    fn before_decision_insert(
        &mut self,
        decisions: &mut HashMap<DurableAtomicGroup, DurableAtomicDecision>,
    ) -> Result<(), Self::Error> {
        // Keep the actual allocation/prefix outside this loop before even the
        // first arithmetic or admission can refuse. Completed derived scratch
        // state may be returned to the shared loop only after full admission.
        self.0.recovered_decisions = std::mem::take(decisions);
        let count = add(self.0.recovered_decisions.len(), 1)?;
        let bytes = mul(
            count,
            size_of::<(DurableAtomicGroup, DurableAtomicDecision)>(),
        )?;
        self.0.work.reserve_values(bytes, 0, count, bytes)?;
        self.0
            .work
            .reserve_table_probe::<(DurableAtomicGroup, DurableAtomicDecision)>(
                self.0.recovered_decisions.capacity(),
                "resume.sink.decisions.grow",
            )?;
        self.0.work.check()?;
        self.0.setup_allocation_error = self.0.recovered_decisions.try_reserve(1).err();
        self.0.work.check()?;
        if self.0.setup_allocation_error.is_some() {
            return Err(NativeResumeFailure::Allocation);
        }
        self.0
            .work
            .reserve_capacity_excess::<(DurableAtomicGroup, DurableAtomicDecision)>(
                self.0.recovered_decisions.capacity(),
                count,
            )?;
        self.0
            .work
            .reserve_table_probe::<(DurableAtomicGroup, DurableAtomicDecision)>(
                self.0.recovered_decisions.capacity(),
                "resume.sink.decisions.insert",
            )?;
        *decisions = std::mem::take(&mut self.0.recovered_decisions);
        Ok(())
    }

    fn before_decision_scan(&mut self, capacity: usize) -> Result<(), Self::Error> {
        // HashMap iteration scans capacity, including empty buckets.
        self.0
            .work
            .reserve_table_probe::<(DurableAtomicGroup, DurableAtomicDecision)>(
                capacity,
                "resume.sink.decisions.scan",
            )?;
        Ok(())
    }

    fn before_pending_group(&mut self, partitions: usize) -> Result<(), Self::Error> {
        self.0.work.reserve_groups(1)?;
        self.0
            .work
            .reserve_values(0, 0, 0, mul(partitions, size_of::<TopicPartition>())?)?;
        Ok(())
    }

    fn before_pending_push(
        &mut self,
        pending: &mut Vec<DurableAtomicGroup>,
    ) -> Result<(), Self::Error> {
        // Retain the physical prefix before arithmetic, initial fee/check or
        // allocation can refuse. No fallible phase precedes the custody move.
        self.0.pending_decisions = std::mem::take(pending);
        let count = add(self.0.pending_decisions.len(), 1)?;
        let bytes = mul(count, size_of::<DurableAtomicGroup>())?;
        self.0.work.reserve_values(bytes, 0, count, bytes)?;
        self.0.work.check()?;
        self.0.setup_allocation_error = self.0.pending_decisions.try_reserve_exact(1).err();
        self.0.work.check()?;
        if self.0.setup_allocation_error.is_some() {
            return Err(NativeResumeFailure::Allocation);
        }
        self.0.work.reserve_capacity_excess::<DurableAtomicGroup>(
            self.0.pending_decisions.capacity(),
            count,
        )?;
        *pending = std::mem::take(&mut self.0.pending_decisions);
        Ok(())
    }

    fn checkpoint(
        &mut self,
        partition: TopicPartition,
    ) -> Result<DurableSinkCheckpoint, Self::Error> {
        self.0.sink_checkpoint(partition)
    }
    fn watermarks(
        &mut self,
        partition: TopicPartition,
    ) -> Result<PartitionWatermarks, Self::Error> {
        self.0.watermarks(partition)
    }
    fn fetch_stored_internal(
        &mut self,
        request: FetchRequest,
        unresolved: bool,
    ) -> Result<Vec<StoredBatch>, Self::Error> {
        // The strict replication delegate uses this same real retained-fetch
        // producer and charged metadata/Bytes copies, including unresolved WAL.
        StrictReplication(&mut *self.0).fetch_stored_internal(request, unresolved)
    }
    fn make_append(&mut self, batch: StoredBatch) -> Result<DurableAppend, Self::Error> {
        let bytes = size_of::<DurableAppend>() + size_of::<RecordId>();
        self.0.work.reserve_values(
            bytes,
            0,
            2,
            add(
                bytes,
                size_of::<(shard_stream_core::TopicId, LogicalPartitionId, BatchId)>(),
            )?,
        )?;
        self.0.work.check()?;
        let append = durable_append_from_stored(&self.0.owner.config, batch);
        self.0.work.check()?;
        Ok(append)
    }
    fn enqueue_replay(
        &mut self,
        append: DurableAppend,
        expected: DurableSinkCheckpoint,
        next: DurableSinkCheckpoint,
    ) -> Result<(), Self::Error> {
        self.0.sink_append(expected, append, next)
    }
    fn inspect_atomic_group(
        &mut self,
        partition: TopicPartition,
        transaction: u128,
    ) -> Result<AtomicGroupInspection, Self::Error> {
        self.0.inspect_group(DurableAtomicGroup {
            topic_partition: partition,
            transaction_id: transaction,
        })
    }
    fn enqueue_replay_decision(
        &mut self,
        decision: DurableAtomicDecision,
    ) -> Result<(), Self::Error> {
        self.0.sink_decision(decision)
    }
    fn clear_dirty(&mut self, partition: TopicPartition) -> Result<(), Self::Error> {
        self.cache(NativeResumeSinkCacheOperation::ClearDirty(partition))
    }
    fn recovery_mode(&self) -> DurableSinkRecoveryMode {
        // This immutable option read is always bracketed by actual bounded
        // phases in replay_sink; strict apply/decision commands settle in both
        // modes, so Background never means detached strict work.
        self.0
            .owner
            .durable_sink
            .as_ref()
            .map_or(DurableSinkRecoveryMode::Blocking, |sink| {
                sink.recovery_mode()
            })
    }
    fn wait_for(&mut self, checkpoint: DurableSinkCheckpoint) -> Result<(), Self::Error> {
        self.cache(NativeResumeSinkCacheOperation::Checkpoint(checkpoint))
    }
    fn wait_for_decision(&mut self, group: DurableAtomicGroup) -> Result<(), Self::Error> {
        self.cache(NativeResumeSinkCacheOperation::Decision(group))
    }
    fn collect_garbage(
        &mut self,
        partition: TopicPartition,
        start: LogicalOffset,
    ) -> Result<(), Self::Error> {
        // Preserve the ordinary sink-checkpoint clamp using the real strict
        // callback, then consume the genuine owned native stores on each lane.
        let checkpoint = self.0.sink_checkpoint(partition)?;
        self.0
            .collect_owned(partition, start.min(checkpoint.next_offset))
    }
    fn pending_atomic_groups(&mut self) -> Result<Vec<DurableAtomicGroup>, Self::Error> {
        self.0.sink_pending()
    }
}

#[cfg(test)]
pub(super) fn test_pending_push(
    scope: &mut Scope,
    pending: &mut Vec<DurableAtomicGroup>,
) -> Result<(), NativeResumeFailure> {
    StrictSink(scope).before_pending_push(pending)
}

#[cfg(test)]
pub(super) fn test_decision_growth(
    scope: &mut Scope,
    decisions: &mut HashMap<DurableAtomicGroup, DurableAtomicDecision>,
) -> Result<(), NativeResumeFailure> {
    StrictSink(scope).before_decision_insert(decisions)
}

impl EngineHandle {
    /// Replays actual durable-sink state with finite work and owned native GC.
    ///
    /// Uses the SAME engine, sink workers, cumulative fetch allowance and
    /// min-only directory deadline. Entered callbacks and GC commands settle
    /// before return even after expiry. Blocking mode observes the real native
    /// checkpoint/decision caches without an ordinary retry or Condvar wait;
    /// Background mode also physically settles every strict callback.
    ///
    /// Sink providers must implement their genuine bounded transaction methods.
    /// Missing strict methods refuse without calling a legacy callback. The GC
    /// policy must refer to the actual owned stores and captured health. This
    /// does not open sink handles or grant serving/currentness permission.
    ///
    /// The SAME directory checker vetoes later native fetch phases after a
    /// tighter minimum or denial. Already-entered jobs physically settle and
    /// already-published native batches stay retained. This does not add custody
    /// of the current unpublished raw read buffer or open file after a late
    /// post-read refusal; that stronger carrier remains a separate prerequisite.
    ///
    /// # Errors
    /// Retains this same engine, actual partial reads, native/sink/cache/GC
    /// results and original unwind payloads. Committed sink effects can coexist
    /// with a later deadline/cache/GC refusal; no rollback is claimed.
    #[allow(
        clippy::result_large_err,
        reason = "retain entered sink/GC outcomes and the same engine without a new error allocation"
    )]
    pub fn resume_durable_sink_until(
        &self,
        partitions: &[TopicPartition],
        policy: NativeResumeGcPolicy,
        limits: NativeResumeLimits,
        original_expiry: Instant,
    ) -> Result<NativeResumeReport, RetainedNativeResumeError> {
        let mut scope = gc::start_owned(self, limits, original_expiry, policy)?;
        let run = catch_unwind(AssertUnwindSafe(|| {
            scope.work.check()?;
            if partitions.len() > limits.max_partitions {
                return Err(NativeResumeRefusal::Capacity.into());
            }
            if scope.owner.durable_sink.is_none() {
                return Err(NativeResumeFailure::MissingSink);
            }
            let diagnostic = "max_fetch_bytes exceeds the sink recovery limit".len();
            scope
                .work
                .reserve_values(diagnostic, diagnostic, 1, diagnostic)?;
            let max_bytes = u32::try_from(scope.owner.config.max_fetch_bytes).map_err(|_| {
                EngineError::InvalidConfig("max_fetch_bytes exceeds the sink recovery limit".into())
            })?;
            scope.work.check()?;
            replay_sink(&mut StrictSink(&mut scope), partitions, max_bytes)
        }));
        let run = match run {
            Ok(result) => result,
            Err(payload) => {
                scope.work.deny();
                scope.panic_payload = Some(payload);
                Err(NativeResumeFailure::Unwind)
            }
        };
        if let Err(error) = run {
            return Err(scope.fail(error));
        }
        scope.finish()
    }
}
