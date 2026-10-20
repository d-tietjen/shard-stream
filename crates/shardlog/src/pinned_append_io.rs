//! Actual descriptor-bound calls and their retained, prepaid settlement.

use super::*;

pub(super) const NATIVE_PACK_NAME_BYTES: usize = "pack-".len() + 20 + ".sse".len();

impl Scope {
    pub(super) fn run(
        &mut self,
        store: &mut ShardStore,
        pin: &PinnedShardDirectory,
        sync: bool,
        seal: bool,
        b: &mut PinnedShardAppendBudget,
        c: &dyn PinnedShardAppendChecks,
    ) -> Result<(), PinnedShardAppendFailure> {
        self.check(b, c)?;
        let next = b
            .usage
            .operations
            .checked_add(1)
            .ok_or(PinnedShardAppendFailure::Arithmetic)?;
        if next > b.limits.max_operations
            || self.inputs.len() > b.limits.max_frames.saturating_sub(b.usage.frames)
        {
            return Err(PinnedShardAppendFailure::Capacity);
        }
        b.usage.operations = next;
        self.work(
            self.inputs
                .len()
                .checked_mul(size_of::<PinnedShardAppendInput>())
                .ok_or(PinnedShardAppendFailure::Arithmetic)?,
            b,
            c,
        )?;
        let mut payload = 0usize;
        for input in &self.inputs {
            payload = payload
                .checked_add(input.payload.len())
                .ok_or(PinnedShardAppendFailure::Arithmetic)?;
        }
        let next = b
            .usage
            .payload_bytes
            .checked_add(payload)
            .ok_or(PinnedShardAppendFailure::Arithmetic)?;
        if next > b.limits.max_payload_bytes {
            return Err(PinnedShardAppendFailure::Capacity);
        }
        b.usage.payload_bytes = next;
        b.usage.frames = b
            .usage
            .frames
            .checked_add(self.inputs.len())
            .ok_or(PinnedShardAppendFailure::Arithmetic)?;
        self.allocate(b, c)?;
        self.identify(store, pin, b, c)?;
        self.prepare(store, b, c)?;
        if seal {
            if store.active.bytes != 0 {
                // identify() paid its own probe. Pay this distinct seal probe
                // before accessing the actual retained BTree and pathname.
                let seal_lookup_work = store
                    .pack_paths
                    .len()
                    .checked_add(1)
                    .and_then(|n| n.checked_mul(size_of::<(u64, PathBuf)>()))
                    .ok_or(PinnedShardAppendFailure::Arithmetic)?;
                #[cfg(test)]
                if let Some(limit) = crate::api::pinned::write::tests::seal_probe_limit(
                    false,
                    b.usage.work_bytes,
                    seal_lookup_work,
                ) {
                    // Test-only downward fault injection. No production cap,
                    // saved expiry, counter or other limit is changed.
                    if limit <= b.limits.max_work_bytes {
                        b.limits.max_work_bytes = limit;
                    }
                }
                self.work(seal_lookup_work, b, c)?;
                #[cfg(test)]
                let _ = crate::api::pinned::write::tests::seal_probe_limit(
                    true,
                    b.usage.work_bytes,
                    seal_lookup_work,
                );
                let actual = store
                    .pack_paths
                    .get(&store.active.sequence)
                    .ok_or(PinnedShardAppendFailure::Refused)?;
                let bytes = actual.as_os_str().len();
                if bytes > b.limits.max_path_bytes {
                    return Err(PinnedShardAppendFailure::Capacity);
                }
                self.work(bytes, b, c)?;
                // Preserve the native opener's literal accepted path, including
                // leading-zero decimal spellings. The SAME map remains owned.
                self.sealed = Some(SealedPack {
                    sequence: store.active.sequence,
                    path: actual.clone(),
                    bytes: store.active.bytes,
                });
                self.after(PinnedShardAppendPhase::Prepared, b, c)?;
                self.rotate(store, pin, b, c)?;
                self.sealed_completed = true;
            }
            return self.check(b, c);
        }
        let mut first = 0;
        while first < self.prepared.len() {
            self.check(b, c)?;
            let bytes = u64::try_from(self.prepared[first].encoded.frame_len)
                .map_err(|_| PinnedShardAppendFailure::Arithmetic)?;
            if store.active.bytes > 0
                && store
                    .active
                    .bytes
                    .checked_add(bytes)
                    .is_none_or(|n| n > store.config.target_pack_bytes)
            {
                self.rotate(store, pin, b, c)?;
            }
            self.work(
                MAX_EXTENTS_PER_WRITE
                    .checked_mul(size_of::<PreparedExtent>())
                    .ok_or(PinnedShardAppendFailure::Arithmetic)?,
                b,
                c,
            )?;
            let result = prepared_segment_end(
                first,
                self.prepared.len(),
                store.active.bytes,
                store.config.target_pack_bytes,
                |i| self.prepared[i].encoded.frame_len,
            );
            let end = match result {
                Ok(end) => end,
                Err(e) => {
                    self.native_error = Some(e);
                    self.check(b, c)?;
                    return Err(PinnedShardAppendFailure::Native);
                }
            };
            self.segment(store, first, end, b, c)?;
            first = end;
        }
        if sync && !self.prepared.is_empty() {
            self.start_file_settlement(store.active.sequence, None, b, c)?;
            self.flush_active(store, b, c)?;
            self.sync_active(store, b, c)?;
            self.pending_file = None;
        }
        self.check(b, c)
    }

    fn stat(
        &mut self,
        p: PinnedShardAppendPhase,
        b: &mut PinnedShardAppendBudget,
        c: &dyn PinnedShardAppendChecks,
        call: impl FnOnce() -> io::Result<Stat>,
    ) -> Result<usize, PinnedShardAppendFailure> {
        self.reserve_calls(1, b, c)?;
        self.enter(p, b)?;
        let index = self.outcomes.len();
        self.outcomes.push(Outcome::Stat(p, call()));
        self.after(p, b, c)?;
        if matches!(&self.outcomes[index], Outcome::Stat(_, Err(_))) {
            return Err(PinnedShardAppendFailure::Io);
        }
        Ok(index)
    }
    fn stat_value(&self, index: usize) -> Result<&Stat, PinnedShardAppendFailure> {
        match self.outcomes.get(index) {
            Some(Outcome::Stat(_, Ok(v))) => Ok(v),
            _ => Err(PinnedShardAppendFailure::Identity),
        }
    }
    fn identify(
        &mut self,
        store: &ShardStore,
        pin: &PinnedShardDirectory,
        b: &mut PinnedShardAppendBudget,
        c: &dyn PinnedShardAppendChecks,
    ) -> Result<(), PinnedShardAppendFailure> {
        let path_bytes = store.config.directory.as_os_str().len();
        if path_bytes > b.limits.max_path_bytes
            || pin.directory().as_os_str().len() > b.limits.max_path_bytes
        {
            return Err(PinnedShardAppendFailure::Capacity);
        }
        self.work(
            path_bytes
                .checked_add(pin.directory().as_os_str().len())
                .ok_or(PinnedShardAppendFailure::Arithmetic)?,
            b,
            c,
        )?;
        if store.config.directory.as_os_str() != pin.directory().as_os_str() {
            return Err(PinnedShardAppendFailure::Identity);
        }
        self.work(
            path_bytes
                .checked_add(1)
                .and_then(|n| n.checked_mul(4))
                .ok_or(PinnedShardAppendFailure::Arithmetic)?,
            b,
            c,
        )?;
        match CString::new(pin.directory().as_os_str().as_bytes()) {
            Ok(v) => self.directory_name = Some(v),
            Err(e) => self.directory_name_error = Some(e),
        }
        self.after(PinnedShardAppendPhase::Prepared, b, c)?;
        if self.directory_name_error.is_some() {
            return Err(PinnedShardAppendFailure::Name);
        }
        let directory = self.stat(PinnedShardAppendPhase::Directory, b, c, || {
            fstat(&pin.scope.directory).map_err(io::Error::from)
        })?;
        // Borrow independent retained input, not a name copied after work entry.
        self.reserve_calls(1, b, c)?;
        self.enter(PinnedShardAppendPhase::Directory, b)?;
        let named = self.outcomes.len();
        self.outcomes.push(Outcome::Stat(
            PinnedShardAppendPhase::Directory,
            statat(
                rustix::fs::CWD,
                self.directory_name
                    .as_deref()
                    .ok_or(PinnedShardAppendFailure::Name)?,
                AtFlags::SYMLINK_NOFOLLOW,
            )
            .map_err(io::Error::from),
        ));
        self.after(PinnedShardAppendPhase::Directory, b, c)?;
        if matches!(&self.outcomes[named], Outcome::Stat(_, Err(_))) {
            return Err(PinnedShardAppendFailure::Io);
        }
        let lock = self.stat(PinnedShardAppendPhase::Lock, b, c, || {
            fstat(&pin.scope.guard.guard().file).map_err(io::Error::from)
        })?;
        let named_lock = self.stat(PinnedShardAppendPhase::Lock, b, c, || {
            statat(
                &pin.scope.directory,
                c".shard-store.lock",
                AtFlags::SYMLINK_NOFOLLOW,
            )
            .map_err(io::Error::from)
        })?;
        let store_lock = self.stat(PinnedShardAppendPhase::Lock, b, c, || {
            fstat(&store._lock_file).map_err(io::Error::from)
        })?;
        let d = self.stat_value(directory)?;
        let n = self.stat_value(named)?;
        let l = self.stat_value(lock)?;
        let nl = self.stat_value(named_lock)?;
        let sl = self.stat_value(store_lock)?;
        if FileType::from_raw_mode(d.st_mode) != FileType::Directory
            || FileType::from_raw_mode(n.st_mode) != FileType::Directory
            || d.st_dev != n.st_dev
            || d.st_ino != n.st_ino
            || [l, nl, sl].into_iter().any(|v| {
                FileType::from_raw_mode(v.st_mode) != FileType::RegularFile
                    || v.st_nlink != 1
                    || v.st_dev != l.st_dev
                    || v.st_ino != l.st_ino
            })
        {
            return Err(PinnedShardAppendFailure::Identity);
        }
        self.identify_active(store, pin, b, c)
    }
    fn identify_active(
        &mut self,
        store: &ShardStore,
        pin: &PinnedShardDirectory,
        b: &mut PinnedShardAppendBudget,
        c: &dyn PinnedShardAppendChecks,
    ) -> Result<(), PinnedShardAppendFailure> {
        if store.pack_paths.len() > b.limits.max_packs {
            return Err(PinnedShardAppendFailure::Capacity);
        }
        self.work(
            store
                .pack_paths
                .len()
                .checked_mul(size_of::<(u64, PathBuf)>())
                .ok_or(PinnedShardAppendFailure::Arithmetic)?,
            b,
            c,
        )?;
        let actual = store
            .pack_paths
            .get(&store.active.sequence)
            .ok_or(PinnedShardAppendFailure::Refused)?;
        let bytes = actual.as_os_str().len();
        self.active_path_bytes = Some(bytes);
        if bytes > b.limits.max_path_bytes {
            return Err(PinnedShardAppendFailure::Capacity);
        }
        // Path::file_name scans the actual raw path. Bound/pay the COMPLETE
        // scan before calling it, rather than paying a guessed canonical name.
        let name_work = b
            .limits
            .max_name_bytes
            .checked_add(1)
            .and_then(|n| n.checked_mul(4))
            .ok_or(PinnedShardAppendFailure::Arithmetic)?;
        self.work(
            bytes
                .checked_add(name_work)
                .ok_or(PinnedShardAppendFailure::Arithmetic)?,
            b,
            c,
        )?;
        let original = actual
            .file_name()
            .ok_or(PinnedShardAppendFailure::Identity)?;
        let name_bytes = original.as_bytes().len();
        if name_bytes > b.limits.max_name_bytes {
            return Err(PinnedShardAppendFailure::Capacity);
        }
        self.active_original_name = Some(original.to_owned());
        self.after(PinnedShardAppendPhase::Prepared, b, c)?;
        let original = self
            .active_original_name
            .as_deref()
            .ok_or(PinnedShardAppendFailure::Identity)?;
        self.active_name = None;
        self.active_name_error = None;
        match CString::new(original.as_bytes()) {
            Ok(v) => self.active_name = Some(v),
            Err(e) => self.active_name_error = Some(e),
        }
        self.after(PinnedShardAppendPhase::Prepared, b, c)?;
        if self.active_name_error.is_some() {
            return Err(PinnedShardAppendFailure::Name);
        }
        let fd = self.stat(PinnedShardAppendPhase::ActiveIdentity, b, c, || {
            fstat(&store.active.file).map_err(io::Error::from)
        })?;
        self.reserve_calls(1, b, c)?;
        self.enter(PinnedShardAppendPhase::ActiveIdentity, b)?;
        let name = self.outcomes.len();
        self.outcomes.push(Outcome::Stat(
            PinnedShardAppendPhase::ActiveIdentity,
            statat(
                &pin.scope.directory,
                self.active_name
                    .as_deref()
                    .ok_or(PinnedShardAppendFailure::Name)?,
                AtFlags::SYMLINK_NOFOLLOW,
            )
            .map_err(io::Error::from),
        ));
        self.after(PinnedShardAppendPhase::ActiveIdentity, b, c)?;
        if matches!(&self.outcomes[name], Outcome::Stat(_, Err(_))) {
            return Err(PinnedShardAppendFailure::Io);
        }
        let a = self.stat_value(fd)?;
        let n = self.stat_value(name)?;
        if [a, n]
            .into_iter()
            .any(|v| FileType::from_raw_mode(v.st_mode) != FileType::RegularFile || v.st_nlink != 1)
            || a.st_dev != n.st_dev
            || a.st_ino != n.st_ino
            || a.st_size < 0
            || a.st_size as u64 != store.active.bytes
            || a.st_size != n.st_size
        {
            return Err(PinnedShardAppendFailure::Identity);
        }
        self.check(b, c)
    }

    fn prepare(
        &mut self,
        store: &mut ShardStore,
        b: &mut PinnedShardAppendBudget,
        c: &dyn PinnedShardAppendChecks,
    ) -> Result<(), PinnedShardAppendFailure> {
        if store.pack_paths.len() > b.limits.max_packs
            || store.by_batch.capacity() > b.limits.max_batch_slots
            || store.index.capacity() > b.limits.max_topic_slots
        {
            return Err(PinnedShardAppendFailure::Capacity);
        }
        self.work(
            store
                .index
                .capacity()
                .checked_mul(size_of::<(TopicPartition, Vec<ExtentLocation>)>())
                .ok_or(PinnedShardAppendFailure::Arithmetic)?,
            b,
            c,
        )?;
        let mut slots = 0usize;
        for locations in store.index.values() {
            slots = slots
                .checked_add(locations.capacity())
                .ok_or(PinnedShardAppendFailure::Arithmetic)?;
        }
        if slots
            .checked_add(self.inputs.len())
            .is_none_or(|n| n > b.limits.max_location_slots)
            || store
                .by_batch
                .len()
                .checked_add(self.inputs.len())
                .is_none_or(|n| n > b.limits.max_batch_slots)
            || store
                .index
                .len()
                .checked_add(self.inputs.len())
                .is_none_or(|n| n > b.limits.max_topic_slots)
        {
            return Err(PinnedShardAppendFailure::Capacity);
        }
        // Admit all actual native diagnostic/checksum/header bytes before the
        // native validator formats any scalar error or the header is allocated.
        for i in 0..self.inputs.len() {
            self.work(
                self.inputs[i]
                    .payload
                    .len()
                    .checked_add(HEADER_LEN)
                    .and_then(|n| n.checked_add(1024))
                    .and_then(|n| {
                        n.checked_add(store.by_batch.capacity().checked_mul(size_of::<(
                            TopicPartition,
                            BatchId,
                        )>(
                        ))?)
                    })
                    .and_then(|n| {
                        n.checked_add(self.inputs.len().checked_mul(size_of::<(
                            TopicPartition,
                            BatchId,
                        )>(
                        ))?)
                    })
                    .ok_or(PinnedShardAppendFailure::Arithmetic)?,
                b,
                c,
            )?;
            let result = store.validate_append(self.inputs[i].reservation, &self.inputs[i].payload);
            self.native(result, b, c)?;
            let result = validate_prepared_group_key(&mut self.keys, self.inputs[i].reservation);
            self.native(result, b, c)?;
            let input = &self.inputs[i];
            let lengths = extent_frame_lengths(input.payload.len());
            let (frame_len, frame_len_u64, payload_len_u64) = match lengths {
                Ok(v) => v,
                Err(e) => {
                    self.native_error = Some(e);
                    self.check(b, c)?;
                    return Err(PinnedShardAppendFailure::Native);
                }
            };
            self.prepared.push(PreparedExtent {
                reservation: input.reservation,
                producer: input.producer,
                atomic_group: input.atomic_group,
                payload: input.payload.clone(),
                encoded: EncodedExtent {
                    header: Vec::new(),
                    checksum: [0; CHECKSUM_LEN],
                    frame_len,
                },
            });
            let r = self.prepared[i]
                .encoded
                .header
                .try_reserve_exact(HEADER_LEN);
            self.allocations.push(r);
            self.after(PinnedShardAppendPhase::Prepared, b, c)?;
            if self.allocations.last().is_some_and(Result::is_err) {
                return Err(PinnedShardAppendFailure::Allocation);
            }
            let p = &mut self.prepared[i];
            let result = encode_extent_header(
                &mut p.encoded.header,
                p.reservation,
                p.producer,
                p.atomic_group,
                frame_len_u64,
                payload_len_u64,
            );
            self.native(result, b, c)?;
            let p = &mut self.prepared[i];
            p.encoded.checksum = extent_checksum(&p.encoded.header, &p.payload).to_le_bytes();
            self.after(PinnedShardAppendPhase::Prepared, b, c)?;
        }
        self.work(
            self.inputs
                .len()
                .checked_mul(size_of::<ExtentLocation>())
                .ok_or(PinnedShardAppendFailure::Arithmetic)?,
            b,
            c,
        )?;
        let r = store.by_batch.try_reserve(self.inputs.len());
        self.allocations.push(r);
        self.check(b, c)?;
        let r = store.index.try_reserve(self.inputs.len());
        self.allocations.push(r);
        self.check(b, c)?;
        if self.allocations.iter().any(Result::is_err) {
            return Err(PinnedShardAppendFailure::Allocation);
        }
        if store.by_batch.capacity() > b.limits.max_batch_slots
            || store.index.capacity() > b.limits.max_topic_slots
        {
            return Err(PinnedShardAppendFailure::Capacity);
        }
        for i in 0..self.inputs.len() {
            self.work(
                self.inputs
                    .len()
                    .checked_mul(2)
                    .and_then(|n| n.checked_mul(size_of::<PinnedShardAppendInput>()))
                    .ok_or(PinnedShardAppendFailure::Arithmetic)?,
                b,
                c,
            )?;
            let r = self.inputs[i].reservation;
            let topic = TopicPartition::new(r.topic_id, r.partition_id);
            if self.inputs[..i].iter().any(|v| {
                v.reservation.topic_id == r.topic_id && v.reservation.partition_id == r.partition_id
            }) {
                continue;
            }
            let count = self
                .inputs
                .iter()
                .filter(|v| {
                    v.reservation.topic_id == r.topic_id
                        && v.reservation.partition_id == r.partition_id
                })
                .count();
            let lookup_work = store
                .index
                .capacity()
                .checked_mul(size_of::<(TopicPartition, Vec<ExtentLocation>)>())
                .ok_or(PinnedShardAppendFailure::Arithmetic)?;
            #[cfg(test)]
            super::tests::point_lookup_checkpoint(
                "prepare",
                false,
                b,
                lookup_work,
                (store.index.capacity(), store.by_batch.capacity()),
            );
            // This point lookup has its own capacity fee. The earlier full
            // source scan and later movement fees do not pay for this probe.
            self.work(lookup_work, b, c)?;
            if let Some(locations) = store.index.get_mut(&topic) {
                let result = locations.try_reserve_exact(count);
                self.allocations.push(result);
            } else {
                self.new_topics.push(NewTopic {
                    topic,
                    locations: Some(Vec::new()),
                });
                let pending = self
                    .new_topics
                    .last_mut()
                    .ok_or(PinnedShardAppendFailure::Refused)?;
                let result = pending
                    .locations
                    .as_mut()
                    .ok_or(PinnedShardAppendFailure::Refused)?
                    .try_reserve_exact(count);
                self.allocations.push(result);
            }
            #[cfg(test)]
            super::tests::point_lookup_checkpoint(
                "prepare",
                true,
                b,
                lookup_work,
                (store.index.capacity(), store.by_batch.capacity()),
            );
            self.check(b, c)?;
            if self.allocations.last().is_some_and(Result::is_err) {
                return Err(PinnedShardAppendFailure::Allocation);
            }
        }
        self.work(
            store
                .index
                .capacity()
                .checked_add(self.new_topics.len())
                .and_then(|n| n.checked_mul(size_of::<NewTopic>()))
                .ok_or(PinnedShardAppendFailure::Arithmetic)?,
            b,
            c,
        )?;
        let mut actual_slots = 0usize;
        for values in store.index.values() {
            actual_slots = actual_slots
                .checked_add(values.capacity())
                .ok_or(PinnedShardAppendFailure::Arithmetic)?;
        }
        for pending in &self.new_topics {
            actual_slots = actual_slots
                .checked_add(pending.locations.as_ref().map_or(0, Vec::capacity))
                .ok_or(PinnedShardAppendFailure::Arithmetic)?;
        }
        if actual_slots > b.limits.max_location_slots {
            return Err(PinnedShardAppendFailure::Capacity);
        }
        self.check(b, c)
    }

    fn start_file_settlement(
        &mut self,
        sequence: u64,
        rollback: Option<u64>,
        b: &mut PinnedShardAppendBudget,
        c: &dyn PinnedShardAppendChecks,
    ) -> Result<(), PinnedShardAppendFailure> {
        if self.pending_file.is_some() {
            return Err(PinnedShardAppendFailure::Refused);
        }
        // Before any write: rollback, flush and sync are reserved together. The
        // still-unentered calls physically settle even after original E passes.
        self.reserve_calls(2 + usize::from(rollback.is_some()), b, c)?;
        self.pending_file = Some(PendingFile {
            sequence,
            rollback,
            rollback_entered: false,
            flush_entered: false,
            sync_entered: false,
        });
        Ok(())
    }
    fn flush_active(
        &mut self,
        store: &mut ShardStore,
        b: &mut PinnedShardAppendBudget,
        c: &dyn PinnedShardAppendChecks,
    ) -> Result<(), PinnedShardAppendFailure> {
        self.check(b, c)?;
        self.enter(PinnedShardAppendPhase::Flush, b)?;
        self.pending_file
            .as_mut()
            .ok_or(PinnedShardAppendFailure::Refused)?
            .flush_entered = true;
        let index = self.outcomes.len();
        self.outcomes.push(Outcome::Unit(
            PinnedShardAppendPhase::Flush,
            store.active.file.flush(),
        ));
        self.after(PinnedShardAppendPhase::Flush, b, c)?;
        if matches!(&self.outcomes[index], Outcome::Unit(_, Err(_))) {
            return Err(PinnedShardAppendFailure::Io);
        }
        Ok(())
    }
    fn sync_active(
        &mut self,
        store: &mut ShardStore,
        b: &mut PinnedShardAppendBudget,
        c: &dyn PinnedShardAppendChecks,
    ) -> Result<(), PinnedShardAppendFailure> {
        self.check(b, c)?;
        self.enter(PinnedShardAppendPhase::SyncData, b)?;
        self.pending_file
            .as_mut()
            .ok_or(PinnedShardAppendFailure::Refused)?
            .sync_entered = true;
        let index = self.outcomes.len();
        self.outcomes.push(Outcome::Unit(
            PinnedShardAppendPhase::SyncData,
            fdatasync(&store.active.file).map_err(io::Error::from),
        ));
        self.after(PinnedShardAppendPhase::SyncData, b, c)?;
        if matches!(&self.outcomes[index], Outcome::Unit(_, Err(_))) {
            return Err(PinnedShardAppendFailure::Io);
        }
        Ok(())
    }
    fn write_part(
        &mut self,
        store: &mut ShardStore,
        frame: usize,
        part: usize,
        b: &mut PinnedShardAppendBudget,
        c: &dyn PinnedShardAppendChecks,
    ) -> Result<(), PinnedShardAppendFailure> {
        let mut written = 0usize;
        loop {
            let p = &self.prepared[frame];
            let bytes = match part {
                0 => p.encoded.header.as_slice(),
                1 => p.payload.as_ref(),
                _ => p.encoded.checksum.as_slice(),
            };
            if written == bytes.len() {
                return Ok(());
            }
            let count = bytes.len() - written;
            self.work(count, b, c)?;
            let calls = b
                .usage
                .write_calls
                .checked_add(1)
                .ok_or(PinnedShardAppendFailure::Arithmetic)?;
            let requested = b
                .usage
                .write_bytes
                .checked_add(count)
                .ok_or(PinnedShardAppendFailure::Arithmetic)?;
            if calls > b.limits.max_write_calls || requested > b.limits.max_write_bytes {
                return Err(PinnedShardAppendFailure::Capacity);
            }
            self.reserve_calls(1, b, c)?;
            b.usage.write_calls = calls;
            b.usage.write_bytes = requested;
            self.enter(PinnedShardAppendPhase::Write, b)?;
            let p = &self.prepared[frame];
            let bytes = match part {
                0 => p.encoded.header.as_slice(),
                1 => p.payload.as_ref(),
                _ => p.encoded.checksum.as_slice(),
            };
            let index = self.outcomes.len();
            self.outcomes.push(Outcome::Write(WriteAttempt {
                sequence: store.active.sequence,
                frame,
                part,
                offset: written,
                requested: count,
                result: store.active.file.write(&bytes[written..]),
            }));
            self.after(PinnedShardAppendPhase::Write, b, c)?;
            match &self.outcomes[index] {
                Outcome::Write(WriteAttempt { result: Ok(0), .. }) => {
                    return Err(PinnedShardAppendFailure::Io);
                }
                Outcome::Write(WriteAttempt { result: Ok(n), .. }) => {
                    written = written
                        .checked_add(*n)
                        .ok_or(PinnedShardAppendFailure::Arithmetic)?
                }
                Outcome::Write(WriteAttempt { result: Err(e), .. })
                    if e.kind() == io::ErrorKind::Interrupted => {}
                _ => return Err(PinnedShardAppendFailure::Io),
            }
        }
    }
    fn segment(
        &mut self,
        store: &mut ShardStore,
        first: usize,
        end: usize,
        b: &mut PinnedShardAppendBudget,
        c: &dyn PinnedShardAppendChecks,
    ) -> Result<(), PinnedShardAppendFailure> {
        let segments = b
            .usage
            .segments
            .checked_add(1)
            .ok_or(PinnedShardAppendFailure::Arithmetic)?;
        if segments > b.limits.max_segments {
            return Err(PinnedShardAppendFailure::Capacity);
        }
        b.usage.segments = segments;
        let start = store.active.bytes;
        let mut end_offset = start;
        for p in &self.prepared[first..end] {
            end_offset = end_offset
                .checked_add(p.encoded.frame_len as u64)
                .ok_or(PinnedShardAppendFailure::Arithmetic)?;
        }
        self.reserve_calls(1, b, c)?;
        self.enter(PinnedShardAppendPhase::Seek, b)?;
        let index = self.outcomes.len();
        self.outcomes.push(Outcome::Seek(
            store.active.file.seek(SeekFrom::Start(start)),
        ));
        self.after(PinnedShardAppendPhase::Seek, b, c)?;
        if matches!(&self.outcomes[index], Outcome::Seek(Err(_))) {
            return Err(PinnedShardAppendFailure::Io);
        }
        self.start_file_settlement(store.active.sequence, Some(start), b, c)?;
        for frame in first..end {
            for part in 0..3 {
                self.write_part(store, frame, part, b, c)?;
            }
        }
        self.flush_active(store, b, c)?;
        // Native sorted insertion may move every existing location. Pay the
        // actual current capacities and new-location values before first edit.
        let mut work = 0usize;
        for i in first..end {
            let p = &self.prepared[i];
            let topic = TopicPartition::new(p.reservation.topic_id, p.reservation.partition_id);
            let lookup_work = store
                .index
                .capacity()
                .checked_mul(size_of::<(TopicPartition, Vec<ExtentLocation>)>())
                .ok_or(PinnedShardAppendFailure::Arithmetic)?;
            #[cfg(test)]
            super::tests::point_lookup_checkpoint(
                "segment",
                false,
                b,
                lookup_work,
                (store.index.capacity(), store.by_batch.capacity()),
            );
            self.work(lookup_work, b, c)?;
            let capacity = store.index.get(&topic).map_or(0, Vec::capacity);
            #[cfg(test)]
            super::tests::point_lookup_checkpoint(
                "segment",
                true,
                b,
                lookup_work,
                (store.index.capacity(), store.by_batch.capacity()),
            );
            work = work
                .checked_add(
                    capacity
                        .checked_add(end - first)
                        .and_then(|n| n.checked_mul(size_of::<ExtentLocation>()))
                        .ok_or(PinnedShardAppendFailure::Arithmetic)?,
                )
                .ok_or(PinnedShardAppendFailure::Arithmetic)?;
        }
        self.work(work, b, c)?;
        self.pending_file
            .as_mut()
            .ok_or(PinnedShardAppendFailure::Refused)?
            .rollback = None;
        let mut next = start;
        for i in first..end {
            self.check(b, c)?;
            let p = &self.prepared[i];
            let topic = TopicPartition::new(p.reservation.topic_id, p.reservation.partition_id);
            self.work(
                self.new_topics
                    .len()
                    .checked_add(store.index.capacity())
                    .and_then(|n| n.checked_mul(size_of::<NewTopic>()))
                    .ok_or(PinnedShardAppendFailure::Arithmetic)?,
                b,
                c,
            )?;
            if !store.index.contains_key(&topic) {
                let insert_work = store
                    .index
                    .capacity()
                    .checked_mul(size_of::<(TopicPartition, Vec<ExtentLocation>)>())
                    .ok_or(PinnedShardAppendFailure::Arithmetic)?;
                #[cfg(test)]
                super::tests::point_lookup_checkpoint(
                    "install_insert",
                    false,
                    b,
                    insert_work,
                    (store.index.capacity(), store.by_batch.capacity()),
                );
                // The preceding contains/search fee does not pay this insert.
                self.work(insert_work, b, c)?;
                let pending = self
                    .new_topics
                    .iter_mut()
                    .find(|v| v.topic == topic)
                    .ok_or(PinnedShardAppendFailure::Refused)?;
                let locations = pending
                    .locations
                    .take()
                    .ok_or(PinnedShardAppendFailure::Refused)?;
                store.index.insert(topic, locations);
                #[cfg(test)]
                super::tests::point_lookup_checkpoint(
                    "install_insert",
                    true,
                    b,
                    insert_work,
                    (store.index.capacity(), store.by_batch.capacity()),
                );
            }
            let p = &self.prepared[i];
            let location = prepared_extent_location(p, store.active.sequence, next);
            next = next
                .checked_add(p.encoded.frame_len as u64)
                .ok_or(PinnedShardAppendFailure::Arithmetic)?;
            let install_work = store
                .index
                .capacity()
                .checked_mul(size_of::<(TopicPartition, Vec<ExtentLocation>)>())
                .and_then(|n| {
                    n.checked_add(store.by_batch.capacity().checked_mul(size_of::<(
                        (TopicPartition, BatchId),
                        ExtentLocation,
                    )>(
                    ))?)
                })
                .ok_or(PinnedShardAppendFailure::Arithmetic)?;
            #[cfg(test)]
            super::tests::point_lookup_checkpoint(
                "install_location",
                false,
                b,
                install_work,
                (store.index.capacity(), store.by_batch.capacity()),
            );
            // Recompute after any insertion/growth. The unchanged native helper
            // performs its own index.entry and by_batch.insert point probes.
            self.work(install_work, b, c)?;
            store.install_location(location);
            #[cfg(test)]
            super::tests::point_lookup_checkpoint(
                "install_location",
                true,
                b,
                install_work,
                (store.index.capacity(), store.by_batch.capacity()),
            );
            self.installed_frames += 1;
            self.after(PinnedShardAppendPhase::Index, b, c)?;
        }
        store.active.bytes = end_offset;
        self.pending_file = None;
        self.check(b, c)
    }

    fn rotate(
        &mut self,
        store: &mut ShardStore,
        pin: &PinnedShardDirectory,
        b: &mut PinnedShardAppendBudget,
        c: &dyn PinnedShardAppendChecks,
    ) -> Result<(), PinnedShardAppendFailure> {
        let rotations = b
            .usage
            .rotations
            .checked_add(1)
            .ok_or(PinnedShardAppendFailure::Arithmetic)?;
        if rotations > b.limits.max_rotations
            || store.pack_paths.len() >= b.limits.max_packs
            || NATIVE_PACK_NAME_BYTES > b.limits.max_name_bytes
            || store
                .config
                .directory
                .as_os_str()
                .len()
                .checked_add(1)
                .and_then(|n| n.checked_add(NATIVE_PACK_NAME_BYTES))
                .is_none_or(|n| n > b.limits.max_path_bytes)
        {
            return Err(PinnedShardAppendFailure::Capacity);
        }
        self.identify_active(store, pin, b, c)?;
        b.usage.rotations = rotations;
        self.start_file_settlement(store.active.sequence, None, b, c)?;
        self.flush_active(store, b, c)?;
        self.sync_active(store, b, c)?;
        self.pending_file = None;
        let next = match store.active.sequence.checked_add(1) {
            Some(n) => n,
            None => {
                self.native_error = Some(StorageError::LimitExceeded(
                    "pack sequence exhausted".into(),
                ));
                self.check(b, c)?;
                return Err(PinnedShardAppendFailure::Native);
            }
        };
        self.work(
            store
                .config
                .directory
                .as_os_str()
                .len()
                .checked_add(NATIVE_PACK_NAME_BYTES)
                .and_then(|n| n.checked_mul(4))
                .ok_or(PinnedShardAppendFailure::Arithmetic)?,
            b,
            c,
        )?;
        self.rotations.push(Rotation {
            sequence: next,
            name: native_pack_name(next),
            native_name: None,
            name_error: None,
            path: None,
            created_outcome: None,
            retired: None,
            transferred: false,
        });
        let r = self
            .rotations
            .last_mut()
            .ok_or(PinnedShardAppendFailure::Refused)?;
        match CString::new(r.name.as_bytes()) {
            Ok(v) => r.native_name = Some(v),
            Err(e) => r.name_error = Some(e),
        }
        r.path = Some(store.config.directory.join(&r.name));
        self.after(PinnedShardAppendPhase::Prepared, b, c)?;
        let index = self.rotations.len() - 1;
        if self.rotations[index].name_error.is_some() {
            return Err(PinnedShardAppendFailure::Name);
        }
        self.work(
            store
                .pack_paths
                .len()
                .checked_mul(size_of::<(u64, PathBuf)>())
                .ok_or(PinnedShardAppendFailure::Arithmetic)?,
            b,
            c,
        )?;
        if store.pack_paths.contains_key(&next) {
            return Err(PinnedShardAppendFailure::Identity);
        }
        // Parent fsync is prepaid BEFORE create. Uncertain create never means
        // that no name exists, and no unknown name is unlinked on any path.
        self.reserve_calls(2, b, c)?;
        self.enter(PinnedShardAppendPhase::Create, b)?;
        self.pending_directory_sync = true;
        self.directory_sync_entered = false;
        let created = self.outcomes.len();
        let result = openat(
            &pin.scope.directory,
            self.rotations[index]
                .native_name
                .as_deref()
                .ok_or(PinnedShardAppendFailure::Name)?,
            OFlags::RDWR | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::WGRP | Mode::ROTH | Mode::WOTH,
        );
        self.outcomes.push(match result {
            Ok(fd) => Outcome::Created(Some(File::from(fd)), None),
            Err(e) => Outcome::Created(None, Some(io::Error::from(e))),
        });
        self.rotations[index].created_outcome = Some(created);
        self.after(PinnedShardAppendPhase::Create, b, c)?;
        if matches!(&self.outcomes[created], Outcome::Created(_, Some(_))) {
            return Err(PinnedShardAppendFailure::Io);
        }
        // Preserve ordinary native map-before-parent-sync-before-active-swap.
        let path = self.rotations[index]
            .path
            .take()
            .ok_or(PinnedShardAppendFailure::Refused)?;
        store.pack_paths.insert(next, path);
        self.after(PinnedShardAppendPhase::PackIndex, b, c)?;
        self.check(b, c)?;
        self.enter(PinnedShardAppendPhase::DirectorySync, b)?;
        self.directory_sync_entered = true;
        let sync = self.outcomes.len();
        self.outcomes.push(Outcome::Unit(
            PinnedShardAppendPhase::DirectorySync,
            fsync(&pin.scope.directory).map_err(io::Error::from),
        ));
        self.after(PinnedShardAppendPhase::DirectorySync, b, c)?;
        if matches!(&self.outcomes[sync], Outcome::Unit(_, Err(_))) {
            return Err(PinnedShardAppendFailure::Io);
        }
        self.pending_directory_sync = false;
        let file = match &mut self.outcomes[created] {
            Outcome::Created(file, None) => file.take().ok_or(PinnedShardAppendFailure::Refused)?,
            _ => return Err(PinnedShardAppendFailure::Refused),
        };
        let old = std::mem::replace(
            &mut store.active,
            ActivePack {
                sequence: next,
                file,
                bytes: 0,
                opened_at: Instant::now(),
            },
        );
        self.rotations[index].retired = Some(old);
        self.rotations[index].transferred = true;
        self.after(PinnedShardAppendPhase::Rotation, b, c)
    }

    fn settle_one(
        &mut self,
        phase: PinnedShardAppendPhase,
        slot: usize,
        b: &mut PinnedShardAppendBudget,
        call: impl FnOnce() -> io::Result<()>,
    ) {
        let result = catch_unwind(AssertUnwindSafe(|| {
            self.enter(phase, b)?;
            match phase {
                PinnedShardAppendPhase::Rollback => {
                    self.pending_file
                        .as_mut()
                        .ok_or(PinnedShardAppendFailure::Refused)?
                        .rollback_entered = true
                }
                PinnedShardAppendPhase::Flush => {
                    self.pending_file
                        .as_mut()
                        .ok_or(PinnedShardAppendFailure::Refused)?
                        .flush_entered = true
                }
                PinnedShardAppendPhase::SyncData => {
                    self.pending_file
                        .as_mut()
                        .ok_or(PinnedShardAppendFailure::Refused)?
                        .sync_entered = true
                }
                PinnedShardAppendPhase::DirectorySync => self.directory_sync_entered = true,
                _ => return Err(PinnedShardAppendFailure::Refused),
            }
            let index = self.outcomes.len();
            self.outcomes.push(Outcome::Unit(phase, call()));
            self.unfinished = None;
            self.last_completed = Some(phase);
            // Original IO Result is owned before any completion hook can unwind.
            if matches!(&self.outcomes[index], Outcome::Unit(_, Err(_))) {
                self.settlement_failure = Some(PinnedShardAppendFailure::Io);
            }
            completed(phase);
            Ok::<(), PinnedShardAppendFailure>(())
        }));
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => self.settlement_failure = Some(e),
            Err(p) => self.settlement_phase_panics[slot] = Some(p),
        }
    }
    pub(super) fn settle(
        &mut self,
        store: &mut ShardStore,
        pin: &PinnedShardDirectory,
        b: &mut PinnedShardAppendBudget,
    ) -> Result<(), PinnedShardAppendFailure> {
        // No new admission, retry or E after denial: ONLY already-prepaid slots
        // settle on the SAME descriptor. Each phase has independent unwind
        // custody so an earlier panic cannot skip a later prepaid durability call.
        if let Some(pending) = &self.pending_file
            && pending.sequence != store.active.sequence
        {
            return Err(PinnedShardAppendFailure::Refused);
        }
        if self
            .pending_file
            .as_ref()
            .is_some_and(|p| p.rollback.is_some() && !p.rollback_entered)
        {
            let start = self
                .pending_file
                .as_ref()
                .and_then(|p| p.rollback)
                .ok_or(PinnedShardAppendFailure::Refused)?;
            self.settle_one(PinnedShardAppendPhase::Rollback, 0, b, || {
                ftruncate(&store.active.file, start).map_err(io::Error::from)
            });
        }
        if self.pending_file.as_ref().is_some_and(|p| !p.flush_entered) {
            self.settle_one(PinnedShardAppendPhase::Flush, 1, b, || {
                store.active.file.flush()
            });
        }
        if self.pending_file.as_ref().is_some_and(|p| !p.sync_entered) {
            self.settle_one(PinnedShardAppendPhase::SyncData, 2, b, || {
                fdatasync(&store.active.file).map_err(io::Error::from)
            });
        }
        if self.pending_directory_sync && !self.directory_sync_entered {
            self.settle_one(PinnedShardAppendPhase::DirectorySync, 3, b, || {
                fsync(&pin.scope.directory).map_err(io::Error::from)
            });
        }
        Ok(())
    }
}
