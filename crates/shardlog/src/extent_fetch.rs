//! Shared ordinary and bounded native extent fetch planning and reads.

use super::*;

#[cfg(test)]
#[path = "extent_fetch_tests.rs"]
mod tests;

impl ShardStore {
    pub fn plan_fetch(
        &self,
        topic_partition: TopicPartition,
        start_offset: LogicalOffset,
        max_bytes: usize,
        max_batches: usize,
    ) -> StorageResult<Vec<StoredBatchMetadata>> {
        self.plan_fetch_inner(topic_partition, start_offset, max_bytes, max_batches, None)
            .map_err(|error| error.failure.into_storage_error())
    }

    /// Plan native metadata under the same original operation budget/E.
    ///
    /// Zero-payload history still pays finite metadata slots. Refusal retains
    /// every completed plan entry; no source/storage authority is constructed.
    pub fn plan_fetch_until(
        &self,
        topic_partition: TopicPartition,
        start_offset: LogicalOffset,
        max_bytes: usize,
        max_batches: usize,
        budget: &crate::StorageFetchBudget,
    ) -> Result<Vec<StoredBatchMetadata>, crate::StorageFetchPlanError> {
        self.plan_fetch_inner(
            topic_partition,
            start_offset,
            max_bytes,
            max_batches,
            Some(budget),
        )
    }

    fn plan_fetch_inner(
        &self,
        topic_partition: TopicPartition,
        start_offset: LogicalOffset,
        max_bytes: usize,
        max_batches: usize,
        budget: Option<&crate::StorageFetchBudget>,
    ) -> Result<Vec<StoredBatchMetadata>, crate::StorageFetchPlanError> {
        let mut result = Vec::new();
        let observed = (|| {
            fetch_check(budget)?;
            if let Some(budget) = budget {
                if max_batches > budget.limits().max_plan_batches {
                    return Err(crate::StorageFetchFailure::Capacity);
                }
                budget.charge(
                    size_of::<Vec<StoredBatchMetadata>>(),
                    1,
                    0,
                    0,
                    size_of::<TopicPartition>(),
                )?;
            }
            if max_bytes == 0 || max_batches == 0 {
                return Err(StorageError::InvalidInput(
                    "fetch byte and batch limits must be nonzero".into(),
                )
                .into());
            }
            let Some(locations) = self.index.get(&topic_partition) else {
                return Ok(());
            };
            if let Some(budget) = budget {
                // partition_point visits at most the machine-word width plus
                // one native entry, irrespective of retained history length.
                let work = (usize::BITS as usize + 1)
                    .checked_mul(size_of::<ExtentLocation>())
                    .ok_or(crate::StorageFetchFailure::Capacity)?;
                budget.charge(0, 0, 0, 0, work)?;
            }
            fetch_check(budget)?;
            let first = locations
                .partition_point(|location| location.reservation.last_offset < start_offset);
            fetch_check(budget)?;
            let capacity = FETCH_PLAN_INITIAL_CAPACITY
                .min(max_batches)
                .min(locations.len().saturating_sub(first));
            let capacity = if budget.is_some() {
                max_batches.min(locations.len().saturating_sub(first))
            } else {
                capacity
            };
            if let Some(budget) = budget {
                let bytes = capacity
                    .checked_mul(size_of::<StoredBatchMetadata>())
                    .ok_or(crate::StorageFetchFailure::Capacity)?;
                budget.charge(bytes, capacity, 0, 0, bytes)?;
            }
            fetch_check(budget)?;
            result = Vec::with_capacity(capacity);
            fetch_check(budget)?;
            let mut used = 0usize;
            for location in &locations[first..] {
                fetch_check(budget)?;
                if result.len() == max_batches {
                    break;
                }
                if let Some(budget) = budget {
                    budget.charge(
                        0,
                        0,
                        0,
                        0,
                        size_of::<ExtentLocation>() + size_of::<StoredBatchMetadata>(),
                    )?;
                }
                let Some(next) = selected_fetch_bytes(
                    used,
                    location.payload_len,
                    !result.is_empty(),
                    max_bytes,
                )?
                else {
                    break;
                };
                fetch_check(budget)?;
                result.push(self.metadata_for(location));
                used = next;
                fetch_check(budget)?;
            }
            Ok(())
        })();
        match finish_fetch(budget, observed) {
            Ok(()) => Ok(result),
            Err(failure) => Err(crate::StorageFetchPlanError {
                failure,
                planned: result,
            }),
        }
    }

    pub fn read_fetch_plan(
        &mut self,
        planned: &[StoredBatchMetadata],
    ) -> StorageResult<Vec<StoredBatch>> {
        self.read_fetch_plan_inner(planned, None)
            .map_err(|error| error.failure.into_storage_error())
    }

    /// Read a real native plan with finite metadata, buffer/read fees and E.
    ///
    /// Every physical read attempt is charged before entry. Completed batches
    /// and their actual buffer owners remain in a refused result. The caller
    /// retains its real storage/engine until this synchronous work settles.
    pub fn read_fetch_plan_until(
        &mut self,
        planned: &[StoredBatchMetadata],
        budget: &crate::StorageFetchBudget,
    ) -> Result<Vec<StoredBatch>, crate::StorageFetchReadError> {
        self.read_fetch_plan_inner(planned, Some(budget))
    }

    fn read_fetch_plan_inner(
        &mut self,
        planned: &[StoredBatchMetadata],
        budget: Option<&crate::StorageFetchBudget>,
    ) -> Result<Vec<StoredBatch>, crate::StorageFetchReadError> {
        let mut result = Vec::new();
        let observed = (|| {
            fetch_check(budget)?;
            if let Some(budget) = budget {
                if planned.len() > budget.limits().max_plan_batches {
                    return Err(crate::StorageFetchFailure::Capacity);
                }
                let bytes = planned
                    .len()
                    .checked_mul(size_of::<StoredBatch>())
                    .and_then(|value| value.checked_add(size_of::<Vec<StoredBatch>>()))
                    .ok_or(crate::StorageFetchFailure::Capacity)?;
                let work = planned
                    .len()
                    .checked_mul(size_of::<StoredBatchMetadata>())
                    .ok_or(crate::StorageFetchFailure::Capacity)?;
                budget.charge(
                    bytes,
                    planned
                        .len()
                        .checked_add(1)
                        .ok_or(crate::StorageFetchFailure::Capacity)?,
                    0,
                    0,
                    work,
                )?;
            }
            fetch_check(budget)?;
            if planned.iter().any(|metadata| {
                metadata.store_token != self.store_token || metadata.plan_epoch != self.plan_epoch
            }) {
                return Err(StorageError::InvalidInput(
                    "fetch plan does not belong to the current store generation".into(),
                )
                .into());
            }
            fetch_check(budget)?;
            result = Vec::with_capacity(planned.len());
            fetch_check(budget)?;
            let mut first = 0usize;
            while first < planned.len() {
                fetch_check(budget)?;
                let mut end = first + 1;
                let mut range_end = payload_end(&planned[first])?;
                while end < planned.len() {
                    fetch_check(budget)?;
                    if let Some(budget) = budget {
                        budget.charge(0, 0, 0, 0, size_of::<StoredBatchMetadata>())?;
                    }
                    let next = planned[end];
                    if !coalesces(&planned[first], &next, range_end) {
                        break;
                    }
                    range_end = payload_end(&next)?;
                    end += 1;
                }
                let range_start = planned[first].payload_offset;
                let range_len = usize::try_from(range_end - range_start)
                    .map_err(|_| StorageError::LimitExceeded("fetch range exceeds usize".into()))?;
                if let Some(budget) = budget {
                    let path_bytes = self
                        .pack_paths
                        .get(&planned[first].pack_sequence)
                        .map_or(0, |path| path.as_os_str().as_encoded_bytes().len());
                    if path_bytes > budget.limits().max_path_bytes {
                        return Err(crate::StorageFetchFailure::Capacity);
                    }
                    let bytes = path_bytes
                        .checked_add(1)
                        .ok_or(crate::StorageFetchFailure::Capacity)?;
                    budget.charge(bytes, 1, 0, 0, bytes)?;
                }
                fetch_check(budget)?;
                let opened = self.open_pack_reader(planned[first].pack_sequence);
                #[cfg(test)]
                if budget.is_some() {
                    tests::completed("open");
                }
                let reader = match budget {
                    Some(budget) => budget.completed(opened)?,
                    None => opened?,
                };
                if let Some(budget) = budget {
                    let bytes = range_len
                        .checked_add(size_of::<ReadBufferOwner>())
                        .and_then(|value| value.checked_add(size_of::<Bytes>()))
                        .and_then(|value| value.checked_add(size_of::<[usize; 2]>()))
                        .ok_or(crate::StorageFetchFailure::Capacity)?;
                    budget.charge(bytes, 3, 0, 0, range_len)?;
                }
                fetch_check(budget)?;
                // Bounded reads allocate exactly the requested logical buffer;
                // they do not acquire an oversized historical pool capacity.
                let mut buffer = if budget.is_some() {
                    vec![0; range_len]
                } else {
                    self.read_buffer_pool.take(range_len)
                };
                fetch_check(budget)?;
                let read = match budget {
                    Some(budget) => {
                        read_exact_at_inner(&reader, &mut buffer, range_start, Some(budget))
                    }
                    None => read_exact_at(&reader, &mut buffer, range_start)
                        .map_err(StorageError::Io)
                        .map_err(crate::StorageFetchFailure::from),
                };
                if let Err(error) = read {
                    self.read_buffer_pool.recycle_now(buffer);
                    return Err(error);
                }
                fetch_check(budget)?;
                let buffer = self.read_buffer_pool.lease_bytes(buffer);
                fetch_check(budget)?;
                for metadata in &planned[first..end] {
                    fetch_check(budget)?;
                    if let Some(budget) = budget {
                        budget.charge(
                            0,
                            0,
                            0,
                            0,
                            size_of::<StoredBatchMetadata>() + size_of::<StoredBatch>(),
                        )?;
                    }
                    let payload_start = usize::try_from(metadata.payload_offset - range_start)
                        .map_err(|_| {
                            StorageError::LimitExceeded("payload offset exceeds usize".into())
                        })?;
                    let payload_end =
                        payload_start
                            .checked_add(metadata.payload_len)
                            .ok_or_else(|| {
                                StorageError::LimitExceeded("payload range overflow".into())
                            })?;
                    fetch_check(budget)?;
                    result.push(StoredBatch {
                        reservation: metadata.reservation,
                        producer: metadata.producer,
                        atomic_group: metadata.atomic_group,
                        payload: buffer.slice(payload_start..payload_end),
                    });
                    fetch_check(budget)?;
                }
                first = end;
            }
            Ok(())
        })();
        match finish_fetch(budget, observed) {
            Ok(()) => Ok(result),
            Err(failure) => Err(crate::StorageFetchReadError {
                failure,
                batches: result,
            }),
        }
    }
}

pub(super) fn payload_end(metadata: &StoredBatchMetadata) -> StorageResult<u64> {
    metadata
        .payload_offset
        .checked_add(
            u64::try_from(metadata.payload_len)
                .map_err(|_| StorageError::LimitExceeded("payload length exceeds u64".into()))?,
        )
        .ok_or_else(|| StorageError::LimitExceeded("payload end overflow".into()))
}

fn fetch_check(
    budget: Option<&crate::StorageFetchBudget>,
) -> Result<(), crate::StorageFetchFailure> {
    match budget {
        Some(budget) => budget.check(),
        None => Ok(()),
    }
}

fn finish_fetch(
    budget: Option<&crate::StorageFetchBudget>,
    observed: Result<(), crate::StorageFetchFailure>,
) -> Result<(), crate::StorageFetchFailure> {
    let Some(budget) = budget else {
        return observed;
    };
    let observed = if Instant::now() >= budget.absolute_expiry() {
        Err(match observed {
            Ok(()) => crate::StorageFetchFailure::Expired(None),
            Err(error) => error.late(),
        })
    } else {
        observed.and_then(|()| budget.check())
    };
    if observed.is_err() {
        budget.deny();
    }
    observed
}

fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> std::io::Result<()> {
    read_exact_at_inner(file, buffer, offset, None).map_err(|error| {
        match error.into_storage_error() {
            StorageError::Io(error) => error,
            error => std::io::Error::other(error),
        }
    })
}

fn read_exact_at_inner(
    file: &File,
    mut buffer: &mut [u8],
    mut offset: u64,
    budget: Option<&crate::StorageFetchBudget>,
) -> Result<(), crate::StorageFetchFailure> {
    while !buffer.is_empty() {
        fetch_check(budget)?;
        if let Some(budget) = budget {
            budget.charge(0, 1, 1, buffer.len(), buffer.len())?;
        }
        fetch_check(budget)?;
        let observed = file.read_at(buffer, offset).map_err(StorageError::Io);
        #[cfg(test)]
        if budget.is_some() {
            tests::completed("read");
        }
        let read = match budget {
            Some(budget) => budget.completed(observed)?,
            None => observed?,
        };
        match read {
            0 => {
                return Err(StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "planned extent ended before its payload",
                ))
                .into());
            }
            read => {
                offset = offset.checked_add(read as u64).ok_or_else(|| {
                    StorageError::Io(std::io::Error::other("read offset overflow"))
                })?;
                buffer = &mut buffer[read..];
            }
        }
        fetch_check(budget)?;
    }
    Ok(())
}

// Shared selection and coalescing policy; the caller owns all charging and IO.
pub(super) fn selected_fetch_bytes(
    used: usize,
    payload_len: usize,
    has_batches: bool,
    max_bytes: usize,
) -> StorageResult<Option<usize>> {
    let next = used
        .checked_add(payload_len)
        .ok_or_else(|| StorageError::LimitExceeded("fetch byte count overflow".into()))?;
    if has_batches && next > max_bytes {
        return Ok(None);
    }
    Ok(Some(next))
}
pub(super) fn coalesces(
    first: &StoredBatchMetadata,
    next: &StoredBatchMetadata,
    range_end: u64,
) -> bool {
    !(next.pack_sequence != first.pack_sequence
        || next.payload_offset < range_end
        || next.payload_offset - range_end > MAX_COALESCED_READ_GAP)
}
