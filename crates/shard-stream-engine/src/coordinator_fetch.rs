//! Same-E coordinator reads and finite native committed-candidate windows.

use super::*;
use shardlog::{StorageFetchBudget, StorageFetchFailure};
use std::mem::size_of;
use std::sync::mpsc::TrySendError;
use std::time::Instant;

#[derive(Debug)]
pub(crate) enum Failure {
    Budget(StorageFetchFailure),
    Engine(EngineError),
    LateEngine(EngineError),
}
impl From<EngineError> for Failure {
    fn from(error: EngineError) -> Self {
        Self::Engine(error)
    }
}
impl From<StorageFetchFailure> for Failure {
    fn from(error: StorageFetchFailure) -> Self {
        Self::Budget(error)
    }
}
impl Failure {
    pub(super) fn into_engine_error(self) -> EngineError {
        match self {
            Self::Engine(error) | Self::LateEngine(error) => error,
            Self::Budget(error) => EngineError::InvalidConfig(error.to_string()),
        }
    }
}
fn check(budget: Option<&StorageFetchBudget>) -> Result<(), Failure> {
    match budget {
        Some(budget) => budget.check().map_err(Failure::Budget),
        None => Ok(()),
    }
}
fn finish<T>(budget: &StorageFetchBudget, observed: Result<T, Failure>) -> Result<T, Failure> {
    let observed = if Instant::now() >= budget.absolute_expiry() {
        Err(match observed {
            Err(Failure::Engine(error)) | Err(Failure::LateEngine(error)) => {
                Failure::LateEngine(error)
            }
            _ => Failure::Budget(StorageFetchFailure::Expired(None)),
        })
    } else {
        observed.and_then(|value| {
            budget.check()?;
            Ok(value)
        })
    };
    if observed.is_err() {
        budget.deny();
    }
    observed
}

pub(super) enum Command {
    Snapshot {
        topic_partition: TopicPartition,
        start_offset: LogicalOffset,
        budget: StorageFetchBudget,
        response: SyncSender<Result<FetchBounds, Failure>>,
    },
    Candidates {
        topic_partition: TopicPartition,
        candidates: Vec<BatchId>,
        include_unresolved: bool,
        max_window: usize,
        budget: StorageFetchBudget,
        response: SyncSender<Result<CommittedBatchSet, Failure>>,
    },
}

impl CoordinatorPool {
    fn send_bounded_fetch(
        &self,
        topic_partition: TopicPartition,
        command: Command,
    ) -> Result<(), Failure> {
        let index = coordinator_index(topic_partition, self.workers.len());
        let sender = self.workers[index]
            .sender
            .as_ref()
            .ok_or_else(|| coordinator_stopped("partition coordinator"))?;
        sender
            .try_send(CoordinatorCommand::BoundedFetch(command))
            .map_err(|error| match error {
                TrySendError::Full(_) => Failure::Engine(EngineError::InvalidConfig(
                    "bounded fetch coordinator queue is full".into(),
                )),
                TrySendError::Disconnected(_) => {
                    Failure::Engine(coordinator_stopped("partition coordinator"))
                }
            })
    }
    pub(crate) fn fetch_snapshot_until(
        &self,
        topic_partition: TopicPartition,
        start_offset: LogicalOffset,
        budget: &StorageFetchBudget,
    ) -> Result<FetchBounds, Failure> {
        budget.charge_values(
            size_of::<Command>()
                + size_of::<(
                    SyncSender<Result<FetchBounds, Failure>>,
                    Receiver<Result<FetchBounds, Failure>>,
                )>()
                + size_of::<Result<FetchBounds, Failure>>(),
            4,
            size_of::<Command>(),
        )?;
        budget.check()?;
        let (response, receiver) = sync_channel(1);
        budget.check()?;
        let sent = self.send_bounded_fetch(
            topic_partition,
            Command::Snapshot {
                topic_partition,
                start_offset,
                budget: budget.clone(),
                response,
            },
        );
        if let Err(error) = sent {
            return finish(budget, Err(error));
        }
        // A real enqueue is always physically settled, even when queue residence
        // crosses E. The actual worker checks the same budget before native work.
        let observed = receiver
            .recv()
            .map_err(|_| Failure::Engine(coordinator_stopped("bounded fetch coordinator")))
            .and_then(|result| result);
        finish(budget, observed)
    }
    pub(crate) fn committed_candidates_until(
        &self,
        topic_partition: TopicPartition,
        candidates: Vec<BatchId>,
        include_unresolved: bool,
        max_window: usize,
        budget: &StorageFetchBudget,
    ) -> Result<CommittedBatchSet, Failure> {
        budget.charge_values(
            size_of::<Command>()
                + size_of::<(
                    SyncSender<Result<CommittedBatchSet, Failure>>,
                    Receiver<Result<CommittedBatchSet, Failure>>,
                )>()
                + size_of::<Result<CommittedBatchSet, Failure>>(),
            4,
            size_of::<Command>(),
        )?;
        budget.check()?;
        let (response, receiver) = sync_channel(1);
        budget.check()?;
        let sent = self.send_bounded_fetch(
            topic_partition,
            Command::Candidates {
                topic_partition,
                candidates,
                include_unresolved,
                max_window,
                budget: budget.clone(),
                response,
            },
        );
        if let Err(error) = sent {
            return finish(budget, Err(error));
        }
        let observed = receiver
            .recv()
            .map_err(|_| Failure::Engine(coordinator_stopped("bounded fetch coordinator")))
            .and_then(|result| result);
        finish(budget, observed)
    }
}

pub(super) fn execute(states: &HashMap<TopicPartition, PartitionState>, command: Command) {
    match command {
        Command::Snapshot {
            topic_partition,
            start_offset,
            budget,
            response,
        } => {
            let observed = (|| {
                budget.check()?;
                let observed = state(states, topic_partition)
                    .and_then(|state| state.fetch_bounds(start_offset));
                observed.map_err(Failure::Engine)
            })();
            let _ = response.send(finish(&budget, observed));
        }
        Command::Candidates {
            topic_partition,
            candidates,
            include_unresolved,
            max_window,
            budget,
            response,
        } => {
            let observed = (|| {
                budget.check()?;
                let state = state(states, topic_partition)?;
                committed_candidates(
                    state,
                    &candidates,
                    include_unresolved,
                    Some(&budget),
                    max_window,
                )
            })();
            let _ = response.send(finish(&budget, observed));
        }
    }
}

pub(super) fn committed_candidates(
    state: &PartitionState,
    candidates: &[BatchId],
    include_unresolved: bool,
    budget: Option<&StorageFetchBudget>,
    max_window: usize,
) -> Result<CommittedBatchSet, Failure> {
    check(budget)?;
    if let Some(budget) = budget {
        let bytes = candidates
            .len()
            .checked_mul(size_of::<BatchId>())
            .ok_or(StorageFetchFailure::Capacity)?;
        budget.charge_values(0, 0, bytes)?;
    }
    check(budget)?;
    let Some(first) = candidates.iter().map(|batch_id| batch_id.get()).min() else {
        return Ok(CommittedBatchSet::new(0, 0));
    };
    check(budget)?;
    if let Some(budget) = budget {
        let bytes = candidates
            .len()
            .checked_mul(size_of::<BatchId>())
            .ok_or(StorageFetchFailure::Capacity)?;
        budget.charge_values(0, 0, bytes)?;
    }
    check(budget)?;
    let last = candidates
        .iter()
        .map(|batch_id| batch_id.get())
        .max()
        .expect("nonempty candidates have a maximum");
    check(budget)?;
    let len = last
        .checked_sub(first)
        .and_then(|span| span.checked_add(1))
        .and_then(|span| usize::try_from(span).ok())
        .ok_or_else(|| EngineError::InvalidConfig("fetch candidate window exceeds usize".into()))?;
    if let Some(budget) = budget {
        if len > max_window {
            return Err(Failure::Budget(StorageFetchFailure::Capacity));
        }
        let words = len.div_ceil(u64::BITS as usize);
        let bytes = words
            .checked_mul(size_of::<u64>())
            .and_then(|value| value.checked_add(size_of::<CommittedBatchSet>()))
            .ok_or(StorageFetchFailure::Capacity)?;
        budget.charge_values(
            bytes,
            words.checked_add(1).ok_or(StorageFetchFailure::Capacity)?,
            bytes,
        )?;
    }
    check(budget)?;
    let mut committed = CommittedBatchSet::new(first, len);
    check(budget)?;
    for batch_id in candidates {
        check(budget)?;
        if let Some(budget) = budget {
            budget.charge_values(0, 0, size_of::<BatchId>() + size_of::<BatchRuntime>())?;
        }
        if state.batches.get(batch_id.get()).is_some_and(|batch| {
            batch.status == BatchStatus::Committed
                || (include_unresolved && batch.status == BatchStatus::Unresolved)
        }) {
            committed.insert(*batch_id);
        }
        check(budget)?;
    }
    Ok(committed)
}
