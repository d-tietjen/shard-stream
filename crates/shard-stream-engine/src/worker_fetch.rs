//! Finite native fetch queue entries with actual completion on unwind/drop.

use super::*;
use shardlog::{
    StorageFetchBudget, StorageFetchFailure, StorageFetchPlanError, StorageFetchReadError,
};
#[cfg(test)]
use std::cell::RefCell;

#[cfg(test)]
type Observer = Arc<dyn Fn(&str) + Send + Sync>;
#[cfg(test)]
thread_local! { static OBSERVER: RefCell<Option<Observer>> = RefCell::new(None); }
#[cfg(test)]
pub(crate) struct Observe;
#[cfg(test)]
impl Drop for Observe {
    fn drop(&mut self) {
        OBSERVER.with(|observer| *observer.borrow_mut() = None);
    }
}
#[cfg(test)]
pub(crate) fn observe(callback: Observer) -> Observe {
    OBSERVER.with(|observer| {
        assert!(observer.borrow().is_none());
        *observer.borrow_mut() = Some(callback);
    });
    Observe
}
#[cfg(test)]
fn observer() -> Option<Observer> {
    OBSERVER.with(|observer| observer.borrow().clone())
}

pub(crate) enum BoundedFetchCompletion {
    Plan {
        lane: usize,
        result: Result<Vec<StoredBatchMetadata>, StorageFetchPlanError>,
    },
    Read {
        lane: usize,
        result: Result<Vec<StoredBatch>, StorageFetchReadError>,
    },
    Stopped {
        lane: usize,
        shard: ShardId,
    },
}

pub(crate) enum DispatchError {
    Budget {
        failure: StorageFetchFailure,
        queue_error: Option<EngineError>,
    },
    Queue(EngineError),
}

struct Reply {
    lane: usize,
    shard: ShardId,
    completion: Option<Sender<BoundedFetchCompletion>>,
}
impl Reply {
    fn complete(mut self, completion: BoundedFetchCompletion) {
        if let Some(sender) = self.completion.take() {
            let _ = sender.send(completion);
        }
    }
    fn disarm(&mut self) {
        self.completion.take();
    }
}
impl Drop for Reply {
    fn drop(&mut self) {
        // A real queued command that unwinds, or is destroyed with a stopped
        // receiver, still settles its waiting caller. This is no authority.
        if let Some(sender) = self.completion.take() {
            let _ = sender.send(BoundedFetchCompletion::Stopped {
                lane: self.lane,
                shard: self.shard,
            });
        }
    }
}

pub(crate) struct PlanInput {
    pub lane: usize,
    pub topic_partition: TopicPartition,
    pub start_offset: LogicalOffset,
    pub max_bytes: usize,
    pub max_batches: usize,
}

enum Operation {
    Taken,
    Plan {
        topic_partition: TopicPartition,
        start_offset: LogicalOffset,
        max_bytes: usize,
        max_batches: usize,
    },
    Read(Vec<StoredBatchMetadata>),
}

pub(super) struct Command {
    operation: Operation,
    budget: StorageFetchBudget,
    pub(super) byte_charge: Option<Budgeted<()>>,
    #[cfg(test)]
    observer: Option<Observer>,
    // Last on drop: any actual plan/payload inputs and retained queue charge
    // settle before a fallback completion reports command destruction.
    reply: Reply,
}

pub(crate) const fn bounded_fetch_command_bytes() -> usize {
    std::mem::size_of::<Command>()
}

impl ShardHandle {
    pub(crate) fn begin_bounded_fetch_plan(
        &self,
        input: PlanInput,
        budget: &StorageFetchBudget,
        completion: Sender<BoundedFetchCompletion>,
    ) -> Result<(), DispatchError> {
        let PlanInput {
            lane,
            topic_partition,
            start_offset,
            max_bytes,
            max_batches,
        } = input;
        budget
            .charge_values(
                std::mem::size_of::<Command>(),
                1,
                std::mem::size_of::<Command>(),
            )
            .map_err(|failure| DispatchError::Budget {
                failure,
                queue_error: None,
            })?;
        let bytes = std::mem::size_of::<Command>()
            .checked_add(COMMAND_OVERHEAD_BYTES)
            .ok_or_else(|| DispatchError::Budget {
                failure: StorageFetchFailure::Capacity,
                queue_error: None,
            })?;
        budget.check().map_err(|failure| DispatchError::Budget {
            failure,
            queue_error: None,
        })?;
        let command = Command {
            operation: Operation::Plan {
                topic_partition,
                start_offset,
                max_bytes,
                max_batches,
            },
            budget: budget.clone(),
            byte_charge: None,
            #[cfg(test)]
            observer: observer(),
            reply: Reply {
                lane,
                shard: self.shard_id,
                completion: Some(completion),
            },
        };
        self.send_bounded_fetch(command, bytes)
    }
    pub(crate) fn begin_bounded_fetch_read(
        &self,
        lane: usize,
        planned: Vec<StoredBatchMetadata>,
        budget: &StorageFetchBudget,
        completion: Sender<BoundedFetchCompletion>,
    ) -> Result<(), DispatchError> {
        budget
            .charge_values(
                std::mem::size_of::<Command>(),
                1,
                std::mem::size_of::<Command>(),
            )
            .map_err(|failure| DispatchError::Budget {
                failure,
                queue_error: None,
            })?;
        let bytes = planned
            .capacity()
            .checked_mul(std::mem::size_of::<StoredBatchMetadata>())
            .and_then(|value| value.checked_add(COMMAND_OVERHEAD_BYTES))
            .and_then(|value| value.checked_add(std::mem::size_of::<Command>()))
            .ok_or_else(|| DispatchError::Budget {
                failure: StorageFetchFailure::Capacity,
                queue_error: None,
            })?;
        budget.check().map_err(|failure| DispatchError::Budget {
            failure,
            queue_error: None,
        })?;
        let command = Command {
            operation: Operation::Read(planned),
            budget: budget.clone(),
            byte_charge: None,
            #[cfg(test)]
            observer: observer(),
            reply: Reply {
                lane,
                shard: self.shard_id,
                completion: Some(completion),
            },
        };
        self.send_bounded_fetch(command, bytes)
    }
    fn send_bounded_fetch(&self, mut command: Command, bytes: usize) -> Result<(), DispatchError> {
        if let Err(failure) = command.budget.check() {
            command.reply.disarm();
            return Err(DispatchError::Budget {
                failure,
                queue_error: None,
            });
        }
        let Some(sender) = &self.sender else {
            command.reply.disarm();
            return Err(DispatchError::Queue(EngineError::WorkerStopped(
                self.shard_id,
            )));
        };
        match sender.try_send(ShardCommand::BoundedFetch(command), bytes) {
            // A successful physical enqueue remains an actual pending command,
            // even if it settles past E. The caller counts and drains it.
            Ok(()) => Ok(()),
            Err(error) => {
                let (mut command, error) = match error {
                    TrySendError::TooLarge { item, .. } => (
                        item,
                        EngineError::AdmissionLimited {
                            shard_id: self.shard_id,
                            reason: "bounded fetch command exceeds shard byte budget",
                        },
                    ),
                    TrySendError::ByteBudget { item, .. } => (
                        item,
                        EngineError::AdmissionLimited {
                            shard_id: self.shard_id,
                            reason: "bounded fetch shard byte budget exhausted",
                        },
                    ),
                    TrySendError::Full(item) => (
                        item,
                        EngineError::AdmissionLimited {
                            shard_id: self.shard_id,
                            reason: "bounded fetch shard slot budget exhausted",
                        },
                    ),
                    TrySendError::Disconnected(item) => {
                        (item, EngineError::WorkerStopped(self.shard_id))
                    }
                };
                if let ShardCommand::BoundedFetch(command) = &mut command {
                    command.reply.disarm(); // this command was never queued
                    return match command.budget.check() {
                        Ok(()) => Err(DispatchError::Queue(error)),
                        Err(failure) => Err(DispatchError::Budget {
                            failure,
                            queue_error: Some(error),
                        }),
                    };
                }
                Err(DispatchError::Queue(error))
            }
        }
    }
}

pub(super) fn execute(stores: &mut LaneStores, mut command: Command) {
    // Keep the actual command intact through native work. Its last reply field
    // reports unwind only AFTER the real input/charge fields have dropped.
    #[cfg(test)]
    if let Some(observer) = &command.observer {
        observer("entered");
    }
    let lane = command.reply.lane;
    let operation = std::mem::replace(&mut command.operation, Operation::Taken);
    let result = match operation {
        Operation::Plan {
            topic_partition,
            start_offset,
            max_bytes,
            max_batches,
        } => BoundedFetchCompletion::Plan {
            lane,
            result: stores.primary.plan_fetch_until(
                topic_partition,
                start_offset,
                max_bytes,
                max_batches,
                &command.budget,
            ),
        },
        Operation::Read(planned) => BoundedFetchCompletion::Read {
            lane,
            result: stores
                .primary
                .read_fetch_plan_until(&planned, &command.budget),
        },
        Operation::Taken => BoundedFetchCompletion::Stopped {
            lane,
            shard: command.reply.shard,
        },
    };
    let Command {
        operation,
        budget,
        byte_charge,
        reply,
        #[cfg(test)]
        observer,
    } = command;
    drop(operation);
    drop(byte_charge);
    drop(budget);
    #[cfg(test)]
    drop(observer);
    reply.complete(result);
}
