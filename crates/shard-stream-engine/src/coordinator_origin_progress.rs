//! Actual bounded same-worker producer lookup, with physical ticket custody.

use super::*;
use crate::engine::origin_progress::{
    NativeOriginProgressBudget, NativeOriginProgressObservation, NativeOriginProgressQueryError,
};
use shardlog::StorageFetchFailure;
use std::mem::size_of;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::mpsc::TrySendError;

#[cfg(test)]
type Observer = std::sync::Arc<dyn Fn(&str) + Send + Sync>;
#[cfg(test)]
thread_local! { static OBSERVER: std::cell::RefCell<Option<Observer>> = const { std::cell::RefCell::new(None) }; }
#[cfg(test)]
pub(crate) struct Observe;
#[cfg(test)]
impl Drop for Observe {
    fn drop(&mut self) {
        OBSERVER.with(|slot| *slot.borrow_mut() = None);
    }
}
#[cfg(test)]
pub(crate) fn observe(callback: Observer) -> Observe {
    OBSERVER.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some(callback);
    });
    Observe
}

struct Reply(Option<SyncSender<NativeOriginProgressObservation>>);
impl Reply {
    fn complete(mut self, observed: NativeOriginProgressObservation) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(observed);
        }
    }
}
impl Drop for Reply {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(NativeOriginProgressObservation {
                result: Err(NativeOriginProgressQueryError::WorkerStopped),
                refusal: None,
                post_panic: None,
            });
        }
    }
}

pub(crate) struct Ticket {
    receiver: Option<Receiver<NativeOriginProgressObservation>>,
    budget: NativeOriginProgressBudget,
}
impl Ticket {
    pub(crate) fn settle(&mut self) -> NativeOriginProgressObservation {
        let actual = match self.receiver.as_ref() {
            Some(receiver) => receiver.recv(),
            None => {
                return NativeOriginProgressObservation::completed(
                    Err(NativeOriginProgressQueryError::WorkerStopped),
                    &self.budget,
                );
            }
        };
        self.receiver.take();
        match actual {
            Ok(observed) => observed,
            Err(_) => NativeOriginProgressObservation::completed(
                Err(NativeOriginProgressQueryError::WorkerStopped),
                &self.budget,
            ),
        }
    }
}
impl Drop for Ticket {
    fn drop(&mut self) {
        if let Some(receiver) = &self.receiver {
            self.budget.deny();
            let _ = receiver.recv();
        }
    }
}

pub(super) struct Command {
    partition: TopicPartition,
    producer_id: u128,
    budget: NativeOriginProgressBudget,
    #[cfg(test)]
    observer: Option<Observer>,
    // LAST: actual command inputs/counters retire before fallback completion.
    reply: Reply,
}

pub(crate) fn command_cost() -> Option<(usize, usize)> {
    let bytes = size_of::<Command>()
        .checked_add(size_of::<Ticket>())?
        .checked_add(size_of::<(
            SyncSender<NativeOriginProgressObservation>,
            Receiver<NativeOriginProgressObservation>,
        )>())?
        .checked_add(size_of::<NativeOriginProgressObservation>())?;
    Some((bytes, 5))
}

impl CoordinatorPool {
    // Keep the actual coordinator refusal inline without allocating after admission fails.
    #[allow(clippy::result_large_err)]
    pub(crate) fn begin_origin_progress(
        &self,
        partition: TopicPartition,
        producer_id: u128,
        budget: &NativeOriginProgressBudget,
    ) -> Result<Ticket, NativeOriginProgressObservation> {
        let before = (|| {
            let (bytes, entries) = command_cost().ok_or(StorageFetchFailure::Capacity)?;
            budget.storage().charge_values(bytes, entries, bytes)?;
            budget.check()
        })();
        if let Err(error) = before {
            return Err(NativeOriginProgressObservation::completed(
                Err(NativeOriginProgressQueryError::Budget(error)),
                budget,
            ));
        }
        let (sender, receiver) = sync_channel(1);
        if let Err(error) = budget.check() {
            return Err(NativeOriginProgressObservation::completed(
                Err(NativeOriginProgressQueryError::Budget(error)),
                budget,
            ));
        }
        let index = coordinator_index(partition, self.workers.len());
        let Some(queue) = self.workers[index].sender.as_ref() else {
            return Err(NativeOriginProgressObservation::completed(
                Err(NativeOriginProgressQueryError::WorkerStopped),
                budget,
            ));
        };
        let command = Command {
            partition,
            producer_id,
            budget: budget.share(),
            #[cfg(test)]
            observer: OBSERVER.with(|slot| slot.borrow().clone()),
            reply: Reply(Some(sender)),
        };
        match queue.try_send(CoordinatorCommand::BoundedOriginProgress(command)) {
            Ok(()) => Ok(Ticket {
                receiver: Some(receiver),
                budget: budget.share(),
            }),
            Err(error) => Err(NativeOriginProgressObservation::completed(
                Err(match error {
                    TrySendError::Full(_) => NativeOriginProgressQueryError::QueueFull,
                    TrySendError::Disconnected(_) => NativeOriginProgressQueryError::WorkerStopped,
                }),
                budget,
            )),
        }
    }
}

fn probe<T>(
    capacity: usize,
    budget: &NativeOriginProgressBudget,
) -> Result<(), NativeOriginProgressQueryError> {
    let bytes = capacity
        .checked_add(1)
        .and_then(|n| n.checked_mul(size_of::<T>().max(1)))
        .ok_or(StorageFetchFailure::Capacity)
        .map_err(NativeOriginProgressQueryError::Budget)?;
    budget
        .storage()
        .charge_values(0, 1, bytes)
        .map_err(NativeOriginProgressQueryError::Budget)?;
    budget
        .check()
        .map_err(NativeOriginProgressQueryError::Budget)
}

pub(super) fn execute(states: &HashMap<TopicPartition, PartitionState>, command: Command) {
    let actual = catch_unwind(AssertUnwindSafe(|| {
        command
            .budget
            .check()
            .map_err(NativeOriginProgressQueryError::Budget)?;
        probe::<(TopicPartition, PartitionState)>(states.capacity(), &command.budget)?;
        let state =
            state(states, command.partition).map_err(NativeOriginProgressQueryError::Engine)?;
        probe::<(u128, ProducerCursor)>(state.producers.capacity(), &command.budget)?;
        Ok(state
            .producers
            .get(&command.producer_id)
            .map(|cursor| ProducerProgress {
                epoch: cursor.epoch,
                next_sequence: cursor.next_sequence,
                requires_new_epoch: cursor.failed,
            }))
    }));
    let result = match actual {
        Ok(result) => result,
        Err(payload) => Err(NativeOriginProgressQueryError::Unwind(payload)),
    };
    // The actual cursor/absence/error is owned before post-E or a test-only
    // physical completion barrier. No result is replaced by the later clock.
    let mut observed = NativeOriginProgressObservation {
        result,
        refusal: None,
        post_panic: None,
    };
    #[cfg(test)]
    if let Some(observer) = &command.observer {
        observed.post_panic = catch_unwind(AssertUnwindSafe(|| observer("observed"))).err();
    }
    observed.refusal = command.budget.check().err();
    let Command { budget, reply, .. } = command;
    drop(budget);
    reply.complete(observed);
}
