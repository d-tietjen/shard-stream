//! Original-E coordinator observations for actual native replay continuations.

use super::*;
use crate::engine::native_resume::{CoordinatorValue, NativeResumeObservation};
use crate::{NativeResumeCallbackError, NativeResumeWork};
use std::mem::size_of;
use std::sync::mpsc::TrySendError;

#[derive(Clone, Copy)]
enum Read {
    Watermarks,
    AtomicGroup(u128),
}
struct Reply(Option<SyncSender<NativeResumeObservation<CoordinatorValue>>>);
impl Reply {
    fn complete(mut self, observed: NativeResumeObservation<CoordinatorValue>) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(observed);
        }
    }
}
impl Drop for Reply {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let result = Err(NativeResumeCallbackError::worker_stopped(
                "native resume coordinator",
            ));
            // No native result was received. This is an actual stopped-command
            // failure, never a positive callback or whole-worker join claim.
            let _ = sender.send(NativeResumeObservation {
                result,
                refusal: None,
            });
        }
    }
}

struct EnteredJob {
    receiver: Option<Receiver<NativeResumeObservation<CoordinatorValue>>>,
    work: NativeResumeWork,
}
impl EnteredJob {
    fn receive(mut self) -> NativeResumeObservation<CoordinatorValue> {
        let result = match self.receiver.as_ref() {
            Some(receiver) => receiver.recv(),
            None => {
                return NativeResumeObservation::completed(
                    Err(NativeResumeCallbackError::invalid_state(
                        "bounded coordinator completion was already consumed",
                    )),
                    &self.work,
                );
            }
        };
        self.receiver.take();
        match result {
            Ok(observed) => observed,
            Err(_) => NativeResumeObservation::completed(
                Err(NativeResumeCallbackError::worker_stopped(
                    "native resume coordinator",
                )),
                &self.work,
            ),
        }
    }
}
impl Drop for EnteredJob {
    fn drop(&mut self) {
        if let Some(receiver) = self.receiver.as_ref() {
            // Even an unwinding caller settles this actual entered command
            // before its outside EngineHandle can release native custody.
            self.work.deny();
            let _ = receiver.recv();
        }
    }
}

pub(super) struct Command {
    topic_partition: TopicPartition,
    read: Read,
    work: NativeResumeWork,
    // Last: real command inputs and counters settle before fallback reply.
    reply: Reply,
}

impl CoordinatorPool {
    pub(crate) fn resume_watermarks(
        &self,
        partition: TopicPartition,
        work: &NativeResumeWork,
    ) -> NativeResumeObservation<CoordinatorValue> {
        self.resume_read(partition, Read::Watermarks, work)
    }
    pub(crate) fn resume_atomic_group(
        &self,
        partition: TopicPartition,
        transaction_id: u128,
        work: &NativeResumeWork,
    ) -> NativeResumeObservation<CoordinatorValue> {
        self.resume_read(partition, Read::AtomicGroup(transaction_id), work)
    }
    fn resume_read(
        &self,
        partition: TopicPartition,
        read: Read,
        work: &NativeResumeWork,
    ) -> NativeResumeObservation<CoordinatorValue> {
        let values = size_of::<Command>()
            + size_of::<EnteredJob>()
            + size_of::<(
                SyncSender<NativeResumeObservation<CoordinatorValue>>,
                Receiver<NativeResumeObservation<CoordinatorValue>>,
            )>()
            + size_of::<NativeResumeObservation<CoordinatorValue>>();
        // The shared native inspector's only formatted error is its bounded
        // u128 transaction id between these exact literal fragments.
        let strings =
            "atomic group ".len() + (u128::MAX.ilog10() as usize + 1) + " has no chunks".len();
        if let Err(error) = work.reserve_command(values + strings, strings, 5, 0) {
            return NativeResumeObservation::refused(error);
        }
        if let Err(error) = work.check() {
            return NativeResumeObservation::refused(error);
        }
        let (sender, receiver) = sync_channel(1);
        if let Err(error) = work.check() {
            return NativeResumeObservation::refused(error);
        }
        let index = coordinator_index(partition, self.workers.len());
        let Some(queue) = self.workers[index].sender.as_ref() else {
            return NativeResumeObservation::completed(
                Err(NativeResumeCallbackError::worker_stopped(
                    "native resume coordinator",
                )),
                work,
            );
        };
        let sent = queue.try_send(CoordinatorCommand::BoundedResume(Command {
            topic_partition: partition,
            read,
            work: work.clone(),
            reply: Reply(Some(sender)),
        }));
        if let Err(error) = sent {
            let error = match error {
                TrySendError::Full(_) => {
                    NativeResumeCallbackError::queue_full("native resume coordinator")
                }
                TrySendError::Disconnected(_) => {
                    NativeResumeCallbackError::worker_stopped("native resume coordinator")
                }
            };
            return NativeResumeObservation::completed(Err(error), work);
        }
        // An actual enqueue is always drained. Queue residence cannot replace E;
        // the worker checks the same counters before any native observation.
        EnteredJob {
            receiver: Some(receiver),
            work: work.clone(),
        }
        .receive()
    }
}

pub(super) fn execute(states: &HashMap<TopicPartition, PartitionState>, command: Command) {
    // Keep the real command intact through native work/unwind. Its reply is
    // declared last, after every owned input and carried budget field.
    let result = (|| {
        command
            .work
            .check()
            .map_err(NativeResumeCallbackError::refused)?;
        command
            .work
            .reserve_table_probe::<(TopicPartition, PartitionState)>(
                states.capacity(),
                "coordinator.states",
            )
            .map_err(NativeResumeCallbackError::refused)?;
        let state = state(states, command.topic_partition)
            .map_err(NativeResumeCallbackError::from_engine)?;
        match command.read {
            Read::Watermarks => Ok(CoordinatorValue::Watermarks(state.watermarks())),
            Read::AtomicGroup(transaction) => {
                command
                    .work
                    .reserve_table_probe::<(u128, AtomicGroupRuntime)>(
                        state.atomic_groups.capacity(),
                        "coordinator.atomic_groups",
                    )
                    .map_err(NativeResumeCallbackError::refused)?;
                inspect_atomic_group(state, transaction)
                    .map(CoordinatorValue::AtomicGroup)
                    .map_err(NativeResumeCallbackError::from_engine)
            }
        }
    })();
    let observed = NativeResumeObservation::completed(result, &command.work);
    let Command { work, reply, .. } = command;
    drop(work);
    reply.complete(observed);
}
