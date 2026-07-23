use std::path::PathBuf;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread::{self, JoinHandle};

use shard_stream_core::{
    ByteBoundedSender, LogicalOffset, Reservation, ShardId, TopicPartition, TrySendError,
    byte_bounded_channel,
};
use shard_stream_storage::{
    ExtentCatalogEntry, ShardStore, ShardStoreConfig, StorageResult, StoredBatch,
};

use crate::config::COMMAND_OVERHEAD_BYTES;
use crate::{EngineConfig, EngineError, EngineResult};

enum ShardCommand {
    Append {
        reservation: Reservation,
        payload: Vec<u8>,
        sync: bool,
        response: SyncSender<StorageResult<u32>>,
    },
    Fetch {
        topic_partition: TopicPartition,
        start_offset: LogicalOffset,
        max_bytes: usize,
        response: SyncSender<StorageResult<Vec<StoredBatch>>>,
    },
    Sync {
        response: SyncSender<StorageResult<()>>,
    },
}

pub(crate) struct ShardHandle {
    pub(crate) shard_id: ShardId,
    sender: Option<ByteBoundedSender<ShardCommand>>,
    thread: Option<JoinHandle<()>>,
}

impl ShardHandle {
    pub(crate) fn spawn(
        config: &EngineConfig,
        shard_id: ShardId,
    ) -> EngineResult<(Self, Vec<ExtentCatalogEntry>)> {
        let directory = shard_directory(&config.data_dir, shard_id);
        let primary = ShardStore::open(ShardStoreConfig {
            directory,
            shard_id,
            target_pack_bytes: config.target_pack_bytes,
            max_batch_bytes: config.max_batch_bytes,
        })?;
        let catalog = primary.catalog();
        let mut replicas = Vec::with_capacity(config.replication_factor.saturating_sub(1) as usize);
        for replica_index in 1..config.replication_factor {
            let mut replica = ShardStore::open(ShardStoreConfig {
                directory: replica_directory(&config.data_dir, replica_index, shard_id),
                shard_id,
                target_pack_bytes: config.target_pack_bytes,
                max_batch_bytes: config.max_batch_bytes,
            })?;
            catch_up_replica(&primary, &mut replica, &catalog)?;
            replicas.push(Some(replica));
        }
        let stores = LaneStores { primary, replicas };
        let (sender, receiver) =
            byte_bounded_channel(config.queue_slots_per_shard, config.queue_bytes_per_shard)
                .map_err(|error| EngineError::InvalidConfig(error.to_string()))?;
        let thread = thread::Builder::new()
            .name(format!("shard-stream-lane-{shard_id}"))
            .spawn(move || run_worker(stores, receiver))
            .map_err(|error| {
                EngineError::InvalidConfig(format!("failed to spawn shard worker: {error}"))
            })?;
        Ok((
            Self {
                shard_id,
                sender: Some(sender),
                thread: Some(thread),
            },
            catalog,
        ))
    }

    pub(crate) fn append(
        &self,
        reservation: Reservation,
        payload: Vec<u8>,
        sync: bool,
    ) -> EngineResult<u32> {
        let (response, receiver) = sync_channel(1);
        let charged_bytes = payload.len().checked_add(COMMAND_OVERHEAD_BYTES).ok_or(
            EngineError::AdmissionLimited {
                shard_id: self.shard_id,
                reason: "append byte charge overflow",
            },
        )?;
        self.try_send(
            ShardCommand::Append {
                reservation,
                payload,
                sync,
                response,
            },
            charged_bytes,
        )?;
        let durable_replicas = receiver
            .recv()
            .map_err(|_| EngineError::WorkerStopped(self.shard_id))??;
        Ok(durable_replicas)
    }

    pub(crate) fn begin_fetch(
        &self,
        topic_partition: TopicPartition,
        start_offset: LogicalOffset,
        max_bytes: usize,
    ) -> EngineResult<Receiver<StorageResult<Vec<StoredBatch>>>> {
        let (response, receiver) = sync_channel(1);
        let charged_bytes =
            max_bytes
                .checked_add(COMMAND_OVERHEAD_BYTES)
                .ok_or(EngineError::AdmissionLimited {
                    shard_id: self.shard_id,
                    reason: "fetch byte charge overflow",
                })?;
        self.try_send(
            ShardCommand::Fetch {
                topic_partition,
                start_offset,
                max_bytes,
                response,
            },
            charged_bytes,
        )?;
        Ok(receiver)
    }

    pub(crate) fn sync(&self) -> EngineResult<()> {
        let (response, receiver) = sync_channel(1);
        self.try_send(ShardCommand::Sync { response }, COMMAND_OVERHEAD_BYTES)?;
        receiver
            .recv()
            .map_err(|_| EngineError::WorkerStopped(self.shard_id))??;
        Ok(())
    }

    fn try_send(&self, command: ShardCommand, bytes: usize) -> EngineResult<()> {
        let Some(sender) = &self.sender else {
            return Err(EngineError::WorkerStopped(self.shard_id));
        };
        sender
            .try_send(command, bytes)
            .map_err(|error| match error {
                TrySendError::TooLarge { .. } => EngineError::AdmissionLimited {
                    shard_id: self.shard_id,
                    reason: "command exceeds shard byte budget",
                },
                TrySendError::ByteBudget { .. } => EngineError::AdmissionLimited {
                    shard_id: self.shard_id,
                    reason: "shard byte budget exhausted",
                },
                TrySendError::Full(_) => EngineError::AdmissionLimited {
                    shard_id: self.shard_id,
                    reason: "shard slot budget exhausted",
                },
                TrySendError::Disconnected(_) => EngineError::WorkerStopped(self.shard_id),
            })
    }
}

impl Drop for ShardHandle {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn run_worker(
    mut stores: LaneStores,
    receiver: shard_stream_core::ByteBoundedReceiver<ShardCommand>,
) {
    while let Ok(command) = receiver.recv() {
        match &*command {
            ShardCommand::Append {
                reservation,
                payload,
                sync,
                response,
            } => {
                let result = stores.append(*reservation, payload, *sync);
                let _ = response.send(result);
            }
            ShardCommand::Fetch {
                topic_partition,
                start_offset,
                max_bytes,
                response,
            } => {
                let _ = response.send(stores.primary.fetch(
                    *topic_partition,
                    *start_offset,
                    *max_bytes,
                ));
            }
            ShardCommand::Sync { response } => {
                let _ = response.send(stores.sync());
            }
        }
    }
}

fn shard_directory(data_dir: &std::path::Path, shard_id: ShardId) -> PathBuf {
    data_dir.join("shards").join(format!("shard-{shard_id}"))
}

fn replica_directory(data_dir: &std::path::Path, replica_index: u32, shard_id: ShardId) -> PathBuf {
    data_dir
        .join("replicas")
        .join(format!("replica-{replica_index}"))
        .join(format!("shard-{shard_id}"))
}

struct LaneStores {
    primary: ShardStore,
    replicas: Vec<Option<ShardStore>>,
}

impl LaneStores {
    fn append(
        &mut self,
        reservation: Reservation,
        payload: &[u8],
        sync: bool,
    ) -> StorageResult<u32> {
        self.primary.append(reservation, payload, sync)?;
        let mut durable_replicas = 1u32;
        for replica in &mut self.replicas {
            let Some(store) = replica else {
                continue;
            };
            match store.append(reservation, payload, sync) {
                Ok(()) => durable_replicas = durable_replicas.saturating_add(1),
                Err(_) => *replica = None,
            }
        }
        Ok(durable_replicas)
    }

    fn sync(&mut self) -> StorageResult<()> {
        self.primary.sync()?;
        for replica in &mut self.replicas {
            let Some(store) = replica else {
                continue;
            };
            if store.sync().is_err() {
                *replica = None;
            }
        }
        Ok(())
    }
}

fn catch_up_replica(
    primary: &ShardStore,
    replica: &mut ShardStore,
    primary_catalog: &[ExtentCatalogEntry],
) -> StorageResult<()> {
    let primary_batches = primary_catalog
        .iter()
        .map(|entry| {
            (
                (
                    TopicPartition::new(entry.reservation.topic_id, entry.reservation.partition_id),
                    entry.reservation.batch_id,
                ),
                entry.reservation,
            )
        })
        .collect::<std::collections::HashMap<_, _>>();
    for entry in replica.catalog() {
        let key = (
            TopicPartition::new(entry.reservation.topic_id, entry.reservation.partition_id),
            entry.reservation.batch_id,
        );
        if primary_batches.get(&key) != Some(&entry.reservation) {
            return Err(shard_stream_storage::StorageError::InvalidInput(
                "replica contains an extent absent or different on the primary".into(),
            ));
        }
    }
    for entry in primary_catalog {
        if replica.contains(&entry.reservation) {
            replica.read_batch(&entry.reservation)?;
            continue;
        }
        let batch = primary.read_batch(&entry.reservation)?.ok_or_else(|| {
            shard_stream_storage::StorageError::InvalidInput(
                "primary catalog entry is unreadable".into(),
            )
        })?;
        replica.append(entry.reservation, &batch.payload, true)?;
    }
    Ok(())
}
