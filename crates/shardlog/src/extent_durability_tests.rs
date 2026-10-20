//! Real active-file sync, late outcome and cumulative-slot regression coverage.

use super::*;
use crate::{
    StorageDurabilityBudget, StorageDurabilityFailure, StorageDurabilityLimits,
    StorageDurabilityOperation,
};
use std::time::{SystemTime, UNIX_EPOCH};

struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "shardlog-native-sync-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        Self(path)
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn open(root: &Root) -> ShardStore {
    ShardStore::open(ShardStoreConfig {
        directory: root.0.clone(),
        shard_id: ShardId::new(0),
        target_pack_bytes: 1024 * 1024,
        max_batch_bytes: 1024,
    })
    .expect("actual native pack and exclusive lease")
}

#[test]
fn completed_actual_flush_and_sync_crossing_same_expiry_keep_their_native_outcomes() {
    for selected in [
        StorageDurabilityOperation::Flush,
        StorageDurabilityOperation::SyncData,
    ] {
        let root = Root::new();
        let mut store = open(&root);
        let original = Instant::now() + Duration::from_millis(200);
        let mut budget =
            StorageDurabilityBudget::new(StorageDurabilityLimits { max_io_calls: 2 }, original)
                .expect("finite actual outcome allocation");
        let mut observed = false;
        let error = store
            .sync_until_with_observer(&mut budget, &mut |actual| {
                if actual == selected {
                    observed = true;
                    while Instant::now() < original {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                }
            })
            .expect_err("actual completed native phase settles late");
        assert!(observed);
        assert_eq!(error, StorageDurabilityFailure::Expired);
        assert_eq!(budget.absolute_expiry(), original);
        let expected = if selected == StorageDurabilityOperation::Flush {
            1
        } else {
            2
        };
        assert_eq!(budget.outcomes().len(), expected);
        assert!(
            budget
                .outcomes()
                .iter()
                .all(|row| row.entered() && matches!(row.result(), Some(Ok(()))))
        );
        assert!(
            ShardStore::open(ShardStoreConfig {
                directory: root.0.clone(),
                shard_id: ShardId::new(0),
                target_pack_bytes: 1024 * 1024,
                max_batch_bytes: 1024,
            })
            .is_err(),
            "same actual store still retains the exclusive physical lock"
        );
        let count = budget.outcomes().len();
        assert_eq!(
            store.sync_until(&mut budget),
            Err(StorageDurabilityFailure::Expired)
        );
        assert_eq!(
            budget.outcomes().len(),
            count,
            "no new native call after original E"
        );
        drop(budget);
        drop(store);
        drop(open(&root));
    }
}

#[test]
fn one_less_native_call_slot_refuses_before_sync_and_slots_never_recycle() {
    let root = Root::new();
    let mut store = open(&root);
    let original = Instant::now() + Duration::from_secs(30);
    let mut one =
        StorageDurabilityBudget::new(StorageDurabilityLimits { max_io_calls: 1 }, original)
            .expect("one actual slot");
    assert_eq!(
        store.sync_until(&mut one),
        Err(StorageDurabilityFailure::Capacity)
    );
    assert_eq!(one.outcomes().len(), 1);
    assert_eq!(
        one.outcomes()[0].operation(),
        StorageDurabilityOperation::Flush
    );
    assert!(matches!(one.outcomes()[0].result(), Some(Ok(()))));
    assert_eq!(
        store.sync_until(&mut one),
        Err(StorageDurabilityFailure::Denied)
    );
    assert_eq!(one.outcomes().len(), 1);
    let mut exact =
        StorageDurabilityBudget::new(StorageDurabilityLimits { max_io_calls: 2 }, original)
            .expect("separate actual two-slot attempt, unchanged original E");
    store
        .sync_until(&mut exact)
        .expect("real active file synchronized");
    assert_eq!(exact.outcomes().len(), 2);
    assert_eq!(
        store.sync_until(&mut exact),
        Err(StorageDurabilityFailure::Capacity)
    );
    assert_eq!(exact.outcomes().len(), 2);
    store
        .sync()
        .expect("ordinary existing native API still works");
}

#[test]
fn actual_completed_native_phase_unwind_retains_outcome_and_denies_later_entry() {
    let root = Root::new();
    let mut store = open(&root);
    let mut budget = StorageDurabilityBudget::new(
        StorageDurabilityLimits { max_io_calls: 2 },
        Instant::now() + Duration::from_secs(30),
    )
    .expect("actual finite slots");
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store.sync_until_with_observer(&mut budget, &mut |_| {
            panic!("actual native flush completed before caller completion unwind");
        })
    }));
    assert!(unwound.is_err());
    assert_eq!(budget.outcomes().len(), 1);
    assert!(budget.outcomes()[0].entered());
    assert!(matches!(budget.outcomes()[0].result(), Some(Ok(()))));
    assert_eq!(
        store.sync_until(&mut budget),
        Err(StorageDurabilityFailure::Denied)
    );
    assert_eq!(budget.outcomes().len(), 1);
    store
        .sync()
        .expect("same actual handle remains usable by ordinary explicit caller");
}

#[test]
#[cfg(target_os = "linux")]
fn actual_device_fd_sync_refusal_keeps_original_kernel_error_in_budget() {
    let device = File::open("/dev/null").expect("actual Linux character-device descriptor");
    let mut budget = StorageDurabilityBudget::new(
        StorageDurabilityLimits { max_io_calls: 1 },
        Instant::now() + Duration::from_secs(30),
    )
    .expect("actual one-slot counter");
    let error = budget
        .execute(
            StorageDurabilityOperation::SyncData,
            || device.sync_data(),
            &mut |_| {},
        )
        .expect_err("actual Linux fdatasync on character device refuses");
    assert_eq!(error, StorageDurabilityFailure::Io { outcome_index: 0 });
    let actual = budget.outcomes()[0]
        .result()
        .expect("physical native operation completed");
    assert!(
        actual
            .as_ref()
            .expect_err("actual kernel error retained")
            .raw_os_error()
            .is_some()
    );
}
