use super::*;
use crate::{
    BatchId, LeaderEpoch, LogicalOffset, LogicalPartitionId, Placement, PlacementSequence,
    Reservation, RingEpoch, ShardStore, ShardStoreConfig, TierExtent, TopicId, VirtualLaneId,
};
use std::fs;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "tier-path-{}-{nonce}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("real unique fixture directory");
        Self(path)
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn expiry() -> Instant {
    Instant::now() + Duration::from_secs(30)
}
fn limits() -> RecoveryTierPathLimits {
    RecoveryTierPathLimits {
        max_path_bytes: 8192,
        max_declared_bytes: 1 << 20,
        max_entries: 1 << 16,
        max_work_bytes: 4 << 20,
    }
}
struct Fixture {
    current: PathBuf,
    manifest: PathBuf,
    object: PathBuf,
    native: crate::TierManifest,
    _root: Temporary,
}
impl Fixture {
    fn new() -> Self {
        Self::with_name(std::ffi::OsStr::new("tier"))
    }
    fn with_name(name: &std::ffi::OsStr) -> Self {
        let root = Temporary::new();
        let shard = ShardId::new(7);
        let mut store = ShardStore::open(ShardStoreConfig {
            directory: root.0.join("primary"),
            shard_id: shard,
            target_pack_bytes: 1 << 20,
            max_batch_bytes: 4096,
        })
        .expect("actual native pack writer");
        let reservation = Reservation {
            topic_id: TopicId::new(9),
            partition_id: LogicalPartitionId::new(2),
            batch_id: BatchId::new(1),
            first_offset: LogicalOffset::new(0),
            last_offset: LogicalOffset::new(0),
            record_count: NonZeroU32::new(1).expect("native count"),
            placement: Placement {
                virtual_lane_id: VirtualLaneId::new(3),
                ring_epoch: RingEpoch::new(1),
                leader_epoch: LeaderEpoch::new(u64::MAX),
                sequence: PlacementSequence::new(1),
            },
        };
        store
            .append(reservation, None, b"actual complete tier object", true)
            .expect("native frame");
        let pack = store.active_path().to_owned();
        drop(store);
        let tier_root = root.0.join(name);
        let mut tier =
            super::super::LocalObjectTier::open(&tier_root, shard).expect("actual native tier");
        tier.offload_pack(0, &pack, vec![TierExtent::from_reservation(reservation)])
            .expect("actual native object, manifest and CURRENT publication");
        let native = tier.manifest().clone();
        let current = current_path(&tier_root, shard);
        let manifest = manifest_path(&tier_root, shard, native.generation);
        let object = object_path(&tier_root, &native.objects[0].object_key)
            .expect("actual native object path");
        drop(tier);
        Self {
            current,
            manifest,
            object,
            native,
            _root: root,
        }
    }
    fn bytes(&self) -> [Vec<u8>; 3] {
        [&self.current, &self.manifest, &self.object]
            .map(|p| fs::read(p).expect("actual published native file"))
    }
    fn shard(&self) -> ShardId {
        ShardId::new(self.native.shard_id)
    }
}

#[test]
fn actual_native_publication_paths_match_and_wrong_root_suffix_does_not() {
    let f = Fixture::new();
    let before = f.bytes();
    assert!(
        recovery_tier_manifest_path_matches(
            &f.current,
            &f.manifest,
            f.shard(),
            f.native.generation,
            limits(),
            expiry()
        )
        .expect("actual selected manifest")
    );
    let key = &f.native.objects[0].object_key;
    assert!(
        recovery_tier_object_path_matches(
            &f.current,
            f.shard(),
            key,
            &f.object,
            limits(),
            expiry()
        )
        .expect("actual selected object")
    );
    let other = Temporary::new();
    let wrong = other.0.join(key);
    fs::create_dir_all(wrong.parent().expect("object parent")).expect("other real root");
    fs::copy(&f.object, &wrong).expect("byte-identical body in another physical root");
    assert!(
        wrong.ends_with(key),
        "the old suffix shortcut would accept this"
    );
    assert!(
        !recovery_tier_object_path_matches(&f.current, f.shard(), key, &wrong, limits(), expiry())
            .expect("ordinary path mismatch")
    );
    assert_eq!(f.bytes(), before);
}

#[test]
fn same_directory_wrong_manifest_filename_generation_or_shard_refuses_match() {
    let f = Fixture::new();
    let wrong = f.manifest.with_file_name("unselected-copy.json");
    fs::copy(&f.manifest, &wrong).expect("actual same decoded manifest at wrong native name");
    let native = super::super::decode_recovery_tier_manifest(&fs::read(&wrong).expect("copy"))
        .expect("unchanged real native decoder accepts same bytes");
    assert_eq!(native, f.native);
    for (path, shard, generation) in [
        (&wrong, f.shard(), native.generation),
        (&f.manifest, ShardId::new(8), native.generation),
        (&f.manifest, f.shard(), native.generation + 1),
    ] {
        assert!(
            !recovery_tier_manifest_path_matches(
                &f.current,
                path,
                shard,
                generation,
                limits(),
                expiry()
            )
            .expect("exact actual native selected path")
        );
    }
    let wrong_current = f.current.with_file_name("different-current");
    assert!(
        !recovery_tier_manifest_path_matches(
            &wrong_current,
            &f.manifest,
            f.shard(),
            native.generation,
            limits(),
            expiry()
        )
        .expect("CURRENT must match native producer too")
    );
}

#[test]
fn exact_cost_and_one_less_axes_precede_native_key_validation() {
    let f = Fixture::new();
    let key = &f.native.objects[0].object_key;
    let cost = recovery_tier_path_inspection_cost(
        f.current.as_os_str().as_encoded_bytes().len(),
        f.object.as_os_str().as_encoded_bytes().len(),
        key.len(),
        limits(),
    )
    .expect("ordinary facts");
    let mut exact = limits();
    exact.max_declared_bytes = cost.declared_bytes();
    exact.max_entries = cost.entries();
    exact.max_work_bytes = cost.work_bytes();
    assert!(
        recovery_tier_object_path_matches(&f.current, f.shard(), key, &f.object, exact, expiry())
            .expect("exact genuine cost fits")
    );
    let bad = format!("../{}", "x".repeat(key.len() - 3));
    assert!(matches!(
        recovery_tier_object_path_matches(&f.current, f.shard(), &bad, &f.object, exact, expiry()),
        Err(RecoveryTierPathError::Native(_))
    ));
    for axis in 0..3 {
        let mut short = exact;
        match axis {
            0 => short.max_declared_bytes -= 1,
            1 => short.max_entries -= 1,
            _ => short.max_work_bytes -= 1,
        }
        assert!(matches!(
            recovery_tier_object_path_matches(
                &f.current,
                f.shard(),
                &bad,
                &f.object,
                short,
                expiry()
            ),
            Err(RecoveryTierPathError::Capacity)
        ));
    }
    let mut short = limits();
    short.max_path_bytes = f.current.as_os_str().as_encoded_bytes().len() - 1;
    assert!(matches!(
        recovery_tier_manifest_path_matches(
            &f.current,
            &f.manifest,
            f.shard(),
            f.native.generation,
            short,
            expiry()
        ),
        Err(RecoveryTierPathError::Capacity)
    ));
    assert!(matches!(
        recovery_tier_path_inspection_cost(usize::MAX, 1, 1, limits()),
        Err(RecoveryTierPathError::Capacity)
    ));
}

std::thread_local! { static PROBE: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) }; }
pub(super) fn after_root(saved: Instant) {
    PROBE.with(|p| {
        if let Some(n) = p.get() {
            p.set(Some(n + 1));
            std::thread::sleep(
                saved.saturating_duration_since(Instant::now()) + Duration::from_millis(1),
            );
        }
    });
}
struct Observe;
impl Drop for Observe {
    fn drop(&mut self) {
        PROBE.with(|p| p.set(None));
    }
}

#[test]
fn original_expiry_between_native_root_and_next_path_prevents_later_work() {
    let f = Fixture::new();
    let before = f.bytes();
    let _observe = Observe;
    for object in [false, true] {
        PROBE.with(|p| p.set(Some(0)));
        let saved = Instant::now() + Duration::from_secs(1);
        let result = if object {
            recovery_tier_object_path_matches(
                &f.current,
                f.shard(),
                &f.native.objects[0].object_key,
                &f.object,
                limits(),
                saved,
            )
        } else {
            recovery_tier_manifest_path_matches(
                &f.current,
                &f.manifest,
                f.shard(),
                f.native.generation,
                limits(),
                saved,
            )
        };
        assert!(matches!(result, Err(RecoveryTierPathError::Deadline)));
        PROBE.with(|p| assert_eq!(p.get(), Some(1)));
    }
    let mut invalid = limits();
    invalid.max_entries = 0;
    assert!(matches!(
        recovery_tier_manifest_path_matches(
            &f.current,
            &f.manifest,
            f.shard(),
            f.native.generation,
            invalid,
            Instant::now()
        ),
        Err(RecoveryTierPathError::Deadline)
    ));
    assert!(matches!(
        recovery_tier_manifest_path_matches(
            &f.current,
            &f.manifest,
            f.shard(),
            f.native.generation,
            invalid,
            expiry()
        ),
        Err(RecoveryTierPathError::InvalidLimits)
    ));
    assert_eq!(f.bytes(), before);
}

#[cfg(unix)]
#[test]
fn actual_non_utf8_native_root_retains_exact_encoded_path_bytes() {
    use std::os::unix::ffi::OsStrExt;
    let f = Fixture::with_name(std::ffi::OsStr::from_bytes(b"tier-\xff"));
    assert!(f.current.to_str().is_none());
    assert!(
        recovery_tier_manifest_path_matches(
            &f.current,
            &f.manifest,
            f.shard(),
            f.native.generation,
            limits(),
            expiry()
        )
        .expect("actual original nonUTF8 path")
    );
    assert!(
        recovery_tier_object_path_matches(
            &f.current,
            f.shard(),
            &f.native.objects[0].object_key,
            &f.object,
            limits(),
            expiry()
        )
        .expect("actual original object path")
    );
    let lossy = PathBuf::from(f.object.to_string_lossy().as_ref());
    assert!(
        !recovery_tier_object_path_matches(
            &f.current,
            f.shard(),
            &f.native.objects[0].object_key,
            &lossy,
            limits(),
            expiry()
        )
        .expect("lossy spelling is not raw identity")
    );
}
