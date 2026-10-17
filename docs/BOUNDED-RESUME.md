# Bounded native replay continuation

`EngineHandle::resume_recovered_replication_until` replays retained native
batches using finite `NativeResumeLimits` and one absolute operation expiry.
It retains the same ordinary or recovered engine allocation through native
coordinator observations, stored fetches and transport completion. It returns
ordinary replay data in `NativeResumeReport`, or the actual engine and partial
outcomes in `RetainedNativeResumeError`. Neither result grants currentness,
authenticated placement, write eligibility or serving permission.

The ordinary `StreamEngine::resume_recovered_replication` and
`StreamEngine::resume_durable_sink` use the same native replay loops. Their
original transport, sink retry, background and checkpoint behavior remains
selected by the ordinary delegates. In particular, ordinary zero-follower
replication still means background enqueue rather than physical completion.

## Original expiry and cumulative costs

`native_resume_cost` derives checked logical setup and cumulative ceilings from
the actual engine configuration and limits. Callers must admit those real facts
before constructing continuation work. `NativeResumeWork` carries the same
absolute expiry, sticky refusal and cumulative counters through native workers
and bounded provider callbacks. Its clones share those counters. A fetch pass
never constructs a fresh storage allowance: every replay pass uses the same
`StorageFetchBudget`.

The limits cover partition passes, fetch passes, batches, atomic-group
observations, callback reservations, entered jobs, typed outcome slots and
source-visible byte/entry/work requests. String bytes are a subset of declared
bytes. Outcome storage is bounded before work enters, and additional native
values, diagnostic buffers, queue values and provider allocations are charged
before materialization. Actual fees include reserved phases that later refuse;
there is no counter reset or refund on an error.

These are explicit source-visible logical costs. They do not claim bounds on
allocator overhead, process RSS, kernel resources or elapsed CPU performance.
Providers must bound their actual decoded/database/network allocations and
physical job registries as well. Measuring those domains remains separate.

## Actual replication tickets

`ReplicationTransport::begin_recovered_replicate_until` is a separate strict
producer contract. Its default refuses before invoking an ordinary transport.
An implementation returns an error only when no job entered or all entered work
has physically settled. Otherwise it must return the actual pending
`ReplicationTicket`, including when entry itself finishes late.

A strict ticket completes after every entered target, socket, TLS, follower
append and associated cleanup has settled. An early quorum, a queue admission
or the ordinary `required_followers = 0` acknowledgement cannot represent that
completion. An actual empty target set may finish without jobs; a zero count
cannot substitute for joining jobs that did enter.

The native continuation stores the returned ticket before checking expiry and
borrows that stored ticket across its real blocking receive. It drains pending
tickets on late/error paths and owning-frame unwind while retaining the actual
engine outside the unwind boundary. Original native start and receive results,
including errors and subsequent expiry refusals, remain separate observations.
Provider compliance with the physical ticket contract must itself be verified;
a channel value is not independent evidence that a transport implemented it.

## Bounded sink callback producers

The bounded callback surfaces are `load_checkpoint_until`,
`pending_atomic_groups_until`, `DurableAppendSink::apply_until` and
`finalize_atomic_group_until`. They receive the same `NativeResumeWork`.
Refusing defaults do not call a legacy callback. A provider must charge actual
values before allocation, check the unchanged expiry between physical phases,
and retain/settle entered work before returning a typed result or error.
Clipping an already allocated ordinary pending-group list does not implement
that contract.

Native callback commands use the actual existing sink workers, factory and
shard sink handles. Existing native lag slots bound queued commands. Strict
admission refuses capacity rather than waiting on the ordinary lag Condvar.
An entered command is drained even when its queue residence or physical callback
finishes late. Its raw callback result is saved before the next expiry check;
native cache publication has a separate retained result. A committed callback
effect can therefore remain observable together with a later refusal.
The coordinator and sink command receivers also drain their actual entered
commands during owning-frame unwind before the outside engine can be released.

The actual sink map stays in its original worker frame. No additional strong
map in the dispatcher moves ordinary sink destruction to its caller's Drop.
The unchanged ordinary dispatcher fields and bounded-detach behavior remain
intact. A recovered engine still joins its actual sink workers before releasing
its retained native storage custody.

AllDurable mode preserves the original no-op pending-group/decision policy;
`callback_entered()` distinguishes that policy from an entered provider call.
A successful ordinary no-op is not evidence of an atomic publish transaction.

These callback producers are not yet a complete public strict sink-resume
entry. Genuine consuming Core GC is available through the actual lane command
below. Concrete SDK callbacks, bounded dirty/wait/cache operations and earlier
sink-handle opening must still implement their own bounded physical phases.
The legacy garbage-collection and opening paths cannot be qualified by outer
expiry checks or by these callback APIs.

## Regression source scope

The new regression source uses real native WAL production and recovery,
actual follower writes, entered target threads that are physically joined,
blocked native sink workers, unchanged absolute expiries and actual OS owner
contention. Actual failed file opens test retained typed errors after fitting
and late cleanup, including the original I/O error as a callback panic payload.
Memory checkpoint callbacks test worker/custody mechanics and do
not qualify a production durable SDK transaction. All new groups remain
uncompiled and unexecuted pending the normal native validation gates.

## Actual Core store distribution

On Linux/Android, `NativeRecoveryCensus::read_for_engine_with_primary` adds
explicit `NativePrimaryRecovery` policy and the SAME cumulative
`NativeDirectoryBudget`. Its acquired-guard route transfers the actual retained
lock descriptor to `try_acquire_until`, then the actual Core directory to
`pin_shard_directory_until` and `open_store_until`. It never converts a plain
legacy guard, reopens a pathname to invent ownership, or extracts a pin/FD.
The configured root spelling must equal the retained canonical root bytes.
Typed clone/acquisition/pin/open errors and real unwind payloads remain borrowed
from `NativeRecoveryError::primary_outcomes()` with their original custody.

The whole `OwnedPinnedShardStore` moves through census storage into the actual
lane. Both lane and sync thread closures retain clones of its SAME opaque
native directory outside all request, File, result and loop frames. A returned
lane thread is insufficient when a real sync request still owns a File.
The real worker Drop joins lane and sync threads; pending lane inputs remain in
a retained shared slot until real thread entry, so failed spawning cannot drop
the sole store with a failed `FnOnce`. Partial startup retains actual spawn and
readiness results, the whole input and joining handles in the returned error.
The owned route uses two explicit 2 MiB stack ceilings, charged with its real
input-slot, readiness, name and directory-holder values before worker startup.
Ordinary constructors retain their existing native worker settings.
Owned startup also checks the SAME min-only directory budget and actual
composing checks at setup, physical spawn and readiness boundaries. A budget
tightened after sync-thread creation can therefore refuse the later lane
spawn while retaining the real sync worker, store and typed refusal. Startup
policy stays in the parent setup frame and drops on successful transfer;
long-lived worker closures capture only genuine directory/root custody.

`EngineHandle::collect_garbage_until` accepts `NativeResumeGcPolicy`, finite
resume limits and the original E. The policy carries the SAME directory budget,
actual native GC limits, and required composing checks. An internal per-job
adapter checks the SAME `NativeResumeWork` and the supplied actual captured
health. Its denial latches work first and invokes actual health denial before
prepaid native durability settlement. A captured-health callback is attempted
once even if it unwinds. No Session moves into a worker.

An entered command's actual receiver is stored before post-enqueue checks and
physically drained on late/error/unwind. Consuming GC restores the WHOLE store
on success and retains the ACTUAL native report in `OwnedPinnedShardGcReport`,
whose original directory is held last. Native refusal instead retains the whole
store in its Core error; the lane becomes vacant and subsequent commands refuse.
Neither an error nor an enqueue constructs a replacement store or completion.
The same actual command is used by the additive owned bootstrap retention leg.
Even a refusal before command entry denies the supplied actual context and
retains its policy and any denial unwind. The exact command-fee regression
distinguishes queue entry from the later independent Core admission: fitting
only the queue fee does not claim that collection completed.

This is physical ownership and reusable GC composition only. Existing source
validators/readers, object restoration, coordinator opening, catalog/tier,
ordinary append/fetch/rotation and complete strict sink replay remain separate
bounded-producer prerequisites. Owned idle lanes skip the ordinary seal path.
No authenticated C1 health, all-nine evidence join, currentness or serving grant
is manufactured by these ordinary native policy/callback types. Public native
callback fixtures verify mechanics; they do not qualify a private captured-C1
integration or an unchanged-cap composed startup positive. All new tests remain
uncompiled and unexecuted until the normal validation owner runs them.

Each strict coordinator/sink HashMap or HashSet point operation prepays a
checked `(actual capacity + 1) * actual logical element size` work fee on the
same `NativeResumeWork` before entry. The extra slot also pays an empty-table
probe. Outer partition state, nested atomic groups, partition/checkpoint cache
reads and writes, finalized-group membership and insertion, and sink selection
all pay independently. Growth/movement reservations remain separate, and a
later entry or insertion reads capacity again after any real reservation.
These are source-visible logical table-element work ceilings, with no private
hash-table layout, total CPU, allocator or RSS claim.

Six additional source groups use real native catalog/history/retention and
dispatcher commands, including a genuinely sparse retained atomic-group table,
actual cache/set growth, synchronized fixture sink effects and typed callback
success retained across a later cache refusal. Exact/one-less probe cases use
explicit test-only downward work consumption; production limits and original E
never change. Exact probe entry is distinct from a complete command fitting all
later fees. Existing source tests remain whole; all fifteen resume groups and
eight owned-worker groups are uncompiled and unexecuted. The existing ordered
pending-cache BTree traversal contract remains a separate unestablished
assessment; this correction does not claim to qualify it or full sink replay.

The queued GC bridge prepays a named maximum of its actual mutually exclusive
wrong-primary-kind and arithmetic diagnostic buffers before allocating command
inputs or entering the lane queue. Its checked command fee includes that buffer
in declared and work bytes, records the same string-byte subset, and pays one
additional entry. The composing `NativeResumeWork` pays the identical diagnostic
subset before its own command allocation. These fees belong to the bridge even
when an Ordinary or Vacant primary refuses before the Core collector is called;
the collector's separate fee cannot cover them. There is no independent native
`max_string_bytes` limit, and the real native GC path/name ceilings remain
positive.

One additional genuine ordinary-worker group uses unchanged cumulative limits
and E with actual remaining declared/work allowance at exact and one-less fees.
The unpaid case refuses before queue entry; fitting fees deliver the real
wrong-kind error and its diagnostic usage through the actual ticket. Native
files remain unchanged, no Core call is entered, and the real lane/sync join
releases the ordinary root while that plain outcome remains alive. All fifteen
resume and nine owned-worker groups remain source only, uncompiled and
unexecuted; these mechanics supply no C1 or whole-host startup qualification.
