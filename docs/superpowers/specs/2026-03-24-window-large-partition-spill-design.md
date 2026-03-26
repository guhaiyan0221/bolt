# Window Large-Partition Spill Design

Chinese version: [2026-03-24-window-large-partition-spill-design.zh-CN.md](/home/guhaiyan/1ws/bolt/docs/superpowers/specs/2026-03-24-window-large-partition-spill-design.zh-CN.md)

## Background

The current window execution paths can run out of memory when a single logical
partition becomes very large. This can happen even if spilling is enabled:

- in the ordered-input path, when the active partition keeps growing;
- in the sort-based path, after input has been ordered and a single logical
  partition is still too large to keep in memory.

This design targets correctness-first support for large single-partition window
 execution under memory pressure. Performance regressions are acceptable in the
new path. The trigger must be passive: the large-partition spill path is
allowed to activate only when reclaim is invoked.

## Goals

- Support large single-partition window execution without OOM.
- Support arbitrary window functions and arbitrary frame types.
- Use passive triggering only: no proactive spill threshold for large
  partitions.
- Keep the classification of `WindowBuild` implementations based only on
  whether input is already ordered.
- Support both `RowVector` and `CompositeRowVector` input paths.

## Non-Goals

- No planner-time classification of "large partition".
- No third `WindowBuild` type dedicated to large partitions.
- No proactive bytes- or rows-based large-partition trigger.
- No first-version optimization for random access or reduced scan count.
- No attempt to preserve the full old `WindowPartition` interface in the new
  storage layer.

## Final `WindowBuild` Classification

After this change, `WindowBuild` should be understood as having only two
categories:

1. `StreamingWindowBuild`
   Used when input is already ordered by `partition keys + order keys`.

2. `SortWindowBuild`
   Used when input is not ordered and must be sorted first.

Large-partition OOM handling is not a third build type. It is a runtime
capability shared by both of the above categories.

## Key Design Decisions

### 1. No runtime "upgrade" from old storage to new storage

Do not implement a model where the current partition is first stored in legacy
WindowBuild-owned memory structures and then migrated to a new spillable
container when reclaim fires.

Reason:

- reclaim is the worst time to introduce a source-of-truth migration;
- migration would require partition data transfer while memory is already under
  pressure;
- migration would create a large state-space around partial transfer,
  partition-boundary continuity, and function state continuity.

Instead, when the new feature flag is enabled, the current logical partition is
owned by a single spillable partition buffer from the beginning.

### 2. Shared partition buffer for both build paths

`StreamingWindowBuild` and `SortWindowBuild` must share one abstraction for
holding the current logical partition:

- append rows belonging to the current partition;
- seal the partition when a boundary is reached;
- keep data in memory while possible;
- spill internal storage only when reclaim is invoked;
- provide replay-oriented read access after seal.

### 3. Passive triggering only

The large-partition spill behavior is allowed to activate only from reclaim.
No `window_large_partition_spill_threshold_bytes`-style proactive switch is
introduced in the first version.

### 4. Iterator-first read model

The shared partition buffer must expose replay-oriented read APIs:

- row iterator from a given offset;
- thin projection over a continuous row interval.

It must not become a new full-featured `WindowPartition` replacement with
strong random column extraction semantics.

### 5. `CompositeRowVector` is part of the supported input contract

The input contract for `WindowBuild` remains `RowVectorPtr`. This includes
ordinary `RowVector` and `CompositeRowVector`. The shared partition buffer must
not depend on the physical input vector layout. Vector-layout differences stay
inside `WindowBuild` decode and row-materialization logic.

## Input Format into `WindowBuild`

The external input to `WindowBuild` remains `RowVectorPtr`.

Important details:

- The input may be a regular `RowVector`.
- The input may also be a `CompositeRowVector`.
- `WindowBuild` must continue to rely on `RowVector` public APIs rather than on
  assumptions about physical child layout.

Internally, `WindowBuild` already normalizes input before storing rows:

1. reorder channels into:
   - partition keys;
   - sort keys;
   - remaining columns;
2. create the reordered `inputType_`;
3. decode input vectors based on `inputChannels_`;
4. materialize rows using the reordered schema.

The shared partition buffer must receive data only after this normalization. It
does not ingest raw input vectors directly.

## Shared Partition Buffer

### Role

`SpillablePartitionBuffer` is the source of truth for one logical partition.
Its lifecycle starts when the first row of a partition is observed and ends
after the partition has been fully consumed and cleared.

The same buffer instance survives both:

- in-memory execution before reclaim;
- spilled execution after reclaim.

There is no object identity switch when reclaim happens.

### Responsibilities

- Hold rows for the current logical partition.
- Accept append operations while the partition is open.
- Become sealed when the partition boundary is reached.
- Release memory through spill when reclaim is invoked.
- Provide replay-oriented read access for execution after seal.
- Report spill and reclaim statistics.

### Non-Responsibilities

The buffer must not:

- decide whether input is already ordered;
- perform global sort;
- identify partition boundaries;
- compute peers or frame bounds;
- execute window function semantics;
- split output batches.

## Proposed Minimal Interface

### Write-side API

The names below are illustrative; exact naming can follow repository style.

- `appendDecodedRow(...)`
  Append one normalized internal row.

- `appendDecodedRows(...)`
  Append a contiguous set of normalized input rows in batch form.

- `seal()`
  Mark the logical partition as complete.

- `isSealed()`
  Return whether no more rows may be appended.

- `numRows()`
  Return the logical row count of the partition.

- `reclaim(targetBytes)`
  Try to release memory by spilling internal storage. This API is called only
  from reclaim.

- `clear()`
  Free all memory and spill resources once the partition is no longer needed.

- `stats()`
  Report spill/reclaim metrics.

### Read-side API

The read model is `iterator-first + thin projection`.

Primary APIs:

- `iterator(startRow = 0)`
  Create a replay iterator from a row offset.

- `numRows()`
  Return logical row count.

- `isSpilled()`
  Report whether the buffer has entered spill-backed mode.

Thin projection API:

- `project(columns, startRow, numRows, outputVectors)`
  Project a continuous row interval and a small set of columns.

Constraints:

- The projection is sequential and interval-based.
- It is not a general sparse row-number extraction API.
- It does not attempt to preserve the full semantics of the old
  `WindowPartition::extractColumn(...)`.

## Build-Path Integration

### `StreamingWindowBuild`

`StreamingWindowBuild` keeps its role as the ordered-input build path.

Behavior with the new buffer:

- Maintain one active `SpillablePartitionBuffer`.
- While incoming rows belong to the same partition, append them to the active
  buffer.
- When a partition boundary is detected:
  - seal the active buffer;
  - enqueue it for consumption;
  - create a new active buffer for the next partition.

Reclaim behavior:

- reclaim only targets the active open partition buffer;
- no migration from legacy storage is needed.

### `SortWindowBuild`

`SortWindowBuild` keeps its role as the unordered-input build path.

Behavior with the new buffer:

- Sorting remains a separate concern.
- Existing sort spill behavior remains in the sort stage.
- After ordered rows are available, scan them by logical partition.
- For each logical partition:
  - append rows into a `SpillablePartitionBuffer`;
  - seal the buffer at the partition boundary;
  - enqueue it for consumption.

Important separation:

- sort spill handles global ordering memory pressure;
- partition spill handles oversized logical partitions after ordering;
- these two state machines must remain conceptually separate.

## Reclaim Semantics

### Trigger

Only reclaim may trigger partition spill.

### First-Version Scope

The first version handles reclaim for the active partition that is still in the
build phase.

It does not attempt to reclaim from an already sealed partition that is
currently being consumed by window execution.

Reason:

- reclaim during consumption would significantly enlarge the execution state
  space;
- the correctness risk is much higher because compute progress, frame progress,
  and replay state would all need to survive interruption.

### Behavior

When reclaim hits a `WindowBuild` using the new buffer:

1. identify the active open partition buffer;
2. call `reclaim(targetBytes)` on that buffer;
3. the buffer may:
   - spill staged in-memory rows;
   - keep only minimal metadata required for later replay;
   - continue accepting future rows of the same partition after reclaim;
4. return bytes released and updated spill statistics.

No victim selection logic is required in the first version:

- `StreamingWindowBuild` should have exactly one heavy active partition;
- in `SortWindowBuild`, sort spill remains separate, and partition spill only
  starts once ordered logical partitions are being materialized.

## `Window.cpp` Execution Changes

The current `Window.cpp` path relies heavily on `WindowPartition` semantics such
as:

- `extractColumn(...)`
- `computePeerBuffers(...)`
- `computeKRangeFrameBounds(...)`

The new partition buffer must not absorb these semantics.

Therefore, `Window.cpp` must grow a new consumption path for spillable
partition-buffer execution.

### Execution Model

The new path is based on replay and thin projection:

- replay iterators provide sequential access;
- thin projection provides continuous-range column access when needed;
- peer and frame calculations are rebuilt in the window execution layer rather
  than delegated to the storage layer.

### Explicit State in `Window.cpp`

The new path should explicitly maintain:

1. replay state
   - current iterator;
   - current row offset;
   - restart offset for multi-pass scans;

2. peer/frame scan state
   - peer-group boundaries;
   - ROWS frame boundaries derived from row offsets;
   - RANGE frame boundaries derived from ordered-key replay;

3. function execution state
   - per-function progress;
   - any replay-driven helper state.

### Complexity Placement

This design deliberately keeps semantic complexity in `Window.cpp`, not in the
storage layer. The storage layer stays a replayable partition store.

### Performance Assumption

The first version is allowed to use multiple sequential scans and replay passes
for correctness. Avoiding these scans is a later optimization task.

## Error Handling

### Spill Failure During Reclaim

If reclaim triggers spill and the buffer cannot spill successfully, fail the
query.

The error must include enough diagnostic context:

- operator or plan node identifier;
- active partition row count;
- target reclaim bytes;
- underlying spill failure reason.

### Replay or Projection Read Failure

If replay or projection fails after the buffer has become spill-backed, fail the
query immediately. Do not attempt to fall back to the legacy in-memory path.

### No Silent Fallback

Do not silently fall back to a path that may reintroduce OOM risk. Failure is
preferable to an unsafe fallback once reclaim has already determined that memory
pressure is real.

## Testing Strategy

The test plan is a matrix, not a list of unrelated cases.

### Dimensions

1. Build path
   - streaming ordered-input path;
   - sort-based unordered-input path.

2. Partition distribution
   - many small partitions;
   - one very large partition.

3. Reclaim behavior
   - no reclaim;
   - one reclaim during build;
   - multiple reclaims during build.

4. Function family
   - aggregate functions;
   - ranking functions;
   - offset/value-access functions such as `lead`, `lag`, `first_value`,
     `last_value`, `nth_value`.

5. Frame type
   - `ROWS`;
   - `RANGE`.

6. Frame bounds
   - preceding;
   - following;
   - current-row cases;
   - unbounded variants.

### Required Validation Layers

1. Shared buffer unit tests
   - append, seal, clear;
   - reclaim correctness;
   - replay correctness before and after spill;
   - thin projection correctness over continuous intervals.

2. Build-path tests
   - `StreamingWindowBuild` integration with the shared buffer;
   - `SortWindowBuild` integration with the shared buffer;
   - correct partition sealing and queuing.

3. Window semantic tests
   - verify output correctness across the full matrix above.

4. Reclaim-trigger tests
   - reclaim in the middle of building a very large partition;
   - multiple reclaims on the same large partition;
   - continued append after reclaim;
   - later replay from spill-backed storage.

### Correctness Oracle

All new-path correctness tests should compare against an external correctness
oracle or existing expected results. The correctness bar is unchanged; only the
storage and replay model changes.

## Main Risks

### 1. `Window.cpp` rewrite cost

The largest implementation cost is not the buffer itself, but building a new
execution path in `Window.cpp` that no longer depends on strong
`WindowPartition` storage semantics.

### 2. Performance regression

The first version may need several replay passes for correctness. This is
acceptable, but the cost should be visible in metrics and benchmarks.

### 3. Sort-stage and partition-stage spill confusion

In `SortWindowBuild`, sort spill and partition spill are both present. Their
states, metrics, and ownership must remain separate to avoid opaque behavior.

## Summary

This design keeps `WindowBuild` classification simple:

- `StreamingWindowBuild` for ordered input;
- `SortWindowBuild` for unordered input.

Large-partition OOM handling is implemented as a shared runtime capability
through a `SpillablePartitionBuffer` that:

- owns a logical partition from the start;
- spills only when reclaim is invoked;
- supports replay-oriented reading through iterator-first access plus thin
  projection;
- leaves window semantics in `Window.cpp` rather than in the storage layer.

The design intentionally favors correctness, bounded state complexity, and clear
ownership over first-version performance.
