# Generic Post-Commit Summary Accumulator

## Goals

- Provide a **single, reusable core** that records a summary of what changed during an Iceberg commit without any extra metadata-table scans.
- Make the summary useful for multiple downstream consumers:
  1. Spark driver logs (near-term, for the testing project).
  2. `OpenHouseCommitEvent` lineage events.
  3. An attachment sent to the OpenHouse server inside `OpenHouseTableOperations`.
- Keep lineage-, Spark-, and server-specific code in pluggable **adapters**, not in the core.
- Avoid additional I/O and minimize compute by intercepting `DataFile` objects while they are already in memory during the commit path.

## Non-Goals

- Re-implement Iceberg's `TableOperations` or catalog logic.
- Replace the existing `TableStatsCollectionSparkApp` batch collectors.  This is a lightweight, **post-commit** complement, not a replacement.
- Persist the summary inside table metadata as a first-class snapshot field.

## Core Idea

Wrap the Iceberg `Table` returned by an OpenHouse catalog with a `CommitSummaryTable`.  The wrapper overrides the entry points that produce data-file mutations (`newAppend`, `newFastAppend`, `newOverwrite`, `newReplacePartitions`, `newDelete`, `newTransaction`) and installs small, generic interceptors around the `SnapshotUpdate` / `Transaction` objects.  As the caller adds `DataFile`s, the interceptors accumulate:

- a set of unique partitions,
- total row and file counts,
- per-column aggregates: sum of `nullCount`, min of `lowerBounds`, max of `upperBounds`.

When the update or transaction commits, the wrapper builds an immutable `CommitSummary` and hands it to a `CommitSummaryListener`.  Different listeners implement the lineage-, log-, and request-attachment use cases.

Because the data already flows through the driver on commit, this avoids the later `all_entries` / `data_files` / `snapshots` metadata-table scans that the current batch collectors perform.

## Proposed Module Layout

```
libs:commit-summary                    // core, tiny dependency footprint
├── CommitSummary                      // POJO / builder returned after commit
├── CommitAccumulator                  // interface that accepts DataFiles
├── DefaultCommitAccumulator           // partition + column-metric aggregation
├── CommitSummaryListener              // callback after a commit produces a summary
├── CommitSummaryTable                 // Table wrapper
├── CommitSummaryAppendFiles           // AppendFiles wrapper
├── CommitSummaryOverwriteFiles        // OverwriteFiles wrapper
├── CommitSummaryReplacePartitions     // ReplacePartitions wrapper
├── CommitSummaryDeleteFiles           // DeleteFiles wrapper (optional)
└── CommitSummaryTransaction           // Transaction wrapper

apps/spark
├── LogCommitSummaryListener           // prints one-line summary to driver logs
└── OpenHouseCommitEventAdapter        // maps CommitSummary to CommitEvent* models

integrations/java/iceberg-1.2/openhouse-java-runtime
└── OpenHouseTableOperationsAttachmentListener  // attaches summary to commit request
```

The core module should depend only on `com.linkedin.iceberg:iceberg-api` (and `iceberg-core` for `Conversions`) so it can be consumed by the Java client, the Spark runtime, and the server.

## Core API

### 1. CommitSummary

```java
@Data
@Builder(toBuilder = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class CommitSummary implements Serializable {
  private String tableName;
  private long snapshotId;
  private String operation;            // e.g. "append", "overwrite", "replace"
  private long commitTimestampMs;
  private long addedRecords;
  private int addedDataFiles;
  private Set<PartitionKey> partitions;
  private Map<String, ColumnMetrics> columnMetrics;          // table-level aggregate
  private Map<PartitionKey, Map<String, ColumnMetrics>> partitionColumnMetrics;
}
```

`PartitionKey` is a small, immutable, hashable wrapper around the ordered partition column values:

```java
@Value
public class PartitionKey {
  List<String> names;
  List<Object> values;
}
```

`ColumnMetrics` keeps raw min/max values as `Object` so adapters can convert them to the right `ColumnData` subtype:

```java
@Data
@Builder
public class ColumnMetrics {
  private String columnName;
  private long nullCount;
  private long valueCount;
  private Object minValue;
  private Object maxValue;
}
```

### 2. CommitAccumulator

```java
public interface CommitAccumulator {
  void observeAdded(DataFile dataFile, PartitionSpec spec, Schema schema);
  void observeDeleted(DataFile dataFile, PartitionSpec spec, Schema schema);
  CommitSummary summarize(Table table, Snapshot snapshot);
  void reset();
}
```

`DefaultCommitAccumulator` is the generic implementation.  For each `DataFile` it:

1. Builds a `PartitionKey` from `dataFile.partition()` using `spec.fields()`.
2. Adds the `PartitionKey` to a `Set` and to a per-partition metrics map.
3. For each field id in `dataFile.nullCounts()` it adds to `nullCount`.
4. For each field id in `dataFile.lowerBounds()` / `upperBounds()` it converts the `ByteBuffer` to a Java value with `org.apache.iceberg.types.Conversions.fromByteBuffer(field.type(), buffer)` and merges using `Comparable`.

```java
public class DefaultCommitAccumulator implements CommitAccumulator {
  private long totalRecords;
  private int totalFiles;
  private final Set<PartitionKey> partitions = new HashSet<>();
  private final Map<PartitionKey, PartitionAccumulator> partitionAccumulators = new HashMap<>();
  private final ColumnMetricAccumulator tableMetrics = new ColumnMetricAccumulator();

  @Override
  public void observeAdded(DataFile dataFile, PartitionSpec spec, Schema schema) {
    totalFiles++;
    totalRecords += dataFile.recordCount();
    PartitionKey key = partitionKey(dataFile, spec);
    partitions.add(key);
    partitionAccumulators.computeIfAbsent(key, k -> new PartitionAccumulator()).add(dataFile, schema);
    tableMetrics.add(dataFile, schema);
  }

  @Override
  public CommitSummary summarize(Table table, Snapshot snapshot) {
    return CommitSummary.builder()
        .tableName(table.name())
        .snapshotId(snapshot.snapshotId())
        .operation(snapshot.summary().get("operation"))
        .commitTimestampMs(snapshot.timestampMillis())
        .addedRecords(totalRecords)
        .addedDataFiles(totalFiles)
        .partitions(Collections.unmodifiableSet(partitions))
        .columnMetrics(tableMetrics.toMap())
        .partitionColumnMetrics(
            partitionAccumulators.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toMap())))
        .build();
  }
}
```

### 3. CommitSummaryListener

```java
@FunctionalInterface
public interface CommitSummaryListener {
  void onCommit(CommitSummary summary);
}
```

Multiple listeners can be composed:

```java
public class CompositeCommitSummaryListener implements CommitSummaryListener {
  private final List<CommitSummaryListener> listeners;
  @Override public void onCommit(CommitSummary summary) {
    listeners.forEach(l -> l.onCommit(summary));
  }
}
```

## Wrapped Table and SnapshotUpdate Interceptors

`CommitSummaryTable` delegates every `Table` method to the real table and overrides only the mutation entry points.  The interceptors are thin; they observe `DataFile`s and publish a summary after `commit()` / `commitTransaction()`.

```java
public class CommitSummaryTable implements Table {
  private final Table delegate;
  private final Supplier<CommitAccumulator> accumulatorFactory;
  private final CommitSummaryListener listener;

  // --- forwarding implementations for all Table methods ---

  @Override
  public AppendFiles newAppend() {
    CommitAccumulator acc = accumulatorFactory.get();
    return new CommitSummaryAppendFiles(delegate.newAppend(), this, acc, CommitOperation.APPEND);
  }

  @Override
  public AppendFiles newFastAppend() {
    CommitAccumulator acc = accumulatorFactory.get();
    return new CommitSummaryAppendFiles(delegate.newFastAppend(), this, acc, CommitOperation.APPEND);
  }

  @Override
  public OverwriteFiles newOverwrite() {
    CommitAccumulator acc = accumulatorFactory.get();
    return new CommitSummaryOverwriteFiles(delegate.newOverwrite(), this, acc, CommitOperation.OVERWRITE);
  }

  @Override
  public ReplacePartitions newReplacePartitions() {
    CommitAccumulator acc = accumulatorFactory.get();
    return new CommitSummaryReplacePartitions(delegate.newReplacePartitions(), this, acc, CommitOperation.REPLACE);
  }

  @Override
  public DeleteFiles newDelete() {
    CommitAccumulator acc = accumulatorFactory.get();
    return new CommitSummaryDeleteFiles(delegate.newDelete(), this, acc, CommitOperation.DELETE);
  }

  @Override
  public Transaction newTransaction() {
    CommitAccumulator acc = accumulatorFactory.get();
    return new CommitSummaryTransaction(delegate.newTransaction(), this, acc);
  }

  void publish(CommitAccumulator accumulator) {
    Snapshot snapshot = delegate.currentSnapshot();
    if (snapshot == null) {
      return;
    }
    CommitSummary summary = accumulator.summarize(delegate, snapshot);
    listener.onCommit(summary);
  }
}
```

`CommitSummaryAppendFiles` is representative:

```java
public class CommitSummaryAppendFiles implements AppendFiles {
  private final AppendFiles delegate;
  private final CommitSummaryTable table;
  private final CommitAccumulator accumulator;

  @Override
  public AppendFiles appendFile(DataFile file) {
    accumulator.observeAdded(file, table.spec(), table.schema());
    delegate.appendFile(file);
    return this;
  }

  @Override
  public AppendFiles appendFiles(Iterable<DataFile> files) {
    for (DataFile file : files) {
      accumulator.observeAdded(file, table.spec(), table.schema());
    }
    delegate.appendFiles(files);
    return this;
  }

  @Override
  public void commit() {
    delegate.commit();
    table.publish(accumulator);
  }

  // ... delegate all other SnapshotUpdate methods (set, deleteWith, etc.)
}
```

`CommitSummaryTransaction` is similar but shares a single `CommitAccumulator` among the inner `newAppend` / `newOverwrite` / `newReplacePartitions` calls and publishes once in `commitTransaction()`.

## Where to Install the Wrapper

### Spark driver / testing project

`OpenHouseCatalog` (both `integrations/java/iceberg-1.2` and `iceberg-1.5`) returns the table from `BaseMetastoreCatalog`.  Override `loadTable` to wrap it:

```java
@Override
public Table loadTable(TableIdentifier identifier) {
  Table table = super.loadTable(identifier);
  return CommitSummaryTable.builder()
      .delegate(table)
      .accumulatorFactory(DefaultCommitAccumulator::new)
      .listener(new CompositeCommitSummaryListener(
          new LogCommitSummaryListener(),
          new OpenHouseCommitEventAdapter(cluster, ...)))
      .build();
}
```

For the testing project the listener list can start with only `LogCommitSummaryListener`.

### OpenHouse internal catalog

`OpenHouseInternalCatalog` can use the same core but with a listener that writes the summary to a `ThreadLocal` so `OpenHouseInternalTableOperations.doCommit` can read it and attach it to the server-side commit handling.

## Adapter Examples

### 1. Driver log adapter

```java
@Slf4j
public class LogCommitSummaryListener implements CommitSummaryListener {
  @Override
  public void onCommit(CommitSummary summary) {
    log.info(
        "CommitSummary table={} snapshot={} operation={} addedFiles={} addedRecords={} partitions={} columnMetrics={}",
        summary.getTableName(),
        summary.getSnapshotId(),
        summary.getOperation(),
        summary.getAddedDataFiles(),
        summary.getAddedRecords(),
        summary.getPartitions().size(),
        summary.getColumnMetrics());
  }
}
```

For the testing project this is the primary consumer: one compact line per commit in the driver log with partition count and null/min/max aggregates.

### 2. OpenHouseCommitEvent adapter

`OpenHouseCommitEventAdapter` maps the generic `CommitSummary` to the existing lineage models in `com.linkedin.openhouse.common.stats.model`:

- `CommitEventTable` from the top-level snapshot/operation.
- `CommitEventTablePartitions` from `summary.getPartitions()`.
- `CommitEventTablePartitionStats` from `summary.getPartitionColumnMetrics()` and `summary.getColumnMetrics()` for unpartitioned tables.

It stays isolated in `apps/spark` (or `services/common`) so `libs:commit-summary` never depends on lineage POJOs.

### 3. OpenHouseTableOperations request attachment

`OpenHouseTableOperationsAttachmentListener` holds a reference to the `OpenHouseTableOperations` (or a `ThreadLocal` slot) and, in `onCommit`, serializes `CommitSummary` to JSON and sets it as a transient property on the next commit request.

Design options for the wire format:

1. **Property bag (no API change).**  Add `openhouse.commitSummary = <json>` to `metadata.properties()` before `doCommit`.  `OpenHouseInternalTableOperations` can read and strip it.  This reuses the existing `CreateUpdateTableRequestBody.tableProperties` / `IcebergSnapshotsRequestBody` flow but introduces a property that briefly lives in table metadata.
2. **Dedicated request field (cleaner).**  Add an optional `commitSummary` string field to `CreateUpdateTableRequestBody` and `IcebergSnapshotsRequestBody` in the OpenAPI spec.  `OpenHouseTableOperations` attaches the JSON there, and the server parses it in `doCommit`.

The property-bag approach can be used immediately for the testing project; the dedicated-field approach should be the long-term shape.

## Why This Avoids Extra Work

- **No extra I/O:** the `DataFile` objects and their metrics (`nullCounts`, `lowerBounds`, `upperBounds`) are already in memory on the driver when `appendFile` is called.  We observe them there instead of scanning `all_entries` / `data_files` later.
- **Minimal compute:** aggregation is `O(files × columns)` with simple `HashMap` / `HashSet` updates.  The only conversion is `Conversions.fromByteBuffer` per bound, which Iceberg already does internally for metadata-table reads.
- **Pluggable sinks:** the same accumulator feeds logs, events, and server requests without re-implementation.

## Open Questions

1. Should deletes be tracked as negative row/bound/null contributions, or simply ignored in the first version?
2. For transactions that mix `append` and `delete`, should the published `operation` be derived from `Snapshot.summary()` or from the dominant observed operation?
3. How should `PartitionKey` be serialized in the request attachment so the server can reconstruct typed `ColumnData` without the original `PartitionSpec`?
4. Where should the core module live: a new `libs:commit-summary` or inside `services:common`?
5. Do we want the wrapper to be enabled by default in `OpenHouseCatalog`, or gated by a catalog property such as `openhouse.commit-summary.enabled`?
