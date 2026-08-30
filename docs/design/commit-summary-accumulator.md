# Generic Post-Commit Summary Accumulator

## Goals

- Provide a **single, reusable core** that records a summary of what changed during an Iceberg commit without any extra metadata-table scans.
- Use the **same object shape** that `TableStatsCollector` already produces for `CommitEventTablePartitions` and `CommitEventTablePartitionStats`.
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

Introduce a generic `OpenHouseTable` wrapper that delegates every `Table` method to the real table and overrides only the data-file mutation entry points (`newAppend`, `newFastAppend`, `newOverwrite`, `newReplacePartitions`, `newDelete`, `newTransaction`).  `OpenHouseTable` dispatches `OpenHouseTableListener` events for every `DataFile` added/deleted and for every successful commit or transaction commit.

One listener, `CommitSummaryAccumulator`, collects the events into a `CommitSummary`.  `CommitSummary` is a plain POJO whose partition-level objects have the **same field layout** as `CommitEventTablePartitions` and `CommitEventTablePartitionStats`.  Other business logic can be added later as additional `OpenHouseTableListener` implementations without changing the wrapper.

Because the data already flows through the driver on commit, this avoids the later `all_entries` / `data_files` / `snapshots` metadata-table scans that the current batch collectors perform.

## Proposed Module Layout

Everything core lives in the existing `openhouse-java-runtime` module so it can be used by both the Java client and the Spark runtime.  Adapters that map to the existing `services:common` lineage models live in `apps/spark`, which already depends on `services:common`.

```
integrations/java/iceberg-1.2/openhouse-java-runtime/src/main/java/com/linkedin/openhouse/javaclient
├── OpenHouseTable                          // generic Table wrapper
├── OpenHouseTableListener                  // extension point
├── OpenHouseAppendFiles                    // AppendFiles wrapper
├── OpenHouseOverwriteFiles                // OverwriteFiles wrapper
├── OpenHouseReplacePartitions             // ReplacePartitions wrapper
├── OpenHouseDeleteFiles                   // DeleteFiles wrapper (optional)
├── OpenHouseTransaction                   // Transaction wrapper
├── CommitSummary                          // core output POJO
├── CommitSummaryAccumulator               // OpenHouseTableListener implementation
└── OpenHouseTableOperationsAttachmentListener // attaches summary to commit request

apps/spark/src/main/java/com/linkedin/openhouse/jobs/util
└── CommitSummaryToLineageAdapter          // maps CommitSummary to CommitEventTable* models

apps/spark/src/main/java/com/linkedin/openhouse/jobs/spark
└── LogCommitSummaryListener               // prints summary to driver logs
```

No new Gradle module is required.  `openhouse-java-runtime` already depends on `iceberg-core`, so it has access to `Table`, `DataFile`, `PartitionSpec`, `Schema`, `Snapshot`, and `Conversions`.

## Generic `OpenHouseTable` Wrapper

`OpenHouseTable` is a generic `Table` decorator.  It forwards all `Table` methods to the delegate and overrides only the mutation entry points.  Business logic is injected via `OpenHouseTableListener`.

```java
package com.linkedin.openhouse.javaclient;

import org.apache.iceberg.Table;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.DataFile;

public interface OpenHouseTableListener {
  default void onDataFileAdded(DataFile dataFile, OpenHouseTable table, OperationType operation) {}
  default void onDataFileDeleted(DataFile dataFile, OpenHouseTable table, OperationType operation) {}
  default void onCommit(Snapshot snapshot, OpenHouseTable table, OperationType operation) {}
}

public class OpenHouseTable implements Table {
  private final Table delegate;
  private final String clusterName;
  private final List<OpenHouseTableListenerFactory> listenerFactories;

  // ... constructor and all Table delegate methods ...

  @Override
  public AppendFiles newAppend() {
    return new OpenHouseAppendFiles(delegate.newAppend(), this, OperationType.APPEND, createListeners());
  }

  @Override
  public AppendFiles newFastAppend() {
    return new OpenHouseAppendFiles(delegate.newFastAppend(), this, OperationType.APPEND, createListeners());
  }

  @Override
  public OverwriteFiles newOverwrite() {
    return new OpenHouseOverwriteFiles(delegate.newOverwrite(), this, OperationType.OVERWRITE, createListeners());
  }

  @Override
  public ReplacePartitions newReplacePartitions() {
    return new OpenHouseReplacePartitions(delegate.newReplacePartitions(), this, OperationType.REPLACE, createListeners());
  }

  @Override
  public DeleteFiles newDelete() {
    return new OpenHouseDeleteFiles(delegate.newDelete(), this, OperationType.DELETE, createListeners());
  }

  @Override
  public Transaction newTransaction() {
    List<OpenHouseTableListener> txListeners = createListeners();
    return new OpenHouseTransaction(delegate.newTransaction(), this, txListeners);
  }

  private List<OpenHouseTableListener> createListeners() {
    return listenerFactories.stream()
        .map(OpenHouseTableListenerFactory::create)
        .collect(Collectors.toList());
  }
}
```

`OpenHouseTableListenerFactory` creates a fresh listener instance for each operation (or a single shared instance for a transaction).  This keeps state isolated between commits.

```java
public interface OpenHouseTableListenerFactory {
  OpenHouseTableListener create();
}
```

`OpenHouseAppendFiles` is representative:

```java
public class OpenHouseAppendFiles implements AppendFiles {
  private final AppendFiles delegate;
  private final OpenHouseTable table;
  private final OperationType operation;
  private final List<OpenHouseTableListener> listeners;

  @Override
  public AppendFiles appendFile(DataFile file) {
    listeners.forEach(l -> l.onDataFileAdded(file, table, operation));
    delegate.appendFile(file);
    return this;
  }

  @Override
  public AppendFiles appendFiles(Iterable<DataFile> files) {
    files.forEach(f -> listeners.forEach(l -> l.onDataFileAdded(f, table, operation)));
    delegate.appendFiles(files);
    return this;
  }

  @Override
  public void commit() {
    delegate.commit();
    listeners.forEach(l -> l.onCommit(table.currentSnapshot(), table, operation));
  }

  // ... delegate all other SnapshotUpdate methods ...
}
```

`OpenHouseTransaction` creates the same inner wrappers but shares one listener list across the transaction and fires `onCommit` once in `commitTransaction()` with the final snapshot and `OperationType.MIXED`.

This design makes `OpenHouseTable` a generic hook point for future OpenHouse business logic (e.g., policy checks, client-side metrics, request enrichment) without changing the wrapper class each time.

## `CommitSummary` Shape

`CommitSummary` is intentionally a plain POJO that mirrors the field layout of `CommitEventTablePartitions` and `CommitEventTablePartitionStats` from `services:common`.  It lives in `openhouse-java-runtime` so the client jar does not have to depend on `services:common`.

```java
@Data
@Builder
public class CommitSummary {
  // table / commit identification (same fields as BaseTableIdentifier + CommitMetadata)
  private String databaseName;
  private String tableName;
  private String clusterName;
  private String tableLocation;
  private String partitionSpec;
  private long commitId;                 // snapshot id
  private long commitTimestampMs;
  private String commitAppId;
  private String commitAppName;
  private String commitOperation;        // e.g. "APPEND", "OVERWRITE"
  private long eventTimestampMs;

  // one entry per unique partition affected by the commit
  private List<CommitSummaryPartition> partitions;
}
```

```java
@Data
@Builder
public class CommitSummaryPartition {
  // matches CommitEventTablePartitions.partitionData
  private List<CommitSummaryColumnData> partitionData;

  // matches CommitEventTablePartitionStats
  private long rowCount;
  private long columnCount;
  private List<CommitSummaryColumnData> nullCount;
  private List<CommitSummaryColumnData> nanCount;
  private List<CommitSummaryColumnData> minValue;
  private List<CommitSummaryColumnData> maxValue;
  private List<CommitSummaryColumnData> columnSizeInBytes;
}
```

```java
@Data
@Builder
public class CommitSummaryColumnData {
  private String columnName;
  private Object value;  // typed Long, Double, or String; adapters convert to ColumnData subclasses
}
```

The per-partition object contains both the partition values (`partitionData`) and the stats, so a single `CommitSummaryPartition` can be mapped to both `CommitEventTablePartitions` and `CommitEventTablePartitionStats` by selecting the relevant fields.

## `CommitSummaryAccumulator`

`CommitSummaryAccumulator` implements `OpenHouseTableListener` and builds the `CommitSummary`.

```java
public class CommitSummaryAccumulator implements OpenHouseTableListener {
  private final PartitionKeyAccumulator partitions = new PartitionKeyAccumulator();
  private final ColumnMetricAccumulator tableMetrics = new ColumnMetricAccumulator();
  private final Map<PartitionKey, ColumnMetricAccumulator> partitionMetrics = new HashMap<>();

  @Override
  public void onDataFileAdded(DataFile dataFile, OpenHouseTable table, OperationType operation) {
    PartitionKey key = PartitionKey.from(dataFile.partition(), table.spec());
    partitions.add(key, dataFile.recordCount());
    tableMetrics.add(dataFile, table.schema());
    partitionMetrics.computeIfAbsent(key, k -> new ColumnMetricAccumulator()).add(dataFile, table.schema());
  }

  @Override
  public void onCommit(Snapshot snapshot, OpenHouseTable table, OperationType operation) {
    CommitSummary summary = buildSummary(snapshot, table);
    // hand-off to a downstream sink (configured via the listener factory)
    sink.publish(summary);
  }
}
```

`ColumnMetricAccumulator` extracts values from `DataFile.nullCounts()`, `DataFile.nanCounts()`, `DataFile.lowerBounds()`, `DataFile.upperBounds()`, and `DataFile.columnSizes()` and merges them per column:

- `nullCount` and `nanCount`: sum across files.
- `columnSizeInBytes`: sum across files.
- `minValue`: minimum of `lowerBounds` values.
- `maxValue`: maximum of `upperBounds` values.

Values are converted from `ByteBuffer` using `org.apache.iceberg.types.Conversions.fromByteBuffer(Type, ByteBuffer)` and compared as `Comparable`.  The type-to-Java mapping matches `TableStatsCollectorUtil.convertValueToColumnData` (integer/long/date/time → `Long`, float/double/decimal → `Double`, everything else → `String`) so the adapter can trivially wrap them in `ColumnData.LongColumnData`, `ColumnData.DoubleColumnData`, or `ColumnData.StringColumnData`.

`commitAppId` and `commitAppName` are populated from `Snapshot.summary()` exactly like `TableStatsCollectorUtil` does today:

- `commitAppId`: first non-null of `spark.app.id` or `trino_query_id`.
- `commitAppName`: `spark.app.name` when `spark.app.id` is present, otherwise `trino`.

## Where to Install `OpenHouseTable`

### Java / Spark client

`OpenHouseCatalog` already builds `OpenHouseTableOperations`.  Override `loadTable` to wrap the returned `Table` with `OpenHouseTable`:

```java
@Override
public Table loadTable(TableIdentifier identifier) {
  Table table = super.loadTable(identifier);
  return OpenHouseTable.builder()
      .delegate(table)
      .clusterName(cluster)
      .listenerFactory(() -> new CommitSummaryAccumulator(new LogCommitSummarySink()))
      .build();
}
```

The factory can be extended later to include other listeners.

### OpenHouse internal catalog

`OpenHouseInternalCatalog` can use the same `OpenHouseTable` wrapper if it wants driver-side accumulation.  More importantly, `OpenHouseInternalTableOperations` should be able to read a `CommitSummary` sent by the client and attach it to the existing `CommitEvent` publishing path.  The wire format is discussed below.

## Adapter Examples

### 1. Driver log adapter

```java
@Slf4j
public class LogCommitSummarySink implements CommitSummarySink {
  @Override
  public void publish(CommitSummary summary) {
    log.info(
        "CommitSummary table={}.{} snapshot={} op={} files={} rows={} partitions={}",
        summary.getDatabaseName(),
        summary.getTableName(),
        summary.getCommitId(),
        summary.getCommitOperation(),
        summary.getPartitions().size(),
        summary.getPartitions().stream().mapToLong(CommitSummaryPartition::getRowCount).sum(),
        summary.getPartitions().size());
  }
}
```

This is the near-term consumer for the testing project: one compact line per commit in the driver log.

### 2. OpenHouseCommitEvent adapter

`CommitSummaryToLineageAdapter` in `apps/spark` maps `CommitSummary` to the existing `CommitEventTablePartitions` and `CommitEventTablePartitionStats` models.  The mapping is 1:1 because the field names and value types already match.  The adapter supplies the `BaseTableIdentifier` and `CommitMetadata` objects from `CommitSummary`'s top-level fields and converts each `CommitSummaryColumnData` to the appropriate `ColumnData` subclass.

### 3. `OpenHouseTableOperations` request attachment

`OpenHouseTableOperationsAttachmentListener` holds a reference to a `ThreadLocal` or to `OpenHouseTableOperations` itself.  In `publish(CommitSummary)` it serializes the summary to JSON and makes it available to `OpenHouseTableOperations.doCommit`, which then attaches it to the commit request.

Wire-format options:

1. **Property bag (no API change).**  Add `openhouse.commitSummary = <json>` to the table properties before `doCommit`.  `OpenHouseInternalTableOperations` can read and strip it.  This reuses the existing `CreateUpdateTableRequestBody.tableProperties` / `IcebergSnapshotsRequestBody` flow but introduces a property that briefly lives in table metadata.
2. **Dedicated request field (cleaner long-term).**  Add an optional `commitSummary` string field to `CreateUpdateTableRequestBody` and `IcebergSnapshotsRequestBody` in the OpenAPI spec.  `OpenHouseTableOperations` attaches the JSON there, and the server parses it in `doCommit`.

The property-bag approach can be used immediately for the testing project; the dedicated-field approach should be the long-term shape.

## Why This Avoids Extra Work

- **No extra I/O:** the `DataFile` objects and their metrics (`nullCounts`, `lowerBounds`, `upperBounds`, `nanCounts`, `columnSizes`) are already in memory on the driver when `appendFile` is called.  We observe them there instead of scanning `all_entries` / `data_files` later.
- **Minimal compute:** aggregation is `O(files × columns)` with simple `HashMap` / `HashSet` updates.  The only conversion is `Conversions.fromByteBuffer` per bound, which Iceberg already does internally for metadata-table reads.
- **Pluggable sinks:** the same accumulator feeds logs, events, and server requests without re-implementation.
- **Extensible wrapper:** `OpenHouseTable` is a generic hook point; the commit summary is one `OpenHouseTableListener` among potential future OpenHouse business logic.

## Open Questions

1. Should deletes be tracked as negative row/bound/null contributions, or simply ignored in the first version?
2. For `OpenHouseTable` to know `clusterName`, `OpenHouseCatalog` must pass it in.  Is `cluster` always available in the catalog properties?
3. Should the final `CommitSummary` include a top-level object for unpartitioned tables (one `CommitSummaryPartition` with `partitionData = null`) or a separate unpartitioned summary object?
4. How should the server parse the attached `CommitSummary` and route it to the existing `CommitEvent` publishing pipeline?
5. Does `openhouse-java-runtime` need to shade any new classes, or will the existing shadow configuration handle `CommitSummary` automatically?
