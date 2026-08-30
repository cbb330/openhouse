# Generic OpenHouseTable Operation Summary Accumulator (Commits and Scans)

## Goals

- Provide a **single, reusable core** that records a summary of what changed during an Iceberg commit or scan without any extra metadata-table scans.
- Use the **same object shape** that `TableStatsCollector` already produces for `CommitEventTablePartitions` and `CommitEventTablePartitionStats`.
- Make the summary useful for multiple downstream consumers:
  1. Spark driver logs (near-term, for the testing project).
  2. `OpenHouseCommitEvent` lineage events.
  3. An attachment sent to the OpenHouse server inside `OpenHouseTableOperations`.
  4. Scan summaries accumulated during table reads.
- Keep lineage-, Spark-, and server-specific code in pluggable **adapters**, not in the core.
- Avoid additional I/O and minimize compute by intercepting `DataFile` objects while they are already in memory during the commit or scan path.

## Non-Goals

- Re-implement Iceberg's `TableOperations` or catalog logic.
- Replace the existing `TableStatsCollectionSparkApp` batch collectors.  This is a lightweight, operation-time complement, not a replacement.
- Persist the summary inside table metadata as a first-class snapshot field.

## Core Idea

Introduce a generic `OpenHouseTable` wrapper that delegates every `Table` method to the real table and overrides the data-file mutation entry points (`newAppend`, `newFastAppend`, `newOverwrite`, `newReplacePartitions`, `newDelete`, `newTransaction`) and the scan entry points (`newScan`, `newIncrementalAppendScan`, `newIncrementalChangelogScan`).  `OpenHouseTable` dispatches `OpenHouseTableListener` events for every `DataFile` added/deleted, for every successful commit or transaction commit, and for every `DataFile` observed during a scan.

One listener, `OpenHouseTableSummaryAccumulator`, collects the events into an `OpenHouseTableSummary`.  `OpenHouseTableSummary` is a plain POJO whose partition-level objects have the **same field layout** as `CommitEventTablePartitions` and `CommitEventTablePartitionStats`.  Other business logic can be added later as additional `OpenHouseTableListener` implementations without changing the wrapper.

Because the data already flows through the driver on commit and scan, this avoids the later `all_entries` / `data_files` / `snapshots` metadata-table scans that the current batch collectors perform.

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
├── OpenHouseTableScan                     // TableScan wrapper
├── OpenHouseIncrementalAppendScan         // IncrementalAppendScan wrapper
├── OpenHouseIncrementalChangelogScan      // IncrementalChangelogScan wrapper
├── OpenHouseTableSummary                  // core output POJO
├── OpenHouseTableSummarySink              // sink interface for summaries
├── OpenHouseTableSummaryAccumulator       // OpenHouseTableListener implementation
└── OpenHouseTableOperationsAttachmentListener // attaches summary to commit request

apps/spark/src/main/java/com/linkedin/openhouse/jobs/util
└── OpenHouseTableSummaryToLineageAdapter          // maps OpenHouseTableSummary to CommitEventTable* models

apps/spark/src/main/java/com/linkedin/openhouse/jobs/spark
└── LogOpenHouseTableSummaryListener               // prints summary to driver logs
```

No new Gradle module is required.  `openhouse-java-runtime` already depends on `iceberg-core`, so it has access to `Table`, `DataFile`, `PartitionSpec`, `Schema`, `Snapshot`, and `Conversions`.

## Generic `OpenHouseTable` Wrapper

`OpenHouseTable` is a generic `Table` decorator.  It forwards all `Table` methods to the delegate and overrides the data-file mutation and scan entry points.  Business logic is injected via `OpenHouseTableListener`.

```java
package com.linkedin.openhouse.javaclient;

import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.DataFile;

public enum OperationType {
  APPEND, FAST_APPEND, OVERWRITE, REPLACE, DELETE, MIXED,
  SCAN, INCREMENTAL_APPEND_SCAN, INCREMENTAL_CHANGELOG_SCAN
}

public interface OpenHouseTableListener {
  default void onDataFileAdded(DataFile dataFile, OpenHouseTable table, OperationType operation) {}
  default void onDataFileDeleted(DataFile dataFile, OpenHouseTable table, OperationType operation) {}
  default void onCommit(Snapshot snapshot, OpenHouseTable table, OperationType operation) {}
  default void onScanFile(DataFile dataFile, OpenHouseTable table, OperationType operation) {}
  default void onScanComplete(Snapshot snapshot, OpenHouseTable table, OperationType operation) {}
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
    return new OpenHouseAppendFiles(delegate.newFastAppend(), this, OperationType.FAST_APPEND, createListeners());
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

  @Override
  public TableScan newScan() {
    return new OpenHouseTableScan(delegate.newScan(), this, OperationType.SCAN, createListeners());
  }

  @Override
  public IncrementalAppendScan newIncrementalAppendScan() {
    return new OpenHouseIncrementalAppendScan(
        delegate.newIncrementalAppendScan(), this, OperationType.INCREMENTAL_APPEND_SCAN, createListeners());
  }

  @Override
  public IncrementalChangelogScan newIncrementalChangelogScan() {
    return new OpenHouseIncrementalChangelogScan(
        delegate.newIncrementalChangelogScan(), this, OperationType.INCREMENTAL_CHANGELOG_SCAN, createListeners());
  }

  private List<OpenHouseTableListener> createListeners() {
    return listenerFactories.stream()
        .map(OpenHouseTableListenerFactory::create)
        .collect(Collectors.toList());
  }
}
```

`OpenHouseTableListenerFactory` creates a fresh listener instance for each operation (or a single shared instance for a transaction).  This keeps state isolated between operations.

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

`OpenHouseTableScan` wraps a `TableScan` and intercepts `planFiles()` so each `FileScanTask` fires `onScanFile`, then `onScanComplete` is called with the scan's snapshot:

```java
package com.linkedin.openhouse.javaclient;

import org.apache.iceberg.TableScan;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;

public class OpenHouseTableScan implements TableScan {
  private final TableScan delegate;
  private final OpenHouseTable table;
  private final OperationType operation;
  private final List<OpenHouseTableListener> listeners;

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    CloseableIterable<FileScanTask> tasks = delegate.planFiles();
    CloseableIterable<FileScanTask> notifying = CloseableIterable.transform(tasks, task -> {
      listeners.forEach(l -> l.onScanFile(task.file(), table, operation));
      return task;
    });
    return CloseableIterable.whenComplete(notifying, () -> {
      Snapshot snapshot = delegate.snapshot();
      listeners.forEach(l -> l.onScanComplete(snapshot, table, operation));
    });
  }

  @Override
  public CloseableIterable<CombinedScanTask> planTasks() {
    return delegate.planTasks();
  }

  @Override
  public TableScan useSnapshot(long snapshotId) {
    return new OpenHouseTableScan(delegate.useSnapshot(snapshotId), table, operation, listeners);
  }

  @Override
  public TableScan filter(Expression expr) {
    return new OpenHouseTableScan(delegate.filter(expr), table, operation, listeners);
  }

  @Override
  public Snapshot snapshot() {
    return delegate.snapshot();
  }

  // ... delegate all other TableScan methods ...
}
```

`OpenHouseIncrementalAppendScan` and `OpenHouseIncrementalChangelogScan` are analogous.  The changelog wrapper unwraps `ChangelogScanTask` to its underlying `ContentScanTask<DataFile>` and emits `onScanFile` (or, for deletes, `onDataFileDeleted`) according to the task's `ChangelogOperation`.

`OpenHouseTransaction` creates the same inner wrappers but shares one listener list across the transaction and fires `onCommit` once in `commitTransaction()` with the final snapshot and `OperationType.MIXED`.

This design makes `OpenHouseTable` a generic hook point for future OpenHouse business logic (e.g., policy checks, client-side metrics, request enrichment) without changing the wrapper class each time.

## `OpenHouseTableSummary` Shape

`OpenHouseTableSummary` is intentionally a plain POJO that mirrors the field layout of `CommitEventTablePartitions` and `CommitEventTablePartitionStats` from `services:common`.  It lives in `openhouse-java-runtime` so the client jar does not have to depend on `services:common`.

The same POJO is used for both commits and scans.  For scans, the `commit*` fields are populated from the scanned snapshot and the current Spark/Trino app context, and `commitOperation` is set to the scan type (e.g. `SCAN`, `INCREMENTAL_APPEND_SCAN`).

```java
@Data
@Builder
public class OpenHouseTableSummary {
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
  private String commitOperation;        // e.g. "APPEND", "OVERWRITE", "SCAN"
  private long eventTimestampMs;

  // one entry per unique partition affected by the operation
  private List<OpenHouseTableSummaryPartition> partitions;
}
```

```java
@Data
@Builder
public class OpenHouseTableSummaryPartition {
  // matches CommitEventTablePartitions.partitionData
  private List<OpenHouseTableSummaryColumnData> partitionData;

  // matches CommitEventTablePartitionStats
  private long rowCount;
  private long columnCount;
  private List<OpenHouseTableSummaryColumnData> nullCount;
  private List<OpenHouseTableSummaryColumnData> nanCount;
  private List<OpenHouseTableSummaryColumnData> minValue;
  private List<OpenHouseTableSummaryColumnData> maxValue;
  private List<OpenHouseTableSummaryColumnData> columnSizeInBytes;
}
```

```java
@Data
@Builder
public class OpenHouseTableSummaryColumnData {
  private String columnName;
  private Object value;  // typed Long, Double, or String; adapters convert to ColumnData subclasses
}
```

The per-partition object contains both the partition values (`partitionData`) and the stats, so a single `OpenHouseTableSummaryPartition` can be mapped to both `CommitEventTablePartitions` and `CommitEventTablePartitionStats` by selecting the relevant fields.

## `OpenHouseTableSummaryAccumulator`

`OpenHouseTableSummaryAccumulator` implements `OpenHouseTableListener` and builds the `OpenHouseTableSummary` for both commits and scans.  It publishes to a simple sink interface so consumers can choose what to do with each summary.

```java
public interface OpenHouseTableSummarySink {
  void publish(OpenHouseTableSummary summary);
}
```

```java
public class OpenHouseTableSummaryAccumulator implements OpenHouseTableListener {
  private final OpenHouseTableSummarySink sink;
  private final PartitionKeyAccumulator partitions = new PartitionKeyAccumulator();
  private final ColumnMetricAccumulator tableMetrics = new ColumnMetricAccumulator();
  private final Map<PartitionKey, ColumnMetricAccumulator> partitionMetrics = new HashMap<>();

  public OpenHouseTableSummaryAccumulator(OpenHouseTableSummarySink sink) {
    this.sink = sink;
  }

  @Override
  public void onDataFileAdded(DataFile dataFile, OpenHouseTable table, OperationType operation) {
    accumulate(dataFile, table);
  }

  @Override
  public void onScanFile(DataFile dataFile, OpenHouseTable table, OperationType operation) {
    accumulate(dataFile, table);
  }

  @Override
  public void onCommit(Snapshot snapshot, OpenHouseTable table, OperationType operation) {
    publish(snapshot, table, operation);
  }

  @Override
  public void onScanComplete(Snapshot snapshot, OpenHouseTable table, OperationType operation) {
    publish(snapshot, table, operation);
  }

  private void accumulate(DataFile dataFile, OpenHouseTable table) {
    PartitionKey key = PartitionKey.from(dataFile.partition(), table.spec());
    partitions.add(key, dataFile.recordCount());
    tableMetrics.add(dataFile, table.schema());
    partitionMetrics.computeIfAbsent(key, k -> new ColumnMetricAccumulator()).add(dataFile, table.schema());
  }

  private void publish(Snapshot snapshot, OpenHouseTable table, OperationType operation) {
    OpenHouseTableSummary summary = buildSummary(snapshot, table, operation);
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

`commitAppId` and `commitAppName` are populated from `Snapshot.summary()` for commits exactly like `TableStatsCollectorUtil` does today:

- `commitAppId`: first non-null of `spark.app.id` or `trino_query_id`.
- `commitAppName`: `spark.app.name` when `spark.app.id` is present, otherwise `trino`.

For scans, the same fields can be populated from the current Spark/Trino app context if available, or left empty; the `commitOperation` field clearly distinguishes scan operations.

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
      .listenerFactory(() -> new OpenHouseTableSummaryAccumulator(new LogOpenHouseTableSummarySink()))
      .build();
}
```

The factory can be extended later to include other listeners.

### OpenHouse internal catalog

`OpenHouseInternalCatalog` can use the same `OpenHouseTable` wrapper if it wants driver-side accumulation.  More importantly, `OpenHouseInternalTableOperations` should be able to read a `OpenHouseTableSummary` sent by the client and attach it to the existing `CommitEvent` publishing path.  The wire format is discussed below.

## Adapter Examples

### 1. Driver log adapter

```java
@Slf4j
public class LogOpenHouseTableSummarySink implements OpenHouseTableSummarySink {
  @Override
  public void publish(OpenHouseTableSummary summary) {
    log.info(
        "OpenHouseTableSummary table={}.{} snapshot={} op={} files={} rows={} partitions={}",
        summary.getDatabaseName(),
        summary.getTableName(),
        summary.getCommitId(),
        summary.getCommitOperation(),
        summary.getPartitions().size(),
        summary.getPartitions().stream().mapToLong(OpenHouseTableSummaryPartition::getRowCount).sum(),
        summary.getPartitions().size());
  }
}
```

This is the near-term consumer for the testing project: one compact line per operation in the driver log.

### 2. OpenHouseCommitEvent adapter

`OpenHouseTableSummaryToLineageAdapter` in `apps/spark` maps `OpenHouseTableSummary` to the existing `CommitEventTablePartitions` and `CommitEventTablePartitionStats` models.  The mapping is 1:1 because the field names and value types already match.  The adapter supplies the `BaseTableIdentifier` and `CommitMetadata` objects from `OpenHouseTableSummary`'s top-level fields and converts each `OpenHouseTableSummaryColumnData` to the appropriate `ColumnData` subclass.

This adapter is only used for commit operations; scan summaries are routed to the log sink or other non-lineage consumers.

### 3. `OpenHouseTableOperations` request attachment

`OpenHouseTableOperationsAttachmentListener` holds a reference to a `ThreadLocal` or to `OpenHouseTableOperations` itself.  In `publish(OpenHouseTableSummary)` it serializes the summary to JSON and makes it available to `OpenHouseTableOperations.doCommit`, which then attaches it to the commit request.

Wire-format options:

1. **Property bag (no API change).**  Add `openhouse.commitSummary = <json>` to the table properties before `doCommit`.  `OpenHouseInternalTableOperations` can read and strip it.  This reuses the existing `CreateUpdateTableRequestBody.tableProperties` / `IcebergSnapshotsRequestBody` flow but introduces a property that briefly lives in table metadata.
2. **Dedicated request field (cleaner long-term).**  Add an optional `commitSummary` string field to `CreateUpdateTableRequestBody` and `IcebergSnapshotsRequestBody` in the OpenAPI spec.  `OpenHouseTableOperations` attaches the JSON there, and the server parses it in `doCommit`.

The property-bag approach can be used immediately for the testing project; the dedicated-field approach should be the long-term shape.

## Why This Avoids Extra Work

- **No extra I/O:** the `DataFile` objects and their metrics (`nullCounts`, `lowerBounds`, `upperBounds`, `nanCounts`, `columnSizes`) are already in memory on the driver when `appendFile` is called and during `planFiles()` in a scan.  We observe them there instead of scanning `all_entries` / `data_files` later.
- **Minimal compute:** aggregation is `O(files × columns)` with simple `HashMap` / `HashSet` updates.  The only conversion is `Conversions.fromByteBuffer` per bound, which Iceberg already does internally for metadata-table reads.
- **Pluggable sinks:** the same accumulator feeds logs, events, and server requests without re-implementation.
- **Extensible wrapper:** `OpenHouseTable` is a generic hook point; the operation summary is one `OpenHouseTableListener` among potential future OpenHouse business logic.

## Open Questions

1. Should deletes be tracked as negative row/bound/null contributions, or simply ignored in the first version?
2. For `OpenHouseTable` to know `clusterName`, `OpenHouseCatalog` must pass it in.  Is `cluster` always available in the catalog properties?
3. Should the final `OpenHouseTableSummary` include a top-level object for unpartitioned tables (one `OpenHouseTableSummaryPartition` with `partitionData = null`) or a separate unpartitioned summary object?
4. How should the server parse the attached `OpenHouseTableSummary` and route it to the existing `CommitEvent` publishing pipeline?
5. Does `openhouse-java-runtime` need to shade any new classes, or will the existing shadow configuration handle `OpenHouseTableSummary` automatically?
6. For changelog scans, should `DeletedDataFileScanTask` and `DeletedRowsScanTask` contribute to `onDataFileDeleted` or be treated as scan observations?
7. For incremental append scans, should the summary be constrained to the files added between `fromSnapshot` and `toSnapshot`, or include all files in the `toSnapshot`?
