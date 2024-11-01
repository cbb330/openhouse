package com.linkedin.openhouse.catalog.e2e;

import static org.apache.iceberg.types.Types.NestedField.*;

import com.linkedin.openhouse.jobs.spark.Operations;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@Slf4j
public class MinimalSparkMoRTest extends OpenHouseSparkITest {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), required(2, "data", Types.StringType.get()));

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();

  private static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=0")
          .withRecordCount(1)
          .build();
  private static final DataFile FILE_B =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=1")
          .withRecordCount(1)
          .build();

  @Test
  void testWapWorkflow() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      spark.sql("CREATE TABLE openhouse.d1.t1 (name string)");
      Operations operations = Operations.withCatalog(spark, null);
      Table table = operations.getTable("d1.t1");

      table.newAppend().appendFile(FILE_A).commit();
      long snapshotIdMain = table.currentSnapshot().snapshotId();
      table.newAppend().appendFile(FILE_B).set("wap.id", "wap1").stageOnly().commit();
      List<Snapshot> snapshotList = new ArrayList<>();
      Iterator<Snapshot> it = table.snapshots().iterator();
      while (it.hasNext()) {
        snapshotList.add(it.next());
      }

      // verify that there is one committed snapshot and one staged snapshot
      Assertions.assertEquals(snapshotIdMain, table.currentSnapshot().snapshotId());
      Assertions.assertEquals(
          snapshotIdMain, table.refs().get(SnapshotRef.MAIN_BRANCH).snapshotId());
      Assertions.assertEquals(2, snapshotList.size());

      spark.sql("DROP TABLE openhouse.d1.t1");
    }
  }

  @Test
  public void testDataCompactionPartialProgressNonPartitionedTable() throws Exception {
    final String tableName = "db.test_data_compaction";
    final int numInserts = 3;

    BiFunction<Operations, Table, RewriteDataFiles.Result> rewriteFunc =
        (ops, table) ->
            ops.rewriteDataFiles(
                table,
                1024 * 1024, // 1MB
                1024, // 1KB
                1024 * 1024 * 2, // 2MB
                2,
                1,
                true,
                10);

    try (Operations ops = Operations.withCatalog(getSparkSession(), null)) {
      prepareTable(ops, tableName, false);
      populateTable(ops, tableName, numInserts);
      Table table = ops.getTable(tableName);
      log.info("Loaded table {}, location {}", table.name(), table.location());
      RewriteDataFiles.Result result = rewriteFunc.apply(ops, table);
      log.info(
          "Added {} data files, rewritten {} data files, rewritten {} bytes",
          result.addedDataFilesCount(),
          result.rewrittenDataFilesCount(),
          result.rewrittenBytesCount());
      Assertions.assertEquals(1, result.addedDataFilesCount());
      Assertions.assertEquals(3, result.rewrittenDataFilesCount());
    }
    // restart the app to reload catalog cache
    try (Operations ops = Operations.withCatalog(getSparkSession(), null)) {
      long expectedNumSnapshots = numInserts + 1;
      List<Long> snapshotIds = getSnapshotIds(ops, tableName);
      Assertions.assertEquals(
          expectedNumSnapshots,
          snapshotIds.size(),
          String.format(
              "There must be %d snapshot(s) after %d inserts and 1 data files rewrite",
              expectedNumSnapshots, numInserts));
      // check that no rewrite happens second time
      Table table = ops.getTable(tableName);
      log.info("Loaded table {}, location {}", table.name(), table.location());
      RewriteDataFiles.Result result = rewriteFunc.apply(ops, table);
      log.info(
          "Added {} data files, rewritten {} data files, rewritten {} bytes",
          result.addedDataFilesCount(),
          result.rewrittenDataFilesCount(),
          result.rewrittenBytesCount());
      Assertions.assertEquals(0, result.addedDataFilesCount());
      Assertions.assertEquals(0, result.rewrittenDataFilesCount());
      Assertions.assertEquals(0, result.rewrittenBytesCount());
    }
  }

  private static void prepareTable(Operations ops, String tableName, boolean isPartitioned) {
    ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", tableName)).show();
    if (isPartitioned) {
      ops.spark()
          .sql(
              String.format(
                  "CREATE TABLE %s (data string, ts timestamp) partitioned by (days(ts))",
                  tableName))
          .show();
    } else {
      ops.spark()
          .sql(String.format("CREATE TABLE %s (data string, ts timestamp)", tableName))
          .show();
    }
    ops.spark().sql(String.format("DESCRIBE %s", tableName)).show();
  }

  private static List<Long> getSnapshotIds(Operations ops, String tableName) {
    log.info("Getting snapshot Ids");
    List<Row> snapshots =
        ops.spark().sql(String.format("SELECT * FROM %s.snapshots", tableName)).collectAsList();
    snapshots.forEach(s -> log.info(s.toString()));
    return snapshots.stream()
        .map(r -> r.getLong(r.fieldIndex("snapshot_id")))
        .collect(Collectors.toList());
  }

  private static void populateTable(Operations ops, String tableName, int numRows) {
    populateTable(ops, tableName, numRows, 0);
  }

  private static void populateTable(Operations ops, String tableName, int numRows, int dayLag) {
    populateTable(ops, tableName, numRows, dayLag, System.currentTimeMillis() / 1000);
  }

  private static void populateTable(
      Operations ops, String tableName, int numRows, int dayLag, long timestampSeconds) {
    String timestampEntry =
        String.format("date_sub(from_unixtime(%d), %d)", timestampSeconds, dayLag);
    for (int row = 0; row < numRows; ++row) {
      ops.spark()
          .sql(String.format("INSERT INTO %s VALUES ('v%d', %s)", tableName, row, timestampEntry))
          .show();

      ops.spark().sql(String.format("DELETE FROM %s WHERE data = 'v%d'", tableName, row)).show();
    }
  }
}
