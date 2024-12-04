package com.linkedin.openhouse.catalog.e2e;

import static org.apache.iceberg.TableProperties.*;
import static org.apache.iceberg.types.Types.NestedField.*;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;

import com.linkedin.openhouse.jobs.spark.Operations;
import com.linkedin.openhouse.jobs.util.SimpleRecord;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class MinimalSparkMoRTest extends OpenHouseSparkITest {
  static final String tableName = "db.test_data_compaction";

  final BiFunction<Operations, Table, RewriteDataFiles.Result> rewriteFunc =
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

  static Operations ops;

  @BeforeAll
  public static void startUp() throws Exception {
    ops = Operations.withCatalog(getSparkSession(), null);
  }

  @AfterAll
  public static void shutdown() throws Exception {
    ops.close();
  }

  //  @Test
  //  public void testDeleteFilesCreated() {
  //    createDeletes();
  //    Table table = ops.getTable(tableName);
  //
  //    Dataset<Row> filesMetadataTable =
  //        SparkTableUtil.loadMetadataTable(ops.spark(), table, MetadataTableType.FILES)
  //            .selectExpr("content", "file_path", "file_size_in_bytes")
  //            .dropDuplicates(
  //                "file_path",
  //                "file_size_in_bytes"); // tOdo: can the same file_path have two diff sizes
  //
  //    Dataset<Row> contentSizes =
  //        filesMetadataTable
  //            .groupBy("content")
  //            .agg(
  //                functions.count("content").alias("count"),
  //                functions.sum("file_size_in_bytes").alias("total_size"));
  //
  //    Map<Integer, Row> statsMap =
  //        contentSizes.collectAsList().stream()
  //            .collect(
  //                Collectors.toMap(
  //                    row -> row.getInt(0), // content value (0, 1, or 2)
  //                    row -> row // Row containing count and total_size
  //                ));
  //
  //    Assertions.assertEquals(0, statsMap.get(1));
  //
  //      setupTestTable();
  //      create table with two big datafiles and it creates two delete files
  //      assertDeleteFilesCreated();
  //
  //      setupTestTable();
  //      // same as above, then run compaction, and two delete files exist
  //      assertCompactionCanCreateDanglingDeletes();
  //
  //      setupTestTable();
  //      // a file in an old snapshot is expired / removable if it no longer represents the
  // underlying data
  //      // meaning a newer snapshot has overwritten the data
  //      // so for delete file it means that the equality
  //
  //
  //      // cow will create new datafile
  //
  //      // delete file to represent a new snapshot with one row edit
  //
  //      // cow will create an entirely new datafile
  //
  //      // then delete file will be unused, should expire.
  //
  //
  //      // then do the same but with compaction, it won't expire
  //      assertDanglingDeleteFilesCanNeverExpire();
  //
  //      setupTestTable();
  //      assertRewritePositionDeleteRemovesDanglingDeletes();
  //
  //      setupTestTable();
  //      assertEqualityDeletesNotCompactable();
  //
  //      setupTestTable();
  //      assertProcessToCompactEqualityDeletes();
  //
  //      setupTestTable();
  //      assertCompactionCanRemoveDeletes();

  //      Table table = ops.getTable(tableName);
  //      // log.info("Loaded table {}, location {}", table.name(), table.location());
  //      RewriteDataFiles.Result result = rewriteFunc.apply(ops, table);
  //      populateTable(ops, tableName, 3);
  //      Dataset<Row> metadataTable =
  //          SparkTableUtil.loadMetadataTable(ops.spark(), table, MetadataTableType.FILES)
  //              .selectExpr("content", "file_path", "file_size_in_bytes")
  //              .dropDuplicates(
  //                  "file_path",
  //                  "file_size_in_bytes"); // tOdo: can the same file_path have two diff sizes
  //
  //      // Aggregate counts and sums based on `content` values
  //      Dataset<Row> contentStats =
  //          metadataTable
  //              .groupBy("content")
  //              .agg(
  //                  functions.count("content").alias("count"),
  //                  functions.sum("file_size_in_bytes").alias("total_size"));
  //
  //      // Collect the result as a map for quick lookup of counts and sums by content value
  //      Map<Integer, Row> statsMap =
  //          contentStats.collectAsList().stream()
  //              .collect(
  //                  Collectors.toMap(
  //                      row -> row.getInt(0), // content value (0, 1, or 2)
  //                      row -> row // Row containing count and total_size
  //                      ));
  //      // log.info(
  //      //          "Added {} data files, rewritten {} data files, rewritten {} bytes",
  //      //          result.addedDataFilesCount(),
  //      //          result.rewrittenDataFilesCount(),
  //      //          result.rewrittenBytesCount());
  //      Assertions.assertEquals(0, result.addedDataFilesCount());
  //      Assertions.assertEquals(0, result.rewrittenDataFilesCount());
  //
  //      populateTable(ops, tableName, 3);
  //      ops.spark().sql("DELETE FROM db.test_data_compaction WHERE data = 'v6'").show();
  //      populateTable(ops, tableName, 3);
  //      RewriteDataFiles.Result result2 = rewriteFunc.apply(ops, table);
  //      Assertions.assertEquals(0, result2.addedDataFilesCount());
  //      Assertions.assertEquals(0, result2.rewrittenDataFilesCount());
  //    }
  //  }

  protected List<Object[]> rowsToJava(List<Row> rows) {
    return rows.stream().map(this::toJava).collect(Collectors.toList());
  }

  private Object[] toJava(Row row) {
    return IntStream.range(0, row.size())
        .mapToObj(
            pos -> {
              if (row.isNullAt(pos)) {
                return null;
              }

              Object value = row.get(pos);
              if (value instanceof Row) {
                return toJava((Row) value);
              } else if (value instanceof scala.collection.Seq) {
                return row.getList(pos);
              } else if (value instanceof scala.collection.Map) {
                return row.getJavaMap(pos);
              } else {
                return value;
              }
            })
        .toArray(Object[]::new);
  }

  protected List<Object[]> sql(String query, Object... args) {
    List<Row> rows = ops.spark().sql(String.format(query, args)).collectAsList();
    if (rows.isEmpty()) {
      return Collections.emptyList();
    }

    return rowsToJava(rows);
  }

  protected Object[] row(Object... values) {
    return values;
  }

  protected void initTable() {
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read', 'write.delete.distribution-mode'='range');",
        tableName);
  }

  protected void createAndInitTable(String schema) {
    sql("CREATE TABLE openhouse.%s (%s) USING iceberg %s", tableName, schema, "");
    initTable();
  }

  @Test
  public void testDeleteFilesCanBeCreated() throws NoSuchTableException {
    // TODO: use only spark scala / java api to control the number of files and be consistent
    createAndInitTable("id int, data string");

    // Create a Dataset
    List<SimpleRecord> records = Arrays.asList(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    ops.spark()
        .createDataset(records, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    // Execute 3 separate DELETE operations
    sql("DELETE FROM %s WHERE id = 1 and data='a'", tableName);
    sql("DELETE FROM %s WHERE id = 2 and data='d'", tableName);
    sql("DELETE FROM %s WHERE id = 1 and data='c'", tableName);

    // TODO: put this in the tablestats API
    ops.spark().sql(String.format("DESCRIBE %s", tableName)).show();
    Table table = ops.getTable(tableName);
    Table deleteFilesTable =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.DELETE_FILES);
    long deleteFileCount =
        StreamSupport.stream(deleteFilesTable.newScan().planFiles().spliterator(), false).count();
    assertThat(deleteFileCount).isEqualTo(1L);

    // 2. Verify the remaining rows are correct after all deletes
    List<Object[]> expected = Arrays.asList(row(1, "b"), row(2, "e"));
    List<Object[]> actual = sql("SELECT * FROM %s ORDER BY id ASC", tableName);
    assertIterableEquals(expected, actual, "Should have expected rows");
  }
}
