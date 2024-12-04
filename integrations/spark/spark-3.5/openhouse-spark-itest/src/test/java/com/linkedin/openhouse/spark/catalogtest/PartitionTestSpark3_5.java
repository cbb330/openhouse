package com.linkedin.openhouse.spark.catalogtest;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class PartitionTestSpark3_5 extends OpenHouseSparkITest {

  static final String tableName = "openhouse.db.test_data_compaction";

  SparkSession spark;

  @Test
  public void testCreateTablePartitionedWithNestedColumn2() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      List<String> transformList =
          Arrays.asList("days(time)", "header.time", "truncate(10, header.time)");
      List<String> expectedResult =
          Arrays.asList("days(time)", "bigint", "truncate(10, header.time)");
      for (int i = 0; i < transformList.size(); i++) {
        String transform = transformList.get(i);
        String tableName =
            transform
                .replaceAll("\\.", "_")
                .replaceAll("\\(", "_")
                .replaceAll("\\)", "")
                .replaceAll(", ", "_");
        spark.sql(
            String.format(
                "CREATE TABLE openhouse.d1.%s (time timestamp, header struct<time:long, name:string>) partitioned by (%s)",
                tableName, transform));
        // verify that partition spec is correct
        List<String> description =
            spark.sql(String.format("DESCRIBE TABLE openhouse.d1.%s", tableName))
                .select("data_type").collectAsList().stream()
                .map(row -> row.getString(0))
                .collect(Collectors.toList());
        assertTrue(description.contains(expectedResult.get(i)));
        spark.sql(String.format("DROP TABLE openhouse.d1.%s", tableName));
      }
    }
  }

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
    List<Row> rows = spark.sql(String.format(query, args)).collectAsList();
    if (rows.isEmpty()) {
      return Collections.emptyList();
    }

    return rowsToJava(rows);
  }

  protected Object[] row(Object... values) {
    return values;
  }

  protected void createAndInitTable(String schema) {
    sql("CREATE TABLE %s (%s) USING iceberg %s", tableName, schema, "");
    initTable();
  }

  protected void initTable() {
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read', 'write.delete.distribution-mode'='range');",
        tableName);
  }

  @Test
  public void testDeleteFilesCanBeCreated() throws Exception {
    // TODO: use only spark scala / java api to control the number of files and be consistent
    spark = getSparkSession();
    createAndInitTable("id int, data string");

    // Create a Dataset
    List<SimpleRecord> records = Arrays.asList(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    spark
        .createDataset(records, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    // Execute 3 separate DELETE operations
    sql("DELETE FROM %s WHERE id = 1 and data='a'", tableName);
    sql("DELETE FROM %s WHERE id = 2 and data='d'", tableName);
    sql("DELETE FROM %s WHERE id = 1 and data='c'", tableName);

    // TODO: put this in the tablestats API
    spark.sql(String.format("DESCRIBE %s", tableName)).show();
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
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
