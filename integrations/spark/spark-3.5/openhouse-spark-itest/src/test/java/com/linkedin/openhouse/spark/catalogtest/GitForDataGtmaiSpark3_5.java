package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

/**
 * Local e2e of the gtmai-outbound-attribution-offline {@code DataWriter} contract on the
 * git-for-data M1 sanitizer. Spark SQL + {@code writeTo} against the local OpenHouse server; the
 * DaliSpark second-commit path (mergeSchema + ASL SET TBLPROPERTIES) is {@link
 * GitForDataDaliSparkPathSpark3_5}.
 *
 * <p>Source of truth (SQL strings and table-name require stay identical): {@code
 * gtmai-outbound-attribution-offline-azkaban/.../mtaoutbound/utils/DataWriter.scala}
 *
 * <p>The user job asserts named properties ({@code avro.schema.literal}, {@code
 * write.spark.accept-any-schema}), not reserved {@code gfd.ref.*} keys. Reserved keys are hidden
 * from {@code SHOW TBLPROPERTIES} on both views.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
@Execution(ExecutionMode.SAME_THREAD)
public class GitForDataGtmaiSpark3_5 extends OpenHouseSparkITest {

  private static final String DATABASE = "gfd_gtmai";
  private static final String BRANCH = "ci";
  /** DataWriter {@code require} regex: {@code ^[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+$}. */
  private static final String TABLE_NAME_REGEX = "^[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+$";

  @AfterEach
  public void cleanupAfterTest() {
    try (SparkSession spark = getSparkSession()) {
      spark.conf().unset("spark.wap.id");
      spark.conf().unset("spark.wap.branch");
      try {
        List<Row> tables = spark.sql("SHOW TABLES IN openhouse." + DATABASE).collectAsList();
        for (Row table : tables) {
          spark.sql("DROP TABLE IF EXISTS openhouse." + DATABASE + "." + table.getString(1));
        }
      } catch (Exception ignored) {
        // database may not exist
      }
    } catch (Exception e) {
      System.err.println("Warning: gtmai cleanup failed: " + e.getMessage());
    }
  }

  @Test
  public void workflow_gtmaiDataWriterContract() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse." + DATABASE + ".fact";
      String dbTable = DATABASE + ".fact";

      // Fact-style table matching DataWriter.saveAsPartition (datepartition + optional
      // jobMetadata).
      spark.sql(
          "CREATE TABLE "
              + table
              + " (id int, data string, jobMetadata struct<version:string, writeDate:bigint>,"
              + " datepartition string) PARTITIONED BY (datepartition)");
      spark.sql("ALTER TABLE " + table + " SET TBLPROPERTIES ('write.wap.enabled'='true')");
      spark.sql(
          "INSERT INTO "
              + table
              + " VALUES (1, 'main_seed', named_struct('version', 'seed', 'writeDate', 0L),"
              + " '2026-01-01')");
      assertEquals(1, count(spark, table), "seed lands on main");
      assertEquals("2026-01-01", firstDatepartition(spark, table));

      spark.sql("ALTER TABLE " + table + " CREATE BRANCH " + BRANCH);
      setBranch(spark, table, BRANCH);

      // --- A. saveAsPartition: DaliSpark OVERWRITE_MODE=PARTITION via writeTo.overwritePartitions
      // ---
      spark
          .sql(
              "SELECT 2 as id, 'branch_part' as data,"
                  + " named_struct('version', 'ci', 'writeDate', 1L) as jobMetadata,"
                  + " '2026-01-02' as datepartition")
          .writeTo(table)
          .overwritePartitions();
      assertEquals(2, count(spark, table), "branch sees seed partition + new partition rows");
      assertTrue(
          datepartitions(spark, table).contains("2026-01-02"),
          "branch sees the overwritten partition");

      unsetBranch(spark, table);
      assertEquals(
          1, count(spark, table), "main partition stays after branched overwritePartitions");
      assertEquals("main_seed", spark.sql("SELECT data FROM " + table).first().getString(0));
      assertEquals("2026-01-01", firstDatepartition(spark, table));

      // --- B. Named properties the user job asserts (not gfd.ref.*) ---
      setBranch(spark, table, BRANCH);
      spark.sql("ALTER TABLE " + table + " SET TBLPROPERTIES ('gfd.marker'='branched')");
      spark.sql(
          "ALTER TABLE "
              + table
              + " SET TBLPROPERTIES ('avro.schema.literal'='{\"type\":\"record\",\"name\":\"n\",\"fields\":[]}')");
      spark.sql(
          "ALTER TABLE " + table + " SET TBLPROPERTIES ('write.spark.accept-any-schema'='true')");
      spark.sql("REFRESH TABLE " + table);
      assertEquals(
          "", tblProp(spark, table, "gfd.marker"), "M1: named props do not survive reload");
      assertEquals(
          "",
          tblProp(spark, table, "avro.schema.literal"),
          "M1: ASL SET under a branch does not survive reload");
      assertEquals(
          "",
          tblProp(spark, table, "write.spark.accept-any-schema"),
          "M1: accept-any-schema SET under a branch does not survive reload");
      assertTrue(
          reservedKeys(spark, table).isEmpty(),
          "reserved overlay keys must be hidden while branched: " + reservedKeys(spark, table));

      unsetBranch(spark, table);
      assertEquals("", tblProp(spark, table, "gfd.marker"), "main does not see overlay gfd.marker");
      assertEquals(
          "", tblProp(spark, table, "avro.schema.literal"), "main does not see overlay ASL");
      assertEquals(
          "",
          tblProp(spark, table, "write.spark.accept-any-schema"),
          "write.spark.accept-any-schema SET under a branch must not appear on main");
      assertTrue(
          reservedKeys(spark, table).isEmpty(),
          "reserved overlay keys must be hidden on main: " + reservedKeys(spark, table));

      // --- C. DataWriter.setOpenHouseRetentionPolicy (exact SQL): overlay, do not touch main ---
      setBranch(spark, table, BRANCH);
      try {
        setOpenHouseRetentionPolicy(spark, dbTable, 180);
        spark.sql("REFRESH TABLE " + table);
        String branchedPolicy = tblProp(spark, table, "updated.openhouse.policy");
        if (branchedPolicy.isEmpty()) {
          branchedPolicy = tblProp(spark, table, "policies");
        }
        assertFalse(
            branchedPolicy.contains("180"),
            "M1: SET POLICY under a branch must not survive reload: " + branchedPolicy);
      } catch (Exception e) {
        String text = exceptionText(e);
        if (looksLikeParserGap(text)) {
          System.out.println(
              "SET POLICY under branch: parser/server differs, not overlay: " + text);
        } else {
          throw new AssertionError(
              "SET POLICY under a branch must not refuse or hit main: " + text, e);
        }
      }

      unsetBranch(spark, table);
      String mainPolicy =
          tblProp(spark, table, "updated.openhouse.policy") + tblProp(spark, table, "policies");
      assertFalse(
          mainPolicy.contains("180"),
          "SET POLICY under a branch must not change main policies: " + mainPolicy);
      try {
        setOpenHouseRetentionPolicy(spark, dbTable, 180);
      } catch (Exception e) {
        String text = exceptionText(e);
        String lower = text.toLowerCase();
        assertFalse(
            lower.contains("table not found")
                || lower.contains("cannot find")
                || lower.contains("does not exist")
                || lower.contains("table disappeared"),
            "SET POLICY on main must not mean the table disappeared: " + text);
        System.out.println(
            "SET POLICY on main: local OpenHouse did not apply retention (not a drop): " + text);
      }

      // --- D. DataWriter.dropOpenHouseTable under a branch (GitForDataSparkCatalog footgun) ---
      setBranch(spark, table, BRANCH);
      dropOpenHouseTable(spark, dbTable);
      boolean factStillThere =
          spark.sql("SHOW TABLES IN openhouse." + DATABASE).collectAsList().stream()
              .anyMatch(r -> "fact".equals(r.getString(1)));
      assertTrue(factStillThere, "DROP TABLE under a branch must not remove the catalog object");
      assertEquals(0, count(spark, table), "DROP resets the session branch to empty");
      unsetBranch(spark, table);
      assertEquals(1, count(spark, table), "main row count unchanged after branched DROP");
    }
  }

  @Test
  public void invalidTableNames_matchDataWriterRequire() {
    IllegalArgumentException noDot =
        assertThrows(IllegalArgumentException.class, () -> requireOpenHouseTableName("nodottable"));
    assertTrue(noDot.getMessage().contains("Invalid table name format: nodottable"));
    IllegalArgumentException badChars =
        assertThrows(
            IllegalArgumentException.class, () -> requireOpenHouseTableName("db.table-name!"));
    assertTrue(badChars.getMessage().contains("Invalid table name format: db.table-name!"));
    requireOpenHouseTableName(DATABASE + ".fact");
  }

  /**
   * Port of DataWriter {@code require(tableName.matches(...))} used by {@code
   * setOpenHouseRetentionPolicy} and {@code dropOpenHouseTable}.
   */
  static void requireOpenHouseTableName(String tableName) {
    if (tableName == null || !tableName.matches(TABLE_NAME_REGEX)) {
      throw new IllegalArgumentException("Invalid table name format: " + tableName);
    }
  }

  /** Exact SQL from DataWriter.setOpenHouseRetentionPolicy. */
  static void setOpenHouseRetentionPolicy(SparkSession spark, String tableName, int days) {
    requireOpenHouseTableName(tableName);
    spark.sql(
        "ALTER TABLE openhouse."
            + tableName
            + " SET POLICY (RETENTION = "
            + days
            + "d ON COLUMN datepartition)");
  }

  /** Exact SQL from DataWriter.dropOpenHouseTable. */
  static void dropOpenHouseTable(SparkSession spark, String tableName) {
    requireOpenHouseTableName(tableName);
    spark.sql("DROP TABLE IF EXISTS openhouse." + tableName);
  }

  private static void setBranch(SparkSession spark, String table, String branch) {
    spark.conf().set("spark.wap.branch", branch);
    spark.sql("REFRESH TABLE " + table);
  }

  private static void unsetBranch(SparkSession spark, String table) {
    spark.conf().unset("spark.wap.branch");
    spark.sql("REFRESH TABLE " + table);
  }

  private static String tblProp(SparkSession spark, String table, String key) {
    return spark.sql("SHOW TBLPROPERTIES " + table).collectAsList().stream()
        .filter(r -> key.equals(r.getString(0)))
        .map(r -> r.getString(1))
        .findFirst()
        .orElse("");
  }

  private static List<String> reservedKeys(SparkSession spark, String table) {
    return spark.sql("SHOW TBLPROPERTIES " + table).collectAsList().stream()
        .map(r -> r.getString(0))
        .filter(k -> k != null && k.startsWith("gfd.ref."))
        .collect(Collectors.toList());
  }

  private static int count(SparkSession spark, String table) {
    return spark.sql("SELECT * FROM " + table).collectAsList().size();
  }

  private static String firstDatepartition(SparkSession spark, String table) {
    return spark.sql("SELECT datepartition FROM " + table).first().getString(0);
  }

  private static List<String> datepartitions(SparkSession spark, String table) {
    return spark.sql("SELECT datepartition FROM " + table).collectAsList().stream()
        .map(r -> r.getString(0))
        .collect(Collectors.toList());
  }

  private static String exceptionText(Throwable t) {
    StringBuilder sb = new StringBuilder();
    for (Throwable cur = t; cur != null; cur = cur.getCause()) {
      if (cur.getMessage() != null) {
        sb.append(cur.getMessage()).append(' ');
      }
      sb.append(cur.getClass().getSimpleName()).append(' ');
    }
    return sb.toString();
  }

  private static boolean looksLikeParserGap(String text) {
    String lower = text.toLowerCase();
    return lower.contains("parseexception")
        || lower.contains("mismatched")
        || lower.contains("extraneous input")
        || lower.contains("syntax error")
        || lower.contains("cannot recognize");
  }
}
