package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

/**
 * DaliSpark {@code writeExistingOpenHouseTable} second metadata commits against M1 sanitizer.
 * DaliSpark is not on this itest classpath; the sequence is the same calls: {@code writeTo}
 * (snapshot), {@code write.spark.accept-any-schema}, mergeSchema write, then {@code SET
 * TBLPROPERTIES ('avro.schema.literal'=…)}.
 *
 * <p>Real DaliSpark {@code writeDataFrame} / {@code createDataFrame} coverage lives in {@code
 * dali-mp} {@code TestOpenHouseSessionBranch}.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
@Execution(ExecutionMode.SAME_THREAD)
public class GitForDataDaliSparkPathSpark3_5 extends OpenHouseSparkITest {

  private static final String DATABASE = "gfd_dalispark";
  private static final String BRANCH = "ci";
  private static final String ASL =
      "{\"type\":\"record\",\"name\":\"Fact\",\"fields\":["
          + "{\"name\":\"id\",\"type\":\"int\"},"
          + "{\"name\":\"data\",\"type\":\"string\"},"
          + "{\"name\":\"extra\",\"type\":[\"null\",\"string\"],\"default\":null}]}";

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
      System.err.println("Warning: dalispark-path cleanup failed: " + e.getMessage());
    }
  }

  @Test
  public void mergeSchemaAndAslStayOffMain() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse." + DATABASE + ".fact";
      spark.sql("CREATE TABLE " + table + " (id int, data string)");
      spark.sql("ALTER TABLE " + table + " SET TBLPROPERTIES ('write.wap.enabled'='true')");
      spark.sql("INSERT INTO " + table + " VALUES (1, 'main_seed')");

      spark.sql("ALTER TABLE " + table + " CREATE BRANCH " + BRANCH);
      spark.conf().set("spark.wap.branch", BRANCH);
      spark.sql("REFRESH TABLE " + table);

      spark.sql("SELECT 2 as id, 'branch_row' as data").writeTo(table).append();

      spark.sql(
          "ALTER TABLE " + table + " SET TBLPROPERTIES ('write.spark.accept-any-schema'='true')");
      spark.sql("ALTER TABLE " + table + " ADD COLUMNS (extra string)");
      assertFalse(
          describeCols(spark, table).contains("extra"),
          "M1: ADD COLUMN under a branch does not stick after commit");
      AnalysisException extraWrite =
          assertThrows(
              AnalysisException.class,
              () ->
                  spark
                      .sql("SELECT 3 as id, 'with_extra' as data, 'x' as extra")
                      .writeTo(table)
                      .option("mergeSchema", "true")
                      .append(),
              "M1: extra-column mergeSchema write cannot land without a persisted schema");
      assertTrue(
          extraWrite.getMessage().contains("extra")
              || extraWrite.getMessage().contains("TOO_MANY_DATA_COLUMNS"),
          extraWrite.getMessage());
      spark.sql(
          "ALTER TABLE "
              + table
              + " SET TBLPROPERTIES ('avro.schema.literal'='"
              + ASL.replace("'", "\\'")
              + "')");
      spark.sql("REFRESH TABLE " + table);

      assertFalse(
          describeCols(spark, table).contains("extra"),
          "M1: mergeSchema column does not survive reload");
      assertEquals(
          "", tblProp(spark, table, "avro.schema.literal"), "M1: ASL does not survive reload");
      assertEquals(
          "",
          tblProp(spark, table, "write.spark.accept-any-schema"),
          "M1: accept-any-schema does not survive reload");
      assertEquals(2, count(spark, table), "branch sees seed + matching-schema append");

      spark.conf().unset("spark.wap.branch");
      spark.sql("REFRESH TABLE " + table);
      assertFalse(
          describeCols(spark, table).contains("extra"),
          "main schema must not see mergeSchema column");
      assertEquals("", tblProp(spark, table, "avro.schema.literal"), "main ASL must not move");
      assertEquals(
          "",
          tblProp(spark, table, "write.spark.accept-any-schema"),
          "accept-any-schema SET under a branch must not appear on main");
      assertEquals(1, count(spark, table), "main row count unchanged by DaliSpark second commits");
      assertEquals(
          2, spark.sql("SELECT * FROM " + table).first().size(), "main row still two columns");
    }
  }

  @Test
  public void saveAsSnapshotOverwriteAllStaysOffMain() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse." + DATABASE + ".snapshot";
      spark.sql("CREATE TABLE " + table + " (id int, data string)");
      spark.sql("ALTER TABLE " + table + " SET TBLPROPERTIES ('write.wap.enabled'='true')");
      spark.sql("INSERT INTO " + table + " VALUES (1, 'main_seed')");
      spark.sql("ALTER TABLE " + table + " CREATE BRANCH " + BRANCH);
      spark.conf().set("spark.wap.branch", BRANCH);
      spark.sql("REFRESH TABLE " + table);

      spark
          .sql("SELECT 9 as id, 'replaced' as data")
          .writeTo(table)
          .overwrite(org.apache.spark.sql.functions.lit(true));
      assertEquals(1, count(spark, table), "branch overwrite-all replaced the branch snapshot");
      assertEquals("replaced", spark.sql("SELECT data FROM " + table).first().getString(0));

      spark.conf().unset("spark.wap.branch");
      spark.sql("REFRESH TABLE " + table);
      assertEquals(1, count(spark, table), "main still has the seed row");
      assertEquals("main_seed", spark.sql("SELECT data FROM " + table).first().getString(0));
    }
  }

  private static Set<String> describeCols(SparkSession spark, String table) {
    return spark.sql("DESCRIBE TABLE " + table).collectAsList().stream()
        .map(r -> r.getString(0))
        .collect(Collectors.toSet());
  }

  private static String tblProp(SparkSession spark, String table, String key) {
    return spark.sql("SHOW TBLPROPERTIES " + table).collectAsList().stream()
        .filter(r -> key.equals(r.getString(0)))
        .map(r -> r.getString(1))
        .findFirst()
        .orElse("");
  }

  private static int count(SparkSession spark, String table) {
    return spark.sql("SELECT * FROM " + table).collectAsList().size();
  }
}
