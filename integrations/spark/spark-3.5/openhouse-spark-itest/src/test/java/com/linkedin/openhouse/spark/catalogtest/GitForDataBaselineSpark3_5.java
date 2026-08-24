package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import java.util.Set;
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
 * Overlay e2e for git-for-data on real jars: OpenHouse local server, Spark 3.5, and the
 * openhouse-spark-3.5-runtime shadow jar.
 *
 * <p>M1: schema, user properties, and policies must not move prod. After a reload the branch looks
 * like prod again (no sidecar overlay file). Catalog identity (rename, location, GRANT) is ignored
 * on the house table. DROP resets the session branch and stamps REST delete. A missing session
 * branch is created from current main on load.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
@Execution(ExecutionMode.SAME_THREAD)
public class GitForDataBaselineSpark3_5 extends OpenHouseSparkITest {

  private static final String DATABASE = "gfd_baseline";
  private static final String BRANCH = "ci";

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
      System.err.println("Warning: baseline cleanup failed: " + e.getMessage());
    }
  }

  @Test
  public void workflow_openhouseSessionBranchBaseline() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      String table = "openhouse." + DATABASE + ".fact";
      spark.sql("CREATE TABLE " + table + " (id int, data string)");
      spark.sql("ALTER TABLE " + table + " SET TBLPROPERTIES ('write.wap.enabled'='true')");
      spark.sql("INSERT INTO " + table + " VALUES (1, 'main_seed')");
      assertEquals(1, count(spark, table), "seed lands on main");
      assertRefs(spark, table, "main");

      // --- 1. DML with session branch: writeTo.append is the DaliSpark path ---
      spark.sql("ALTER TABLE " + table + " CREATE BRANCH " + BRANCH);
      spark.conf().set("spark.wap.branch", BRANCH);
      spark.sql("SELECT 2 as id, 'branch_row' as data").writeTo(table).append();

      assertEquals(2, count(spark, table), "session branch read sees seed + branch row");
      assertEquals(
          2,
          count(spark, table + " VERSION AS OF '" + BRANCH + "'"),
          "explicit branch matches session read");
      spark.conf().unset("spark.wap.branch");
      assertEquals(1, count(spark, table), "main unchanged after writeTo.append");
      assertEquals("main_seed", spark.sql("SELECT data FROM " + table).first().getString(0));

      // --- 2. Missing-ref read creates the branch from current main (does not follow live main)
      // ---
      spark.conf().set("spark.wap.branch", "does_not_exist");
      spark.sql("REFRESH TABLE " + table);
      assertEquals(
          1,
          count(spark, table),
          "first read of a missing spark.wap.branch creates from current main");
      assertRefs(spark, table, "main", BRANCH, "does_not_exist");
      spark.conf().unset("spark.wap.branch");
      spark.sql("INSERT INTO " + table + " VALUES (99, 'later_main')");
      assertEquals(2, count(spark, table), "main advanced after the missing-ref was created");
      spark.conf().set("spark.wap.branch", "does_not_exist");
      spark.sql("REFRESH TABLE " + table);
      assertEquals(
          1,
          count(spark, table),
          "missing-ref branch stays at create-time main and must not follow live main");
      spark.conf().unset("spark.wap.branch");
      spark.sql("REFRESH TABLE " + table);
      spark.sql("DELETE FROM " + table + " WHERE id = 99");
      assertEquals(1, count(spark, table), "restore seed-only main for later steps");

      // --- 3. Missing-ref write creates the branch (do not throw) ---
      spark.conf().set("spark.wap.branch", "created_by_write");
      spark.sql("INSERT INTO " + table + " VALUES (3, 'from_missing_ref')");
      assertRefs(spark, table, "main", BRANCH, "created_by_write");
      assertEquals(2, count(spark, table + " VERSION AS OF 'created_by_write'"));
      spark.conf().unset("spark.wap.branch");
      assertEquals(1, count(spark, table), "main still seed-only after implicit branch create");

      // --- 4. ADD COLUMN must not move prod schema (M1: does not stick after commit) ---
      spark.conf().set("spark.wap.branch", BRANCH);
      spark.sql("ALTER TABLE " + table + " ADD COLUMNS (extra string)");
      assertFalse(
          describeCols(spark, table).contains("extra"),
          "M1: ADD COLUMN under a branch does not stick after commit");
      assertEquals(2, count(spark, table), "branch still sees seed + branch_row");
      spark.conf().unset("spark.wap.branch");
      spark.sql("REFRESH TABLE " + table);
      assertEquals(
          "",
          tblProp(spark, table, "gfd.ref.ci.schema"),
          "reserved overlay keys are hidden from SHOW TBLPROPERTIES on main");
      assertFalse(describeCols(spark, table).contains("extra"), "main DESCRIBE does not see extra");
      List<Row> mainAfterAdd = spark.sql("SELECT * FROM " + table).collectAsList();
      assertEquals(1, mainAfterAdd.size(), "main row count unchanged by ADD COLUMN");
      assertEquals(2, mainAfterAdd.get(0).size(), "main row has the original two columns");

      // --- 5. User properties must not move prod (M1: gone after reload) ---
      spark.conf().set("spark.wap.branch", BRANCH);
      spark.sql("ALTER TABLE " + table + " SET TBLPROPERTIES ('gfd.marker'='branched')");
      spark.sql(
          "ALTER TABLE "
              + table
              + " SET TBLPROPERTIES ('avro.schema.literal'='{\"type\":\"record\",\"name\":\"n\",\"fields\":[]}')");
      spark.sql("REFRESH TABLE " + table);
      assertEquals(
          "", tblProp(spark, table, "gfd.marker"), "M1: named props do not survive commit");
      assertEquals("", tblProp(spark, table, "avro.schema.literal"));
      spark.conf().unset("spark.wap.branch");
      spark.sql("REFRESH TABLE " + table);
      assertEquals("", tblProp(spark, table, "gfd.marker"), "main does not see branch property");
      assertEquals("", tblProp(spark, table, "avro.schema.literal"), "main ASL unchanged");

      // --- 6. CREATE TABLE while branched ensures the branch; insert does not invent main ---
      String created = "openhouse." + DATABASE + ".created_under_branch";
      spark.conf().set("spark.wap.branch", BRANCH);
      spark.sql("CREATE TABLE " + created + " (id int)");
      assertRefs(spark, created, BRANCH);
      spark.sql("INSERT INTO " + created + " VALUES (1)");
      assertRefs(spark, created, BRANCH);
      assertEquals(1, count(spark, created), "session branch sees the create-path insert");
      spark.conf().unset("spark.wap.branch");
      spark.sql("REFRESH TABLE " + created);
      assertEquals(
          0,
          count(spark, created),
          "SELECT without branch sees 0 rows when only the ci ref has data");

      // --- 7. DROP TABLE while branched empties the branch and leaves the table ---
      spark.conf().set("spark.wap.branch", BRANCH);
      spark.sql("DROP TABLE " + table);
      boolean factStillThere =
          spark.sql("SHOW TABLES IN openhouse." + DATABASE).collectAsList().stream()
              .anyMatch(r -> "fact".equals(r.getString(1)));
      assertTrue(factStillThere, "DROP TABLE under a branch must not remove the catalog object");
      assertEquals(0, count(spark, table), "DROP resets the session branch to empty");
      spark.conf().unset("spark.wap.branch");
      assertEquals(1, count(spark, table), "main is unchanged after branched DROP");

      // --- 8. RENAME, location, GRANT succeed and leave the house table / ACL unchanged ---
      spark.conf().set("spark.wap.branch", BRANCH);
      spark.sql("ALTER TABLE " + table + " RENAME TO " + DATABASE + ".renamed");
      boolean factStillNamedFact =
          spark.sql("SHOW TABLES IN openhouse." + DATABASE).collectAsList().stream()
              .anyMatch(r -> "fact".equals(r.getString(1)));
      boolean renamedAppeared =
          spark.sql("SHOW TABLES IN openhouse." + DATABASE).collectAsList().stream()
              .anyMatch(r -> "renamed".equals(r.getString(1)));
      assertTrue(factStillNamedFact, "RENAME under a branch must not rename the catalog object");
      assertFalse(renamedAppeared, "RENAME under a branch must not create the destination name");

      String locationBefore = tblProp(spark, table, "openhouse.tableLocation");
      try {
        spark.sql("ALTER TABLE " + table + " SET TBLPROPERTIES ('location'='/tmp/nope')");
      } catch (Exception e) {
        String msg = exceptionText(e).toLowerCase();
        assertFalse(
            msg.contains("refus"),
            "SET location under a branch must be ignored, not refused: " + exceptionText(e));
      }
      spark.sql("REFRESH TABLE " + table);
      assertEquals(
          locationBefore,
          tblProp(spark, table, "openhouse.tableLocation"),
          "SET location under a branch must not move the house table");
      assertFalse(
          "/tmp/nope".equals(tblProp(spark, table, "location")),
          "SET location under a branch must not overlay a fake location");

      java.util.List<String> grantsBefore = grants(spark, table);
      spark.sql("GRANT SELECT ON TABLE " + table + " TO ci_principal");
      spark.sql("REVOKE SELECT ON TABLE " + table + " FROM ci_principal");
      assertEquals(
          grantsBefore,
          grants(spark, table),
          "GRANT/REVOKE under a branch must not change prod ACL");
      spark.conf().unset("spark.wap.branch");
      spark.sql("REFRESH TABLE " + table);
      assertEquals(
          grantsBefore,
          grants(spark, table),
          "prod ACL stays unchanged after unsetting the branch");
    }
  }

  private static Set<String> describeCols(SparkSession spark, String table) {
    return spark.sql("DESCRIBE TABLE " + table).collectAsList().stream()
        .map(r -> r.getString(0))
        .collect(Collectors.toSet());
  }

  private static java.util.List<String> grants(SparkSession spark, String table) {
    return spark.sql("SHOW GRANTS ON TABLE " + table).collectAsList().stream()
        .map(Row::toString)
        .sorted()
        .collect(Collectors.toList());
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

  private static Set<String> refs(SparkSession spark, String table) {
    return spark.sql("SELECT name FROM " + table + ".refs").collectAsList().stream()
        .map(r -> r.getString(0))
        .collect(Collectors.toSet());
  }

  private static void assertRefs(SparkSession spark, String table, String... expected) {
    Set<String> actual = refs(spark, table);
    for (String name : expected) {
      assertTrue(actual.contains(name), "expected ref " + name + " in " + actual);
    }
  }

  private static String exceptionText(Throwable t) {
    StringBuilder sb = new StringBuilder();
    for (Throwable cur = t; cur != null; cur = cur.getCause()) {
      if (cur.getMessage() != null) {
        sb.append(cur.getMessage()).append(' ');
      }
    }
    return sb.toString();
  }
}
