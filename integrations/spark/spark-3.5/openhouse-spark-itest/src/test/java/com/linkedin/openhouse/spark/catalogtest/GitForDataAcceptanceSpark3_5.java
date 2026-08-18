package com.linkedin.openhouse.spark.catalogtest;

import static com.linkedin.openhouse.spark.catalogtest.GitForDataAcceptanceSpark3_5.Expect.BRANCH;
import static com.linkedin.openhouse.spark.catalogtest.GitForDataAcceptanceSpark3_5.Expect.DROP;
import static com.linkedin.openhouse.spark.catalogtest.GitForDataAcceptanceSpark3_5.Expect.FREEZE;
import static com.linkedin.openhouse.spark.catalogtest.GitForDataAcceptanceSpark3_5.Expect.PROD;
import static com.linkedin.openhouse.spark.catalogtest.GitForDataAcceptanceSpark3_5.Expect.THROW;
import static com.linkedin.openhouse.spark.catalogtest.GitForDataAcceptanceSpark3_5.Setup.MISSING_REF;
import static com.linkedin.openhouse.spark.catalogtest.GitForDataAcceptanceSpark3_5.Setup.NONE;
import static com.linkedin.openhouse.spark.catalogtest.GitForDataAcceptanceSpark3_5.Setup.NO_BRANCH;
import static com.linkedin.openhouse.spark.catalogtest.GitForDataAcceptanceSpark3_5.Setup.NO_WAP;
import static com.linkedin.openhouse.spark.catalogtest.GitForDataAcceptanceSpark3_5.Setup.TABLE;
import static com.linkedin.openhouse.spark.catalogtest.GitForDataAcceptanceSpark3_5.Setup.WITH_GRANT;
import static com.linkedin.openhouse.spark.catalogtest.GitForDataAcceptanceSpark3_5.Setup.WITH_REPLICATION;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.stream.Stream;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Acceptance matrix for {@code git-for-data-ddl.md}. Rows are the verbs; columns are milestones.
 *
 * <p>{@link #CURRENT} is the shipped milestone and the only column asserted. Flip it when M2/M3
 * land. DDL strings stay fixed.
 *
 * <pre>
 *   BRANCH  prod unchanged; after reload the branch still shows the change
 *   DROP    prod unchanged; after reload the branch does not show it either
 *   PROD    change is on the house table, so main and the branch both show it
 *   THROW   the statement errors; nothing is committed
 *   FREEZE  first read copied prod; later prod rows do not show up on that branch
 * </pre>
 */
@Execution(ExecutionMode.SAME_THREAD)
public class GitForDataAcceptanceSpark3_5 extends OpenHouseSparkITest {

  static final Milestone CURRENT = Milestone.M1;
  static final String DB = "gfd_accept";
  static final String REF = "ci";

  enum Milestone {
    M0,
    M1,
    M2,
    M3
  }

  enum Expect {
    BRANCH,
    DROP,
    PROD,
    THROW,
    FREEZE
  }

  enum Setup {
    TABLE,
    WITH_GRANT,
    WITH_REPLICATION,
    NO_BRANCH,
    NO_WAP,
    MISSING_REF,
    NONE
  }

  @FunctionalInterface
  interface Act {
    void run(SparkSession spark, String table) throws Exception;
  }

  @FunctionalInterface
  interface Seen {
    boolean test(SparkSession spark, String table);
  }

  private SparkSession spark;

  /**
   * verb / ddl / M0 / M1 / M2 / M3
   *
   * <p>M2 isolation matches M1 (M2 is “run the job”). M3 is clone metadata: schema/props/policy
   * stick on the branch after reload.
   */
  static Stream<Arguments> ddl() {
    return Stream.of(
        // Snapshot DML
        c(
            "INSERT",
            TABLE,
            sql("INSERT INTO {t} VALUES (2, 'b', '2026-01-02')"),
            data("b"),
            BRANCH,
            BRANCH,
            BRANCH,
            BRANCH),
        c(
            "writeTo.append",
            TABLE,
            append("2", "b", "2026-01-02"),
            data("b"),
            BRANCH,
            BRANCH,
            BRANCH,
            BRANCH),
        c("overwritePartitions", TABLE, overwritePart(), data("p"), BRANCH, BRANCH, BRANCH, BRANCH),
        c("overwriteAll", TABLE, overwriteAll(), data("all"), BRANCH, BRANCH, BRANCH, BRANCH),
        c(
            "DELETE",
            TABLE,
            sql("DELETE FROM {t} WHERE id = 1"),
            empty(),
            BRANCH,
            BRANCH,
            BRANCH,
            BRANCH),
        c(
            "UPDATE",
            TABLE,
            sql("UPDATE {t} SET data = 'u' WHERE id = 1"),
            data("u"),
            BRANCH,
            BRANCH,
            BRANCH,
            BRANCH),
        c(
            "MERGE",
            TABLE,
            sql(
                "MERGE INTO {t} t USING (SELECT 1 id, 'm' data, '2026-01-01' datepartition) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.data = 'm'"),
            data("m"),
            BRANCH,
            BRANCH,
            BRANCH,
            BRANCH),

        // Missing ref
        c(
            "missing-ref create",
            NO_BRANCH,
            setBranch("missing"),
            hasRef("missing"),
            PROD,
            PROD,
            PROD,
            PROD),
        c(
            "missing-ref freeze",
            MISSING_REF,
            laterMain(),
            data("later_main"),
            FREEZE,
            FREEZE,
            FREEZE,
            FREEZE),

        // Schema / user props / policy — overlay on the ref is M3; M1 drops the definition
        c(
            "ADD COLUMN",
            TABLE,
            sql("ALTER TABLE {t} ADD COLUMNS (extra string)"),
            col("extra"),
            PROD,
            DROP,
            DROP,
            BRANCH),
        c("mergeSchema", TABLE, mergeExtra(), col("extra"), PROD, THROW, THROW, BRANCH),
        c(
            "SET gfd.marker",
            TABLE,
            sql("ALTER TABLE {t} SET TBLPROPERTIES ('gfd.marker'='branched')"),
            prop("gfd.marker", "branched"),
            PROD,
            DROP,
            DROP,
            BRANCH),
        c(
            "SET avro.schema.literal",
            TABLE,
            sql(
                "ALTER TABLE {t} SET TBLPROPERTIES ('avro.schema.literal'='{\"type\":\"record\",\"name\":\"n\",\"fields\":[]}')"),
            propHas("avro.schema.literal", "record"),
            PROD,
            DROP,
            DROP,
            BRANCH),
        c(
            "SET accept-any-schema",
            TABLE,
            sql("ALTER TABLE {t} SET TBLPROPERTIES ('write.spark.accept-any-schema'='true')"),
            prop("write.spark.accept-any-schema", "true"),
            PROD,
            DROP,
            DROP,
            BRANCH),
        c(
            "UNSET TBLPROPERTIES",
            TABLE,
            sql("ALTER TABLE {t} UNSET TBLPROPERTIES ('gfd.keep')"),
            missing("gfd.keep"),
            PROD,
            DROP,
            DROP,
            BRANCH),
        c(
            "SET POLICY retention",
            TABLE,
            sql("ALTER TABLE {t} SET POLICY (RETENTION = 180d ON COLUMN datepartition)"),
            policy("180"),
            PROD,
            DROP,
            DROP,
            BRANCH),
        c(
            "UNSET POLICY replication",
            WITH_REPLICATION,
            sql("ALTER TABLE {t} UNSET POLICY (REPLICATION)"),
            gone(policy("WAR")),
            PROD,
            DROP,
            DROP,
            BRANCH),
        c(
            "MODIFY COLUMN SET TAG",
            TABLE,
            sql("ALTER TABLE {t} MODIFY COLUMN data SET TAG = (PII)"),
            policy("PII"),
            PROD,
            DROP,
            DROP,
            BRANCH),
        c(
            "SET POLICY sharing",
            TABLE,
            sql("ALTER TABLE {t} SET POLICY (SHARING=TRUE)"),
            policy("true"),
            PROD,
            DROP,
            DROP,
            BRANCH),

        // Identity — ignore, do not overlay. Spark 3.5 rejects SET TBLPROPERTIES('location').
        c(
            "SET LOCATION",
            TABLE,
            sql("ALTER TABLE {t} SET LOCATION '/tmp/gfd-nope'"),
            loc("/tmp/gfd-nope"),
            DROP,
            DROP,
            DROP,
            DROP),
        c("LOCK", TABLE, lock(), policy("lock"), DROP, DROP, DROP, DROP),

        // CREATE / DROP / RENAME / GRANT
        c("CREATE missing", NONE, createAndInsert(), data("b"), BRANCH, BRANCH, BRANCH, BRANCH),
        c("CREATE existing", NO_BRANCH, createExisting(), hasRef(REF), PROD, PROD, PROD, PROD),
        c("DROP TABLE", TABLE, sql("DROP TABLE {t}"), empty(), BRANCH, BRANCH, BRANCH, BRANCH),
        c(
            "DROP TABLE PURGE",
            TABLE,
            sql("DROP TABLE {t} PURGE"),
            empty(),
            BRANCH,
            BRANCH,
            BRANCH,
            BRANCH),
        c(
            "RENAME",
            TABLE,
            sql("ALTER TABLE {t} RENAME TO " + DB + ".renamed"),
            exists(DB + ".renamed"),
            DROP,
            DROP,
            DROP,
            DROP),
        c(
            "GRANT",
            TABLE,
            sql("GRANT SELECT ON TABLE {t} TO ci_principal"),
            grant("ci_principal"),
            DROP,
            DROP,
            DROP,
            DROP),
        c(
            "REVOKE",
            WITH_GRANT,
            sql("REVOKE SELECT ON TABLE {t} FROM ci_principal"),
            grant("ci_principal"),
            DROP,
            DROP,
            DROP,
            DROP),
        c(
            "GRANT ON DATABASE",
            TABLE,
            sql("GRANT CREATE TABLE ON DATABASE openhouse." + DB + " TO ci_db"),
            dbGrant("ci_db"),
            DROP,
            DROP,
            DROP,
            DROP),

        // WAP must land on the table Iceberg actually scans
        c(
            "write.wap.enabled",
            NO_WAP,
            setBranch(REF),
            prop("write.wap.enabled", "true"),
            PROD,
            PROD,
            PROD,
            PROD),

        // Already unsupported — ignore, do not refuse
        c(
            "DROP COLUMN",
            TABLE,
            sql("ALTER TABLE {t} DROP COLUMN data"),
            goneCol("data"),
            DROP,
            DROP,
            DROP,
            DROP),
        c(
            "RENAME COLUMN",
            TABLE,
            sql("ALTER TABLE {t} RENAME COLUMN data TO payload"),
            col("payload"),
            DROP,
            DROP,
            DROP,
            DROP),
        c(
            "ADD PARTITION FIELD",
            TABLE,
            sql("ALTER TABLE {t} ADD PARTITION FIELD bucket(4, id)"),
            spec("bucket"),
            DROP,
            DROP,
            DROP,
            DROP));
  }

  @BeforeEach
  public void openSession() throws Exception {
    spark = getSparkSession();
    spark.conf().unset("spark.wap.id");
    spark.conf().unset("spark.wap.branch");
  }

  @AfterEach
  public void cleanup() {
    spark.conf().unset("spark.wap.id");
    spark.conf().unset("spark.wap.branch");
    try {
      for (Row table : spark.sql("SHOW TABLES IN openhouse." + DB).collectAsList()) {
        spark.sql("DROP TABLE IF EXISTS openhouse." + DB + "." + table.getString(1));
      }
    } catch (Exception ignored) {
      // database may not exist
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("ddl")
  public void ddl(
      String verb, Setup setup, Act act, Seen seen, Expect m0, Expect m1, Expect m2, Expect m3)
      throws Exception {
    Expect expect = new Expect[] {m0, m1, m2, m3}[CURRENT.ordinal()];
    String table = "openhouse." + DB + "." + slug(verb);
    prepare(setup, table);

    Exception err = null;
    try {
      act.run(spark, table);
    } catch (Exception e) {
      err = e;
    }

    boolean onProd = seenOn(seen, table, null);
    boolean onBranch = seenOn(seen, table, branchFor(setup));
    String detail = verb + " " + CURRENT + "=" + expect + " err=" + message(err);

    switch (expect) {
      case THROW:
        assertTrue(isAnalysis(err), detail);
        assertFalse(onProd, detail);
        assertFalse(onBranch, detail);
        break;
      case BRANCH:
        assertNull(err, detail);
        assertFalse(onProd, detail);
        assertTrue(onBranch, detail);
        break;
      case DROP:
        assertNull(err, detail);
        assertFalse(onProd, detail);
        assertFalse(onBranch, detail);
        break;
      case PROD:
        assertNull(err, detail);
        assertTrue(onProd, detail);
        break;
      case FREEZE:
        assertNull(err, detail);
        assertTrue(onProd, detail);
        assertFalse(onBranch, detail);
        assertTrue(
            seenOn(data("seed"), table, branchFor(setup)),
            detail + " frozen branch still has seed");
        break;
      default:
        throw new IllegalStateException(detail);
    }
  }

  private void prepare(Setup setup, String table) {
    spark.conf().unset("spark.wap.branch");
    spark.sql("DROP TABLE IF EXISTS " + table);
    if (setup == NONE) {
      return;
    }
    spark.sql(
        "CREATE TABLE "
            + table
            + " (id int, data string, datepartition string) PARTITIONED BY (datepartition)");
    if (setup != NO_WAP) {
      spark.sql("ALTER TABLE " + table + " SET TBLPROPERTIES ('write.wap.enabled'='true')");
    }
    spark.sql("ALTER TABLE " + table + " SET TBLPROPERTIES ('gfd.keep'='main')");
    spark.sql("INSERT INTO " + table + " VALUES (1, 'seed', '2026-01-01')");
    if (setup == WITH_GRANT) {
      // OPA is unset in this itest, so SHOW GRANTS stays empty. Sharing + GRANT on main
      // still has to succeed (not 400 "not a shared table") before the branched REVOKE.
      spark.sql("ALTER TABLE " + table + " SET POLICY (SHARING=TRUE)");
      spark.sql("GRANT SELECT ON TABLE " + table + " TO ci_principal");
    }
    if (setup == WITH_REPLICATION) {
      spark.sql(
          "ALTER TABLE " + table + " SET POLICY (REPLICATION=({destination:'WAR', interval:12h}))");
      assertTrue(
          policy("WAR").test(spark, table), "seed replication must land on main before UNSET");
    }
    if (setup == TABLE || setup == WITH_GRANT || setup == WITH_REPLICATION) {
      spark.sql("ALTER TABLE " + table + " CREATE BRANCH " + REF);
      spark.conf().set("spark.wap.branch", REF);
      spark.sql("REFRESH TABLE " + table);
    } else if (setup == MISSING_REF) {
      spark.conf().set("spark.wap.branch", "missing");
      spark.sql("REFRESH TABLE " + table);
    }
  }

  private boolean seenOn(Seen seen, String table, String branch) {
    if (branch == null) {
      spark.conf().unset("spark.wap.branch");
    } else {
      spark.conf().set("spark.wap.branch", branch);
    }
    spark.sql("REFRESH TABLE " + table);
    return seen.test(spark, table);
  }

  private static String branchFor(Setup setup) {
    if (setup == MISSING_REF) {
      return "missing";
    }
    if (setup == NO_WAP) {
      return REF;
    }
    return REF;
  }

  private static Arguments c(
      String verb, Setup setup, Act act, Seen seen, Expect m0, Expect m1, Expect m2, Expect m3) {
    return Arguments.of(verb, setup, act, seen, m0, m1, m2, m3);
  }

  private static Act sql(String template) {
    return (spark, table) -> spark.sql(template.replace("{t}", table));
  }

  private static Act append(String id, String data, String part) {
    return (spark, table) ->
        spark
            .sql("SELECT " + id + " id, '" + data + "' data, '" + part + "' datepartition")
            .writeTo(table)
            .append();
  }

  private static Act overwritePart() {
    return (spark, table) ->
        spark
            .sql("SELECT 1 id, 'p' data, '2026-01-01' datepartition")
            .writeTo(table)
            .overwritePartitions();
  }

  private static Act overwriteAll() {
    return (spark, table) ->
        spark
            .sql("SELECT 9 id, 'all' data, '2026-01-09' datepartition")
            .writeTo(table)
            .overwrite(functions.lit(true));
  }

  private static Act mergeExtra() {
    return (spark, table) ->
        spark
            .sql("SELECT 3 id, 'x' data, 'e' extra, '2026-01-03' datepartition")
            .writeTo(table)
            .option("mergeSchema", "true")
            .append();
  }

  private static Act setBranch(String branch) {
    return (spark, table) -> {
      spark.conf().set("spark.wap.branch", branch);
      spark.sql("REFRESH TABLE " + table);
    };
  }

  private static Act laterMain() {
    return (spark, table) -> {
      spark.conf().unset("spark.wap.branch");
      spark.sql("REFRESH TABLE " + table);
      spark.sql("INSERT INTO " + table + " VALUES (99, 'later_main', '2026-02-01')");
    };
  }

  private static Act createExisting() {
    return (spark, table) -> {
      spark.conf().set("spark.wap.branch", REF);
      String name = table.substring(table.lastIndexOf('.') + 1);
      Identifier ident = Identifier.of(new String[] {DB}, name);
      TableCatalog catalog =
          (TableCatalog) spark.sessionState().catalogManager().catalog("openhouse");
      StructType schema =
          new StructType()
              .add("id", DataTypes.IntegerType)
              .add("data", DataTypes.StringType)
              .add("datepartition", DataTypes.StringType);
      catalog.createTable(ident, schema, new Transform[0], Collections.emptyMap());
    };
  }

  private static Act createAndInsert() {
    return (spark, table) -> {
      spark.conf().set("spark.wap.branch", REF);
      spark.sql(
          "CREATE TABLE "
              + table
              + " (id int, data string, datepartition string) PARTITIONED BY (datepartition)");
      spark.sql("INSERT INTO " + table + " VALUES (2, 'b', '2026-01-02')");
    };
  }

  private static Act lock() {
    return (spark, table) -> {
      String[] parts = table.substring("openhouse.".length()).split("\\.");
      URI uri =
          URI.create(
              spark.conf().get("spark.sql.catalog.openhouse.uri")
                  + "/v1/databases/"
                  + parts[0]
                  + "/tables/"
                  + parts[1]
                  + "/lock");
      HttpRequest request =
          HttpRequest.newBuilder(uri)
              .header("Content-Type", "application/json")
              .header(
                  "Authorization",
                  "Bearer " + spark.conf().get("spark.sql.catalog.openhouse.auth-token"))
              .header("X-OH-Wap-Branch", REF)
              .POST(HttpRequest.BodyPublishers.ofString("{\"locked\":true,\"message\":\"ci\"}"))
              .build();
      HttpResponse<Void> response =
          HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.discarding());
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new IllegalStateException("LOCK HTTP " + response.statusCode());
      }
    };
  }

  private static Seen gone(Seen seen) {
    return (spark, table) -> !seen.test(spark, table);
  }

  private static Seen data(String value) {
    return (spark, table) ->
        spark.sql("SELECT data FROM " + table).collectAsList().stream()
            .anyMatch(r -> value.equals(r.getString(0)));
  }

  private static Seen empty() {
    return (spark, table) -> {
      try {
        return spark.sql("SELECT * FROM " + table).collectAsList().isEmpty();
      } catch (RuntimeException e) {
        if (isMissing(e)) {
          return true;
        }
        throw e;
      }
    };
  }

  private static Seen col(String name) {
    return (spark, table) ->
        spark.sql("DESCRIBE TABLE " + table).collectAsList().stream()
            .anyMatch(r -> name.equals(r.getString(0)));
  }

  private static Seen goneCol(String name) {
    return (spark, table) -> !col(name).test(spark, table);
  }

  private static Seen prop(String key, String value) {
    return (spark, table) -> value.equals(tblProp(spark, table, key));
  }

  private static Seen propHas(String key, String substr) {
    return (spark, table) -> tblProp(spark, table, key).contains(substr);
  }

  private static Seen missing(String key) {
    return (spark, table) -> tblProp(spark, table, key).isEmpty();
  }

  private static Seen policy(String substr) {
    return (spark, table) ->
        (tblProp(spark, table, "policies") + tblProp(spark, table, "updated.openhouse.policy"))
            .toLowerCase()
            .contains(substr.toLowerCase());
  }

  private static Seen loc(String value) {
    return (spark, table) ->
        value.equals(tblProp(spark, table, "location"))
            || tblProp(spark, table, "openhouse.tableLocation").contains(value);
  }

  private static Seen hasRef(String name) {
    return (spark, table) ->
        spark.sql("SELECT name FROM " + table + ".refs").collectAsList().stream()
            .anyMatch(r -> name.equals(r.getString(0)));
  }

  private static Seen exists(String fqtn) {
    return (spark, table) -> {
      String[] parts = fqtn.split("\\.");
      String db = parts.length == 2 ? parts[0] : parts[1];
      String name = parts.length == 2 ? parts[1] : parts[2];
      return spark.sql("SHOW TABLES IN openhouse." + db).collectAsList().stream()
          .anyMatch(r -> name.equals(r.getString(1)));
    };
  }

  private static Seen grant(String principal) {
    return (spark, table) ->
        spark.sql("SHOW GRANTS ON TABLE " + table).collectAsList().stream()
            .anyMatch(r -> r.toString().contains(principal));
  }

  private static Seen dbGrant(String principal) {
    return (spark, table) ->
        spark.sql("SHOW GRANTS ON DATABASE openhouse." + DB).collectAsList().stream()
            .anyMatch(r -> r.toString().contains(principal));
  }

  private static Seen spec(String token) {
    return (spark, table) ->
        spark
            .sql("DESCRIBE TABLE EXTENDED " + table)
            .collectAsList()
            .toString()
            .toLowerCase()
            .contains(token.toLowerCase());
  }

  private static String tblProp(SparkSession spark, String table, String key) {
    return spark.sql("SHOW TBLPROPERTIES " + table).collectAsList().stream()
        .filter(r -> key.equals(r.getString(0)))
        .map(r -> r.getString(1))
        .findFirst()
        .orElse("");
  }

  private static String slug(String verb) {
    return verb.toLowerCase().replaceAll("[^a-z0-9]+", "_");
  }

  private static boolean isMissing(Throwable e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof NotFoundException || t instanceof FileNotFoundException) {
        return true;
      }
      String m = t.getMessage();
      if (m != null && m.contains("TABLE_OR_VIEW_NOT_FOUND")) {
        return true;
      }
    }
    return false;
  }

  private static boolean isAnalysis(Exception e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof AnalysisException) {
        String m = t.getMessage() == null ? "" : t.getMessage();
        return m.contains("extra") || m.contains("TOO_MANY_DATA_COLUMNS");
      }
    }
    return false;
  }

  private static String message(Exception e) {
    if (e == null) {
      return "none";
    }
    String m = e.getMessage();
    return e.getClass().getSimpleName() + ":" + (m == null ? "" : m.replace('\n', ' '));
  }
}
