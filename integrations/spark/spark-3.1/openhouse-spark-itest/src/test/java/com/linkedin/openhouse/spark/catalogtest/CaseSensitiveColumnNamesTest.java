package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;

public class CaseSensitiveColumnNamesTest extends OpenHouseSparkITest {
  @Test
  public void testSparkRejectsColumnsDifferingOnlyByCase() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      AnalysisException exception =
          assertThrows(
              AnalysisException.class,
              () ->
                  spark.sql(
                      "CREATE TABLE openhouse.caseSensitiveColumns.t1 (f2 string, F2 string)"));

      String message = exception.getMessage().toLowerCase(Locale.ROOT);
      assertTrue(message.contains("f2"));
      assertTrue(message.contains("duplicate") || message.contains("already exists"));
    }
  }

  @Test
  public void testCatalogCreateTableWithColumnsDifferingOnlyByCase() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog catalog = getOpenHouseCatalog(spark);
      TableIdentifier tableIdentifier =
          TableIdentifier.of("caseSensitiveColumns", "catalog_case_test");
      Schema schema =
          new Schema(
              Types.NestedField.required(1, "f2", Types.StringType.get()),
              Types.NestedField.required(2, "F2", Types.StringType.get()));

      Table table = null;
      try {
        table = catalog.createTable(tableIdentifier, schema);
        assertEquals("f2", table.schema().columns().get(0).name());
        assertEquals("F2", table.schema().columns().get(1).name());
      } finally {
        catalog.dropTable(tableIdentifier);
      }
    }
  }

  @Test
  public void testCaseSensitiveQueryResolvesColumnsDifferingOnlyByCase() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      spark.conf().set("spark.sql.caseSensitive", "true");
      assertEquals("true", spark.conf().get("spark.sql.caseSensitive"));

      Catalog catalog = getOpenHouseCatalog(spark);
      String databaseName = "case_sensitive_columns";
      String tableName = "query_case_test";
      String fullyQualifiedTableName = "openhouse." + databaseName + "." + tableName;
      TableIdentifier tableIdentifier = TableIdentifier.of(databaseName, tableName);
      Schema schema =
          new Schema(
              Types.NestedField.required(1, "f2", Types.StringType.get()),
              Types.NestedField.required(2, "F2", Types.StringType.get()));

      boolean tableCreated = false;
      try {
        catalog.createTable(tableIdentifier, schema);
        tableCreated = true;

        Dataset<Row> queryResult = spark.sql("SELECT f2, F2 FROM " + fullyQualifiedTableName);
        assertEquals("f2", queryResult.schema().fields()[0].name());
        assertEquals("F2", queryResult.schema().fields()[1].name());
        assertTrue(queryResult.collectAsList().isEmpty());
      } finally {
        if (tableCreated) {
          catalog.dropTable(tableIdentifier);
        }
      }
    }
  }

  private Catalog getOpenHouseCatalog(SparkSession spark) {
    final Map<String, String> catalogProperties = new HashMap<>();
    final String catalogPropertyPrefix = "spark.sql.catalog.openhouse.";
    final Map<String, String> sparkProperties = JavaConverters.mapAsJavaMap(spark.conf().getAll());
    for (Map.Entry<String, String> entry : sparkProperties.entrySet()) {
      if (entry.getKey().startsWith(catalogPropertyPrefix)) {
        catalogProperties.put(
            entry.getKey().substring(catalogPropertyPrefix.length()), entry.getValue());
      }
    }
    return CatalogUtil.loadCatalog(
        sparkProperties.get("spark.sql.catalog.openhouse.catalog-impl"),
        "openhouse",
        catalogProperties,
        spark.sparkContext().hadoopConfiguration());
  }
}
