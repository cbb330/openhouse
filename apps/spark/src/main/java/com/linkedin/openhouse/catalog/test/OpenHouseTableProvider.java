package com.linkedin.openhouse.catalog.test;

import com.linkedin.openhouse.jobs.spark.Operations;
import java.io.File;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestTableProvider;
import org.apache.iceberg.TestTables;
import org.apache.spark.sql.SparkSession;

/** Provides OpenHouse table operations to Iceberg core tests. Registered via ServiceLoader. */
public class OpenHouseTableProvider implements TestTableProvider {

  private static SparkSession spark;

  @Override
  public TestTables.TestTable createTable(
      File dir, String name, Schema schema, PartitionSpec spec, int formatVersion) {

    // Create table via OpenHouse catalog
    String tableName = "test_db." + name;
    spark.sql("CREATE DATABASE IF NOT EXISTS openhouse.test_db");
    spark.sql(String.format("CREATE TABLE IF NOT EXISTS openhouse.%s USING iceberg", tableName));

    // Load via OpenHouse
    Operations ops = Operations.withCatalog(spark, null);
    Table table = ops.getTable(tableName);

    // Return wrapper
    return new OpenHouseTestTableWrapper(table, name);
  }

  @Override
  public void beforeAll() throws Exception {
    // Reuse server from OpenHouseCatalogProvider
    spark = OpenHouseCatalogProvider.getSparkSession();
  }
}
