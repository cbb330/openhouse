package com.linkedin.openhouse.tablestest;

import org.apache.spark.sql.SparkSession;

public class OpenHouseSparkITestShim extends OpenHouseSparkITest {
  public static volatile String hiveMetastoreUri;

  public OpenHouseSparkITestShim() {
    super();
  }

  @Override
  protected synchronized SparkSession getSparkSession() throws Exception {
    SparkSession session = super.getSparkSession();

    // Force configuration on the active session to point testhive and testhadoop to
    // OpenHouseShimCatalog
    // This ensures that tests running against these catalogs (which are defaults in Iceberg tests)
    // are actually testing OpenHouse compatibility.
    session
        .conf()
        .set(
            "spark.sql.catalog.openhouse.catalog-impl",
            "org.apache.iceberg.spark.openhouse.OpenHouseShimCatalog");
    session
        .conf()
        .set(
            "spark.sql.catalog.testhive.catalog-impl",
            "org.apache.iceberg.spark.openhouse.OpenHouseShimCatalog");
    session
        .conf()
        .set(
            "spark.sql.catalog.testhadoop.catalog-impl",
            "org.apache.iceberg.spark.openhouse.OpenHouseShimCatalog");
    session
        .conf()
        .set(
            "spark.sql.catalog.spark_catalog.catalog-impl",
            "org.apache.iceberg.spark.openhouse.OpenHouseShimCatalog");

    // Also set explicit implementations if set by base class
    session.conf().unset("spark.sql.catalog.openhouse");
    session.conf().unset("spark.sql.catalog.spark_catalog");
    session.conf().unset("spark.sql.catalog.testhive");
    session.conf().unset("spark.sql.catalog.testhadoop");

    return session;
  }

  @Override
  protected SparkSession.Builder getBuilder() throws Exception {
    System.out.println("DEBUG: Shim getBuilder called. Hive URI: " + hiveMetastoreUri);
    SparkSession.Builder builder = super.getBuilder();
    try {
      Class.forName("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
      builder.config(
          "spark.sql.extensions",
          "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
    } catch (ClassNotFoundException e) {
      // ignore
    }
    try {
      Class.forName("org.apache.spark.sql.hive.HiveSessionStateBuilder");
      builder.enableHiveSupport();
    } catch (ClassNotFoundException e) {
      // ignore
    }
    builder.config(
        "spark.sql.catalog.openhouse",
        "org.apache.iceberg.spark.openhouse.OpenHouseSparkCatalogShim");
    builder.config(
        "spark.sql.catalog.openhouse.catalog-impl",
        "org.apache.iceberg.spark.openhouse.OpenHouseShimCatalog");

    if (hiveMetastoreUri != null) {
      builder.config("spark.hadoop.hive.metastore.uris", hiveMetastoreUri);
    }
    builder.config("spark.sql.hive.metastorePartitionPruningFallbackOnException", "true");
    return builder;
  }
}
