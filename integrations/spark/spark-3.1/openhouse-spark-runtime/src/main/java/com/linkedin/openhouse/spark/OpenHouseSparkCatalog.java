package com.linkedin.openhouse.spark;

import org.apache.iceberg.spark.SparkCatalog;

/**
 * Iceberg Spark catalog name jobs should use. Spark 3.1 does not stamp isolate; that lives on Spark
 * 3.5 {@code OpenHouseSparkCatalog}.
 *
 * <p>Configure: {@code
 * spark.sql.catalog.openhouse=com.linkedin.openhouse.spark.OpenHouseSparkCatalog}
 */
public class OpenHouseSparkCatalog extends SparkCatalog {}
