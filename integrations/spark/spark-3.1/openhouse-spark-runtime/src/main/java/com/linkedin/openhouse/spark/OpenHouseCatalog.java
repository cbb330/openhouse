package com.linkedin.openhouse.spark;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import scala.Option;

/**
 * Catalog implementation to create, read, update and delete tables in OpenHouse. This class
 * leverages Openhouse tableclient to perform CRUD operations on Tables resource in the Catalog
 * service. This implementation provides client side catalog implementation for Iceberg tables in
 * Spark.
 *
 * <p>Catalog can be instantiated as a Iceberg catalog, with following configurations:
 * spark.sql.catalog.openhouse=org.apache.iceberg.spark.SparkCatalog
 * spark.sql.catalog.openhouse.catalog-impl=com.linkedin.openhouse.spark.OpenHouseCatalog
 * spark.sql.catalog.openhouse.metrics-reporter-impl=com.linkedin.openhouse.javaclient.OpenHouseMetricsReporter
 * spark.sql.catalog.openhouse.uri=http://[openhouse service host]:[openhouse service port]
 * spark.sql.catalog.openhouse.cluster=[openhouse cluster name]
 *
 * <p>It can be used in spark shell as follows: spark.sql("USE openhouse")
 *
 * <p>On initialization this catalog forces {@code spark.sql.caseSensitive=false} on the active
 * SparkSession. OpenHouse preserves column casing at create time and rejects case-only duplicate
 * columns server-side, so case-insensitive resolution is the contract for OH tables and matches
 * Iceberg's recommendation. Without this, a session running with {@code caseSensitive=true} fails
 * to resolve column references whose casing differs from what the table stores.
 */
@Slf4j
public class OpenHouseCatalog extends com.linkedin.openhouse.javaclient.OpenHouseCatalog {

  private static final String CASE_SENSITIVE_KEY = "spark.sql.caseSensitive";

  @Override
  public void initialize(String name, Map<String, String> properties) {
    super.initialize(name, properties);
    Option<SparkSession> active = SparkSession.getActiveSession();
    if (active.isDefined()) {
      SparkSession session = active.get();
      String previous = session.conf().get(CASE_SENSITIVE_KEY, "false");
      if (!"false".equalsIgnoreCase(previous)) {
        log.info(
            "OpenHouseCatalog[{}] setting {}=false (was {}) so column references resolve against the table's stored casing.",
            name,
            CASE_SENSITIVE_KEY,
            previous);
      }
      session.conf().set(CASE_SENSITIVE_KEY, "false");
    }
  }
}
