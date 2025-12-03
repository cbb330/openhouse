package org.apache.iceberg.spark.openhouse;

import java.util.Map;
import org.apache.iceberg.spark.SparkCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenHouseSparkCatalogShim extends SparkCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(OpenHouseSparkCatalogShim.class);

  public OpenHouseSparkCatalogShim() {
    super();
    System.out.println("DEBUG: OpenHouseSparkCatalogShim: Constructor called");
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata) {
    // No-op: OpenHouse doesn't support createNamespace, but tests require it to succeed (or at
    // least not fail)
    // for the 'default' namespace setup.
    LOG.info("OpenHouseSparkCatalogShim: Ignoring createNamespace request");
    System.out.println("OpenHouseSparkCatalogShim: Ignoring createNamespace request");
  }
}
