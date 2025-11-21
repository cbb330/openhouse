package com.linkedin.openhouse.catalog.test;

import com.linkedin.openhouse.tablestest.OpenHouseLocalServer;
import com.linkedin.openhouse.tablestest.TestSparkSessionUtil;
import java.net.URI;
import java.util.Map;
import org.apache.iceberg.spark.TestCatalogProvider;
import org.apache.spark.sql.SparkSession;

/**
 * Provides OpenHouse catalog configuration to Iceberg's parameterized tests via ServiceLoader SPI.
 *
 * <p>This allows Iceberg's existing test suites (e.g., WAP tests) to automatically run against the
 * OpenHouse catalog implementation without modifying Iceberg's test code.
 */
public class OpenHouseCatalogProvider implements TestCatalogProvider {

  private static OpenHouseLocalServer server;

  @Override
  public void beforeAll() throws Exception {
    if (server == null) {
      server = new OpenHouseLocalServer();
      server.start();
    }
  }

  @Override
  public Object[][] getCatalogConfigurations() {
    String uri = "http://localhost:" + server.getPort();
    return new Object[][] {
      {
        "openhouse",
        "org.apache.iceberg.spark.SparkCatalog",
        Map.of(
            "catalog-impl", "com.linkedin.openhouse.spark.OpenHouseCatalog",
            "uri", uri,
            "cluster", "local-cluster",
            "client-name", "openhouse-zero-copy-tests")
      }
    };
  }

  @Override
  public void afterAll() throws Exception {
    if (server != null) {
      server.stop();
      server = null;
    }
  }
}

