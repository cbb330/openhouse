package com.linkedin.openhouse.catalog.test;

import com.linkedin.openhouse.tablestest.OpenHouseLocalServer;
import com.linkedin.openhouse.tablestest.TestSparkSessionUtil;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.spark.TestCatalogProvider;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;

/** Provides OpenHouse catalog configuration to Iceberg tests. Registered via ServiceLoader. */
public class OpenHouseCatalogProvider implements TestCatalogProvider {

  private static OpenHouseLocalServer server;
  private static SparkSession spark;

  @Override
  public Object[][] getCatalogConfigurations() {
    Map<String, String> catalogConfig = new HashMap<>();

    // Extract config from Spark session (similar to CatalogOperationTest)
    String catalogPropertyPrefix = "spark.sql.catalog.openhouse.";
    Map<String, String> sparkProps = JavaConverters.mapAsJavaMap(spark.conf().getAll());

    for (Map.Entry<String, String> entry : sparkProps.entrySet()) {
      if (entry.getKey().startsWith(catalogPropertyPrefix)) {
        catalogConfig.put(
            entry.getKey().substring(catalogPropertyPrefix.length()), entry.getValue());
      }
    }

    return new Object[][] {
      {"openhouse", sparkProps.get("spark.sql.catalog.openhouse"), catalogConfig}
    };
  }

  @Override
  public void beforeAll() throws Exception {
    if (server == null) {
      server = new OpenHouseLocalServer();
      server.start();

      // Use OpenHouseSparkITest infrastructure
      SparkSession.Builder builder = TestSparkSessionUtil.getBaseBuilder(URI.create("file:///"));
      TestSparkSessionUtil.configureCatalogs(
          builder, "openhouse", URI.create("http://localhost:" + server.getPort()));
      spark = TestSparkSessionUtil.createSparkSession(builder);
    }
  }

  @Override
  public void afterAll() throws Exception {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
    if (server != null) {
      server.stop();
      server = null;
    }
  }

  /** Get the Spark session for use by other providers. */
  public static SparkSession getSparkSession() {
    return spark;
  }

  /** Get the server port for use by other providers. */
  public static int getPort() {
    return server != null ? server.getPort() : -1;
  }
}
