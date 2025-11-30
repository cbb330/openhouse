package com.linkedin.openhouse.tablestest;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.SparkSession;

public class OpenHouseSparkITestShim extends OpenHouseSparkITest {

  public OpenHouseSparkITestShim() {
    super();
  }

  @Override
  protected synchronized SparkSession getSparkSession() throws Exception {
    SparkSession session = super.getSparkSession();

    // Force configuration on the active session
    session
        .conf()
        .set(
            "spark.sql.catalog.openhouse",
            "org.apache.iceberg.spark.openhouse.OpenHouseSparkCatalogShim");
    session
        .conf()
        .set(
            "spark.sql.catalog.openhouse.catalog-impl",
            "org.apache.iceberg.spark.openhouse.OpenHouseShimCatalog");

    // Unset spark_catalog catalog-impl to prevent it from using OpenHouse defaults
    session.conf().unset("spark.sql.catalog.spark_catalog");
    session.conf().unset("spark.sql.catalog.spark_catalog.catalog-impl");
    session.conf().unset("spark.sql.catalog.spark_catalog.uri");
    session.conf().unset("spark.sql.catalog.spark_catalog.cluster");

    session.conf().unset("spark.sql.catalog.testhadoop.catalog-impl");
    session.conf().unset("spark.sql.catalog.testhive.catalog-impl");
    session.conf().unset("spark.sql.catalog.hive.catalog-impl");

    try {
      Object sessionState = session.sessionState();
      Object catalogManager =
          sessionState.getClass().getMethod("catalogManager").invoke(sessionState);
      Field catalogsField = catalogManager.getClass().getDeclaredField("catalogs");
      catalogsField.setAccessible(true);
      Map<String, Object> catalogs = (Map<String, Object>) catalogsField.get(catalogManager);

      // 1. Clear existing entries to be safe
      if (catalogs.containsKey("openhouse")) {
        catalogs.remove("openhouse");
      }
      if (catalogs.containsKey("spark_catalog")) {
        catalogs.remove("spark_catalog");
      }
      if (catalogs.containsKey("testhadoop")) {
        catalogs.remove("testhadoop");
      }

      // 2. Manually inject OpenHouseShimCatalog instance wrapped in SparkCatalog
      Class<?> sparkCatalogClass =
          Class.forName("org.apache.iceberg.spark.openhouse.OpenHouseSparkCatalogShim");
      Object sparkCatalog = sparkCatalogClass.getDeclaredConstructor().newInstance();

      // Initialize SparkCatalog
      Method initMethod =
          sparkCatalogClass.getMethod(
              "initialize",
              String.class,
              Class.forName("org.apache.spark.sql.util.CaseInsensitiveStringMap"));

      Map<String, String> props = new HashMap<>();
      props.put("uri", getOpenHouseLocalServerURI().toString());
      props.put("cluster", "local-cluster");
      props.put("catalog-impl", "org.apache.iceberg.spark.openhouse.OpenHouseShimCatalog");

      Class<?> mapClass = Class.forName("org.apache.spark.sql.util.CaseInsensitiveStringMap");
      Object options = mapClass.getConstructor(Map.class).newInstance(props);

      initMethod.invoke(sparkCatalog, "openhouse", options);

      catalogs.put("openhouse", sparkCatalog);
      // Also put it as spark_catalog? Maybe not safe if they expect different instances.
      // But clearing it forces reload using the new config.

    } catch (Exception e) {
      e.printStackTrace();
    }

    return session;
  }

  @Override
  protected SparkSession.Builder getBuilder() throws Exception {
    SparkSession.Builder builder = super.getBuilder();
    builder.config(
        "spark.sql.catalog.openhouse",
        "org.apache.iceberg.spark.openhouse.OpenHouseSparkCatalogShim");
    builder.config(
        "spark.sql.catalog.openhouse.catalog-impl",
        "org.apache.iceberg.spark.openhouse.OpenHouseShimCatalog");
    builder.config("spark.sql.catalog.testhadoop.catalog-impl", "");
    builder.config("spark.sql.catalog.testhive.catalog-impl", "");
    builder.config("spark.sql.catalog.hive.catalog-impl", "");
    builder.config("spark.sql.catalog.spark_catalog", "");
    builder.config("spark.sql.catalog.spark_catalog.catalog-impl", "");
    builder.config("spark.sql.catalog.spark_catalog.uri", "");
    builder.config("spark.sql.catalog.spark_catalog.cluster", "");
    return builder;
  }
}
