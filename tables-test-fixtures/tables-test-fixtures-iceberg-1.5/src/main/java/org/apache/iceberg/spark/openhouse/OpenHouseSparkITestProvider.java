/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.openhouse;

import com.linkedin.openhouse.javaclient.OpenHouseCatalog;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.spark.TestCatalogProvider;
import org.apache.iceberg.spark.TestSparkSessionProvider;
import org.apache.spark.sql.SparkSession;

/**
 * SPI provider that integrates OpenHouse with Iceberg's test framework.
 *
 * <p>This adapter uses reflection to interact with {@code OpenHouseSparkITest} from the
 * tables-test-fixtures module, allowing Iceberg's parameterized tests to run against an embedded
 * OpenHouse server without introducing a compile-time dependency.
 *
 * <p>The provider manages a singleton OpenHouse server and SparkSession that persist for the
 * lifetime of the JVM, enabling efficient test execution across multiple test classes.
 */
public class OpenHouseSparkITestProvider implements TestSparkSessionProvider, TestCatalogProvider {

  private static final String OPENHOUSE_ITEST_CLASS =
      "com.linkedin.openhouse.tablestest.OpenHouseSparkITestShim";

  private static final Object SHARED_DELEGATE;
  private static final Method SHARED_GET_SPARK_SESSION;
  private static final Method SHARED_GET_SERVER_URI;

  static {
    try {
      // Disable Spring Boot's logging to avoid conflicts with test logging frameworks
      System.setProperty("org.springframework.boot.logging.LoggingSystem", "none");

      Class<?> clazz = Class.forName(OPENHOUSE_ITEST_CLASS);
      SHARED_DELEGATE = clazz.getDeclaredConstructor().newInstance();

      Method getSparkSession = null;
      try {
        getSparkSession = clazz.getDeclaredMethod("getSparkSession");
      } catch (NoSuchMethodException e) {
        // try superclass
        getSparkSession = clazz.getSuperclass().getDeclaredMethod("getSparkSession");
      }
      SHARED_GET_SPARK_SESSION = getSparkSession;
      SHARED_GET_SPARK_SESSION.setAccessible(true);

      Method getUri = null;
      try {
        getUri = clazz.getDeclaredMethod("getOpenHouseLocalServerURI");
      } catch (NoSuchMethodException e) {
        getUri = clazz.getSuperclass().getDeclaredMethod("getOpenHouseLocalServerURI");
      }
      SHARED_GET_SERVER_URI = getUri;
      SHARED_GET_SERVER_URI.setAccessible(true);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(
          "Failed to locate OpenHouseSparkITest. "
              + "Ensure tables-test-fixtures-iceberg is on the classpath.",
          e);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Unable to bootstrap OpenHouseSparkITest", e);
    }
  }

  private static volatile SparkSession sharedSession;
  private static volatile URI sharedServerUri;

  private SparkSession cachedSession;

  @Override
  public void beforeAll() throws Exception {
    this.cachedSession = ensureSharedSession();
    ensureDefaultDatabaseExists();
  }

  private void ensureDefaultDatabaseExists() {
    try {
      URI uri = ensureServerAndGetUri();
      Map<String, String> properties = new HashMap<>();
      properties.put("uri", uri.toString());
      properties.put("cluster", "local-cluster");

      OpenHouseCatalog catalog = new OpenHouseCatalog();
      catalog.initialize("openhouse", properties);

      // Check if 'default' namespace exists
      if (!catalog.namespaceExists(Namespace.of("default"))) {
        // Use reflection to access private databaseApi to create the database
        // as createNamespace throws UnsupportedOperationException
        Field dbApiField = OpenHouseCatalog.class.getDeclaredField("databaseApi");
        dbApiField.setAccessible(true);
        Object databaseApi = dbApiField.get(catalog);

        // Create CreateDatabaseRequestBody
        Class<?> requestBodyClass =
            Class.forName("com.linkedin.openhouse.tables.client.model.CreateDatabaseRequestBody");
        Object requestBody = requestBodyClass.getDeclaredConstructor().newInstance();

        Method setDatabaseId = requestBodyClass.getMethod("setDatabaseId", String.class);
        setDatabaseId.invoke(requestBody, "default");

        Method setClusterId = requestBodyClass.getMethod("setClusterId", String.class);
        setClusterId.invoke(requestBody, "local-cluster");

        // Call createDatabaseV1
        Method createDatabaseMethod =
            databaseApi.getClass().getMethod("createDatabaseV1", requestBodyClass);
        Object monoResponse = createDatabaseMethod.invoke(databaseApi, requestBody);

        // Block to complete
        Method blockMethod = monoResponse.getClass().getMethod("block");
        blockMethod.invoke(monoResponse);

        System.out.println("Created 'default' database in OpenHouse via reflection");
      }
    } catch (Exception e) {
      // Log warning but don't fail - maybe it exists or something else is wrong
      System.err.println("Failed to ensure 'default' database exists: " + e.getMessage());
      e.printStackTrace();
    }
  }

  @Override
  public SparkSession createSparkSession(SparkSession.Builder defaultBuilder, HiveConf hiveConf)
      throws Exception {
    SparkSession session = cachedSession != null ? cachedSession : ensureSharedSession();

    if (hiveConf != null) {
      // Propagate HiveConf settings (e.g. ephemeral metastore URI) to the shared session
      for (Map.Entry<String, String> entry : hiveConf) {
        session.sparkContext().hadoopConfiguration().set(entry.getKey(), entry.getValue());
        if ("hive.metastore.uris".equals(entry.getKey())) {
          session.conf().set("spark.hadoop.hive.metastore.uris", entry.getValue());
          session.conf().set("spark.sql.catalog.testhive.uri", entry.getValue());
          session.conf().set("spark.sql.catalog.hive.uri", entry.getValue());
        }
      }

      // Force reload of ALL catalogs to pick up new HiveConf
      try {
        Object sessionState = session.sessionState();
        Object catalogManager =
            sessionState.getClass().getMethod("catalogManager").invoke(sessionState);
        Field catalogsField = catalogManager.getClass().getDeclaredField("catalogs");
        catalogsField.setAccessible(true);
        Map<String, Object> catalogs = (Map<String, Object>) catalogsField.get(catalogManager);
        catalogs.clear();
      } catch (Exception e) {
        // ignore
      }
    }
    return session;
  }

  @Override
  public void afterAll() {
    // Keep the shared session/server running for the lifetime of the JVM.
    // Individual test classes should not stop it.
    cachedSession = null;
  }

  @Override
  public Object[][] getCatalogConfigurations() {
    URI uri = ensureServerAndGetUri();
    Map<String, String> properties = new HashMap<>();
    properties.put("catalog-impl", "org.apache.iceberg.spark.openhouse.OpenHouseShimCatalog");
    properties.put("uri", uri.toString());
    properties.put("cluster", "local-cluster");

    return new Object[][] {
      {
        "openhouse",
        "org.apache.iceberg.spark.openhouse.OpenHouseSparkCatalogShim",
        Collections.unmodifiableMap(properties)
      }
    };
  }

  /** Ensures the OpenHouse server is started and returns its URI. Thread-safe. */
  private static synchronized URI ensureServerAndGetUri() {
    if (sharedServerUri == null) {
      try {
        if (sharedSession == null) {
          sharedSession = (SparkSession) SHARED_GET_SPARK_SESSION.invoke(SHARED_DELEGATE);
        }
        sharedServerUri = (URI) SHARED_GET_SERVER_URI.invoke(SHARED_DELEGATE);
        System.out.println("OpenHouse server started at: " + sharedServerUri);
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException("Failed to start OpenHouse server", e);
      }
    }
    return sharedServerUri;
  }

  /** Ensures the shared SparkSession is available, recreating it if stopped. Thread-safe. */
  private static synchronized SparkSession ensureSharedSession()
      throws ReflectiveOperationException {
    if (sharedSession == null || sharedSession.sparkContext().isStopped()) {
      // System.out.println("DEBUG: Invoking getSparkSession on " +
      // SHARED_DELEGATE.getClass().getName());
      sharedSession = (SparkSession) SHARED_GET_SPARK_SESSION.invoke(SHARED_DELEGATE);
      if (sharedServerUri == null) {
        sharedServerUri = (URI) SHARED_GET_SERVER_URI.invoke(SHARED_DELEGATE);
        System.out.println("OpenHouse server started at: " + sharedServerUri);
      }
    }
    return sharedSession;
  }
}
