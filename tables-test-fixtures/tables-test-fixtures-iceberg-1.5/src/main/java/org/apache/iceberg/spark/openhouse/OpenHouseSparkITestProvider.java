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

import java.lang.reflect.Method;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;
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
      "com.linkedin.openhouse.tablestest.OpenHouseSparkITest";

  private static final Object SHARED_DELEGATE;
  private static final Method SHARED_GET_SPARK_SESSION;
  private static final Method SHARED_GET_SERVER_URI;

  static {
    try {
      // Disable Spring Boot's logging to avoid conflicts with test logging frameworks
      System.setProperty("org.springframework.boot.logging.LoggingSystem", "none");

      Class<?> clazz = Class.forName(OPENHOUSE_ITEST_CLASS);
      SHARED_DELEGATE = clazz.getDeclaredConstructor().newInstance();

      SHARED_GET_SPARK_SESSION = clazz.getDeclaredMethod("getSparkSession");
      SHARED_GET_SPARK_SESSION.setAccessible(true);

      SHARED_GET_SERVER_URI = clazz.getDeclaredMethod("getOpenHouseLocalServerURI");
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
  }

  @Override
  public SparkSession createSparkSession(SparkSession.Builder defaultBuilder, HiveConf hiveConf)
      throws Exception {
    return cachedSession != null ? cachedSession : ensureSharedSession();
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
    properties.put("catalog-impl", "com.linkedin.openhouse.spark.OpenHouseCatalog");
    properties.put("uri", uri.toString());
    properties.put("cluster", "local-cluster");

    return new Object[][] {
      {
        "openhouse",
        "org.apache.iceberg.spark.SparkCatalog",
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
      sharedSession = (SparkSession) SHARED_GET_SPARK_SESSION.invoke(SHARED_DELEGATE);
      if (sharedServerUri == null) {
        sharedServerUri = (URI) SHARED_GET_SERVER_URI.invoke(SHARED_DELEGATE);
        System.out.println("OpenHouse server started at: " + sharedServerUri);
      }
    }
    return sharedSession;
  }
}
