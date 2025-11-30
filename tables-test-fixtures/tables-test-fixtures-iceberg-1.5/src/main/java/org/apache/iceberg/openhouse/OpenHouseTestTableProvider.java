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
package org.apache.iceberg.openhouse;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

/**
 * TestTableProvider implementation that creates tables in OpenHouse.
 *
 * <p>This provider starts an embedded OpenHouse server and creates tables via the OpenHouse
 * catalog. It returns TestTables.TestTable instances that wrap DelegatingTableOperations, which
 * delegate to OpenHouse while also syncing metadata to TestTables.METADATA for compatibility with
 * Iceberg's test infrastructure.
 *
 * <p>Note: This class implements org.apache.iceberg.TestTableProvider at runtime (the interface is
 * provided by Iceberg's test classes). It does not implement it at compile time to avoid a
 * dependency on Iceberg's test sources.
 */
public class OpenHouseTestTableProvider {

  private static final String LOCAL_SERVER_CLASS =
      "com.linkedin.openhouse.tablestest.OpenHouseLocalServer";
  private static final String CATALOG_CLASS = "com.linkedin.openhouse.javaclient.OpenHouseCatalog";
  private static final String TEST_TABLES_CLASS = "org.apache.iceberg.TestTables";
  private static final String TEST_TABLE_CLASS = "org.apache.iceberg.TestTables$TestTable";
  private static final String TEST_TABLE_OPS_CLASS =
      "org.apache.iceberg.TestTables$TestTableOperations";

  private static final Constructor<?> LOCAL_SERVER_CTOR;
  private static final Method LOCAL_SERVER_START;
  private static final Method LOCAL_SERVER_STOP;
  private static final Method LOCAL_SERVER_GET_PORT;

  // TestTables reflection
  private static final Class<?> TEST_TABLE_CLS;
  private static final Class<?> TEST_TABLE_OPS_CLS;
  private static final Constructor<?> TEST_TABLE_CTOR;
  private static final Constructor<?> TEST_TABLE_OPS_CTOR;
  private static final Map<String, TableMetadata> METADATA_MAP;
  private static final Map<String, Integer> VERSIONS_MAP;

  private static final Method TEST_TABLES_CREATE;

  static {
    try {
      System.setProperty("org.springframework.boot.logging.LoggingSystem", "none");

      // OpenHouse server reflection
      Class<?> serverClass = Class.forName(LOCAL_SERVER_CLASS);
      LOCAL_SERVER_CTOR = serverClass.getDeclaredConstructor();
      LOCAL_SERVER_START = serverClass.getDeclaredMethod("start");
      LOCAL_SERVER_STOP = serverClass.getDeclaredMethod("stop");
      LOCAL_SERVER_GET_PORT = serverClass.getDeclaredMethod("getPort");

      // TestTables reflection
      Class<?> testTablesClass = Class.forName(TEST_TABLES_CLASS);
      TEST_TABLE_CLS = Class.forName(TEST_TABLE_CLASS);
      TEST_TABLE_OPS_CLS = Class.forName(TEST_TABLE_OPS_CLASS);

      TEST_TABLE_CTOR =
          TEST_TABLE_CLS.getDeclaredConstructor(TestTables.TestTableOperations.class, String.class);
      TEST_TABLE_CTOR.setAccessible(true);

      TEST_TABLE_OPS_CTOR = TEST_TABLE_OPS_CLS.getDeclaredConstructor(String.class, File.class);
      TEST_TABLE_OPS_CTOR.setAccessible(true);

      // Find create method: create(File temp, String name, Schema schema, PartitionSpec spec, int
      // formatVersion)
      TEST_TABLES_CREATE =
          testTablesClass.getDeclaredMethod(
              "create", File.class, String.class, Schema.class, PartitionSpec.class, int.class);
      TEST_TABLES_CREATE.setAccessible(true);

      Field metadataField = testTablesClass.getDeclaredField("METADATA");
      metadataField.setAccessible(true);
      @SuppressWarnings("unchecked")
      Map<String, TableMetadata> metadata = (Map<String, TableMetadata>) metadataField.get(null);
      METADATA_MAP = metadata;

      Field versionsField = testTablesClass.getDeclaredField("VERSIONS");
      versionsField.setAccessible(true);
      @SuppressWarnings("unchecked")
      Map<String, Integer> versions = (Map<String, Integer>) versionsField.get(null);
      VERSIONS_MAP = versions;

    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(
          "Feiled to locate required classes. "
              + "Ensure tables-test-fixtures-iceberg and iceberg-core are on the classpath.",
          e);
    } catch (Exception e) {
      throw new RuntimeException("Unable to bootstrap OpenHouse fixtures", e);
    }
  }

  private static volatile Object sharedServer;
  private static volatile Catalog sharedCatalog;
  private static volatile URI sharedServerUri;

  // Counter for generating unique table names within the same database
  private static final AtomicInteger tableCounter = new AtomicInteger(0);

  // Database name used for all test tables
  private static final String TEST_DATABASE = "db";

  /** Called before any tests run. Starts the OpenHouse server and initializes the catalog. */
  public void beforeAll() throws Exception {
    ensureSharedServer();
    ensureSharedCatalog();
  }

  /** Called after all tests complete. Keeps the server running for the lifetime of the JVM. */
  public void afterAll() throws Exception {
    // Keep the server running for the lifetime of the JVM
    // Individual test classes should not stop it
  }

  /**
   * Creates a test table in OpenHouse and returns a TestTables.TestTable instance.
   *
   * @param dir the directory for the table (used for TestTables compatibility)
   * @param name the table name
   * @param schema the table schema
   * @param spec the partition spec
   * @param formatVersion the format version
   * @return a TestTables.TestTable instance
   */
  public Object createTable(
      File dir, String name, Schema schema, PartitionSpec spec, int formatVersion) {

    if (formatVersion == 1) {
      try {
        return TEST_TABLES_CREATE.invoke(null, dir, name, schema, spec, formatVersion);
      } catch (Exception e) {
        throw new RuntimeException("Failed to create V1 table via TestTables", e);
      }
    }

    // Generate a unique table name to avoid conflicts between tests
    String uniqueTableName = name + "_" + tableCounter.incrementAndGet();
    TableIdentifier tableId = TableIdentifier.of(TEST_DATABASE, uniqueTableName);

    // Create the table in OpenHouse
    Table openhouseTable;
    try {
      Map<String, String> properties = new HashMap<>();
      properties.put("format-version", String.valueOf(formatVersion));
      openhouseTable = sharedCatalog.createTable(tableId, schema, spec, properties);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Failed to create table in OpenHouse: " + tableId, e);
    }

    // Sync metadata to TestTables.METADATA so Iceberg's test infrastructure can find it
    syncMetadataToTestTables(openhouseTable, uniqueTableName);

    // Create symlink from test scaffolding dir to actual table location
    // This allows TestRewriteFiles to find manifest files
    createSymlinkToMetadata(dir, openhouseTable);

    // Create and return a TestTable that delegates to OpenHouse
    return createTestTable(openhouseTable, uniqueTableName, dir);
  }

  private void createSymlinkToMetadata(File testTableDir, Table table) {
    if (table.location() == null) {
      return;
    }

    try {
      String location = table.location().replace("file:", "");
      File realTableDir = new File(location);
      File realMetadataDir = new File(realTableDir, "metadata");

      if (!realMetadataDir.exists()) {
        // If real metadata dir doesn't exist, nothing to link to yet
        return;
      }

      if (!testTableDir.exists()) {
        testTableDir.mkdirs();
      }

      File expectedMetadataDir = new File(testTableDir, "metadata");

      if (expectedMetadataDir.exists()) {
        if (Files.isSymbolicLink(expectedMetadataDir.toPath())) {
          return;
        }
        // If it exists and is a directory, we might have a problem.
        // But usually TableTestBase creates empty tableDir.
      } else {
        Files.createSymbolicLink(expectedMetadataDir.toPath(), realMetadataDir.toPath());
        System.out.println(
            "Created symlink from " + expectedMetadataDir + " to " + realMetadataDir);
      }

    } catch (Exception e) {
      System.err.println("Failed to create symlink for test metadata: " + e.getMessage());
      // Don't fail the test, just log
    }
  }

  private static synchronized void ensureSharedServer() {
    if (sharedServer == null) {
      try {
        sharedServer = LOCAL_SERVER_CTOR.newInstance();
        LOCAL_SERVER_START.invoke(sharedServer);
        int port = (int) LOCAL_SERVER_GET_PORT.invoke(sharedServer);
        sharedServerUri = URI.create("http://127.0.0.1:" + port);
        System.out.println("Started OpenHouseLocalServer on " + sharedServerUri);
      } catch (Exception e) {
        throw new RuntimeException("Failed to start OpenHouseLocalServer", e);
      }
    }
  }

  private static synchronized void ensureSharedCatalog() {
    if (sharedCatalog == null) {
      try {
        Class<?> catalogClass = Class.forName(CATALOG_CLASS);
        sharedCatalog = (Catalog) catalogClass.getDeclaredConstructor().newInstance();

        // Set Hadoop Configuration if supported (required for FileIO)
        if (sharedCatalog instanceof Configurable) {
          ((Configurable) sharedCatalog).setConf(new Configuration());
        }

        // Initialize the catalog with OpenHouse configuration
        Map<String, String> properties = new HashMap<>();
        properties.put("uri", sharedServerUri.toString());
        properties.put("cluster", "local-cluster");

        // Call initialize method
        Method initMethod = catalogClass.getMethod("initialize", String.class, Map.class);
        initMethod.invoke(sharedCatalog, "openhouse", properties);

        System.out.println("Initialized OpenHouseCatalog pointing to " + sharedServerUri);
      } catch (Exception e) {
        throw new RuntimeException("Failed to initialize OpenHouseCatalog", e);
      }
    }
  }

  /** Syncs the table metadata from OpenHouse to TestTables.METADATA. */
  private static void syncMetadataToTestTables(Table table, String tableName) {
    synchronized (METADATA_MAP) {
      // Cast to HasTableOperations to access operations()
      TableOperations ops = ((HasTableOperations) table).operations();
      TableMetadata metadata = ops.current();
      if (metadata != null) {
        // Build clean metadata without pending changes
        TableMetadata.Builder builder = TableMetadata.buildFrom(metadata).discardChanges();

        // Strip file: scheme from location if present, so TableTestBase can find files locally
        String location = metadata.location();
        if (location != null && location.startsWith("file:")) {
          location = location.replaceFirst("file:", "");
          builder.setLocation(location);
        }

        // Ensure metadata directory exists for TableTestBase
        if (location != null) {
          File tableDir = new File(location);
          File metadataDir = new File(tableDir, "metadata");
          if (!metadataDir.exists()) {
            boolean created = metadataDir.mkdirs();
            System.out.println(
                "DEBUG syncMetadataToTestTables: Created metadata dir at "
                    + metadataDir.getAbsolutePath()
                    + ": "
                    + created);
          } else {
            System.out.println(
                "DEBUG syncMetadataToTestTables: Metadata dir exists at "
                    + metadataDir.getAbsolutePath());
          }
        }

        TableMetadata clean = builder.build();
        METADATA_MAP.put(tableName, clean);
        VERSIONS_MAP.putIfAbsent(tableName, 0);
        System.out.println(
            "DEBUG syncMetadataToTestTables: Synced metadata for " + tableName + " at " + location);
        System.out.println("DEBUG syncMetadataToTestTables: Properties: " + clean.properties());
        if (clean.currentSnapshot() != null) {
          System.out.println(
              "DEBUG syncMetadataToTestTables: Current snapshot: " + clean.currentSnapshot());
          System.out.println(
              "DEBUG syncMetadataToTestTables: Manifest list loc: "
                  + clean.currentSnapshot().manifestListLocation());
        }
      } else {
        System.out.println("DEBUG syncMetadataToTestTables: Metadata is null for " + tableName);
      }
    }
  }

  /** Creates a TestTables.TestTable using reflection. */
  private static Object createTestTable(Table openHouseTable, String name, File dir) {
    try {
      // Create DelegatingTableOperations
      TableOperations realOps = ((HasTableOperations) openHouseTable).operations();
      Object delegatingOps = new DelegatingTableOperations(realOps, name, dir);

      // Create TestTable
      return TEST_TABLE_CTOR.newInstance(delegatingOps, name);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create TestTable", e);
    }
  }

  /** DelegatingTableOperations that forwards to OpenHouse and syncs with TestTables.METADATA. */
  public static class DelegatingTableOperations extends TestTables.TestTableOperations {
    private final TableOperations delegate;
    private final String tableName;

    public DelegatingTableOperations(TableOperations delegate, String tableName, File location) {
      super(tableName, location);
      this.delegate = delegate;
      this.tableName = tableName;
    }

    @Override
    public TableMetadata current() {
      return delegate.current();
    }

    @Override
    public TableMetadata refresh() {
      if (delegate != null) {
        TableMetadata metadata = delegate.refresh();
        updateMetadataMap(metadata);
      }
      return super.refresh();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      // Call super to handle failure injection and update in-memory map
      super.commit(base, metadata);
      // Delegate to OpenHouse
      try {
        delegate.commit(base, metadata);
      } catch (Exception e) {
        // If OpenHouse commit fails, we should probably revert the in-memory update?
        // But usually refresh() will fix it on next call.
        throw e;
      }
      updateMetadataMap(delegate.current());
    }

    @Override
    public FileIO io() {
      return delegate.io();
    }

    @Override
    public EncryptionManager encryption() {
      return delegate.encryption();
    }

    @Override
    public LocationProvider locationProvider() {
      return delegate.locationProvider();
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return delegate.metadataFileLocation(fileName);
    }

    @Override
    public TableOperations temp(TableMetadata uncommitted) {
      return new DelegatingTableOperations(
          delegate.temp(uncommitted), tableName, new File("/tmp")); // temp location ignored
    }

    private void updateMetadataMap(TableMetadata metadata) {
      synchronized (METADATA_MAP) {
        if (metadata != null) {
          TableMetadata.Builder builder = TableMetadata.buildFrom(metadata).discardChanges();
          String location = metadata.location();
          if (location != null && location.startsWith("file:")) {
            location = location.replaceFirst("file:", "");
            builder.setLocation(location);
          }
          TableMetadata clean = builder.build();
          METADATA_MAP.put(tableName, clean);
          VERSIONS_MAP.putIfAbsent(tableName, 0);
          System.out.println(
              "DEBUG updateMetadataMap: Updated metadata for " + tableName + " at " + location);
          System.out.println("DEBUG updateMetadataMap: Properties: " + clean.properties());
          if (clean.currentSnapshot() != null) {
            System.out.println(
                "DEBUG updateMetadataMap: Current snapshot: " + clean.currentSnapshot());
            System.out.println(
                "DEBUG updateMetadataMap: Manifest list loc: "
                    + clean.currentSnapshot().manifestListLocation());
            // Try to count manifests if possible, or just print logs
            try {
              System.out.println(
                  "DEBUG updateMetadataMap: Manifests count: "
                      + clean.currentSnapshot().allManifests(delegate.io()).size());
              for (org.apache.iceberg.ManifestFile mf :
                  clean.currentSnapshot().allManifests(delegate.io())) {
                System.out.println(
                    "DEBUG updateMetadataMap: Manifest: "
                        + mf.path()
                        + " (added="
                        + mf.addedFilesCount()
                        + ")");
              }
            } catch (Exception e) {
              System.out.println(
                  "DEBUG updateMetadataMap: Failed to list manifests: " + e.getMessage());
            }
          }
        }
      }
    }
  }
}
