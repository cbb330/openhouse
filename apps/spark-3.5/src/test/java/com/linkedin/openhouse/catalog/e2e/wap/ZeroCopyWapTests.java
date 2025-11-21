package com.linkedin.openhouse.catalog.e2e.wap;

import com.linkedin.openhouse.catalog.e2e.IcebergTestExecutor;
import java.security.CodeSource;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ZERO-COPY execution of Iceberg tests against OpenHouse.
 * 
 * <p>How it works:
 * 1. Test JAR is on classpath via: testRuntimeOnly files(...tests.jar)
 * 2. We load the class programmatically
 * 3. JUnit 4 executes it
 * 4. ServiceLoader injects OpenHouse catalog
 * 5. Tests run against: hadoop, hive, spark, openhouse
 * 
 * <p>NO FILES COPIED!
 */
public class ZeroCopyWapTests {

  @Test
  public void executeIcebergWapTests() {
    configureGlobalSparkClasspath();
    logDriverTypeDescription();
    System.setProperty("iceberg.test.catalog.skip.defaults", "true");
    System.setProperty(
        "spring.autoconfigure.exclude",
        "org.springframework.boot.autoconfigure.groovy.template.GroovyTemplateAutoConfiguration");
    System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════╗");
    System.out.println("║  ZERO-COPY ICEBERG TEST EXECUTION                                            ║");
    System.out.println("║  Test: TestPartitionedWritesToWapBranch                                       ║");
    System.out.println("║  Source: iceberg-spark-3.5_2.12-1.6.0-SNAPSHOT-tests.jar                     ║");
    System.out.println("║  Catalogs: hadoop, hive, spark, openhouse (ServiceLoader)                    ║");
    System.out.println("╚══════════════════════════════════════════════════════════════════════════════╝\n");

    TestExecutionSummary summary = IcebergTestExecutor.runTest(
        "org.apache.iceberg.spark.sql.TestPartitionedWritesToWapBranch");
    assertTrue(summary.getTestsFoundCount() > 0, "Should find Iceberg tests");
    assertEquals(0, summary.getTotalFailureCount(), "Iceberg tests should pass");
  }

  private static final String RUNTIME_CLASSPATH = System.getProperty("java.class.path");

  private static void configureGlobalSparkClasspath() {
    System.setProperty("spark.driver.userClassPathFirst", "true");
    System.setProperty("spark.executor.userClassPathFirst", "true");
    System.setProperty("spark.driver.extraClassPath", RUNTIME_CLASSPATH);
    System.setProperty("spark.executor.extraClassPath", RUNTIME_CLASSPATH);
  }

  private static void logDriverTypeDescription() {
    try {
      Class<?> typeDesc = Class.forName("org.apache.orc.TypeDescription");
      CodeSource source = typeDesc.getProtectionDomain().getCodeSource();
      System.out.println(
          "ℹ️  [DRIVER] ORC TypeDescription loaded from: "
              + (source == null ? "unknown" : source.getLocation()));
      boolean hasCreateRowBatch =
          Arrays.stream(typeDesc.getDeclaredMethods())
              .anyMatch(
                  m ->
                      m.getName().equals("createRowBatch")
                          && m.getParameterCount() == 1
                          && m.getParameterTypes()[0] == int.class);
      System.out.println(
          "ℹ️  [DRIVER] TypeDescription#createRowBatch(int) present? " + hasCreateRowBatch);
    } catch (ClassNotFoundException e) {
      System.out.println("❌ [DRIVER] Could not load org.apache.orc.TypeDescription: " + e);
    }
  }
}

