package com.linkedin.openhouse.catalog.e2e;

import java.io.PrintWriter;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

/**
 * Executes Iceberg test classes that are already on the classpath via Gradle dependencies.
 *
 * <p>The test classes come from:
 *
 * <pre>
 * testImplementation "org.apache.iceberg:iceberg-spark-3.5_2.12:1.6.0-SNAPSHOT:tests"
 * </pre>
 *
 * <p>Gradle automatically resolves ALL transitive dependencies, so the test classes and all their
 * dependencies (Hive, ORC, etc.) are available at runtime.
 *
 * <p>This approach: - ✅ Zero file copying - ✅ Zero manual dependency management - ✅ Gradle handles
 * all transitive dependencies - ✅ Tests run with OpenHouse catalog via ServiceLoader
 */
public class IcebergTestExecutor {

  public static TestExecutionSummary runTest(String testClassName) {
    System.out.println(
        "╔══════════════════════════════════════════════════════════════════════════════╗");
    System.out.println("║  Executing Iceberg Test: " + testClassName);
    System.out.println("║  Source: Gradle dependency (test JAR)");
    System.out.println("║  Dependencies: Resolved by Gradle (transitive)");
    System.out.println("║  Catalogs: hadoop, hive, spark, openhouse (via ServiceLoader)");
    System.out.println(
        "╚══════════════════════════════════════════════════════════════════════════════╝");
    System.out.println();

    LauncherDiscoveryRequest request =
        LauncherDiscoveryRequestBuilder.request()
            .selectors(DiscoverySelectors.selectClass(testClassName))
            .build();

    Launcher launcher = LauncherFactory.create();
    SummaryGeneratingListener summaryListener = new SummaryGeneratingListener();
    launcher.registerTestExecutionListeners(summaryListener, new LoggingListener());
    launcher.execute(request);

    TestExecutionSummary summary = summaryListener.getSummary();
    PrintWriter out = new PrintWriter(System.out, true);
    summary.printTo(out);
    summary.printFailuresTo(out);
    return summary;
  }

  private static class LoggingListener implements TestExecutionListener {
    @Override
    public void executionStarted(TestIdentifier testIdentifier) {
      if (testIdentifier.isTest()) {
        System.out.println("[ICEBERG] START " + testIdentifier.getDisplayName());
      }
    }

    @Override
    public void executionFinished(
        TestIdentifier testIdentifier, TestExecutionResult testExecutionResult) {
      if (!testIdentifier.isTest()) {
        return;
      }
      String status;
      switch (testExecutionResult.getStatus()) {
        case SUCCESSFUL:
          status = "END  ";
          break;
        case FAILED:
          status = "FAIL ";
          break;
        case ABORTED:
        default:
          status = "ABRT ";
          break;
      }
      System.out.println("[ICEBERG] " + status + testIdentifier.getDisplayName());
      testExecutionResult
          .getThrowable()
          .ifPresent(t -> System.out.println("[ICEBERG] MSG   " + t.getMessage()));
    }
  }
}
