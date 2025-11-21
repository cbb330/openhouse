package com.linkedin.openhouse.catalog.e2e.wap;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.security.CodeSource;
import java.util.Arrays;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

/**
 * Utility test that forces a Spark task to run inside the OpenHouse test harness and prints the
 * executor-side ORC class metadata. Use this to confirm that {@code spark.executor.extraClassPath}
 * is propagated when running OpenHouse-driven Iceberg suites.
 */
public class SparkExecutorClasspathProbeTest extends OpenHouseSparkITest {

  @Test
  public void dumpExecutorClassLoader() throws Exception {
    System.setProperty(
        "spring.autoconfigure.exclude",
        "org.springframework.boot.autoconfigure.groovy.template.GroovyTemplateAutoConfiguration");
    SparkSession spark = getSparkSession();
    logDriverOrcView();
    spark
        .range(1)
        .toJavaRDD()
        .foreachPartition(
            (VoidFunction<java.util.Iterator<Long>>)
                iterator -> {
                  Thread t = Thread.currentThread();
                  ClassLoader cl = t.getContextClassLoader();
                  System.out.println(
                      "🧪 [EXECUTOR] Thread="
                          + t.getName()
                          + " ContextClassLoader="
                          + cl);
                  try {
                    Class<?> typeDesc =
                        Class.forName("org.apache.orc.TypeDescription", false, cl);
                    CodeSource source = typeDesc.getProtectionDomain().getCodeSource();
                    boolean hasCreateRowBatch =
                        Arrays.stream(typeDesc.getDeclaredMethods())
                            .anyMatch(
                                m ->
                                    m.getName().equals("createRowBatch")
                                        && m.getParameterCount() == 1
                                        && m.getParameterTypes()[0] == int.class);
                    System.out.println(
                        "🧪 [EXECUTOR] TypeDescription from: "
                            + (source == null ? "unknown" : source.getLocation())
                            + " createRowBatch(int)? "
                            + hasCreateRowBatch);
                    System.out.println(
                        "🧪 [EXECUTOR] java.class.path length="
                            + System.getProperty("java.class.path").length());
                  } catch (Throwable t1) {
                    System.out.println("🧪 [EXECUTOR] Unable to load ORC TypeDescription: " + t1);
                    t1.printStackTrace(System.out);
                  }
                });
  }

  private static void logDriverOrcView() {
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

