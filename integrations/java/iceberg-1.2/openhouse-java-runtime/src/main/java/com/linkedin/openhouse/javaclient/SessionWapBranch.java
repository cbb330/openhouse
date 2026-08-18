package com.linkedin.openhouse.javaclient;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Reads {@code spark.wap.branch} from the active SparkSession when this client is used from Spark.
 * Non-Spark callers (no session on the classpath or no active session) get {@code null} and keep
 * the unbranched path.
 *
 * <p>The 1.5 java-runtime uber jar relocates {@code org.*}. That rewrite also rewrites string
 * literals, including {@code "org"} and {@code "org.apache.spark.sql.SparkSession"}. The Spark
 * class name is therefore built from characters, and Spark catalogs may {@link #push} a value
 * resolved before any relocate.
 */
public final class SessionWapBranch {

  static final String CONF_KEY = "spark.wap.branch";

  private static final ThreadLocal<Deque<String>> BOUND = ThreadLocal.withInitial(ArrayDeque::new);

  private SessionWapBranch() {}

  /** Bind a catalog-resolved branch for the current thread. Empty / null means no branch. */
  public static void push(String branch) {
    BOUND.get().push(branch == null || branch.isEmpty() ? "" : branch);
  }

  public static void pop() {
    Deque<String> stack = BOUND.get();
    if (!stack.isEmpty()) {
      stack.pop();
    }
    if (stack.isEmpty()) {
      BOUND.remove();
    }
  }

  static String get() {
    Deque<String> stack = BOUND.get();
    if (!stack.isEmpty()) {
      String top = stack.peek();
      return top.isEmpty() ? null : top;
    }
    return fromSpark();
  }

  static String sparkSessionClassName() {
    // Characters, not a package-prefix literal, so shadowjar cannot rewrite this name.
    return new String(
        new char[] {
          'o', 'r', 'g', '.', 'a', 'p', 'a', 'c', 'h', 'e', '.', 's', 'p', 'a', 'r', 'k', '.', 's',
          'q', 'l', '.', 'S', 'p', 'a', 'r', 'k', 'S', 'e', 's', 's', 'i', 'o', 'n'
        });
  }

  private static String fromSpark() {
    try {
      Class<?> sparkSession = Class.forName(sparkSessionClassName());
      Object spark = sparkSession.getMethod("active").invoke(null);
      Object conf = sparkSession.getMethod("conf").invoke(spark);
      Object option = conf.getClass().getMethod("getOption", String.class).invoke(conf, CONF_KEY);
      boolean empty = (Boolean) option.getClass().getMethod("isEmpty").invoke(option);
      if (empty) {
        return null;
      }
      String value = String.valueOf(option.getClass().getMethod("get").invoke(option)).trim();
      return value.isEmpty() ? null : value;
    } catch (Throwable t) {
      return null;
    }
  }
}
