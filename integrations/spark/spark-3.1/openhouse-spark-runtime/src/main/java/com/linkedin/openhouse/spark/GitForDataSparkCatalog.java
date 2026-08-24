package com.linkedin.openhouse.spark;

/** @deprecated Use {@link OpenHouseSparkCatalog}. Spark 3.1 isolate is not a product catalog. */
@Deprecated
public class GitForDataSparkCatalog extends OpenHouseSparkCatalog {
  public static final String CONF_KEY = "spark.wap.branch";
  public static final String WAP_ENABLED = "write.wap.enabled";
}
