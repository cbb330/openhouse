package com.linkedin.openhouse.spark;

/**
 * @deprecated Use {@link OpenHouseSparkCatalog}. Isolate belongs on the catalog jobs already load,
 *     not a second product catalog.
 */
@Deprecated
public class GitForDataSparkCatalog extends OpenHouseSparkCatalog {
  public static final String CONF_KEY = OpenHouseSparkCatalog.CONF_KEY;
  public static final String WAP_ENABLED = OpenHouseSparkCatalog.WAP_ENABLED;
}
