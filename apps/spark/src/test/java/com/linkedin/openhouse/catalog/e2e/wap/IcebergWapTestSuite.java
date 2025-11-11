package com.linkedin.openhouse.catalog.e2e.wap;

import org.junit.platform.suite.api.IncludeClassNamePatterns;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;

/**
 * Test suite that runs Iceberg WAP tests against OpenHouse catalog.
 *
 * <p>This suite automatically discovers and runs all Iceberg WAP-related tests using the OpenHouse
 * catalog providers registered via ServiceLoader.
 *
 * <p>To run this suite:
 *
 * <pre>
 * ./gradlew :apps:spark:test --tests "IcebergWapTestSuite"
 * </pre>
 *
 * <p>Or run specific Iceberg tests directly:
 *
 * <pre>
 * ./gradlew :apps:spark:test --tests "org.apache.iceberg.spark.sql.TestPartitionedWritesToWapBranch"
 * ./gradlew :apps:spark:test --tests "org.apache.iceberg.TestWapWorkflow"
 * </pre>
 */
@Suite
@SelectPackages("org.apache.iceberg")
@IncludeClassNamePatterns(".*Wap.*")
public class IcebergWapTestSuite {
  // This suite runs all Iceberg WAP tests with OpenHouse providers automatically loaded
  // via ServiceLoader. No additional code needed - the providers handle everything.
}
