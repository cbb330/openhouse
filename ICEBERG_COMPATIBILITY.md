# Iceberg Compatibility Test Harness

This project includes a "Zero-Touch" test harness that allows Apache Iceberg's integration tests to run against the OpenHouse catalog server without modifying Iceberg's source code (except for build scripts).

## Architecture

1.  **Service Provider Interface (SPI)**:
    *   The harness uses Java's `ServiceLoader` mechanism.
    *   `tables-test-fixtures` jar contains implementations of Iceberg's test interfaces:
        *   `TestTableProvider` -> `OpenHouseTestTableProvider`
        *   `TestCatalogProvider` -> `OpenHouseSparkITestProvider`
        *   `TestSparkSessionProvider` -> `OpenHouseSparkITestProvider`
    *   Registration files are located in `META-INF/services/`.

2.  **Dynamic Exclusions**:
    *   Incompatible Iceberg tests (e.g., features not supported by OpenHouse) are listed in `src/main/resources/openhouse-incompatible-tests.txt` within the `tables-test-fixtures` jar.
    *   The Iceberg build script (`openhouse.gradle`) dynamically reads this file from the classpath at runtime and excludes these tests. This allows managing exclusions purely from the OpenHouse side.

3.  **Snapshot Analysis**:
    *   `SnapshotDiffApplier` (and `SnapshotAnalysis`) in `internalcatalog` handles the translation of Iceberg snapshot operations (WAP, Cherry-pick) to OpenHouse's internal metadata structure, ensuring consistent state.

## Usage

To run the compatibility suite from the Iceberg repository:

```bash
./gradlew clean :iceberg-core:openhouseCompatibilityTest \
  -PopenhouseCompatibilityCoordinate=com.linkedin.openhouse:tables-test-fixtures_2.12:0.0.1-SNAPSHOT:uber
```

Ensure the `tables-test-fixtures` jar is published to your local Maven repository or available in the configured artifact repository.

