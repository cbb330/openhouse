# OpenHouse Iceberg Compatibility Testing

## Overview

This document describes the design and implementation of compatibility testing between OpenHouse and Apache Iceberg. The goal is to ensure OpenHouse correctly implements Iceberg's table format specification by running Iceberg's own test suite against an embedded OpenHouse server.

## Motivation

OpenHouse implements a REST catalog for Apache Iceberg tables. To ensure correctness and maintain compatibility as both projects evolve, we need automated testing that:

1. **Validates Iceberg Spec Compliance** - Ensures OpenHouse correctly handles Iceberg operations (snapshots, commits, WAP workflows, etc.)
2. **Catches Regressions Early** - Detects breaking changes before they reach production
3. **Reduces Manual Testing** - Automates what would otherwise be manual integration testing
4. **Documents Limitations** - Explicitly tracks which Iceberg features OpenHouse doesn't support

## Design Principles

### 1. Minimal Changes to Iceberg

We avoid forking or heavily modifying Iceberg's codebase. Instead, we use **Service Provider Interface (SPI)** patterns to inject OpenHouse as a test catalog:

- `TestCatalogProvider` - Provides catalog configurations for parameterized tests
- `TestSparkSessionProvider` - Provides SparkSession instances configured for OpenHouse
- `TestTableProvider` - Provides table creation for core (non-Spark) tests

This approach:
- Keeps Iceberg changes upstreamable
- Reduces merge conflicts when updating Iceberg versions
- Makes the testing infrastructure reusable by other catalog implementations

### 2. No Code Duplication

Rather than copying Iceberg test classes into OpenHouse, we:
- Run Iceberg's tests directly from their source location
- Use Gradle's test filtering to select compatible tests
- Maintain an exclusion list for incompatible tests

### 3. Self-Contained Test Fixtures

The `tables-test-fixtures-iceberg-1.5` module produces an "uber jar" containing:
- Embedded OpenHouse server (`OpenHouseLocalServer`)
- SPI provider implementations
- All required dependencies (Spring Boot, H2, etc.)

This jar is published to Maven and consumed by Iceberg's test tasks without requiring OpenHouse source code.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Iceberg Repository                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────────┐    ┌──────────────────┐                      │
│  │  iceberg-core    │    │  iceberg-spark   │                      │
│  │                  │    │                  │                      │
│  │  Test Classes:   │    │  Test Classes:   │                      │
│  │  - TestWapWork.. │    │  - TestDelete..  │                      │
│  │  - TestFastApp.. │    │  - TestPartiti.. │                      │
│  │  - TestMergeA..  │    │  - TestSparkSc.. │                      │
│  └────────┬─────────┘    └────────┬─────────┘                      │
│           │                       │                                 │
│           │    SPI Loading        │                                 │
│           ▼                       ▼                                 │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │              tables-test-fixtures uber jar                   │   │
│  │  ┌─────────────────────────────────────────────────────┐    │   │
│  │  │  OpenHouseSparkITestProvider                        │    │   │
│  │  │  - Implements TestCatalogProvider                   │    │   │
│  │  │  - Implements TestSparkSessionProvider              │    │   │
│  │  │  - Starts embedded OpenHouse server                 │    │   │
│  │  │  - Returns catalog config: {uri, cluster, impl}     │    │   │
│  │  └─────────────────────────────────────────────────────┘    │   │
│  │  ┌─────────────────────────────────────────────────────┐    │   │
│  │  │  OpenHouseTestTableProvider                         │    │   │
│  │  │  - Implements TestTableProvider                     │    │   │
│  │  │  - Creates tables via OpenHouse REST API            │    │   │
│  │  └─────────────────────────────────────────────────────┘    │   │
│  │  ┌─────────────────────────────────────────────────────┐    │   │
│  │  │  OpenHouseLocalServer (Spring Boot)                 │    │   │
│  │  │  - Embedded Tomcat on ephemeral port                │    │   │
│  │  │  - H2 in-memory database                            │    │   │
│  │  │  - Full OpenHouse Tables service                    │    │   │
│  │  └─────────────────────────────────────────────────────┘    │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                        OpenHouse Repository                         │
├─────────────────────────────────────────────────────────────────────┤
│  tables-test-fixtures/                                              │
│  ├── tables-test-fixtures_2.12/        (base fixtures)             │
│  └── tables-test-fixtures-iceberg-1.5/ (Iceberg 1.5 specific)      │
│      ├── src/main/java/                                            │
│      │   └── org/apache/iceberg/                                   │
│      │       ├── spark/openhouse/                                  │
│      │       │   └── OpenHouseSparkITestProvider.java              │
│      │       └── openhouse/                                        │
│      │           └── OpenHouseTestTableProvider.java               │
│      ├── src/main/resources/                                       │
│      │   ├── META-INF/services/  (SPI registration)                │
│      │   └── openhouse-incompatible-tests.txt                      │
│      └── build.gradle            (shadow jar configuration)        │
└─────────────────────────────────────────────────────────────────────┘
```

## Components

### OpenHouse Side

#### `OpenHouseSparkITestProvider`
Implements both `TestCatalogProvider` and `TestSparkSessionProvider`:

```java
public class OpenHouseSparkITestProvider 
    implements TestSparkSessionProvider, TestCatalogProvider {
  
  @Override
  public Object[][] getCatalogConfigurations() {
    URI uri = ensureServerAndGetUri();
    return new Object[][] {{
      "openhouse", 
      "org.apache.iceberg.spark.SparkCatalog",
      Map.of("catalog-impl", "com.linkedin.openhouse.spark.OpenHouseCatalog",
             "uri", uri.toString(),
             "cluster", "local-cluster")
    }};
  }
  
  @Override
  public SparkSession createSparkSession(...) {
    return ensureSharedSession();
  }
}
```

#### `OpenHouseTestTableProvider`
Implements `TestTableProvider` for core tests:

```java
public class OpenHouseTestTableProvider implements TestTableProvider {
  @Override
  public Table createTable(File dir, String name, Schema schema, PartitionSpec spec) {
    return catalog.createTable(TableIdentifier.of("default", name), schema, spec);
  }
}
```

#### `openhouse-incompatible-tests.txt`
Lists tests that cannot run against OpenHouse:

```
# Tests with hardcoded catalog parameters
org.apache.iceberg.spark.sql.TestCreateTableAsSelect
org.apache.iceberg.spark.sql.TestFilterPushDown

# Tests for unsupported features
org.apache.iceberg.spark.sql.TestAlterTable  # partition evolution
org.apache.iceberg.spark.sql.TestCreateTable # unsupported properties
```

### Iceberg Side

#### `spark/openhouse.gradle`
Shared Gradle configuration for Spark compatibility tests:

```groovy
tasks.register('openhouseCompatibilityTest', Test) {
  useJUnitPlatform()
  systemProperty "iceberg.test.catalog.provider",
      'org.apache.iceberg.spark.openhouse.OpenHouseSparkITestProvider'
  systemProperty "iceberg.test.catalog.skip.defaults", "true"
  
  filter {
    includeTestsMatching "org.apache.iceberg.spark.sql.*"
    includeTestsMatching "org.apache.iceberg.spark.actions.*"
    includeTestsMatching "org.apache.iceberg.spark.source.*"
  }
  
  // Load exclusions from uber jar
  doFirst {
    configurations.openhouseCompatibilityRuntime.resolve().each { file ->
      if (file.name.contains("tables-test-fixtures")) {
        zipTree(file).matching { include "openhouse-incompatible-tests.txt" }.each { f ->
          f.eachLine { line -> filter.excludeTestsMatching line.trim() }
        }
      }
    }
  }
}
```

#### Core Test Configuration
In `build.gradle` for iceberg-core:

```groovy
tasks.register('openhouseCompatibilityTest', Test) {
  systemProperty "iceberg.test.table.provider",
      'org.apache.iceberg.openhouse.OpenHouseTestTableProvider'
  filter {
    includeTestsMatching "org.apache.iceberg.TestWapWorkflow"
    includeTestsMatching "org.apache.iceberg.TestFastAppend"
    // ... more tests
  }
}
```

## Test Coverage

### Spark SQL Tests
| Category | Status | Notes |
|----------|--------|-------|
| `TestUnpartitionedWrites` | ✅ Passing | Basic write operations |
| `TestPartitionedWrites` | ✅ Passing | Partitioned tables |
| `TestDeleteFrom` | ✅ Passing | Row-level deletes |
| `TestCreateTable` | ❌ Excluded | Unsupported table properties |
| `TestAlterTable` | ❌ Excluded | Partition evolution not supported |

### Spark Actions
| Test Class | Status | Notes |
|------------|--------|-------|
| `TestExpireSnapshotsAction` | ✅ Passing | 1 method excluded |
| `TestRewriteDataFilesAction` | 🔄 Testing | Table compaction |
| `TestRemoveOrphanFilesAction` | 🔄 Testing | Orphan file cleanup |

### Spark Source Tests
| Test Class | Status | Notes |
|------------|--------|-------|
| `TestSnapshotSelection` | ✅ Passing | Time travel queries |
| `TestDataFrameWrites` | 🔄 Testing | DataFrame API writes |
| `TestSparkScan` | 🔄 Testing | Scan operations |

### Core Tests
| Test Class | Status | Notes |
|------------|--------|-------|
| `TestWapWorkflow` | ✅ Passing | Write-Audit-Publish |
| `TestFastAppend` | 🔄 Testing | Fast append commits |
| `TestTransaction` | 🔄 Testing | Multi-operation transactions |

## User Experience

### Running Tests Locally

```bash
# Run Spark compatibility tests
cd iceberg
./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:openhouseCompatibilityTest \
  -PopenhouseCompatibilityCoordinate=com.linkedin.openhouse:tables-test-fixtures-iceberg-1.5_2.12:VERSION:uber

# Run core compatibility tests  
./gradlew :iceberg-core:openhouseCompatibilityTest \
  -PopenhouseCompatibilityCoordinate=com.linkedin.openhouse:tables-test-fixtures-iceberg-1.5_2.12:VERSION:uber
```

### Using Local Development Build

```bash
# Build and publish OpenHouse fixtures locally
cd openhouse
./gradlew :tables-test-fixtures:tables-test-fixtures-iceberg-1.5_2.12:publishToMavenLocal

# Run tests with local snapshot
cd ../iceberg
./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:openhouseCompatibilityTest \
  -PopenhouseCompatibilityCoordinate=com.linkedin.openhouse:tables-test-fixtures-iceberg-1.5_2.12:0.0.6-SNAPSHOT:uber
```

### CI Integration

The compatibility tests can run in CI pipelines:

```yaml
# Example GitHub Actions workflow
jobs:
  compatibility-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Run OpenHouse Compatibility Tests
        run: |
          ./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:openhouseCompatibilityTest \
            :iceberg-core:openhouseCompatibilityTest
```

## Impact So Far

### Bugs Found

The compatibility testing infrastructure has already proven its value by catching a critical bug:

#### WAP Branch Snapshot Handling Bug

**Issue**: OpenHouse's `SnapshotDiffApplier` was incorrectly handling staged snapshots during WAP (Write-Audit-Publish) workflows. When a client performed a `stageOnly()` commit followed by a `cherryPick()`, OpenHouse was auto-appending the staged snapshot to the main branch instead of keeping it unreferenced until explicitly cherry-picked.

**How it was found**: `TestWapWorkflow.testCherryPickOverwrite` failed with:
```
AssertionError: Files should match 
expected:<[/path/to/data-a.parquet]> 
but was:<[/path/to/data-b.parquet]>
```

**Root Cause**: The `SnapshotDiffApplier.computeWapSnapshots()` method was too broad in categorizing which snapshots to append. It was treating all snapshots not on the main branch as "WAP snapshots" to be auto-committed, rather than only appending snapshots that were explicitly referenced by a branch.

**Fix**: Refactored `SnapshotDiffApplier` to use a new `SnapshotAnalysis` class that correctly categorizes snapshots into:
- Snapshots referenced by branches (should be appended)
- Truly staged/unreferenced snapshots (should remain staged)
- Cherry-picked snapshots (require special handling)

**Impact**: This bug would have caused data corruption in production WAP workflows where users expected staged data to remain invisible until audit completion.

---

## Rejected Alternatives

### Alternative 1: Copy Iceberg Tests into OpenHouse

**Approach**: Duplicate Iceberg's test classes into the OpenHouse repository and modify them to work with OpenHouse directly.

**Why Rejected**:
- **Massive code duplication**: Iceberg has thousands of test classes with complex inheritance hierarchies
- **Transitive dependencies**: Tests depend on internal Iceberg test utilities (`TestTables`, `TestHelpers`, `ManifestFiles`, etc.) that would also need to be copied
- **Maintenance nightmare**: Every Iceberg release would require manually merging test changes
- **Version drift**: Copied tests would quickly diverge from upstream, defeating the purpose of compatibility testing
- **No guarantee of spec compliance**: Modified tests might pass but not actually validate Iceberg spec compliance

### Alternative 2: Consume Iceberg Test JARs

**Approach**: Have Iceberg publish test JARs (`iceberg-core-tests.jar`, `iceberg-spark-tests.jar`) and consume them as dependencies in OpenHouse.

**Why Rejected**:
- **Iceberg doesn't publish test JARs**: This would require modifying Iceberg's build to publish test artifacts
- **Dependency hell**: Even if we modified Iceberg to publish test JARs, the transitive dependency graph was unresolvable:
  - Test JARs depend on test-scoped dependencies that aren't published
  - Circular dependencies between test modules
  - Version conflicts between Iceberg's test dependencies and OpenHouse's runtime dependencies
- **Attempted and failed**: We actually tried this approach and spent significant time debugging classpath issues before abandoning it

### Alternative 3: No Compatibility Testing

**Approach**: Skip automated Iceberg compatibility testing and rely on manual QA and production monitoring.

**Why Rejected**:
- **Non-negotiable for upcoming changes**: We have a ~600-line change to the core commit workflow (`SnapshotDiffApplier`, `OpenHouseInternalTableOperations`) that fundamentally changes how OpenHouse handles Iceberg snapshots. This change **must** be vetted against Iceberg's own test suite.
- **Regression risk**: Without automated testing, subtle spec violations could reach production
- **Manual testing doesn't scale**: Iceberg has hundreds of edge cases that are impractical to test manually
- **Delayed feedback**: Bugs found in production are 10-100x more expensive to fix than bugs caught in CI

---

## Known Limitations

### Intentional OpenHouse Limitations

1. **Partition Evolution** - OpenHouse does not support changing partition schemes after table creation
2. **Clustering Evolution** - Similar to partitions, clustering cannot be modified
3. **Certain Table Properties** - Some Iceberg table properties are not supported
4. **Certain WAP-related table properties** - Simplified commit path ignores some WAP configuration properties

### Test Framework Limitations

1. **Hardcoded Catalogs** - Some Iceberg tests hardcode catalog configurations (e.g., `SparkCatalogConfig.HIVE`) and cannot use external providers
2. **Static State** - Tests that rely on static state may conflict with the shared OpenHouse server
3. **File System Assumptions** - Some tests assume local file system access patterns

## Troubleshooting

### Common Issues

**ClassCastException for Catalog**
```
ClassCastException: OpenHouseCatalog cannot be cast to Catalog
```
Solution: Ensure Iceberg classes are NOT relocated in the shadow jar.

**Connection Refused**
```
Connection refused: localhost:0
```
Solution: Server URI is being accessed before server starts. Check `ensureServerAndGetUri()` ordering.

**NoSuchBeanDefinitionException**
```
No qualifying bean of type 'MeterRegistry'
```
Solution: `SpringH2TestApplication` needs to provide a `SimpleMeterRegistry` bean.

### Debugging Tips

1. **Check server logs**: Look for `OpenHouse server started at:` in test output
2. **Verify SPI loading**: Search for `TRACE TestBaseWithCatalog#loadExternalCatalogs`
3. **Inspect uber jar**: `unzip -l tables-test-fixtures-*-uber.jar | grep OpenHouse`

## Future Work

1. **Expand Test Coverage** - Add more action and source tests
2. **Multiple Iceberg Versions** - Support testing against Iceberg 1.4, 1.5, 1.6
3. **Performance Testing** - Add benchmarks for catalog operations
4. **Streaming Tests** - Enable structured streaming compatibility tests
5. **Fix Limitations** - Work to support partition evolution where feasible

## References

- [Apache Iceberg Testing Guide](https://iceberg.apache.org/contribute/#testing)
- [OpenHouse Architecture](./docs/architecture.md)
- [Iceberg REST Catalog Spec](https://iceberg.apache.org/spec/#rest-catalog)

