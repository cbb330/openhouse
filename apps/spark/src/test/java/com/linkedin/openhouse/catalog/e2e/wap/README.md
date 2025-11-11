# OpenHouse WAP Test Coverage

This module enables running Iceberg's WAP (Write-Audit-Publish) test suites against the OpenHouse catalog implementation without code duplication or per-test subclasses.

## Strategy

### Problem Statement

OpenHouse is an implementation of Iceberg with business-specific extensions. We need to ensure that Iceberg's comprehensive WAP test suites also validate the OpenHouse implementation, but we want to:

1. **Avoid code duplication** - Don't copy Iceberg test logic into OpenHouse
2. **Avoid per-test subclasses** - Don't create a subclass for every Iceberg test
3. **Avoid reflection/bytecode generation** - Use standard Java mechanisms
4. **Minimal Iceberg changes** - Add extension points without breaking existing tests

### Solution: Service Provider Interface (SPI)

We use Java's ServiceLoader mechanism to inject OpenHouse catalog configuration into Iceberg's parameterized tests.

#### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Iceberg Test Framework                    │
│                                                              │
│  ┌────────────────────┐      ┌──────────────────────┐      │
│  │ CatalogTestBase    │      │  TableTestBase       │      │
│  │                    │      │                      │      │
│  │ parameters() {     │      │  create() {          │      │
│  │   // Hive, Hadoop  │      │    // Check for      │      │
│  │   // Spark configs │      │    // external       │      │
│  │   +                │      │    // provider       │      │
│  │   loadExternal()   │      │    loadExternal()    │      │
│  │ }                  │      │  }                   │      │
│  └────────┬───────────┘      └──────────┬───────────┘      │
│           │                              │                  │
│           │ ServiceLoader                │ ServiceLoader    │
│           ▼                              ▼                  │
└───────────┼──────────────────────────────┼──────────────────┘
            │                              │
            │                              │
┌───────────┼──────────────────────────────┼──────────────────┐
│           │    OpenHouse Providers       │                  │
│           │                              │                  │
│  ┌────────▼──────────────┐    ┌─────────▼────────────┐     │
│  │ OpenHouseCatalog      │    │ OpenHouseTable       │     │
│  │ Provider              │    │ Provider             │     │
│  │                       │    │                      │     │
│  │ getCatalogConfigs() { │    │ createTable() {      │     │
│  │   return [            │    │   // Use OpenHouse   │     │
│  │     "openhouse",      │    │   // catalog to      │     │
│  │     SparkCatalog,     │    │   // create tables   │     │
│  │     {config}          │    │ }                    │     │
│  │   ]                   │    │                      │     │
│  │ }                     │    │                      │     │
│  └───────────────────────┘    └──────────────────────┘     │
│           │                              │                  │
│           │ Uses OpenHouseSparkITest     │                  │
│           ▼                              ▼                  │
│  ┌──────────────────────────────────────────────────┐      │
│  │       OpenHouseLocalServer + SparkSession        │      │
│  └──────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

### Implementation Details

#### 1. Iceberg SPI Interfaces (Minimal Changes)

**TestCatalogProvider** - For Spark SQL tests:
```java
public interface TestCatalogProvider {
  Object[][] getCatalogConfigurations();  // Returns [catalogName, impl, config]
  default void beforeAll() throws Exception {}
  default void afterAll() throws Exception {}
}
```

**TestTableProvider** - For core API tests:
```java
public interface TestTableProvider {
  TestTables.TestTable createTable(File dir, String name, Schema schema, 
                                    PartitionSpec spec, int formatVersion);
  default void beforeAll() throws Exception {}
  default void afterAll() throws Exception {}
}
```

#### 2. Iceberg Test Base Modifications

**CatalogTestBase.parameters()** - Loads external catalogs:
```java
protected static Object[][] parameters() {
  List<Object[]> params = new ArrayList<>(Arrays.asList(
    // Existing Hive, Hadoop, Spark configs
  ));
  
  // Load external catalog providers via ServiceLoader
  params.addAll(Arrays.asList(loadExternalCatalogs()));
  return params.toArray(new Object[0][]);
}
```

**TableTestBase.create()** - Loads external table providers:
```java
protected TestTables.TestTable create(Schema schema, PartitionSpec spec) {
  TestTableProvider provider = loadExternalTableProvider();
  if (provider != null) {
    return provider.createTable(tableDir, "test", schema, spec, formatVersion);
  }
  return TestTables.create(tableDir, "test", schema, spec, formatVersion);
}
```

#### 3. OpenHouse Provider Implementation

**OpenHouseCatalogProvider** - Injects OpenHouse catalog configuration:
- Starts OpenHouseLocalServer using OpenHouseSparkITest infrastructure
- Creates SparkSession with OpenHouse catalog configured
- Returns catalog configuration for JUnit parameterized tests
- Registered via `META-INF/services/org.apache.iceberg.spark.TestCatalogProvider`

**OpenHouseTableProvider** - Creates tables via OpenHouse catalog:
- Uses Operations.withCatalog() to load tables from OpenHouse
- Wraps OpenHouse tables in TestTable interface for compatibility
- Registered via `META-INF/services/org.apache.iceberg.TestTableProvider`

## Usage

### Running All WAP Tests

Use the provided test suite to run all Iceberg WAP tests against OpenHouse:

```bash
cd openhouse
./gradlew :apps:spark:test --tests "IcebergWapTestSuite"
```

### Running Specific Iceberg Tests

Run any Iceberg test directly - the OpenHouse providers are automatically loaded:

```bash
# Spark SQL tests
./gradlew :apps:spark:test --tests "org.apache.iceberg.spark.sql.TestPartitionedWritesToWapBranch"

# Core API tests
./gradlew :apps:spark:test --tests "org.apache.iceberg.TestWapWorkflow"

# All WAP-related tests
./gradlew :apps:spark:test --tests "*Wap*"
```

### Running from Iceberg Repository

You can also run Iceberg tests with OpenHouse on the classpath:

```bash
cd iceberg
./gradlew :iceberg-spark:iceberg-spark-3.5:test \
  --tests "*TestPartitionedWritesToWapBranch*" \
  -PincludeOpenHouse=true
```

### Using System Properties (Alternative)

Instead of ServiceLoader, you can specify providers via system properties:

```bash
./gradlew test \
  -Diceberg.test.catalog.provider=com.linkedin.openhouse.catalog.test.OpenHouseCatalogProvider \
  -Diceberg.test.table.provider=com.linkedin.openhouse.catalog.test.OpenHouseTableProvider
```

## Test Coverage

The following Iceberg test classes automatically run against OpenHouse:

### Spark SQL Tests
- `TestPartitionedWritesToWapBranch` - WAP branch write operations
- All other Spark tests with "Wap" in the name

### Core API Tests
- `TestWapWorkflow` - Core WAP workflow operations
  - Cherry-pick operations
  - Overwrite with WAP
  - Duplicate WAP commit detection
  - WAP snapshot management

All test methods from these classes run automatically without any code duplication.

## Benefits

✅ **Zero code duplication** - All test logic stays in Iceberg  
✅ **No per-test subclasses** - Run ANY Iceberg test against OpenHouse  
✅ **No reflection** - Uses standard ServiceLoader SPI  
✅ **No bytecode generation** - Pure Java interfaces  
✅ **Minimal Iceberg changes** - Only added SPI hooks (~70 lines)  
✅ **Backward compatible** - Existing Iceberg tests unaffected  
✅ **Reuses infrastructure** - Leverages OpenHouseSparkITest pattern  
✅ **Easy to expand** - Add more Iceberg tests by just running them  

## Adding More Tests

To add coverage for additional Iceberg tests, simply run them:

```bash
./gradlew :apps:spark:test --tests "org.apache.iceberg.NewIcebergTest"
```

The OpenHouse providers are automatically discovered and loaded via ServiceLoader. No additional code needed!

## File Structure

```
openhouse/apps/spark/src/
├── main/
│   ├── java/com/linkedin/openhouse/catalog/test/
│   │   ├── OpenHouseCatalogProvider.java      # Catalog config provider
│   │   ├── OpenHouseTableProvider.java        # Table creation provider
│   │   └── OpenHouseTestTableWrapper.java     # Table wrapper for core tests
│   └── resources/META-INF/services/
│       ├── org.apache.iceberg.spark.TestCatalogProvider
│       └── org.apache.iceberg.TestTableProvider
└── test/
    └── java/com/linkedin/openhouse/catalog/e2e/wap/
        ├── IcebergWapTestSuite.java           # Optional test suite
        └── README.md                           # This file

iceberg/
├── spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/
│   ├── TestCatalogProvider.java               # SPI interface (NEW)
│   └── CatalogTestBase.java                   # Modified to load providers
└── core/src/test/java/org/apache/iceberg/
    ├── TestTableProvider.java                 # SPI interface (NEW)
    └── TableTestBase.java                     # Modified to load providers
```

## How It Works

1. **Test Discovery**: JUnit discovers Iceberg test classes (e.g., `TestPartitionedWritesToWapBranch`)
2. **Parameter Loading**: Iceberg's `@Parameters` method calls `loadExternalCatalogs()`
3. **ServiceLoader**: Java ServiceLoader discovers `OpenHouseCatalogProvider` from `META-INF/services`
4. **Provider Initialization**: `OpenHouseCatalogProvider.beforeAll()` starts OpenHouseLocalServer
5. **Catalog Injection**: Provider returns OpenHouse catalog configuration
6. **Test Execution**: JUnit runs all test methods with OpenHouse catalog parameters
7. **Table Creation**: For core tests, `OpenHouseTableProvider` creates tables via OpenHouse catalog
8. **Cleanup**: `afterAll()` stops the server after tests complete

## Troubleshooting

### Tests not finding OpenHouse catalog

Ensure the ServiceLoader registration files exist:
```bash
ls -la openhouse/apps/spark/src/main/resources/META-INF/services/
```

### Server startup failures

Check OpenHouseLocalServer logs. The provider uses the same infrastructure as `OpenHouseSparkITest`.

### Catalog configuration issues

The catalog config is extracted from the SparkSession created by `TestSparkSessionUtil.configureCatalogs()`, following the same pattern as `CatalogOperationTest`.

## Future Enhancements

- Add more Iceberg test packages to `IcebergWapTestSuite`
- Create separate test suites for different Iceberg feature areas
- Add CI integration to run Iceberg tests as part of OpenHouse validation
- Extend to other Spark versions (3.3, 3.4) by creating similar providers

## References

- [Iceberg WAP Documentation](https://iceberg.apache.org/docs/latest/branching/)
- [Java ServiceLoader Documentation](https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html)
- OpenHouse `OpenHouseSparkITest` - Base test infrastructure
- Iceberg `CatalogOperationTest` - Pattern for loading OpenHouse catalog

