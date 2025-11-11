package com.linkedin.openhouse.catalog.test;

import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TestTables;

/**
 * Wraps an OpenHouse Table to be compatible with TestTables.TestTable. Delegates all operations to
 * the underlying OpenHouse table.
 */
public class OpenHouseTestTableWrapper extends TestTables.TestTable {

  private final Table delegate;

  public OpenHouseTestTableWrapper(Table table, String name) {
    super(new OpenHouseTableOperationsWrapper(table.operations()), name);
    this.delegate = table;
  }

  /** Wrapper for TableOperations to make it compatible with TestTableOperations. */
  private static class OpenHouseTableOperationsWrapper implements TableOperations {
    private final TableOperations delegate;

    OpenHouseTableOperationsWrapper(TableOperations ops) {
      this.delegate = ops;
    }

    @Override
    public org.apache.iceberg.TableMetadata current() {
      return delegate.current();
    }

    @Override
    public org.apache.iceberg.TableMetadata refresh() {
      return delegate.refresh();
    }

    @Override
    public void commit(
        org.apache.iceberg.TableMetadata base, org.apache.iceberg.TableMetadata metadata) {
      delegate.commit(base, metadata);
    }

    @Override
    public org.apache.iceberg.io.FileIO io() {
      return delegate.io();
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return delegate.metadataFileLocation(fileName);
    }

    @Override
    public org.apache.iceberg.io.LocationProvider locationProvider() {
      return delegate.locationProvider();
    }
  }
}
