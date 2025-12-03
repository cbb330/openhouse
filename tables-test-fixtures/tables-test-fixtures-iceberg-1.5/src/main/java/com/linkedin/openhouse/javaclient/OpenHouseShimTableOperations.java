package com.linkedin.openhouse.javaclient;

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

public class OpenHouseShimTableOperations extends OpenHouseTableOperations {

  public OpenHouseShimTableOperations(OpenHouseTableOperations other) {
    super(
        other.getTableIdentifier(),
        other.getFileIO(),
        other.getTableApi(),
        other.getSnapshotApi(),
        other.getCluster());
    System.out.println(
        "DEBUG: OpenHouseShimTableOperations initialized for " + other.getTableIdentifier());
  }

  @Override
  public TableMetadata current() {
    try {
      return super.current();
    } catch (NoSuchTableException e) {
      return null;
    }
  }

  @Override
  public void doRefresh() {
    System.out.println("ShimTableOperations: doRefresh called for " + getTableIdentifier());

    // Pre-check existence to avoid OpenHouseTableOperations infinite recursion on 404
    try {
      System.out.println(
          "ShimTableOperations: Checking table existence for "
              + getTableIdentifier()
              + " on thread "
              + Thread.currentThread().getName());
      long start = System.currentTimeMillis();
      getTableApi()
          .getTableV1(getTableIdentifier().namespace().toString(), getTableIdentifier().name())
          .block(java.time.Duration.ofSeconds(5));
      System.out.println(
          "ShimTableOperations: Table exists (checked in "
              + (System.currentTimeMillis() - start)
              + "ms)");
    } catch (WebClientResponseException.NotFound e) {
      System.out.println("ShimTableOperations: Caught 404, throwing NoSuchTableException");
      throw new NoSuchTableException("Table does not exist: " + getTableIdentifier());
    } catch (Exception e) {
      // Handle relocated WebClientResponseException.NotFound which might be wrapped or directly
      // thrown but not matching the import above
      if (e.getClass().getName().contains("WebClientResponseException$NotFound")) {
        System.out.println(
            "ShimTableOperations: Caught relocated 404, throwing NoSuchTableException");
        // We must throw NoSuchTableException to signal to Iceberg that the table is missing.
        // However, depending on the call stack, throwing this here might abort the createTable flow
        // if it's not expecting it during an initial check.
        throw new NoSuchTableException("Table does not exist: " + getTableIdentifier());
      }
      System.out.println(
          "ShimTableOperations: Pre-check failed with "
              + e.getClass().getName()
              + ": "
              + e.getMessage());
      // ignore other errors, let super.doRefresh handle them
    }

    System.out.println("ShimTableOperations: Calling super.doRefresh()");
    super.doRefresh();
  }
}
