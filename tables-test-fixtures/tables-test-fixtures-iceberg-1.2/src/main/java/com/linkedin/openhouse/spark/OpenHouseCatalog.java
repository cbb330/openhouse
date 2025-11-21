package com.linkedin.openhouse.spark;

/**
 * Minimal catalog shim that delegates to the OpenHouse Java client implementation so Spark-based
 * Iceberg tests can reference {@code com.linkedin.openhouse.spark.OpenHouseCatalog} without
 * pulling in the full Spark runtime module.
 */
public class OpenHouseCatalog extends com.linkedin.openhouse.javaclient.OpenHouseCatalog {}

