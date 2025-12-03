/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.openhouse;

import com.linkedin.openhouse.javaclient.OpenHouseCatalog;
import com.linkedin.openhouse.javaclient.OpenHouseShimTableOperations;
import com.linkedin.openhouse.javaclient.OpenHouseTableOperations;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A test-only subclass of OpenHouseCatalog that shims namespace operations in-memory. OpenHouse
 * service does not support explicit CREATE/DROP NAMESPACE (databases are implicit), but standard
 * Iceberg tests (TestNamespaceSQL, etc.) require these operations.
 */
public class OpenHouseShimCatalog extends OpenHouseCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(OpenHouseShimCatalog.class);
  // Persist namespaces in-memory across catalog instantiations to simulate a metastore
  private static final Set<Namespace> IN_MEMORY_NAMESPACES =
      Collections.synchronizedSet(new HashSet<>());

  static {
    IN_MEMORY_NAMESPACES.add(Namespace.of("default"));
  }

  public OpenHouseShimCatalog() {
    super();
    System.out.println("DEBUG: OpenHouseShimCatalog: Constructor called");
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    super.initialize(name, properties);
  }

  @Override
  public TableOperations newTableOps(TableIdentifier tableIdentifier) {
    OpenHouseTableOperations ops = (OpenHouseTableOperations) super.newTableOps(tableIdentifier);
    return new OpenHouseShimTableOperations(ops);
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> map) {
    LOG.info("Shim: Creating namespace in-memory: {}", namespace);
    IN_MEMORY_NAMESPACES.add(namespace);
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    LOG.info("Shim: Dropping namespace in-memory: {}", namespace);
    // We check for existence in our set AND in the server (via super)
    // Note: super.namespaceExists delegates to listNamespaces, which we override.
    // But we can call super.listNamespaces().
    boolean existsInServer = false;
    try {
      existsInServer = super.listNamespaces().contains(namespace);
    } catch (Exception e) {
      // ignore
    }

    if (!IN_MEMORY_NAMESPACES.contains(namespace) && !existsInServer) {
      return false;
    }

    // Check if namespace is empty (no tables)
    try {
      if (!listTables(namespace).isEmpty()) {
        throw new NamespaceNotEmptyException("Namespace " + namespace + " is not empty");
      }
    } catch (Exception e) {
      // Ignore errors checking for tables (e.g. if OH throws on non-existent namespace)
    }

    return IN_MEMORY_NAMESPACES.remove(namespace);
  }

  @Override
  public List<Namespace> listNamespaces() {
    Set<Namespace> all = new HashSet<>(IN_MEMORY_NAMESPACES);
    try {
      all.addAll(super.listNamespaces());
    } catch (Exception e) {
      // Ignore failure to list from server
    }
    return new ArrayList<>(all);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    if (IN_MEMORY_NAMESPACES.contains(namespace)) {
      return Collections.emptyMap();
    }
    return super.loadNamespaceMetadata(namespace);
  }
}
