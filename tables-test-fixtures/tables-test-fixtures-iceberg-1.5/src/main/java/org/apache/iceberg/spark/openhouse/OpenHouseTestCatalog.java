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
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A test-only subclass of OpenHouseCatalog that overrides createNamespace to be a no-op instead of
 * throwing UnsupportedOperationException. This is necessary because standard Iceberg tests (like
 * TestBaseWithCatalog) unconditionally attempt to create a 'default' namespace, which OpenHouse
 * does not support creating explicitly.
 */
public class OpenHouseTestCatalog extends OpenHouseCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(OpenHouseTestCatalog.class);

  static {
    System.out.println("STATIC DEBUG: OpenHouseTestCatalog class loaded!");
  }

  public OpenHouseTestCatalog() {
    super();
    System.out.println("DEBUG: OpenHouseTestCatalog instantiated!");
    LOG.info("OpenHouseTestCatalog instantiated!");
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    LOG.info("Initializing OpenHouseTestCatalog with properties: {}", properties);
    super.initialize(name, properties);
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> map) {
    // No-op: OpenHouse doesn't support createNamespace, but tests require it to succeed (or at
    // least not fail)
    // for the 'default' namespace setup.
    LOG.info("Ignoring createNamespace request for: {}", namespace);
  }
}
