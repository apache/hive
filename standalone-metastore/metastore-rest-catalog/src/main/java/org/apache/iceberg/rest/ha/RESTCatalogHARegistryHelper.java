/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.rest.ha;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.registry.impl.ZkRegistryBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Helper class for accessing REST Catalog HA Registry.
 * Provides cached access to RESTCatalogHARegistry instances for service discovery.
 * 
 * This is NOT a REST Catalog client - it's a helper utility for accessing the registry.
 * Similar to HS2ActivePassiveHARegistryClient.
 */
public class RESTCatalogHARegistryHelper {
  private static final Logger LOG = LoggerFactory.getLogger(RESTCatalogHARegistryHelper.class);
  private static final Map<String, RESTCatalogHARegistry> registries = new HashMap<>();

  /**
   * Get a REST Catalog HA Registry instance to read from the registry.
   * Only used for service discovery to connect to active REST Catalog instances.
   *
   * @param conf Configuration instance which contains service registry information
   * @return RESTCatalogHARegistry instance for reading from the registry
   * @throws IOException if registry creation fails
   */
  public static synchronized RESTCatalogHARegistry getRegistry(Configuration conf) throws IOException {
    String namespace = MetastoreConf.getVar(conf, ConfVars.REST_CATALOG_HA_REGISTRY_NAMESPACE);
    Preconditions.checkArgument(!StringUtils.isBlank(namespace),
        ConfVars.REST_CATALOG_HA_REGISTRY_NAMESPACE.getVarname() + " cannot be null or empty");
    String nsKey = ZkRegistryBase.getRootNamespace(conf, null, namespace + "-");
    RESTCatalogHARegistry registry = registries.get(nsKey);
    if (registry == null) {
      registry = RESTCatalogHARegistry.create(conf, true);
      registry.start();
      registries.put(nsKey, registry);
      LOG.info("Added REST Catalog registry to cache with namespace: {}", nsKey);
    } else {
      LOG.info("Returning cached REST Catalog registry for namespace: {}", nsKey);
    }
    return registry;
  }
}

