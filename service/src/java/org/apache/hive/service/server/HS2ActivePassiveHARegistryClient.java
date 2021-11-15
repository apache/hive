/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hive.service.server;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.registry.impl.ZkRegistryBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class HS2ActivePassiveHARegistryClient {
  private static final Logger LOG = LoggerFactory.getLogger(HS2ActivePassiveHARegistryClient.class);
  private static final Map<String, HS2ActivePassiveHARegistry> hs2Registries = new HashMap<>();

  /**
   * Helper method to get a HiveServer2HARegistry instance to read from the registry. Only used by clients (JDBC),
   * service discovery to connect to active HS2 instance in Active/Passive HA configuration.
   *
   * @param conf {@link Configuration} instance which contains service registry information.
   * @return HiveServer2HARegistry
   */
  public static synchronized HS2ActivePassiveHARegistry getClient(Configuration conf) throws IOException {
    String namespace = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_ACTIVE_PASSIVE_HA_REGISTRY_NAMESPACE);
    Preconditions.checkArgument(!StringUtils.isBlank(namespace),
      HiveConf.ConfVars.HIVE_SERVER2_ACTIVE_PASSIVE_HA_REGISTRY_NAMESPACE.varname + " cannot be null or empty");
    String nsKey = ZkRegistryBase.getRootNamespace(conf, null, namespace + "-");
    HS2ActivePassiveHARegistry registry = hs2Registries.get(nsKey);
    if (registry == null) {
      registry = HS2ActivePassiveHARegistry.create(conf, true);
      registry.start();
      hs2Registries.put(nsKey, registry);
      LOG.info("Added registry client to cache with namespace: {}", nsKey);
    } else {
      LOG.info("Returning cached registry client for namespace: {}", nsKey);
    }
    return registry;
  }
}
