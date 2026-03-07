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
package org.apache.hadoop.hive.ql.exec.tez;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ExternalSessionsRegistry {
  Logger LOG = LoggerFactory.getLogger(ExternalSessionsRegistry.class);

  /**
   * Returns application of id of the external session.
   * @return application id
   * @throws Exception in case of any exceptions
   */
  String getSession() throws Exception;

  /**
   * Returns external session back to registry.
   * @param appId application id
   */
  void returnSession(String appId);

  /**
   * Closes the external session registry
   */
  void close();

  Map<String, ExternalSessionsRegistry> INSTANCES = new HashMap<>();

  static ExternalSessionsRegistry getClient(final Configuration conf) {
    ExternalSessionsRegistry registry;
    synchronized (INSTANCES) {
      // TODO: change this to TezConfiguration.TEZ_AM_REGISTRY_NAMESPACE after Tez 1.0.0 is released
      String namespace = conf.get("tez.am.registry.namespace");
      // HS2 would need to know about all coordinators running on all compute groups for a given compute (namespace)
      // Setting this config to false in client, will make registry client listen on paths under @compute instead of
      // @compute/compute-group
      // TODO: change this to TezConfiguration.TEZ_AM_REGISTRY_ENABLE_COMPUTE_GROUPS after Tez 1.0.0 is released
      conf.setBoolean("tez.am.registry.enable.compute.groups", false);
      registry = INSTANCES.get(namespace);
      String clazz = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_REGISTRY_CLASS);
      if (registry == null) {
        try {
          registry = JavaUtils.newInstance(JavaUtils.getClass(clazz, ExternalSessionsRegistry.class),
            new Class<?>[]{HiveConf.class}, new Object[]{conf});
        } catch (MetaException e) {
          throw new RuntimeException(e);
        }
        INSTANCES.put(namespace, registry);
      }
      LOG.info("Returning tez external AM registry ({}) for namespace '{}'", System.identityHashCode(registry),
        namespace);
    }
    return registry;
  }
}
