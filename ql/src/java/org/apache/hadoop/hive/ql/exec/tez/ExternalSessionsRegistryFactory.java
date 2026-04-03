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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory for creating and managing {@code ExternalSessionsRegistry} clients.
 * <p>
 * Instances created by this factory are cached and may be reused across calls.
 * Callers do not need to manage the lifecycle or closure of these clients,
 * as this factory is responsible for handling those concerns.
 * </p>
 */
public abstract class ExternalSessionsRegistryFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ExternalSessionsRegistryFactory.class);

  private static final Map<String, ExternalSessionsRegistry> CLIENTS = new HashMap<>();

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> CLIENTS.values().forEach(ExternalSessionsRegistry::close)));
  }

  private ExternalSessionsRegistryFactory() {
  }

  public static ExternalSessionsRegistry getClient(final HiveConf conf) {
    HiveConf newConf = prepareConf(conf);
    // TODO: change this to TezConfiguration.TEZ_AM_REGISTRY_NAMESPACE after Tez 1.0.0 is released.
    String namespace = conf.get("tez.am.registry.namespace");

    ExternalSessionsRegistry registry;
    synchronized (CLIENTS) {
      registry = CLIENTS.computeIfAbsent(namespace, ns -> {
        try {
          String clazz = HiveConf.getVar(newConf, HiveConf.ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_REGISTRY_CLASS);

          return JavaUtils.newInstance(JavaUtils.getClass(clazz, ExternalSessionsRegistry.class),
              new Class<?>[]{HiveConf.class}, new Object[]{newConf});
        } catch (MetaException e) {
          throw new RuntimeException(e);
        }
      });
    }
    LOG.info("Returning tez external AM registry ({}) for namespace '{}'", System.identityHashCode(registry),
        namespace);
    return registry;
  }

  private static HiveConf prepareConf(HiveConf conf) {
    // HS2 would need to know about all coordinators running on all compute groups for a given compute (namespace)
    // Setting this config to false in client, will make registry client listen on paths under @compute instead of
    // @compute/compute-group
    HiveConf newConf = new HiveConf(conf, ExternalSessionsRegistryFactory.class);

    // TODO: change this to TezConfiguration.TEZ_AM_REGISTRY_ENABLE_COMPUTE_GROUPS after Tez 1.0.0 is released.
    newConf.setBoolean("tez.am.registry.enable.compute.groups", false);
    return newConf;
  }
}
