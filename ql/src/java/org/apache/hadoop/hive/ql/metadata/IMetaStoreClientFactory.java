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

package org.apache.hadoop.hive.ql.metadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.client.HookEnabledMetaStoreClient;
import org.apache.hadoop.hive.metastore.client.SynchronizedMetaStoreClient;
import org.apache.hadoop.hive.metastore.client.ThriftHiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;

import java.util.concurrent.ConcurrentHashMap;

public class IMetaStoreClientFactory {

  public static final String CONNECTOR_NAME = "iceberg";

  public static IMetaStoreClient create(HiveConf hiveConf, boolean allowEmbedded,
      ConcurrentHashMap<String, Long> metaCallTimeMap, HiveMetaHookLoader hookLoader)  throws MetaException {

        if (CONNECTOR_NAME.equals(hiveConf.get("connector.name", "hms"))) {
          return getHiveIcebergRESTCatalogClient(hiveConf, hookLoader);
        }
        IMetaStoreClient thriftClient = ThriftHiveMetaStoreClient.newClient(hiveConf, allowEmbedded);
        IMetaStoreClient clientWithLocalCache = HiveMetaStoreClientWithLocalCache.newClient(hiveConf, thriftClient);
        IMetaStoreClient sessionLevelClient = SessionHiveMetaStoreClient.newClient(hiveConf, clientWithLocalCache);
        IMetaStoreClient clientWithHook = HookEnabledMetaStoreClient.newClient(hiveConf, hookLoader, sessionLevelClient);
    
        if (hiveConf.getBoolVar(HiveConf.ConfVars.METASTORE_FASTPATH)) {
          return SynchronizedMetaStoreClient.newClient(hiveConf, clientWithHook);
        } else {
          return RetryingMetaStoreClient.getProxy(
              hiveConf,
              new Class[] {Configuration.class, IMetaStoreClient.class},
              new Object[] {hiveConf, clientWithHook},
              metaCallTimeMap,
              SynchronizedMetaStoreClient.class.getName()
          );
        }
  }
  private static IMetaStoreClient getHiveIcebergRESTCatalogClient(HiveConf conf, HiveMetaHookLoader hookLoader) throws
      MetaException {
    try {
      Class<? extends IMetaStoreClient> handlerClass =
          (Class<? extends IMetaStoreClient>)
              Class.forName("org.apache.iceberg.hive.HiveIcebergRESTCatalogClientAdapter", true, Utilities.getSessionSpecifiedClassLoader());
      Class<?>[] constructorArgTypes = new Class[] { Configuration.class, HiveMetaHookLoader.class};
      Object[] constructorArgs = new Object[] {conf, hookLoader};
      IMetaStoreClient restCatalogMetastoreClient = JavaUtils.newInstance(handlerClass, constructorArgTypes, constructorArgs);
      restCatalogMetastoreClient.reconnect();
      return restCatalogMetastoreClient;
    } catch (ClassNotFoundException e) {
      throw new MetaException("Error in loading HiveIcebergRESTCatalogClientAdapter class."
          + e.getMessage());
    }
  }
}
