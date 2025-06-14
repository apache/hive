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
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;

import java.util.concurrent.ConcurrentHashMap;

public class IMetaStoreClientFactory {

  public static final String REST_METASTORE_TYPE = "rest";

  public static IMetaStoreClient create(HiveConf hiveConf, boolean allowEmbedded,
      ConcurrentHashMap<String, Long> metaCallTimeMap, HiveMetaHookLoader hookLoader)  throws MetaException {

        if (REST_METASTORE_TYPE.equals(hiveConf.get("metastore.type", "hms"))) {
          return getHiveIcebergRESTCatalog(hiveConf, hookLoader);
        }

        if (hiveConf.getBoolVar(HiveConf.ConfVars.METASTORE_FASTPATH)) {
          return new SessionHiveMetaStoreClient(hiveConf, hookLoader, allowEmbedded);
        } else {
          return RetryingMetaStoreClient.getProxy(hiveConf, hookLoader, metaCallTimeMap,
          SessionHiveMetaStoreClient.class.getName(), allowEmbedded);
    }
  }
  private static IMetaStoreClient getHiveIcebergRESTCatalog(HiveConf conf, HiveMetaHookLoader hookLoader) throws
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
