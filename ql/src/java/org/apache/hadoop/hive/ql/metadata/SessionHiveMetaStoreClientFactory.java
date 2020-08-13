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

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

/**
 * Default MetaStoreClientFactory for Hive which produces SessionHiveMetaStoreClient objects.
 *
 */
public final class SessionHiveMetaStoreClientFactory implements HiveMetaStoreClientFactory {

  @Override
  public IMetaStoreClient createMetaStoreClient(HiveConf conf, HiveMetaHookLoader hookLoader,
      boolean allowEmbedded,
      ConcurrentHashMap<String, Long> metaCallTimeMap) throws MetaException {

    checkNotNull(conf, "conf cannot be null!");
    checkNotNull(hookLoader, "hookLoader cannot be null!");
    checkNotNull(metaCallTimeMap, "metaCallTimeMap cannot be null!");

    if (conf.getBoolVar(ConfVars.METASTORE_FASTPATH)) {
      return new SessionHiveMetaStoreClient(conf, hookLoader, allowEmbedded);
    } else {
      return RetryingMetaStoreClient.getProxy(conf, hookLoader, metaCallTimeMap,
          SessionHiveMetaStoreClient.class.getName(), allowEmbedded);
    }
  }
}
