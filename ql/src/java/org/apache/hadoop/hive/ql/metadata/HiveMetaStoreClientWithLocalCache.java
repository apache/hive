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
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.client.HookEnabledMetaStoreClientProxy;
import org.apache.hadoop.hive.metastore.client.BaseMetaStoreClientProxy;
import org.apache.hadoop.hive.metastore.client.SynchronizedMetaStoreClientProxy;
import org.apache.hadoop.hive.metastore.client.ThriftHiveMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.client.LocalCachingMetaStoreClientProxy;

/**
 * This class introduces a caching layer in HS2 for metadata for some selected query APIs. It extends
 * HiveMetaStoreClient, and overrides some of its methods to add this feature.
 * Its design is simple, relying on snapshot information being queried to cache and invalidate the metadata.
 * It helps to reduce the time spent in compilation by using HS2 memory more effectively, and it allows to
 * improve HMS throughput for multi-tenant workloads by reducing the number of calls it needs to serve.
 */
public class HiveMetaStoreClientWithLocalCache extends BaseMetaStoreClientProxy {
  public static void init(Configuration conf) {
    LocalCachingMetaStoreClientProxy.init(conf);
  }

  public HiveMetaStoreClientWithLocalCache(Configuration conf) throws MetaException {
    this(conf, null, true);
  }

  public HiveMetaStoreClientWithLocalCache(Configuration conf, HiveMetaHookLoader hookLoader)
      throws MetaException {
    this(conf, hookLoader, true);
  }

  public HiveMetaStoreClientWithLocalCache(Configuration conf, HiveMetaHookLoader hookLoader,
      Boolean allowEmbedded) throws MetaException {
    super(createUnderlyingClient(conf, hookLoader, allowEmbedded), conf);
  }

  @SuppressWarnings("squid:S2095")
  private static IMetaStoreClient createUnderlyingClient(Configuration conf, HiveMetaHookLoader hookLoader,
      Boolean allowEmbedded) throws MetaException {
    IMetaStoreClient thriftClient = new ThriftHiveMetaStoreClient(conf, allowEmbedded);
    IMetaStoreClient clientWithLocalCache = new LocalCachingMetaStoreClientProxy(conf, thriftClient);
    IMetaStoreClient clientWithHook = new HookEnabledMetaStoreClientProxy(conf, hookLoader, clientWithLocalCache);
    IMetaStoreClient synchronizedClient = new SynchronizedMetaStoreClientProxy(conf, clientWithHook);
    return synchronizedClient;
  }
}
