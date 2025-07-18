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

package org.apache.hadoop.hive.metastore.client.builder;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.client.HookEnabledMetaStoreClient;
import org.apache.hadoop.hive.metastore.client.SynchronizedMetaStoreClient;
import org.apache.hadoop.hive.metastore.client.ThriftHiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class HiveMetaStoreClientBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(HiveMetaStoreClientBuilder.class);

  private final Configuration conf;
  private IMetaStoreClient client;

  public HiveMetaStoreClientBuilder(Configuration conf) {
    this.conf = Objects.requireNonNull(conf);
  }

  public HiveMetaStoreClientBuilder newClient(boolean allowEmbedded) throws MetaException {
    this.client = createClient(conf, allowEmbedded);
    return this;
  }

  public HiveMetaStoreClientBuilder client(IMetaStoreClient client) {
    this.client = client;
    return this;
  }

  public HiveMetaStoreClientBuilder enhanceWith(Function<IMetaStoreClient, IMetaStoreClient> wrapperFunction) {
    client = wrapperFunction.apply(client);
    return this;
  }

  public HiveMetaStoreClientBuilder withHooks(HiveMetaHookLoader hookLoader) {
    this.client = HookEnabledMetaStoreClient.newClient(conf, hookLoader, client);
    return this;
  }

  public HiveMetaStoreClientBuilder withRetry(Map<String, Long> metaCallTimeMap) throws MetaException {
    client = RetryingMetaStoreClient.getProxy(conf, metaCallTimeMap, client);
    return this;
  }

  public HiveMetaStoreClientBuilder threadSafe() {
    this.client = SynchronizedMetaStoreClient.newClient(conf, client);
    return this;
  }

  public IMetaStoreClient build() {
    return Objects.requireNonNull(client);
  }

  public static IMetaStoreClient createClient(Configuration conf, boolean allowEmbedded) throws MetaException {
    Class<? extends IMetaStoreClient> mscClass = MetastoreConf.getClass(
        conf, MetastoreConf.ConfVars.METASTORE_CLIENT_CLASS,
        ThriftHiveMetaStoreClient.class, IMetaStoreClient.class);
    LOG.info("Using {} as a base MetaStoreClient", mscClass.getName());

    IMetaStoreClient baseMetaStoreClient = null;
    try {
      baseMetaStoreClient = JavaUtils.newInstance(mscClass,
          new Class[]{Configuration.class, boolean.class},
          new Object[]{conf, allowEmbedded});
    } catch (Throwable t) {
      // Reflection by JavaUtils will throw RuntimeException, try to get real MetaException here.
      Throwable rootCause = ExceptionUtils.getRootCause(t);
      if (rootCause instanceof MetaException) {
        throw (MetaException) rootCause;
      } else {
        throw new MetaException(rootCause.getMessage());
      }
    }

    return baseMetaStoreClient;
  }
}
