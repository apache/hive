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
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.client.HookEnabledMetaStoreClient;
import org.apache.hadoop.hive.metastore.client.SynchronizedMetaStoreClient;
import org.apache.hadoop.hive.metastore.client.ThriftHiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class HiveMetaStoreClientBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(HiveMetaStoreClientBuilder.class);
  private static final Map<Class<? extends IMetaStoreClient>, MetaStoreClientFactory>
      CLIENT_FACTORIES = new ConcurrentHashMap<>();

  private final Configuration conf;
  private IMetaStoreClient client;

  public HiveMetaStoreClientBuilder(Configuration configuration, boolean allowEmbedded) throws MetaException {
    this.conf = new Configuration(Objects.requireNonNull(configuration));
    boolean isHiveClient = HiveMetaStoreClient.class.getName().equals(
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.METASTORE_CLIENT_IMPL));
    if (isHiveClient) {
      // Prevent stack overflow as HiveMetaStoreClient calls HiveMetaStoreClientBuilder to build the underlying client
      MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_CLIENT_IMPL, ThriftHiveMetaStoreClient.class.getName());
    }
    this.client = createClient(conf, allowEmbedded);
  }

  public HiveMetaStoreClientBuilder(Configuration conf, IMetaStoreClient client) {
    this.conf =  Objects.requireNonNull(conf);
    this.client = Objects.requireNonNull(client);
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

  private static IMetaStoreClient createClient(Configuration conf, boolean allowEmbedded) throws MetaException {
    try {
      Class<? extends IMetaStoreClient> mscClass = MetastoreConf.getClass(
          conf, MetastoreConf.ConfVars.METASTORE_CLIENT_IMPL,
          ThriftHiveMetaStoreClient.class, IMetaStoreClient.class);
      LOG.info("Using {} as a base MetaStoreClient", mscClass.getName());
      MetaStoreClientFactory factory = CLIENT_FACTORIES.get(mscClass);
      if (factory == null) {
        CLIENT_FACTORIES.put(mscClass, factory = new MetaStoreClientFactory(mscClass));
      }
      return factory.createClient(conf, allowEmbedded);
    } catch (Throwable t) {
      // Reflection by JavaUtils will throw RuntimeException, try to get real MetaException here.
      Throwable rootCause = ExceptionUtils.getRootCause(t);
      if (rootCause instanceof MetaException) {
        throw (MetaException) rootCause;
      } else {
        throw new MetaException(rootCause.getMessage());
      }
    }
  }

   private static class MetaStoreClientFactory {
    private Constructor<? extends IMetaStoreClient> bestMatchingCtr;
    private Function<Pair<Configuration, Boolean>, Object[]> argsTransformer;

    MetaStoreClientFactory(Class<? extends IMetaStoreClient> mscClass) {
      Constructor<? extends IMetaStoreClient> candidate =
          ConstructorUtils.getMatchingAccessibleConstructor(mscClass, Configuration.class, boolean.class);
      if (candidate != null) {
        this.bestMatchingCtr = candidate;
        this.argsTransformer = args -> new Object[] {args.getLeft(), (boolean) args.getRight()};
      } else if ((candidate = ConstructorUtils.getMatchingAccessibleConstructor(mscClass, Configuration.class,
          HiveMetaHookLoader.class, Boolean.class)) != null) {
        this.bestMatchingCtr = candidate;
        this.argsTransformer = args ->
            new Object[] {args.getLeft(), null, Boolean.valueOf(args.getRight())};
      } else if ((candidate = ConstructorUtils.getMatchingAccessibleConstructor(mscClass, Configuration.class)) != null) {
        this.bestMatchingCtr = candidate;
        this.argsTransformer = args -> new Object[] {args.getLeft()};
      }
      if (bestMatchingCtr == null) {
        throw new RuntimeException("No matching constructor found for this IMetaStoreClient " + mscClass);
      }
    }

    IMetaStoreClient createClient(Configuration conf, boolean allowEmbedded) throws Exception {
      return bestMatchingCtr.newInstance(argsTransformer.apply(Pair.of(conf, allowEmbedded)));
    }
  }
}
