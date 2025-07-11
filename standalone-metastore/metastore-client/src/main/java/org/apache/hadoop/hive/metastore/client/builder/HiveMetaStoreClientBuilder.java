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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.client.HookEnabledMetaStoreClient;
import org.apache.hadoop.hive.metastore.client.SynchronizedMetaStoreClient;
import org.apache.hadoop.hive.metastore.client.ThriftHiveMetaStoreClient;

import java.util.Map;
import java.util.function.Function;

public class HiveMetaStoreClientBuilder {

    private final Configuration conf;
    private IMetaStoreClient client;

    public HiveMetaStoreClientBuilder(Configuration conf) {
        Preconditions.checkNotNull(conf);
        this.conf = conf;
    }

    public HiveMetaStoreClientBuilder newClient() throws MetaException {
        this.client = new HiveMetaStoreClient(conf);
        return this;
    }

    public HiveMetaStoreClientBuilder newThriftClient(boolean allowEmbedded) throws MetaException {
        this.client = ThriftHiveMetaStoreClient.newClient(conf, allowEmbedded);
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
        Preconditions.checkNotNull(conf);
        return client;
    }
}
