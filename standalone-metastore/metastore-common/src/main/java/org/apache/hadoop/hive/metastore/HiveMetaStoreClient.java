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

package org.apache.hadoop.hive.metastore;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.CreateTableRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.client.HookMetaStoreClientProxy;
import org.apache.hadoop.hive.metastore.client.BaseMetaStoreClientProxy;
import org.apache.hadoop.hive.metastore.client.ThriftHiveMetaStoreClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hive Metastore Client.
 * The public implementation of IMetaStoreClient. Methods not inherited from IMetaStoreClient
 * are not public and can change. Hence this is marked as unstable.
 * For users who require retry mechanism when the connection between metastore and client is
 * broken, RetryingMetaStoreClient class should be used.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HiveMetaStoreClient extends BaseMetaStoreClientProxy
    implements IMetaStoreClient, AutoCloseable {
  public static final String MANUALLY_INITIATED_COMPACTION = "manual";
  public static final String TRUNCATE_SKIP_DATA_DELETION = "truncateSkipDataDeletion";
  public static final String SKIP_DROP_PARTITION = "dropPartitionSkip";
  public static final String RENAME_PARTITION_MAKE_COPY = "renamePartitionMakeCopy";

  private final ThriftHiveMetaStoreClient thriftClient;

  public HiveMetaStoreClient(Configuration conf) throws MetaException {
    this(conf, null, true);
  }

  public HiveMetaStoreClient(Configuration conf, HiveMetaHookLoader hookLoader) throws MetaException {
    this(conf, hookLoader, true);
  }

  public HiveMetaStoreClient(Configuration conf, HiveMetaHookLoader hookLoader, Boolean allowEmbedded)
    throws MetaException {
    super(createUnderlyingClient(conf, hookLoader, allowEmbedded), conf);
    HookMetaStoreClientProxy hookProxy = (HookMetaStoreClientProxy) getDelegate();
    this.thriftClient = (ThriftHiveMetaStoreClient) hookProxy.getDelegate();
  }

  private static IMetaStoreClient createUnderlyingClient(Configuration conf, HiveMetaHookLoader hookLoader,
      Boolean allowEmbedded) throws MetaException {
    IMetaStoreClient thriftClient = new ThriftHiveMetaStoreClient(conf, allowEmbedded);
    IMetaStoreClient clientWithHook = new HookMetaStoreClientProxy(conf, hookLoader, thriftClient);
    return clientWithHook;
  }

  public boolean createType(Type type) throws TException {
    return thriftClient.createType(type);
  }

  public boolean dropType(String type) throws TException {
    return thriftClient.dropType(type);
  }

  public Type getType(String name) throws TException {
    return thriftClient.getType(name);
  }

  public Map<String, Type> getTypeAll(String name) throws TException {
    return thriftClient.getTypeAll(name);
  }

  public void createTable(Table tbl, EnvironmentContext envContext) throws TException {
    CreateTableRequest request = new CreateTableRequest(tbl);
    if (envContext != null) {
      request.setEnvContext(envContext);
    }
    createTable(request);
  }

  public void dropTable(String catName, String dbname, String name, boolean deleteData,
      boolean ignoreUnknownTab, EnvironmentContext envContext) throws TException {
    thriftClient.dropTable(catName, dbname, name, deleteData, ignoreUnknownTab, envContext);
  }

  public Partition add_partition(Partition new_part, EnvironmentContext envContext) throws TException {
    return thriftClient.add_partition(new_part, envContext);
  }

  public Partition appendPartition(String dbName, String tableName, List<String> partVals,
      EnvironmentContext ec) throws TException {
    return thriftClient.appendPartition(dbName, tableName, partVals, ec);
  }

  public Partition appendPartitionByName(String dbName, String tableName, String partName) throws TException {
    return thriftClient.appendPartitionByName(dbName, tableName, partName);
  }

  public Partition appendPartitionByName(String dbName, String tableName, String partName,
      EnvironmentContext envContext) throws TException {
    return thriftClient.appendPartitionByName(dbName, tableName, partName, envContext);
  }

  public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals,
      EnvironmentContext env_context) throws TException {
    return thriftClient.dropPartition(db_name, tbl_name, part_vals, env_context);
  }

  public boolean dropPartition(String dbName, String tableName, String partName, boolean dropData,
      EnvironmentContext ec) throws TException {
    return thriftClient.dropPartition(dbName, tableName, partName, dropData, ec);

  }

  public boolean dropPartition(String dbName, String tableName, List<String> partVals)
      throws TException {
    return thriftClient.dropPartition(dbName, tableName, partVals);
  }

  public boolean dropPartitionByName(String dbName, String tableName, String partName,
      boolean deleteData) throws TException {
    return thriftClient.dropPartitionByName(dbName, tableName, partName, deleteData);
  }

  public boolean dropPartitionByName(String dbName, String tableName, String partName,
      boolean deleteData, EnvironmentContext envContext) throws TException {
    return thriftClient.dropPartitionByName(dbName, tableName, partName, deleteData, envContext);

  }

  @VisibleForTesting
  public TTransport getTTransport() {
    return thriftClient.getTTransport();
  }

  @VisibleForTesting
  protected HttpClientBuilder createHttpClientBuilder() throws MetaException {
    return thriftClient.createHttpClientBuilder();
  }

  public static ThriftHiveMetastore.Iface callEmbeddedMetastore(Configuration conf) throws MetaException {
    return null;
  }

  @VisibleForTesting
  public static AtomicInteger getConnCount() {
    return ThriftHiveMetaStoreClient.getConnCount();
  }

  public static void setProcessorCapabilities(final String[] capabilities) {
    ThriftHiveMetaStoreClient.setProcessorCapabilities(capabilities);
  }

  public static void setProcessorIdentifier(final String id) {
    ThriftHiveMetaStoreClient.setProcessorIdentifier(id);
  }

  public static String[] getProcessorCapabilities() {
    return ThriftHiveMetaStoreClient.getProcessorCapabilities();
  }

  public static String getProcessorIdentifier() {
    return ThriftHiveMetaStoreClient.getProcessorIdentifier();
  }

  /**
   * Creates a synchronized wrapper for any {@link IMetaStoreClient}.
   * This may be used by multi-threaded applications until we have
   * fixed all reentrancy bugs.
   *
   * @param client unsynchronized client
   * @return synchronized client
   */
  public static IMetaStoreClient newSynchronizedClient(
      IMetaStoreClient client) {
    return (IMetaStoreClient) Proxy.newProxyInstance(
        ThriftHiveMetaStoreClient.class.getClassLoader(),
        new Class[]{IMetaStoreClient.class},
        new SynchronizedHandler(client));
  }

  private static class SynchronizedHandler implements InvocationHandler {
    private final IMetaStoreClient client;

    SynchronizedHandler(IMetaStoreClient client) {
      this.client = client;
    }

    @Override
    public synchronized Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      try {
        return method.invoke(client, args);
      } catch (InvocationTargetException e) {
        throw e.getTargetException();
      }
    }
  }
}
