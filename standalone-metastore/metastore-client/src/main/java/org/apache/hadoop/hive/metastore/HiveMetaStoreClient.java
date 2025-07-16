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
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.client.MetaStoreClientWrapper;
import org.apache.hadoop.hive.metastore.client.ThriftHiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.client.builder.HiveMetaStoreClientBuilder;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;

/**
 * Hive Metastore Client.
 * The public implementation of IMetaStoreClient. Methods not inherited from IMetaStoreClient
 * are not public and can change. Hence this is marked as unstable.
 * For users who require retry mechanism when the connection between metastore and client is
 * broken, RetryingMetaStoreClient class should be used.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HiveMetaStoreClient extends MetaStoreClientWrapper implements IMetaStoreClient, AutoCloseable {
  public static final String MANUALLY_INITIATED_COMPACTION = "manual";
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
    this(conf, hookLoader, HiveMetaStoreClientBuilder.createClient(conf, allowEmbedded));
  }

  private HiveMetaStoreClient(Configuration conf, HiveMetaHookLoader hookLoader,
      IMetaStoreClient baseMetaStoreClient) {
    super(createUnderlyingClient(conf, hookLoader, baseMetaStoreClient), conf);

    if (baseMetaStoreClient instanceof ThriftHiveMetaStoreClient) {
      this.thriftClient = (ThriftHiveMetaStoreClient) baseMetaStoreClient;
    } else {
      this.thriftClient = null;
    }
  }

  private static IMetaStoreClient createUnderlyingClient(Configuration conf, HiveMetaHookLoader hookLoader,
      IMetaStoreClient baseMetaStoreClient) {
    return new HiveMetaStoreClientBuilder(conf)
       .client(baseMetaStoreClient)
       .withHooks(hookLoader)
       .threadSafe()
       .build();
  }

  // methods for test

  public boolean createType(Type type) throws TException {
    if (thriftClient != null) {
      return thriftClient.createType(type);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public boolean dropType(String type) throws TException {
    if (thriftClient != null) {
      return thriftClient.dropType(type);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public Type getType(String name) throws TException {
    if (thriftClient != null) {
      return thriftClient.getType(name);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public Map<String, Type> getTypeAll(String name) throws TException {
    if (thriftClient != null) {
      return thriftClient.getTypeAll(name);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public void createTable(Table tbl, EnvironmentContext envContext) throws TException {
    CreateTableRequest request = new CreateTableRequest(tbl);
    if (envContext != null) {
      request.setEnvContext(envContext);
    }
    createTable(request);
  }

  public Table getTable(String catName, String dbName, String tableName,
      boolean getColumnStats, String engine) throws TException {
    if (thriftClient != null) {
      return thriftClient.getTable(catName, dbName, tableName, getColumnStats, engine);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public void dropTable(String catName, String dbname, String name, boolean deleteData,
      boolean ignoreUnknownTab, EnvironmentContext envContext) throws TException {
    if (thriftClient != null) {
      thriftClient.dropTable(catName, dbname, name, deleteData, ignoreUnknownTab, envContext);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public Partition add_partition(Partition new_part, EnvironmentContext envContext) throws TException {
    if (thriftClient != null) {
      return thriftClient.add_partition(new_part, envContext);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public Partition appendPartition(String dbName, String tableName, List<String> partVals,
      EnvironmentContext ec) throws TException {
    if (thriftClient != null) {
      return thriftClient.appendPartition(dbName, tableName, partVals, ec);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public Partition appendPartitionByName(String dbName, String tableName, String partName) throws TException {
    if (thriftClient != null) {
      return thriftClient.appendPartitionByName(dbName, tableName, partName);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public Partition appendPartitionByName(String dbName, String tableName, String partName,
      EnvironmentContext envContext) throws TException {
    if (thriftClient != null) {
      return thriftClient.appendPartitionByName(dbName, tableName, partName, envContext);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals,
      EnvironmentContext env_context) throws TException {
    if (thriftClient != null) {
      return thriftClient.dropPartition(db_name, tbl_name, part_vals, env_context);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public boolean dropPartition(String dbName, String tableName, String partName, boolean dropData,
      EnvironmentContext ec) throws TException {
    if (thriftClient != null) {
      return thriftClient.dropPartition(dbName, tableName, partName, dropData, ec);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public boolean dropPartition(String dbName, String tableName, List<String> partVals)
      throws TException {
    if (thriftClient != null) {
      return thriftClient.dropPartition(dbName, tableName, partVals);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public boolean dropPartitionByName(String dbName, String tableName, String partName,
      boolean deleteData) throws TException {
    if (thriftClient != null) {
      return thriftClient.dropPartitionByName(dbName, tableName, partName, deleteData);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public boolean dropPartitionByName(String dbName, String tableName, String partName,
      boolean deleteData, EnvironmentContext envContext) throws TException {
    if (thriftClient != null) {
      return thriftClient.dropPartitionByName(dbName, tableName, partName, deleteData, envContext);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @VisibleForTesting
  public ThriftHiveMetaStoreClient getThriftClient() {
    return thriftClient;
  }

  // static members

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
}
