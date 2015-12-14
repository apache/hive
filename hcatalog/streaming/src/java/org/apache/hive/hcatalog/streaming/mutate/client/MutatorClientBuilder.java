/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.hcatalog.streaming.mutate.client;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.streaming.mutate.HiveConfFactory;
import org.apache.hive.hcatalog.streaming.mutate.UgiMetaStoreClientFactory;
import org.apache.hive.hcatalog.streaming.mutate.client.lock.Lock;
import org.apache.hive.hcatalog.streaming.mutate.client.lock.LockFailureListener;

/** Convenience class for building {@link MutatorClient} instances. */
public class MutatorClientBuilder {

  private final Map<String, AcidTable> tables = new HashMap<>();
  private HiveConf configuration;
  private UserGroupInformation authenticatedUser;
  private String metaStoreUri;
  public LockFailureListener lockFailureListener;

  public MutatorClientBuilder configuration(HiveConf conf) {
    this.configuration = conf;
    return this;
  }

  public MutatorClientBuilder authenticatedUser(UserGroupInformation authenticatedUser) {
    this.authenticatedUser = authenticatedUser;
    return this;
  }

  public MutatorClientBuilder metaStoreUri(String metaStoreUri) {
    this.metaStoreUri = metaStoreUri;
    return this;
  }

  /** Set a listener to handle {@link Lock} failure events - highly recommended. */
  public MutatorClientBuilder lockFailureListener(LockFailureListener lockFailureListener) {
    this.lockFailureListener = lockFailureListener;
    return this;
  }

  /**
   * Adds a mutation event destination (an ACID table) to be managed by this client, which is either unpartitioned or
   * will is not to have partitions created automatically.
   */
  public MutatorClientBuilder addSourceTable(String databaseName, String tableName) {
    addTable(databaseName, tableName, false, TableType.SOURCE);
    return this;
  }

  /**
   * Adds a mutation event destination (an ACID table) to be managed by this client, which is either unpartitioned or
   * will is not to have partitions created automatically.
   */
  public MutatorClientBuilder addSinkTable(String databaseName, String tableName) {
    return addSinkTable(databaseName, tableName, false);
  }

  /**
   * Adds a partitioned mutation event destination (an ACID table) to be managed by this client, where new partitions
   * will be created as needed.
   */
  public MutatorClientBuilder addSinkTable(String databaseName, String tableName, boolean createPartitions) {
    addTable(databaseName, tableName, createPartitions, TableType.SINK);
    return this;
  }

  private void addTable(String databaseName, String tableName, boolean createPartitions, TableType tableType) {
    if (databaseName == null) {
      throw new IllegalArgumentException("Database cannot be null");
    }
    if (tableName == null) {
      throw new IllegalArgumentException("Table cannot be null");
    }
    String key = (databaseName + "." + tableName).toUpperCase();
    AcidTable previous = tables.get(key);
    if (previous != null) {
      if (tableType == TableType.SINK && previous.getTableType() != TableType.SINK) {
        tables.remove(key);
      } else {
        throw new IllegalArgumentException("Table has already been added: " + databaseName + "." + tableName);
      }
    }

    Table table = new Table();
    table.setDbName(databaseName);
    table.setTableName(tableName);
    tables.put(key, new AcidTable(databaseName, tableName, createPartitions, tableType));
  }

  /** Builds the client. */
  public MutatorClient build() throws ClientException, MetaException {
    String user = authenticatedUser == null ? System.getProperty("user.name") : authenticatedUser.getShortUserName();
    boolean secureMode = authenticatedUser == null ? false : authenticatedUser.hasKerberosCredentials();

    configuration = HiveConfFactory.newInstance(configuration, this.getClass(), metaStoreUri);

    IMetaStoreClient metaStoreClient;
    try {
      metaStoreClient = new UgiMetaStoreClientFactory(metaStoreUri, configuration, authenticatedUser, user, secureMode)
          .newInstance(HCatUtil.getHiveMetastoreClient(configuration));
    } catch (IOException e) {
      throw new ClientException("Could not create meta store client.", e);
    }

    return new MutatorClient(metaStoreClient, configuration, lockFailureListener, user, tables.values());
  }

}
