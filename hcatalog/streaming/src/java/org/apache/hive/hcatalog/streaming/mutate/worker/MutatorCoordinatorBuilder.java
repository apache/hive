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
package org.apache.hive.hcatalog.streaming.mutate.worker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.streaming.mutate.HiveConfFactory;
import org.apache.hive.hcatalog.streaming.mutate.UgiMetaStoreClientFactory;
import org.apache.hive.hcatalog.streaming.mutate.client.AcidTable;

/** Convenience class for building {@link MutatorCoordinator} instances. */
public class MutatorCoordinatorBuilder {

  private HiveConf configuration;
  private MutatorFactory mutatorFactory;
  private UserGroupInformation authenticatedUser;
  private String metaStoreUri;
  private AcidTable table;
  private boolean deleteDeltaIfExists;

  public MutatorCoordinatorBuilder configuration(HiveConf configuration) {
    this.configuration = configuration;
    return this;
  }

  public MutatorCoordinatorBuilder authenticatedUser(UserGroupInformation authenticatedUser) {
    this.authenticatedUser = authenticatedUser;
    return this;
  }

  public MutatorCoordinatorBuilder metaStoreUri(String metaStoreUri) {
    this.metaStoreUri = metaStoreUri;
    return this;
  }

  /** Set the destination ACID table for this client. */
  public MutatorCoordinatorBuilder table(AcidTable table) {
    this.table = table;
    return this;
  }

  /**
   * If the delta file already exists, delete it. THis is useful in a MapReduce setting where a number of task retries
   * will attempt to write the same delta file.
   */
  public MutatorCoordinatorBuilder deleteDeltaIfExists() {
    this.deleteDeltaIfExists = true;
    return this;
  }

  public MutatorCoordinatorBuilder mutatorFactory(MutatorFactory mutatorFactory) {
    this.mutatorFactory = mutatorFactory;
    return this;
  }

  public MutatorCoordinator build() throws WorkerException, MetaException {
    configuration = HiveConfFactory.newInstance(configuration, this.getClass(), metaStoreUri);

    PartitionHelper partitionHelper;
    if (table.createPartitions()) {
      partitionHelper = newMetaStorePartitionHelper();
    } else {
      partitionHelper = newWarehousePartitionHelper();
    }

    return new MutatorCoordinator(configuration, mutatorFactory, partitionHelper, table, deleteDeltaIfExists);
  }

  private PartitionHelper newWarehousePartitionHelper() throws MetaException, WorkerException {
    String location = table.getTable().getSd().getLocation();
    Path tablePath = new Path(location);
    List<FieldSchema> partitionFields = table.getTable().getPartitionKeys();
    List<String> partitionColumns = new ArrayList<>(partitionFields.size());
    for (FieldSchema field : partitionFields) {
      partitionColumns.add(field.getName());
    }
    return new WarehousePartitionHelper(configuration, tablePath, partitionColumns);
  }

  private PartitionHelper newMetaStorePartitionHelper() throws MetaException, WorkerException {
    String user = authenticatedUser == null ? System.getProperty("user.name") : authenticatedUser.getShortUserName();
    boolean secureMode = authenticatedUser == null ? false : authenticatedUser.hasKerberosCredentials();
    try {
      IMetaStoreClient metaStoreClient = new UgiMetaStoreClientFactory(metaStoreUri, configuration, authenticatedUser,
          user, secureMode).newInstance(HCatUtil.getHiveMetastoreClient(configuration));
      String tableLocation = table.getTable().getSd().getLocation();
      Path tablePath = new Path(tableLocation);
      return new MetaStorePartitionHelper(metaStoreClient, table.getDatabaseName(), table.getTableName(), tablePath);
    } catch (IOException e) {
      throw new WorkerException("Could not create meta store client.", e);
    }
  }

}
