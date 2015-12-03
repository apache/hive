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
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PartitionHelper} implementation that uses the {@link IMetaStoreClient meta store} to both create partitions
 * and obtain information concerning partitions. Exercise care when using this from within workers that are running in a
 * cluster as it may overwhelm the meta store database instance. As an alternative, consider using the
 * {@link WarehousePartitionHelper}, collecting the affected partitions as an output of your merge job, and then
 * retrospectively adding partitions in your client.
 */
class MetaStorePartitionHelper implements PartitionHelper {

  private static final Logger LOG = LoggerFactory.getLogger(MetaStorePartitionHelper.class);

  private final IMetaStoreClient metaStoreClient;
  private final String databaseName;
  private final String tableName;
  private final Path tablePath;

  MetaStorePartitionHelper(IMetaStoreClient metaStoreClient, String databaseName, String tableName, Path tablePath) {
    this.metaStoreClient = metaStoreClient;
    this.tablePath = tablePath;
    this.databaseName = databaseName;
    this.tableName = tableName;
  }

  /** Returns the expected {@link Path} for a given partition value. */
  @Override
  public Path getPathForPartition(List<String> newPartitionValues) throws WorkerException {
    if (newPartitionValues.isEmpty()) {
      LOG.debug("Using path {} for unpartitioned table {}.{}", tablePath, databaseName, tableName);
      return tablePath;
    } else {
      try {
        String location = metaStoreClient
            .getPartition(databaseName, tableName, newPartitionValues)
            .getSd()
            .getLocation();
        LOG.debug("Found path {} for partition {}", location, newPartitionValues);
        return new Path(location);
      } catch (NoSuchObjectException e) {
        throw new WorkerException("Table not found '" + databaseName + "." + tableName + "'.", e);
      } catch (TException e) {
        throw new WorkerException("Failed to get path for partitions '" + newPartitionValues + "' on table '"
            + databaseName + "." + tableName + "' with meta store: " + metaStoreClient, e);
      }
    }
  }

  /** Creates the specified partition if it does not already exist. Does nothing if the table is unpartitioned. */
  @Override
  public void createPartitionIfNotExists(List<String> newPartitionValues) throws WorkerException {
    if (newPartitionValues.isEmpty()) {
      return;
    }

    try {
      LOG.debug("Attempting to create partition (if not exists) {}.{}:{}", databaseName, tableName, newPartitionValues);
      Table table = metaStoreClient.getTable(databaseName, tableName);

      Partition partition = new Partition();
      partition.setDbName(table.getDbName());
      partition.setTableName(table.getTableName());
      StorageDescriptor partitionSd = new StorageDescriptor(table.getSd());
      partitionSd.setLocation(table.getSd().getLocation() + Path.SEPARATOR
          + Warehouse.makePartName(table.getPartitionKeys(), newPartitionValues));
      partition.setSd(partitionSd);
      partition.setValues(newPartitionValues);

      metaStoreClient.add_partition(partition);
    } catch (AlreadyExistsException e) {
      LOG.debug("Partition already exisits: {}.{}:{}", databaseName, tableName, newPartitionValues);
    } catch (NoSuchObjectException e) {
      LOG.error("Failed to create partition : " + newPartitionValues, e);
      throw new PartitionCreationException("Table not found '" + databaseName + "." + tableName + "'.", e);
    } catch (TException e) {
      LOG.error("Failed to create partition : " + newPartitionValues, e);
      throw new PartitionCreationException("Failed to create partition '" + newPartitionValues + "' on table '"
          + databaseName + "." + tableName + "'", e);
    }
  }

  @Override
  public void close() throws IOException {
    metaStoreClient.close();
  }

}
