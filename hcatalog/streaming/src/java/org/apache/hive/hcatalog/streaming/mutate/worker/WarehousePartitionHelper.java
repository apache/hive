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
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;

/**
 * A {@link PartitionHelper} implementation that uses the {@link Warehouse} class to obtain partition path information.
 * As this does not require a connection to the meta store database it is safe to use in workers that are distributed on
 * a cluster. However, it does not support the creation of new partitions so you will need to provide a mechanism to
 * collect affected partitions in your merge job and create them from your client.
 */
class WarehousePartitionHelper implements PartitionHelper {

  private final Warehouse warehouse;
  private final Path tablePath;
  private final LinkedHashMap<String, String> partitions;
  private final List<String> partitionColumns;

  WarehousePartitionHelper(Configuration configuration, Path tablePath, List<String> partitionColumns)
      throws MetaException {
    this.tablePath = tablePath;
    this.partitionColumns = partitionColumns;
    this.partitions = new LinkedHashMap<>(partitionColumns.size());
    for (String partitionColumn : partitionColumns) {
      partitions.put(partitionColumn, null);
    }
    warehouse = new Warehouse(configuration);
  }

  @Override
  public Path getPathForPartition(List<String> partitionValues) throws WorkerException {
    if (partitionValues.size() != partitionColumns.size()) {
      throw new IllegalArgumentException("Incorrect number of partition values. columns=" + partitionColumns
          + ",values=" + partitionValues);
    }
    if (partitionColumns.isEmpty()) {
      return tablePath;
    }
    for (int columnIndex = 0; columnIndex < partitionValues.size(); columnIndex++) {
      String partitionColumn = partitionColumns.get(columnIndex);
      String partitionValue = partitionValues.get(columnIndex);
      partitions.put(partitionColumn, partitionValue);
    }
    try {
      return warehouse.getPartitionPath(tablePath, partitions);
    } catch (MetaException e) {
      throw new WorkerException("Unable to determine partition path. tablePath=" + tablePath + ",partition="
          + partitionValues, e);
    }
  }

  /** Throws {@link UnsupportedOperationException}. */
  @Override
  public void createPartitionIfNotExists(List<String> newPartitionValues) throws WorkerException {
    throw new UnsupportedOperationException("You require a connection to the meta store to do this.");
  }

  @Override
  public void close() throws IOException {
    // Nothing to close here.
  }

}
