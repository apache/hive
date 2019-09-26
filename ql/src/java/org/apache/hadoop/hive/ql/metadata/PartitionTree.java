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

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.metastore.Warehouse.makePartName;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.makePartNameMatcher;

/**
 * Always clone objects before adding or returning them so that callers don't modify them
 * via references.
 */
final class PartitionTree {
  private final Map<String, org.apache.hadoop.hive.metastore.api.Partition> parts = new LinkedHashMap<>();
  private final org.apache.hadoop.hive.metastore.api.Table tTable;

  PartitionTree(org.apache.hadoop.hive.metastore.api.Table t) {
    this.tTable = t;
  }

  Partition addPartition(Partition partition, String partName, boolean ifNotExists) throws AlreadyExistsException {
    partition.setDbName(partition.getDbName().toLowerCase());
    partition.setTableName(partition.getTableName().toLowerCase());
    if (!ifNotExists && parts.containsKey(partName)) {
      throw new AlreadyExistsException("Partition " + partName + " already exists");
    }
    return parts.putIfAbsent(partName, partition);
  }

  /**
   * @param partName - "p=1/q=2" full partition name {@link Warehouse#makePartName(List, List)}
   * @return null if doesn't exist
   */
  Partition getPartition(String partName) {
    return parts.get(partName);
  }

  /**
   * Get a partition matching the partition values.
   *
   * @param partVals partition values for this partition, must be in the same order as the
   *                 partition keys of the table.
   * @return the partition object, or if not found null.
   * @throws MetaException
   */
  Partition getPartition(List<String> partVals) throws MetaException {
    String partName = makePartName(tTable.getPartitionKeys(), partVals);
    return getPartition(partName);
  }

  /**
   * Add partitions to the partition tree.
   *
   * @param partitions  The partitions to add
   * @param ifNotExists only add partitions if they don't exist
   * @return the partitions that were added
   * @throws MetaException
   */
  List<Partition> addPartitions(List<Partition> partitions, boolean ifNotExists)
      throws MetaException, AlreadyExistsException {
    List<Partition> partitionsAdded = new ArrayList<>();
    Map<String, Partition> partNameToPartition = new HashMap<>();
    // validate that the new partition values is not already added to the table
    for (Partition partition : partitions) {
      String partName = makePartName(tTable.getPartitionKeys(), partition.getValues());
      if (!ifNotExists && parts.containsKey(partName)) {
        throw new AlreadyExistsException("Partition " + partName + " already exists");
      }
      partNameToPartition.put(partName, partition);
    }

    for (Map.Entry<String, Partition> entry : partNameToPartition.entrySet()) {
      if (addPartition(entry.getValue(), entry.getKey(), ifNotExists) == null) {
        partitionsAdded.add(entry.getValue());
      }
    }

    return partitionsAdded;
  }

  /**
   * Provided values for the 1st N partition columns, will return all matching PartitionS
   * The list is a partial list of partition values in the same order as partition columns.
   * Missing values should be represented as "" (empty strings).  May provide fewer values.
   * So if part cols are a,b,c, {"",2} is a valid list
   * {@link MetaStoreUtils#getPvals(List, Map)}
   */
  List<Partition> getPartitionsByPartitionVals(List<String> partialPartVals) throws MetaException {
    if (partialPartVals == null || partialPartVals.isEmpty()) {
      throw new MetaException("Partition partial vals cannot be null or empty");
    }
    String partNameMatcher = makePartNameMatcher(tTable, partialPartVals, ".*");
    List<Partition> matchedPartitions = new ArrayList<>();
    for (Map.Entry<String, Partition> entry : parts.entrySet()) {
      if (entry.getKey().matches(partNameMatcher)) {
        matchedPartitions.add(entry.getValue());
      }
    }
    return matchedPartitions;
  }

  /**
   * Get all the partitions.
   *
   * @return partitions list
   */
  List<Partition> listPartitions() {
    return new ArrayList<>(parts.values());
  }

  /**
   * Remove a partition from the table.
   * @param partVals partition values, must be not null
   * @return the instance of the dropped partition, if the remove was successful, otherwise false
   * @throws MetaException
   */
  Partition dropPartition(List<String> partVals) throws MetaException, NoSuchObjectException {
    String partName = makePartName(tTable.getPartitionKeys(), partVals);
    if (!parts.containsKey(partName)) {
      throw new NoSuchObjectException(
          "Partition with partition values " + Arrays.toString(partVals.toArray()) + " is not found.");
    }
    return parts.remove(partName);
  }
}
