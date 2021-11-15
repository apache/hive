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
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.metastore.Warehouse.makePartName;
import static org.apache.hadoop.hive.metastore.Warehouse.makeSpecFromName;

/**
 * This stores partition information for a temp table.
 */
public final class TempTable {
  private final org.apache.hadoop.hive.metastore.api.Table tTable;
  private final PartitionTree pTree;

  private static final String EXTERNAL_PARAM = "EXTERNAL";

  TempTable(org.apache.hadoop.hive.metastore.api.Table t) {
    assert t != null;
    this.tTable = t;
    pTree = t.getPartitionKeysSize() > 0 ? new PartitionTree(tTable) : null;
  }

  Partition addPartition(Partition p) throws AlreadyExistsException, MetaException {
    String partName = makePartName(tTable.getPartitionKeys(), p.getValues());
    Partition partition = pTree.addPartition(p, partName, false);
    return partition == null ? pTree.getPartition(partName) : partition;
  }

  boolean isExternal() {
    return tTable.getParameters() != null && "true".equals(tTable.getParameters().get(EXTERNAL_PARAM));
  }

  Partition getPartition(String partName) throws MetaException {
    if (partName == null || partName.isEmpty()) {
      throw new MetaException("Partition name cannot be null or empty");
    }
    return pTree.getPartition(partName);
  }

  Partition getPartition(List<String> partVals) throws MetaException {
    if (partVals == null) {
      throw new MetaException("Partition values cannot be null");
    }
    return pTree.getPartition(partVals);
  }

  List<Partition> addPartitions(List<Partition> partitions, boolean ifNotExists)
      throws MetaException, AlreadyExistsException {
    return pTree.addPartitions(partitions, ifNotExists);
  }

  List<Partition> getPartitionsByNames(List<String> partNames) throws MetaException {
    if (partNames == null) {
      throw new MetaException("Partition names cannot be null");
    }
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = new ArrayList<>();
    for (String partName : partNames) {
      org.apache.hadoop.hive.metastore.api.Partition partition = getPartition(partName);
      if (partition != null) {
        partitions.add(partition);
      }
    }
    return partitions;
  }

  List<Partition> getPartitionsByPartitionVals(List<String> partialPartVals) throws MetaException {
    return pTree.getPartitionsByPartitionVals(partialPartVals);
  }

  Partition getPartitionWithAuthInfo(List<String> partionVals, String userName, List<String> groupNames)
      throws MetaException {
    Partition partition = getPartition(partionVals);
    if (partition == null) {
      return null;
    }
    return checkPrivilegesForPartition(partition, userName, groupNames) ? partition : null;
  }

  List<Partition> listPartitions() {
    return pTree.listPartitions();
  }

  List<Partition> listPartitionsWithAuthInfo(String userName, List<String> groupNames) {
    List<Partition> partitions = listPartitions();
    List<Partition> result = new ArrayList<>();
    partitions.forEach(p -> {
      if (checkPrivilegesForPartition(p, userName, groupNames)) {
        result.add(p);
      }
    });
    return result;
  }

  List<Partition> listPartitionsByPartitionValsWithAuthInfo(List<String> partialVals, String userName,
      List<String> groupNames) throws MetaException {
    List<Partition> partitions = pTree.getPartitionsByPartitionVals(partialVals);
    List<Partition> result = new ArrayList<>();
    partitions.forEach(p -> {
      if (checkPrivilegesForPartition(p, userName, groupNames)) {
        result.add(p);
      }
    });
    return result;
  }

  private boolean checkPrivilegesForPartition(Partition partition, String userName, List<String> groupNames) {
    if (userName == null || userName.isEmpty()) {
      return true;
    }
    if (groupNames == null || groupNames.isEmpty()) {
      return true;
    }
    PrincipalPrivilegeSet privileges = partition.getPrivileges();
    if (privileges == null) {
      return true;
    }
    if (privileges.isSetUserPrivileges()) {
      if (!privileges.getUserPrivileges().containsKey(userName)) {
        return false;
      }
    }
    if (privileges.isSetGroupPrivileges()) {
      for (String group : groupNames) {
        if (!privileges.getGroupPrivileges().containsKey(group)) {
          return false;
        }
      }
    }
    return true;
  }

  Partition dropPartition(List<String> partVals) throws MetaException, NoSuchObjectException {
    return pTree.dropPartition(partVals);
  }

  Partition dropPartition(String partitionName) throws MetaException, NoSuchObjectException {
    Map<String, String> specFromName = makeSpecFromName(partitionName);
    if (specFromName.isEmpty()) {
      throw new NoSuchObjectException("Invalid partition name " + partitionName);
    }
    List<String> pVals = new ArrayList<>();
    for (FieldSchema field : tTable.getPartitionKeys()) {
      String val = specFromName.get(field.getName());
      if (val == null) {
        throw new NoSuchObjectException("Partition name " + partitionName + " and table partition keys " + Arrays
            .toString(tTable.getPartitionKeys().toArray()) + " does not match");
      }
      pVals.add(val);
    }
    return pTree.dropPartition(pVals);
  }

  void alterPartition(Partition partition) throws MetaException, InvalidOperationException, NoSuchObjectException {
    pTree.alterPartition(partition.getValues(), partition, false);
  }

  void alterPartitions(List<Partition> newParts)
      throws MetaException, InvalidOperationException, NoSuchObjectException {
    pTree.alterPartitions(newParts);
  }

  void renamePartition(List<String> partitionVals, Partition newPart)
      throws MetaException, InvalidOperationException, NoSuchObjectException {
    pTree.renamePartition(partitionVals, newPart);
  }

  int getNumPartitionsByFilter(String filter) throws MetaException {
    return pTree.getPartitionsByFilter(filter).size();
  }

  List<Partition> listPartitionsByFilter(String filter) throws MetaException {
    return pTree.getPartitionsByFilter(filter);
  }

}
