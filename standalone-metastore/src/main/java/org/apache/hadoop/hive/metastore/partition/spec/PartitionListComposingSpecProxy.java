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

package org.apache.hadoop.hive.metastore.partition.spec;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionListComposingSpec;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * PartitionSpecProxy implementation that composes a List of Partitions.
 */
public class PartitionListComposingSpecProxy extends PartitionSpecProxy {

  private PartitionSpec partitionSpec;

  protected PartitionListComposingSpecProxy(PartitionSpec partitionSpec) throws MetaException {
    assert partitionSpec.isSetPartitionList()
        : "Partition-list should have been set.";
    PartitionListComposingSpec partitionList = partitionSpec.getPartitionList();
    if (partitionList == null || partitionList.getPartitions() == null) {
      throw new MetaException("The partition list cannot be null.");
    }
    for (Partition partition : partitionList.getPartitions()) {
      if (partition == null) {
        throw new MetaException("Partition cannot be null.");
      }
      if (partition.getValues() == null || partition.getValues().isEmpty()) {
        throw new MetaException("The partition value list cannot be null or empty.");
      }
      if (partition.getValues().contains(null)) {
        throw new MetaException("Partition value cannot be null.");
      }
    }
    this.partitionSpec = partitionSpec;
  }

  @Override
  public String getCatName() {
    return partitionSpec.getCatName();
  }

  @Override
  public String getDbName() {
    return partitionSpec.getDbName();
  }

  @Override
  public String getTableName() {
    return partitionSpec.getTableName();
  }

  @Override
  public PartitionIterator getPartitionIterator() {
    return new Iterator(this);
  }

  @Override
  public List<PartitionSpec> toPartitionSpec() {
    return Arrays.asList(partitionSpec);
  }

  @Override
  public int size() {
    return partitionSpec.getPartitionList().getPartitionsSize();
  }

  @Override
  public void setCatName(String catName) {
    partitionSpec.setCatName(catName);
    for (Partition partition : partitionSpec.getPartitionList().getPartitions()) {
      partition.setCatName(catName);
    }
  }

  @Override
  public void setDbName(String dbName) {
    partitionSpec.setDbName(dbName);
    for (Partition partition : partitionSpec.getPartitionList().getPartitions()) {
      partition.setDbName(dbName);
    }
  }

  @Override
  public void setTableName(String tableName) {
    partitionSpec.setTableName(tableName);
    for (Partition partition : partitionSpec.getPartitionList().getPartitions()) {
      partition.setTableName(tableName);
    }
  }

  @Override
  public void setRootLocation(String newRootPath) throws MetaException {

    String oldRootPath = partitionSpec.getRootPath();

    if (oldRootPath == null) {
      throw new MetaException("No common root-path. Can't replace root-path!");
    }

    if (newRootPath == null) {
      throw new MetaException("Root path cannot be null.");
    }

    for (Partition partition : partitionSpec.getPartitionList().getPartitions()) {
      String location = partition.getSd().getLocation();
      if (location.startsWith(oldRootPath)) {
        partition.getSd().setLocation(location.replace(oldRootPath, newRootPath));
      }
      else {
        throw new MetaException("Common root-path not found. Can't replace root-path!");
      }
    }
  }

  public static class Iterator implements PartitionIterator {

    PartitionListComposingSpecProxy partitionSpecProxy;
    List<Partition> partitionList;
    int index;

    public Iterator(PartitionListComposingSpecProxy partitionSpecProxy) {
      this.partitionSpecProxy = partitionSpecProxy;
      this.partitionList = partitionSpecProxy.partitionSpec.getPartitionList().getPartitions();
      this.index = 0;
    }

    @Override
    public Partition getCurrent() {
      return partitionList.get(index);
    }

    @Override
    public String getCatName() {
      return partitionSpecProxy.getCatName();
    }

    @Override
    public String getDbName() {
      return partitionSpecProxy.getDbName();
    }

    @Override
    public String getTableName() {
      return partitionSpecProxy.getTableName();
    }

    @Override
    public Map<String, String> getParameters() {
      return partitionList.get(index).getParameters();
    }

    @Override
    public void setParameters(Map<String, String> parameters) {
      partitionList.get(index).setParameters(parameters);
    }

    @Override
    public String getLocation() {
      return partitionList.get(index).getSd().getLocation();
    }

    @Override
    public void putToParameters(String key, String value) {
      partitionList.get(index).putToParameters(key, value);
    }

    @Override
    public void setCreateTime(long time) {
      partitionList.get(index).setCreateTime((int)time);
    }

    @Override
    public boolean hasNext() {
      return index < partitionList.size();
    }

    @Override
    public Partition next() {
      return partitionList.get(index++);
    }

    @Override
    public void remove() {
      partitionList.remove(index);
    }
  } // class Iterator;

} // class PartitionListComposingSpecProxy;
