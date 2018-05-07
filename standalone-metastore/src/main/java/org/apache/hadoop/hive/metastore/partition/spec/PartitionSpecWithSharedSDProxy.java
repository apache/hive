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
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionSpecWithSharedSD;
import org.apache.hadoop.hive.metastore.api.PartitionWithoutSD;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Subclass of PartitionSpecProxy that pulls out commonality of
 * StorageDescriptor properties within a Partition-list into a common
 * StorageDescriptor instance.
 */
public class PartitionSpecWithSharedSDProxy extends PartitionSpecProxy {

  private PartitionSpec partitionSpec;

  public PartitionSpecWithSharedSDProxy(PartitionSpec partitionSpec) throws MetaException {
    assert partitionSpec.isSetSharedSDPartitionSpec();
    if (partitionSpec.getSharedSDPartitionSpec().getSd() == null) {
      throw new MetaException("The shared storage descriptor must be set.");
    }
    this.partitionSpec = partitionSpec;
  }

  @Override
  public int size() {
    return partitionSpec.getSharedSDPartitionSpec().getPartitionsSize();
  }

  @Override
  public void setCatName(String catName) {
    partitionSpec.setCatName(catName);
  }

  @Override
  public void setDbName(String dbName) {
    partitionSpec.setDbName(dbName);
  }

  @Override
  public void setTableName(String tableName) {
    partitionSpec.setTableName(tableName);
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

  public PartitionIterator getPartitionIterator() {
    return new Iterator(this);
  }

  @Override
  public List<PartitionSpec> toPartitionSpec() {
    return Arrays.asList(partitionSpec);
  }

  @Override
  public void setRootLocation(String rootLocation) throws MetaException {
    partitionSpec.setRootPath(rootLocation);
    partitionSpec.getSharedSDPartitionSpec().getSd().setLocation(rootLocation);
  }

  /**
   * Iterator implementation to iterate over all Partitions within the PartitionSpecWithSharedSDProxy.
   */
  public static class Iterator implements PartitionIterator {

    private PartitionSpecWithSharedSDProxy partitionSpecWithSharedSDProxy;
    private PartitionSpecWithSharedSD pSpec;
    private int index;

    Iterator(PartitionSpecWithSharedSDProxy partitionSpecWithSharedSDProxy) {
      this.partitionSpecWithSharedSDProxy = partitionSpecWithSharedSDProxy;
      this.pSpec = this.partitionSpecWithSharedSDProxy.partitionSpec.getSharedSDPartitionSpec();
      this.index = 0;
    }

    @Override
    public boolean hasNext() {
      return index < pSpec.getPartitions().size();
    }

    @Override
    public Partition next() {
      Partition partition = getCurrent();
      ++index;
      return partition;
    }

    @Override
    public void remove() {
      pSpec.getPartitions().remove(index);
    }

    @Override
    public Partition getCurrent() {
      PartitionWithoutSD partWithoutSD = pSpec.getPartitions().get(index);
      StorageDescriptor partSD = new StorageDescriptor(pSpec.getSd());
      partSD.setLocation(partSD.getLocation() + partWithoutSD.getRelativePath());

      Partition p = new Partition(
          partWithoutSD.getValues(),
          partitionSpecWithSharedSDProxy.partitionSpec.getDbName(),
          partitionSpecWithSharedSDProxy.partitionSpec.getTableName(),
          partWithoutSD.getCreateTime(),
          partWithoutSD.getLastAccessTime(),
          partSD,
          partWithoutSD.getParameters()
      );
      p.setCatName(partitionSpecWithSharedSDProxy.partitionSpec.getCatName());
      return p;
    }

    @Override
    public String getCatName() {
      return partitionSpecWithSharedSDProxy.partitionSpec.getCatName();
    }

    @Override
    public String getDbName() {
      return partitionSpecWithSharedSDProxy.partitionSpec.getDbName();
    }

    @Override
    public String getTableName() {
      return partitionSpecWithSharedSDProxy.partitionSpec.getTableName();
    }

    @Override
    public Map<String, String> getParameters() {
      return pSpec.getPartitions().get(index).getParameters();
    }

    @Override
    public void setParameters(Map<String, String> parameters) {
      pSpec.getPartitions().get(index).setParameters(parameters);
    }

    @Override
    public String getLocation() {
      return pSpec.getSd().getLocation() + pSpec.getPartitions().get(index).getRelativePath();
    }

    @Override
    public void putToParameters(String key, String value) {
      pSpec.getPartitions().get(index).putToParameters(key, value);
    }

    @Override
    public void setCreateTime(long time) {
      pSpec.getPartitions().get(index).setCreateTime((int)time);
    }

  } // static class Iterator;

}
