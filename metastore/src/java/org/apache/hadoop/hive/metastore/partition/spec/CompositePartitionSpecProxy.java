/**
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of PartitionSpecProxy that composes a list of PartitionSpecProxy.
 */
public class CompositePartitionSpecProxy extends PartitionSpecProxy {

  private String dbName;
  private String tableName;
  private List<PartitionSpec> partitionSpecs;
  private List<PartitionSpecProxy> partitionSpecProxies;
  private int size = 0;

  protected CompositePartitionSpecProxy(List<PartitionSpec> partitionSpecs) {
    this.partitionSpecs = partitionSpecs;
    if (partitionSpecs.isEmpty()) {
      dbName = null;
      tableName = null;
    }
    else {
      dbName = partitionSpecs.get(0).getDbName();
      tableName = partitionSpecs.get(0).getTableName();
      this.partitionSpecProxies = new ArrayList<PartitionSpecProxy>(partitionSpecs.size());
      for (PartitionSpec partitionSpec : partitionSpecs) {
        PartitionSpecProxy partitionSpecProxy = Factory.get(partitionSpec);
        this.partitionSpecProxies.add(partitionSpecProxy);
        size += partitionSpecProxy.size();
      }
    }
    // Assert class-invariant.
    assert isValid() : "Invalid CompositePartitionSpecProxy!";
  }

  protected CompositePartitionSpecProxy(String dbName, String tableName, List<PartitionSpec> partitionSpecs) {
    this.dbName = dbName;
    this.tableName = tableName;
    this.partitionSpecs = partitionSpecs;
    this.partitionSpecProxies = new ArrayList<PartitionSpecProxy>(partitionSpecs.size());
    for (PartitionSpec partitionSpec : partitionSpecs) {
      this.partitionSpecProxies.add(PartitionSpecProxy.Factory.get(partitionSpec));
    }
    // Assert class-invariant.
    assert isValid() : "Invalid CompositePartitionSpecProxy!";
  }

  private boolean isValid() {
    for (PartitionSpecProxy partitionSpecProxy : partitionSpecProxies) {
      if (partitionSpecProxy instanceof CompositePartitionSpecProxy) {
        return false;
      }
    }

    return true;
  }

  @Override
  public int size() {
    return size;
  }

  /**
   * Iterator to iterate over all Partitions, across all PartitionSpecProxy instances within the Composite.
   */
  public static class Iterator implements PartitionIterator {

    private CompositePartitionSpecProxy composite;
    private List<PartitionSpecProxy> partitionSpecProxies;
    private int index = -1; // Index into partitionSpecs.
    private PartitionIterator iterator = null;

    public Iterator(CompositePartitionSpecProxy composite) {
      this.composite = composite;
      this.partitionSpecProxies = composite.partitionSpecProxies;

      if (this.partitionSpecProxies != null && !this.partitionSpecProxies.isEmpty()) {
        this.index = 0;
        this.iterator = this.partitionSpecProxies.get(this.index).getPartitionIterator();
      }
    }

    @Override
    public boolean hasNext() {

      if (iterator == null) {
        return false;
      }

      if (iterator.hasNext()) {
        return true;
      }

      while ( ++index < partitionSpecProxies.size()
          && !(iterator = partitionSpecProxies.get(index).getPartitionIterator()).hasNext());

      return index < partitionSpecProxies.size() && iterator.hasNext();

    }

    @Override
    public Partition next() {

        if (iterator.hasNext())
          return iterator.next();

        while (++index < partitionSpecProxies.size()
            && !(iterator = partitionSpecProxies.get(index).getPartitionIterator()).hasNext());

        return index == partitionSpecProxies.size()? null : iterator.next();

    }

    @Override
    public void remove() {
      iterator.remove();
    }

    @Override
    public Partition getCurrent() {
      return iterator.getCurrent();
    }

    @Override
    public String getDbName() {
      return composite.dbName;
    }

    @Override
    public String getTableName() {
      return composite.tableName;
    }

    @Override
    public Map<String, String> getParameters() {
      return iterator.getParameters();
    }

    @Override
    public void setParameters(Map<String, String> parameters) {
      iterator.setParameters(parameters);
    }

    @Override
    public String getLocation() {
      return iterator.getLocation();
    }

    @Override
    public void putToParameters(String key, String value) {
      iterator.putToParameters(key, value);
    }

    @Override
    public void setCreateTime(long time) {
      iterator.setCreateTime(time);
    }
  }

  @Override
  public void setDbName(String dbName) {
    this.dbName = dbName;
    for (PartitionSpecProxy partSpecProxy : partitionSpecProxies) {
      partSpecProxy.setDbName(dbName);
    }
  }

  @Override
  public void setTableName(String tableName) {
    this.tableName = tableName;
    for (PartitionSpecProxy partSpecProxy : partitionSpecProxies) {
      partSpecProxy.setTableName(tableName);
    }
  }

  @Override
  public String getDbName() {
    return dbName;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public PartitionIterator getPartitionIterator() {
    return new Iterator(this);
  }

  @Override
  public List<PartitionSpec> toPartitionSpec() {
    return partitionSpecs;
  }

  @Override
  public void setRootLocation(String rootLocation) throws MetaException {
    for (PartitionSpecProxy partSpecProxy : partitionSpecProxies) {
      partSpecProxy.setRootLocation(rootLocation);
    }
  }
}
