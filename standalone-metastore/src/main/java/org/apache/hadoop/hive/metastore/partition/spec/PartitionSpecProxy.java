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

import java.util.List;
import java.util.Map;

/**
 * Polymorphic proxy class, equivalent to org.apache.hadoop.hive.metastore.api.PartitionSpec.
 */
public abstract class PartitionSpecProxy {

  /**
   * The number of Partition instances represented by the PartitionSpec.
   * @return Number of partitions.
   */
  public abstract int size();

  /**
   * Setter for name of the DB.
   * @param dbName The name of the DB.
   */
  public abstract void setDbName(String dbName);

  /**
   * Setter for name of the table.
   * @param tableName The name of the table.
   */
  public abstract void setTableName(String tableName);

  /**
   * Getter for name of the DB.
   * @return The name of the DB.
   */
  public abstract String getDbName();

  /**
   * Getter for name of the table.
   * @return The name of the table.
   */
  public abstract String getTableName();

  /**
   * Iterator to the (virtual) sequence of Partitions represented by the PartitionSpec.
   * @return A PartitionIterator to the beginning of the Partition sequence.
   */
  public abstract PartitionIterator getPartitionIterator();

  /**
   * Conversion to a org.apache.hadoop.hive.metastore.api.PartitionSpec sequence.
   * @return A list of org.apache.hadoop.hive.metastore.api.PartitionSpec instances.
   */
  public abstract List<PartitionSpec> toPartitionSpec();

  /**
   * Setter for the common root-location for all partitions in the PartitionSet.
   * @param rootLocation The new common root-location.
   * @throws MetaException
   */
  public abstract void setRootLocation(String rootLocation) throws MetaException;

  /**
   * Factory to construct PartitionSetProxy instances, from PartitionSets.
   */
  public static class Factory {

    /**
     * Factory method. Construct PartitionSpecProxy from raw PartitionSpec.
     * @param partSpec Raw PartitionSpec from the Thrift API.
     * @return PartitionSpecProxy instance.
     */
    public static PartitionSpecProxy get(PartitionSpec partSpec) {

      if (partSpec == null) {
        return null;
      }
      else
      if (partSpec.isSetPartitionList()) {
        return new PartitionListComposingSpecProxy(partSpec);
      }
      else
      if (partSpec.isSetSharedSDPartitionSpec()) {
        return new PartitionSpecWithSharedSDProxy(partSpec);
      }

      assert false : "Unsupported type of PartitionSpec!";
      return null;
    }

    /**
     * Factory method to construct CompositePartitionSpecProxy.
     * @param partitionSpecs List of raw PartitionSpecs.
     * @return A CompositePartitionSpecProxy instance.
     */
    public static PartitionSpecProxy get(List<PartitionSpec> partitionSpecs) {
      return new CompositePartitionSpecProxy(partitionSpecs);
    }

  } // class Factory;

  /**
   * Iterator to iterate over Partitions corresponding to a PartitionSpec.
   */
  public interface PartitionIterator extends java.util.Iterator<Partition> {

    /**
     * Getter for the Partition "pointed to" by the iterator.
     * Like next(), but without advancing the iterator.
     * @return The "current" partition object.
     */
    Partition getCurrent();

    /**
     * Getter for the name of the DB.
     * @return Name of the DB.
     */
    String getDbName();

    /**
     * Getter for the name of the table.
     * @return Name of the table.
     */
    String getTableName();

    /**
     * Getter for the Partition parameters.
     * @return Key-value map for Partition-level parameters.
     */
    Map<String, String> getParameters();

    /**
     * Setter for Partition parameters.
     * @param parameters Key-value map fo Partition-level parameters.
     */
    void setParameters(Map<String, String> parameters);

    /**
     * Insert an individual parameter to a Partition's parameter-set.
     * @param key
     * @param value
     */
    void putToParameters(String key, String value);

    /**
     * Getter for Partition-location.
     * @return Partition's location.
     */
    String getLocation();

    /**
     * Setter for creation-time of a Partition.
     * @param time Timestamp indicating the time of creation of the Partition.
     */
    void setCreateTime(long time);

  } // class PartitionIterator;

  /**
   * Simple wrapper class for pre-constructed Partitions, to expose a PartitionIterator interface,
   * where the iterator-sequence consists of just one Partition.
   */
  public static class SimplePartitionWrapperIterator implements PartitionIterator {
    private Partition partition;
    public SimplePartitionWrapperIterator(Partition partition) {this.partition = partition;}

    @Override public Partition getCurrent() { return partition; }
    @Override public String getDbName() { return partition.getDbName(); }
    @Override public String getTableName() { return partition.getTableName(); }
    @Override public Map<String, String> getParameters() { return partition.getParameters(); }
    @Override public void setParameters(Map<String, String> parameters) { partition.setParameters(parameters); }
    @Override public void putToParameters(String key, String value) { partition.putToParameters(key, value);}
    @Override public String getLocation() { return partition.getSd().getLocation(); }
    @Override public void setCreateTime(long time) { partition.setCreateTime((int)time);}
    @Override public boolean hasNext() { return false; } // No next partition.
    @Override public Partition next() { return null; } // No next partition.
    @Override public void remove() {} // Do nothing.
  } // P

} // class PartitionSpecProxy;
