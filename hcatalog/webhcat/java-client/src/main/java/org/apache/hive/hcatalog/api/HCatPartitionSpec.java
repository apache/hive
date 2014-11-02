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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.api;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hive.hcatalog.common.HCatException;

/**
 * Generalized representation of a set of HCatPartitions.
 */

@InterfaceAudience.LimitedPrivate({"Hive"})
@InterfaceStability.Evolving
public class HCatPartitionSpec {

  protected HCatTable hcatTable;
  protected PartitionSpecProxy partitionSpecProxy;

  protected HCatPartitionSpec(HCatTable hcatTable, PartitionSpecProxy partitionSpecProxy) throws HCatException {
    this.hcatTable = hcatTable;
    this.partitionSpecProxy = partitionSpecProxy;
    assert_invariant();
  }

  /**
   * Getter for DBName of this HCatPartitionSpec.
   * @return The name of the DB.
   */
  public String getDbName() {
    return partitionSpecProxy.getDbName();
  }

  /**
   * Getter for TableName of this HCatPartitionSpec.
   * @return The name of the TableName.
   */
  public String getTableName() {
    return partitionSpecProxy.getTableName();
  }

  /**
   * Setter for HCatTable. Required for deserialization.
   */
  void hcatTable(HCatTable hcatTable) throws HCatException {

    assert this.hcatTable == null : "Expected hcatTable to be null at this point.";
    this.hcatTable = hcatTable;
    assert_invariant();

  }

  /**
   * Conversion to a Hive Metastore API PartitionSpecProxy instance.
   */
  PartitionSpecProxy toPartitionSpecProxy() {
    return partitionSpecProxy;
  }

  /**
   * Getter for the number of HCatPartitions represented by this HCatPartitionSpec instance.
   * @return The number of HCatPartitions.
   * @throws HCatException On failure.
   */
  public int size() throws HCatException {
    return partitionSpecProxy.size();
  }

  /**
   * Setter for the "root" location of the HCatPartitionSpec.
   * @param location The new "root" location of the HCatPartitionSpec.
   * @throws HCatException On failure to set a new location.
   */
  public void setRootLocation(String location) throws HCatException {
    try {
      partitionSpecProxy.setRootLocation(location);
    }
    catch (MetaException metaException) {
      throw new HCatException("Unable to set root-path!", metaException);
    }
  }

  /**
   * Getter for an Iterator to the first HCatPartition in the HCatPartitionSpec.
   * @return HCatPartitionIterator to the first HCatPartition.
   */
  public HCatPartitionIterator getPartitionIterator() {
    return new HCatPartitionIterator(hcatTable, partitionSpecProxy.getPartitionIterator());
  }

  // Assert class invariant.
  private void assert_invariant() throws HCatException {

    if (hcatTable != null) {

      if (!hcatTable.getDbName().equalsIgnoreCase(partitionSpecProxy.getDbName())) {
        String errorMessage = "Invalid HCatPartitionSpec instance: Table's DBName (" + hcatTable.getDbName() + ") " +
            "doesn't match PartitionSpec (" + partitionSpecProxy.getDbName() + ")";
        assert false : errorMessage;
        throw new HCatException(errorMessage);
      }

      if (!hcatTable.getTableName().equalsIgnoreCase(partitionSpecProxy.getTableName())) {
        String errorMessage = "Invalid HCatPartitionSpec instance: Table's TableName (" + hcatTable.getTableName() + ") " +
            "doesn't match PartitionSpec (" + partitionSpecProxy.getTableName() + ")";
        assert false : errorMessage;
        throw new HCatException(errorMessage);
      }
    }
  }


  /**
   * Iterator over HCatPartitions in the HCatPartitionSpec.
   */
  public static class HCatPartitionIterator { // implements java.util.Iterator<HCatPartition> {

    private HCatTable hcatTable;
    private PartitionSpecProxy.PartitionIterator iterator;

    HCatPartitionIterator(HCatTable hcatTable, PartitionSpecProxy.PartitionIterator iterator) {
      this.hcatTable = hcatTable;
      this.iterator = iterator;
    }

    public boolean hasNext() {
      return iterator.hasNext();
    }

    public HCatPartition next() throws HCatException {
      return new HCatPartition(hcatTable, iterator.next());
    }

    public void remove() {
      iterator.remove();
    }

  } // class HCatPartitionIterator;

} // class HCatPartitionSpec;
