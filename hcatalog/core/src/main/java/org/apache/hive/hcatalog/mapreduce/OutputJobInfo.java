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

package org.apache.hive.hcatalog.mapreduce;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

/** The class used to serialize and store the output related information  */
public class OutputJobInfo implements Serializable {

  /** The db and table names. */
  private final String databaseName;
  private final String tableName;

  /** The serialization version. */
  private static final long serialVersionUID = 1L;

  /** The table info provided by user. */
  private HCatTableInfo tableInfo;

  /** The output schema. This is given to us by user.  This wont contain any
   * partition columns ,even if user has specified them.
   * */
  private HCatSchema outputSchema;

  /** The location of the partition being written */
  private String location;

  /** The partition values to publish to, if used for output*/
  private Map<String, String> partitionValues;

  private List<Integer> posOfPartCols;
  private List<Integer> posOfDynPartCols;

  private Properties properties;

  private int maxDynamicPartitions;

  /** List of keys for which values were not specified at write setup time, to be infered at write time */
  private List<String> dynamicPartitioningKeys;

  private boolean harRequested;

  /**
   * Initializes a new OutputJobInfo instance
   * for writing data from a table.
   * @param databaseName the db name
   * @param tableName the table name
   * @param partitionValues The partition values to publish to, can be null or empty Map to
   * work with hadoop security, the kerberos principal name of the server - else null
   * The principal name should be of the form:
   * <servicename>/_HOST@<realm> like "hcat/_HOST@myrealm.com"
   * The special string _HOST will be replaced automatically with the correct host name
   * indicate write to a unpartitioned table. For partitioned tables, this map should
   * contain keys for all partition columns with corresponding values.
   */
  public static OutputJobInfo create(String databaseName,
                     String tableName,
                     Map<String, String> partitionValues) {
    return new OutputJobInfo(databaseName,
      tableName,
      partitionValues);
  }

  private OutputJobInfo(String databaseName,
              String tableName,
              Map<String, String> partitionValues) {
    this.databaseName = (databaseName == null) ? MetaStoreUtils.DEFAULT_DATABASE_NAME : databaseName;
    this.tableName = tableName;
    this.partitionValues = partitionValues;
    this.properties = new Properties();
  }

  /**
   * @return the posOfPartCols
   */
  protected List<Integer> getPosOfPartCols() {
    return posOfPartCols;
  }

  /**
   * @return the posOfDynPartCols
   */
  protected List<Integer> getPosOfDynPartCols() {
    return posOfDynPartCols;
  }

  /**
   * @param posOfPartCols the posOfPartCols to set
   */
  protected void setPosOfPartCols(List<Integer> posOfPartCols) {
    // sorting the list in the descending order so that deletes happen back-to-front
    Collections.sort(posOfPartCols, new Comparator<Integer>() {
      @Override
      public int compare(Integer earlier, Integer later) {
        return (earlier > later) ? -1 : ((earlier == later) ? 0 : 1);
      }
    });
    this.posOfPartCols = posOfPartCols;
  }

  /**
   * @param posOfDynPartCols the posOfDynPartCols to set
   */
  protected void setPosOfDynPartCols(List<Integer> posOfDynPartCols) {
    // Important - no sorting here! We retain order, it's used to match with values at runtime
    this.posOfDynPartCols = posOfDynPartCols;
  }

  /**
   * @return the tableInfo
   */
  public HCatTableInfo getTableInfo() {
    return tableInfo;
  }

  /**
   * @return the outputSchema
   */
  public HCatSchema getOutputSchema() {
    return outputSchema;
  }

  /**
   * @param schema the outputSchema to set
   */
  public void setOutputSchema(HCatSchema schema) {
    this.outputSchema = schema;
  }

  /**
   * @return the location
   */
  public String getLocation() {
    return location;
  }

  /**
   * @param location location to write to
   */
  public void setLocation(String location) {
    this.location = location;
  }

  /**
   * Sets the value of partitionValues
   * @param partitionValues the partition values to set
   */
  void setPartitionValues(Map<String, String> partitionValues) {
    this.partitionValues = partitionValues;
  }

  /**
   * Gets the value of partitionValues
   * @return the partitionValues
   */
  public Map<String, String> getPartitionValues() {
    return partitionValues;
  }

  /**
   * set the tablInfo instance
   * this should be the same instance
   * determined by this object's DatabaseName and TableName
   * @param tableInfo
   */
  void setTableInfo(HCatTableInfo tableInfo) {
    this.tableInfo = tableInfo;
  }

  /**
   * @return database name of table to write to
   */
  public String getDatabaseName() {
    return databaseName;
  }

  /**
   * @return name of table to write to
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Set/Get Property information to be passed down to *StorageHandler implementation
   * put implementation specific storage handler configurations here
   * @return the implementation specific job properties
   */
  public Properties getProperties() {
    return properties;
  }

  /**
   * Set maximum number of allowable dynamic partitions
   * @param maxDynamicPartitions
   */
  public void setMaximumDynamicPartitions(int maxDynamicPartitions) {
    this.maxDynamicPartitions = maxDynamicPartitions;
  }

  /**
   * Returns maximum number of allowable dynamic partitions
   * @return maximum number of allowable dynamic partitions
   */
  public int getMaxDynamicPartitions() {
    return this.maxDynamicPartitions;
  }

  /**
   * Sets whether or not hadoop archiving has been requested for this job
   * @param harRequested
   */
  public void setHarRequested(boolean harRequested) {
    this.harRequested = harRequested;
  }

  /**
   * Returns whether or not hadoop archiving has been requested for this job
   * @return whether or not hadoop archiving has been requested for this job
   */
  public boolean getHarRequested() {
    return this.harRequested;
  }

  /**
   * Returns whether or not Dynamic Partitioning is used
   * @return whether or not dynamic partitioning is currently enabled and used
   */
  public boolean isDynamicPartitioningUsed() {
    return !((dynamicPartitioningKeys == null) || (dynamicPartitioningKeys.isEmpty()));
  }

  /**
   * Sets the list of dynamic partitioning keys used for outputting without specifying all the keys
   * @param dynamicPartitioningKeys
   */
  public void setDynamicPartitioningKeys(List<String> dynamicPartitioningKeys) {
    this.dynamicPartitioningKeys = dynamicPartitioningKeys;
  }

  public List<String> getDynamicPartitioningKeys() {
    return this.dynamicPartitioningKeys;
  }

}
