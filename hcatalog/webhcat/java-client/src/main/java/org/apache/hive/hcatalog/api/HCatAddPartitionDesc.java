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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.api;

import java.util.Map;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hive.hcatalog.common.HCatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class HCatAddPartitionDesc helps users in defining partition attributes.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class HCatAddPartitionDesc {

  private static final Logger LOG = LoggerFactory.getLogger(HCatAddPartitionDesc.class);
  private HCatPartition hcatPartition;

  // The following data members are only required to support the deprecated constructor (and builder).
  String dbName, tableName, location;
  Map<String, String> partitionKeyValues;

  private HCatAddPartitionDesc(HCatPartition hcatPartition) {
    this.hcatPartition = hcatPartition;
  }

  private HCatAddPartitionDesc(String dbName, String tableName, String location, Map<String, String> partitionKeyValues) {
    this.hcatPartition = null;
    this.dbName = dbName;
    this.tableName = tableName;
    this.location = location;
    this.partitionKeyValues = partitionKeyValues;
  }

  HCatPartition getHCatPartition() {
    return hcatPartition;
  }

  HCatPartition getHCatPartition(HCatTable hcatTable) throws HCatException {
    assert hcatPartition == null : "hcatPartition should have been null at this point.";
    assert dbName.equalsIgnoreCase(hcatTable.getDbName()) : "DB names don't match.";
    assert tableName.equalsIgnoreCase(hcatTable.getTableName()) : "Table names don't match.";
    return new HCatPartition(hcatTable, partitionKeyValues, location);
  }

  /**
   * Gets the location.
   *
   * @return the location
   */
  @Deprecated // @deprecated in favour of {@link HCatPartition.#getLocation()}. To be removed in Hive 0.16.
  public String getLocation() {
    return hcatPartition == null? location : hcatPartition.getLocation();
  }

  /**
   * Gets the partition spec.
   *
   * @return the partition spec
   */
  @Deprecated // @deprecated in favour of {@link HCatPartition.#getPartitionKeyValMap()}. To be removed in Hive 0.16.
  public Map<String, String> getPartitionSpec() {
    return hcatPartition == null? partitionKeyValues : hcatPartition.getPartitionKeyValMap();
  }

  /**
   * Gets the table name.
   *
   * @return the table name
   */
  @Deprecated // @deprecated in favour of {@link HCatPartition.#getDbTableName()}. To be removed in Hive 0.16.
  public String getTableName() {
    return hcatPartition == null? tableName : hcatPartition.getTableName();
  }

  /**
   * Gets the database name.
   *
   * @return the database name
   */
  @Deprecated // @deprecated in favour of {@link HCatPartition.#getDatabaseName()}. To be removed in Hive 0.16.
  public String getDatabaseName() {
    return hcatPartition == null? dbName : hcatPartition.getDatabaseName();
  }

  @Override
  public String toString() {
    return "HCatAddPartitionDesc [" + hcatPartition + "]";
  }

  /**
   * Creates the builder for specifying attributes.
   *
   * @param dbName the db name
   * @param tableName the table name
   * @param location the location
   * @param partSpec the part spec
   * @return the builder
   * @throws HCatException
   */
  @Deprecated // @deprecated in favour of {@link HCatAddPartitionDesc.#create(HCatPartition)}. To be removed in Hive 0.16.
  public static Builder create(String dbName,
                               String tableName,
                               String location,
                               Map<String, String> partSpec
                      ) throws HCatException {
    LOG.error("Unsupported! HCatAddPartitionDesc requires HCatTable to be specified explicitly.");
    return new Builder(dbName, tableName, location, partSpec);
  }

  /**
   * Constructs a Builder instance, using an HCatPartition object.
   * @param partition An HCatPartition instance.
   * @return A Builder object that can build an appropriate HCatAddPartitionDesc.
   * @throws HCatException
   */
  public static Builder create(HCatPartition partition) throws HCatException {
    return new Builder(partition);
  }

  /**
   * Builder class for constructing an HCatAddPartition instance.
   */
  public static class Builder {

    private HCatPartition hcatPartition;

    // The following data members are only required to support the deprecated constructor (and builder).
    String dbName, tableName, location;
    Map<String, String> partitionSpec;

    private Builder(HCatPartition hcatPartition) {
      this.hcatPartition = hcatPartition;
    }

    @Deprecated // To be removed in Hive 0.16.
    private Builder(String dbName, String tableName, String location, Map<String, String> partitionSpec) {
      this.hcatPartition = null;
      this.dbName = dbName;
      this.tableName = tableName;
      this.location = location;
      this.partitionSpec = partitionSpec;
    }

    /**
     * Builds the HCatAddPartitionDesc.
     *
     * @return the h cat add partition desc
     * @throws HCatException
     */
    public HCatAddPartitionDesc build() throws HCatException {
      return hcatPartition == null?
                new HCatAddPartitionDesc(dbName, tableName, location, partitionSpec)
              : new HCatAddPartitionDesc(hcatPartition);
    }
  }

}
