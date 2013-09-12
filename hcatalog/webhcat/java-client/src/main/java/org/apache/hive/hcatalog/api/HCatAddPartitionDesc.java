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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.hcatalog.common.HCatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class HCatAddPartitionDesc helps users in defining partition attributes.
 */
public class HCatAddPartitionDesc {

  private static final Logger LOG = LoggerFactory.getLogger(HCatAddPartitionDesc.class);
  private String tableName;
  private String dbName;
  private String location;
  private Map<String, String> partSpec;

  private HCatAddPartitionDesc(String dbName, String tbl, String loc, Map<String, String> spec) {
    this.dbName = dbName;
    this.tableName = tbl;
    this.location = loc;
    this.partSpec = spec;
  }

  /**
   * Gets the location.
   *
   * @return the location
   */
  public String getLocation() {
    return this.location;
  }


  /**
   * Gets the partition spec.
   *
   * @return the partition spec
   */
  public Map<String, String> getPartitionSpec() {
    return this.partSpec;
  }

  /**
   * Gets the table name.
   *
   * @return the table name
   */
  public String getTableName() {
    return this.tableName;
  }

  /**
   * Gets the database name.
   *
   * @return the database name
   */
  public String getDatabaseName() {
    return this.dbName;
  }

  @Override
  public String toString() {
    return "HCatAddPartitionDesc ["
      + (tableName != null ? "tableName=" + tableName + ", " : "tableName=null")
      + (dbName != null ? "dbName=" + dbName + ", " : "dbName=null")
      + (location != null ? "location=" + location + ", " : "location=null")
      + (partSpec != null ? "partSpec=" + partSpec : "partSpec=null") + "]";
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
  public static Builder create(String dbName, String tableName, String location,
                 Map<String, String> partSpec) throws HCatException {
    return new Builder(dbName, tableName, location, partSpec);
  }

  Partition toHivePartition(Table hiveTable) throws HCatException {
    Partition hivePtn = new Partition();
    hivePtn.setDbName(this.dbName);
    hivePtn.setTableName(this.tableName);

    List<String> pvals = new ArrayList<String>();
    for (FieldSchema field : hiveTable.getPartitionKeys()) {
      String val = partSpec.get(field.getName());
      if (val == null || val.length() == 0) {
        throw new HCatException("create partition: Value for key "
          + field.getName() + " is null or empty");
      }
      pvals.add(val);
    }

    hivePtn.setValues(pvals);
    StorageDescriptor sd = new StorageDescriptor(hiveTable.getSd());
    hivePtn.setSd(sd);
    hivePtn.setParameters(hiveTable.getParameters());
    if (this.location != null) {
      hivePtn.getSd().setLocation(this.location);
    } else {
      String partName;
      try {
        partName = Warehouse.makePartName(
          hiveTable.getPartitionKeys(), pvals);
        LOG.info("Setting partition location to :" + partName);
      } catch (MetaException e) {
        throw new HCatException("Exception while creating partition name.", e);
      }
      Path partPath = new Path(hiveTable.getSd().getLocation(), partName);
      hivePtn.getSd().setLocation(partPath.toString());
    }
    hivePtn.setCreateTime((int) (System.currentTimeMillis() / 1000));
    hivePtn.setLastAccessTimeIsSet(false);
    return hivePtn;
  }

  public static class Builder {

    private String tableName;
    private String location;
    private Map<String, String> values;
    private String dbName;

    private Builder(String dbName, String tableName, String location, Map<String, String> values) {
      this.dbName = dbName;
      this.tableName = tableName;
      this.location = location;
      this.values = values;
    }

    /**
     * Builds the HCatAddPartitionDesc.
     *
     * @return the h cat add partition desc
     * @throws HCatException
     */
    public HCatAddPartitionDesc build() throws HCatException {
      if (this.dbName == null) {
        this.dbName = MetaStoreUtils.DEFAULT_DATABASE_NAME;
      }
      HCatAddPartitionDesc desc = new HCatAddPartitionDesc(
        this.dbName, this.tableName, this.location,
        this.values);
      return desc;
    }
  }

}
