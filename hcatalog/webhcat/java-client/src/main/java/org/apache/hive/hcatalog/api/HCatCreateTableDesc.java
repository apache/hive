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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;

/**
 * The Class HCatCreateTableDesc for defining attributes for a new table.
 */
@SuppressWarnings("deprecation")
@InterfaceAudience.Public
@InterfaceStability.Stable
public class HCatCreateTableDesc {

  private boolean ifNotExists;
  private HCatTable hcatTable;

  private HCatCreateTableDesc(HCatTable hcatTable, boolean ifNotExists) {
    this.hcatTable = hcatTable;
    this.ifNotExists = ifNotExists;
  }

  /**
   * Creates a builder for defining attributes.
   *
   * @param dbName the db name
   * @param tableName the table name
   * @param columns the columns
   * @return the builder
   */
  @Deprecated // @deprecated in favour of {@link #create(HCatTable)}. To be removed in Hive 0.16.
  public static Builder create(String dbName, String tableName, List<HCatFieldSchema> columns) {
    return new Builder(dbName, tableName, columns);
  }

  /**
   * Getter for HCatCreateTableDesc.Builder instance.
   * @param table Spec for HCatTable to be created.
   * @param ifNotExists Only create the table if it doesn't already exist.
   * @return Builder instance.
   */
  public static Builder create(HCatTable table, boolean ifNotExists) {
    return new Builder(table, ifNotExists);
  }

  /**
   * Getter for HCatCreateTableDesc.Builder instance. By default, ifNotExists is false.
   * So the attempt to create the table is made even if the table already exists.
   * @param table Spec for HCatTable to be created.
   * @return Builder instance.
   */
  public static Builder create(HCatTable table) {
    return new Builder(table, false);
  }

  /**
   * Getter for underlying HCatTable instance.
   */
  public HCatTable getHCatTable() {
    return this.hcatTable;
  }

  /**
   * Gets the if not exists.
   *
   * @return the if not exists
   */
  public boolean getIfNotExists() {
    return this.ifNotExists;
  }

  /**
   * Gets the table name.
   *
   * @return the table name
   */
  @Deprecated // @deprecated in favour of {@link HCatTable.#getTableName()}. To be removed in Hive 0.16.
  public String getTableName() {
    return this.hcatTable.getTableName();
  }

  /**
   * Gets the cols.
   *
   * @return the cols
   */
  @Deprecated // @deprecated in favour of {@link HCatTable.#getCols()}. To be removed in Hive 0.16.
  public List<HCatFieldSchema> getCols() {
    return this.hcatTable.getCols();
  }

  /**
   * Gets the partition cols.
   *
   * @return the partition cols
   */
  @Deprecated // @deprecated in favour of {@link HCatTable.#getPartCols()}. To be removed in Hive 0.16.
  public List<HCatFieldSchema> getPartitionCols() {
    return this.hcatTable.getPartCols();
  }

  /**
   * Gets the bucket cols.
   *
   * @return the bucket cols
   */
  @Deprecated // @deprecated in favour of {@link HCatTable.#getBucketCols()}. To be removed in Hive 0.16.
  public List<String> getBucketCols() {
    return this.hcatTable.getBucketCols();
  }

  @Deprecated // @deprecated in favour of {@link HCatTable.#getNumBuckets()}.
  public int getNumBuckets() {
    return this.hcatTable.getNumBuckets();
  }

  /**
   * Gets the comments.
   *
   * @return the comments
   */
  @Deprecated // @deprecated in favour of {@link HCatTable.#comment()}. To be removed in Hive 0.16.
  public String getComments() {
    return this.hcatTable.comment();
  }

  /**
   * Gets the storage handler.
   *
   * @return the storage handler
   */
  @Deprecated // @deprecated in favour of {@link HCatTable.#getStorageHandler()}. To be removed in Hive 0.16.
  public String getStorageHandler() {
    return this.hcatTable.getStorageHandler();
  }

  /**
   * Gets the location.
   *
   * @return the location
   */
  @Deprecated // @deprecated in favour of {@link HCatTable.#getLocation()}. To be removed in Hive 0.16.
  public String getLocation() {
    return this.hcatTable.getLocation();
  }

  /**
   * Gets the external.
   *
   * @return the external
   */
  @Deprecated // @deprecated in favour of {@link HCatTable.#getTableType()}. To be removed in Hive 0.16.
  public boolean getExternal() {

    return this.hcatTable.getTabletype()
                         .equalsIgnoreCase(HCatTable.Type.EXTERNAL_TABLE.toString());
  }

  /**
   * Gets the sort cols.
   *
   * @return the sort cols
   */
  @Deprecated // @deprecated in favour of {@link HCatTable.#getSortCols()}. To be removed in Hive 0.16.
  public List<Order> getSortCols() {
    return this.hcatTable.getSortCols();
  }

  /**
   * Gets the tbl props.
   *
   * @return the tbl props
   */
  @Deprecated // @deprecated in favour of {@link HCatTable.#getTblProps()}. To be removed in Hive 0.16.
  public Map<String, String> getTblProps() {
    return this.hcatTable.getTblProps();
  }

  /**
   * Gets the file format.
   *
   * @return the file format
   */
  @Deprecated // @deprecated in favour of {@link HCatTable.#fileFormat()}. To be removed in Hive 0.16.
  public String getFileFormat() {
    return this.hcatTable.fileFormat();
  }

  /**
   * Gets the database name.
   *
   * @return the database name
   */
  @Deprecated // @deprecated in favour of {@link HCatTable.#getDbName()}. To be removed in Hive 0.16.
  public String getDatabaseName() {
    return this.hcatTable.getDbName();
  }

 /**
   * Gets the SerDe parameters; for example see {@link org.apache.hive.hcatalog.api.HCatCreateTableDesc.Builder#fieldsTerminatedBy(char)}
   */
  @Deprecated
  public Map<String, String> getSerdeParams() {
    return this.hcatTable.getSerdeParams();
  }

  @Override
  public String toString() {
    return "HCatCreateTableDesc [ " + hcatTable.toString()
        + ", ifNotExists = " + ifNotExists + "]";
  }

  public static class Builder {

    private boolean ifNotExists;
    private HCatTable hcatTable;

    @Deprecated // @deprecated in favour of {@link #Builder(HCatTable, boolean)}. To be removed in Hive 0.16.
    private Builder(String dbName, String tableName, List<HCatFieldSchema> columns) {
      hcatTable = new HCatTable(dbName, tableName).cols(columns);
    }

    private Builder(HCatTable hcatTable, boolean ifNotExists) {
      this.hcatTable = hcatTable;
      this.ifNotExists = ifNotExists;
    }

    /**
     * If not exists.
     *
     * @param ifNotExists If set to true, hive will not throw exception, if a
     * table with the same name already exists.
     * @return the builder
     */
    public Builder ifNotExists(boolean ifNotExists) {
      this.ifNotExists = ifNotExists;
      return this;
    }


    /**
     * Partition cols.
     *
     * @param partCols the partition cols
     * @return the builder
     */
    @Deprecated // @deprecated in favour of {@link HCatTable.#partCols(List<FieldSchema>)}. To be removed in Hive 0.16.
    public Builder partCols(List<HCatFieldSchema> partCols) {
      this.hcatTable.partCols(partCols);
      return this;
    }


    /**
     * Bucket cols.
     *
     * @param bucketCols the bucket cols
     * @return the builder
     */
    @Deprecated // @deprecated in favour of {@link HCatTable.#bucketCols(List<FieldSchema>) and HCatTable.#numBuckets(int)}.
    // To be removed in Hive 0.16.
    public Builder bucketCols(List<String> bucketCols, int buckets) {
      this.hcatTable.bucketCols(bucketCols).numBuckets(buckets);
      return this;
    }

    /**
     * Storage handler.
     *
     * @param storageHandler the storage handler
     * @return the builder
     */
    @Deprecated // @deprecated in favour of {@link HCatTable.#storageHandler(String)}. To be removed in Hive 0.16.
    public Builder storageHandler(String storageHandler) throws HCatException {
      this.hcatTable.storageHandler(storageHandler);
      return this;
    }

    /**
     * Location.
     *
     * @param location the location
     * @return the builder
     */
    @Deprecated // @deprecated in favour of {@link HCatTable.#location(String)}. To be removed in Hive 0.16.
    public Builder location(String location) {
      this.hcatTable.location(location);
      return this;
    }

    /**
     * Comments.
     *
     * @param comment the comment
     * @return the builder
     */
    @Deprecated // @deprecated in favour of {@link HCatTable.#comment(String)}. To be removed in Hive 0.16.
    public Builder comments(String comment) {
      this.hcatTable.comment(comment);
      return this;
    }

    /**
     * Checks if is table external.
     *
     * @param isExternal the is external
     * @return the builder
     */
    @Deprecated // @deprecated in favour of {@link HCatTable.#tableType(HCatTable.Type)}. To be removed in Hive 0.16.
    public Builder isTableExternal(boolean isExternal) {
      this.hcatTable.tableType(isExternal? HCatTable.Type.EXTERNAL_TABLE : HCatTable.Type.MANAGED_TABLE);
      return this;
    }

    /**
     * Sort cols.
     *
     * @param sortCols the sort cols
     * @return the builder
     */
    @Deprecated // @deprecated in favour of {@link HCatTable.#sortCols(ArrayList<Order>)}. To be removed in Hive 0.16.
    public Builder sortCols(ArrayList<Order> sortCols) {
      this.hcatTable.sortCols(sortCols);
      return this;
    }

    /**
     * Tbl props.
     *
     * @param tblProps the tbl props
     * @return the builder
     */
    @Deprecated // @deprecated in favour of {@link HCatTable.#sortCols(Map<String, String>)}.
    // To be removed in Hive 0.16.
    public Builder tblProps(Map<String, String> tblProps) {
      this.hcatTable.tblProps(tblProps);
      return this;
    }

    /**
     * File format.
     *
     * @param format the format
     * @return the builder
     */
    @Deprecated // @deprecated in favour of {@link HCatTable.#fileFormat(String)}. To be removed in Hive 0.16.
    public Builder fileFormat(String format) {
      this.hcatTable.fileFormat(format);
      return this;
    }
    /**
     * See <i>row_format</i> element of CREATE_TABLE DDL for Hive.
     */
    @Deprecated // @deprecated in favour of {@link HCatTable.#fieldsTerminatedBy()}. To be removed in Hive 0.16.
    public Builder fieldsTerminatedBy(char delimiter) {
      return serdeParam(serdeConstants.FIELD_DELIM, Character.toString(delimiter));
    }
    /**
     * See <i>row_format</i> element of CREATE_TABLE DDL for Hive.
     */
    @Deprecated // @deprecated in favour of {@link HCatTable.#escapeChar()}.
    public Builder escapeChar(char escapeChar) {
      return serdeParam(serdeConstants.ESCAPE_CHAR, Character.toString(escapeChar));
    }
    /**
     * See <i>row_format</i> element of CREATE_TABLE DDL for Hive.
     */
    @Deprecated // @deprecated in favour of {@link HCatTable.#collectionItemsTerminatedBy()}. To be removed in Hive 0.16.
    public Builder collectionItemsTerminatedBy(char delimiter) {
      return serdeParam(serdeConstants.COLLECTION_DELIM, Character.toString(delimiter));
    }
    /**
     * See <i>row_format</i> element of CREATE_TABLE DDL for Hive.
     */
    @Deprecated // @deprecated in favour of {@link HCatTable.#mapKeysTerminatedBy()}. To be removed in Hive 0.16.
    public Builder mapKeysTerminatedBy(char delimiter) {
      return serdeParam(serdeConstants.MAPKEY_DELIM, Character.toString(delimiter));
    }
    /**
     * See <i>row_format</i> element of CREATE_TABLE DDL for Hive.
     */
    @Deprecated // @deprecated in favour of {@link HCatTable.#linesTerminatedBy()}. To be removed in Hive 0.16.
    public Builder linesTerminatedBy(char delimiter) {
      return serdeParam(serdeConstants.LINE_DELIM, Character.toString(delimiter));
    }
    /**
     * See <i>row_format</i> element of CREATE_TABLE DDL for Hive.
     */
    @Deprecated // @deprecated in favour of {@link HCatTable.#nullDefinedAs()}. To be removed in Hive 0.16.
    public Builder nullDefinedAs(char nullChar) {
      return serdeParam(serdeConstants.SERIALIZATION_NULL_FORMAT, Character.toString(nullChar));
    }
    /**
     * used for setting arbitrary SerDe parameter
     */
    @Deprecated // @deprecated in favour of {@link HCatTable.#serdeParam(Map<String, String>)}.
    // To be removed in Hive 0.16.
    public Builder serdeParam(String paramName, String value) {
      hcatTable.serdeParam(paramName, value);
      return this;
    }
    /**
     * Builds the HCatCreateTableDesc.
     *
     * @return HCatCreateTableDesc
     * @throws HCatException
     */
    public HCatCreateTableDesc build() throws HCatException {
      return new HCatCreateTableDesc(this.hcatTable, this.ifNotExists);
    }

  } // class Builder;

} // class HCatAddPartitionDesc;
