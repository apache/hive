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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class HCatCreateTableDesc for defining attributes for a new table.
 */
@SuppressWarnings("deprecation")
public class HCatCreateTableDesc {

  private static final Logger LOG = LoggerFactory.getLogger(HCatCreateTableDesc.class);

  private String tableName;
  private String dbName;
  private boolean isExternal;
  private String comment;
  private String location;
  private List<HCatFieldSchema> cols;
  private List<HCatFieldSchema> partCols;
  private List<String> bucketCols;
  private int numBuckets;
  private List<Order> sortCols;
  private Map<String, String> tblProps;
  private boolean ifNotExists;
  private String fileFormat;
  private String inputformat;
  private String outputformat;
  private String serde;
  private String storageHandler;
  private Map<String, String> serdeParams;

  private HCatCreateTableDesc(String dbName, String tableName, List<HCatFieldSchema> columns) {
    this.dbName = dbName;
    this.tableName = tableName;
    this.cols = columns;
  }

  /**
   * Creates a builder for defining attributes.
   *
   * @param dbName the db name
   * @param tableName the table name
   * @param columns the columns
   * @return the builder
   */
  public static Builder create(String dbName, String tableName, List<HCatFieldSchema> columns) {
    return new Builder(dbName, tableName, columns);
  }

  Table toHiveTable(HiveConf conf) throws HCatException {

    Table newTable = new Table();
    newTable.setDbName(dbName);
    newTable.setTableName(tableName);
    if (tblProps != null) {
      newTable.setParameters(tblProps);
    }

    if (isExternal) {
      newTable.putToParameters("EXTERNAL", "TRUE");
      newTable.setTableType(TableType.EXTERNAL_TABLE.toString());
    } else {
      newTable.setTableType(TableType.MANAGED_TABLE.toString());
    }

    StorageDescriptor sd = new StorageDescriptor();
    sd.setSerdeInfo(new SerDeInfo());
    if (location != null) {
      sd.setLocation(location);
    }
    if (this.comment != null) {
      newTable.putToParameters("comment", comment);
    }
    if (!StringUtils.isEmpty(fileFormat)) {
      sd.setInputFormat(inputformat);
      sd.setOutputFormat(outputformat);
      if (serde != null) {
        sd.getSerdeInfo().setSerializationLib(serde);
      } else {
        LOG.info("Using LazySimpleSerDe for table " + tableName);
        sd.getSerdeInfo()
          .setSerializationLib(
            org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class
              .getName());
      }
    } else {
      try {
        LOG.info("Creating instance of storage handler to get input/output, serder info.");
        HiveStorageHandler sh = HiveUtils.getStorageHandler(conf,
          storageHandler);
        sd.setInputFormat(sh.getInputFormatClass().getName());
        sd.setOutputFormat(sh.getOutputFormatClass().getName());
        sd.getSerdeInfo().setSerializationLib(
          sh.getSerDeClass().getName());
        newTable.putToParameters(
          org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE,
          storageHandler);
      } catch (HiveException e) {
        throw new HCatException(
          "Exception while creating instance of storage handler",
          e);
      }
    }
    newTable.setSd(sd);
    if(serdeParams != null) {
      for(Map.Entry<String, String> param : serdeParams.entrySet()) {
        sd.getSerdeInfo().putToParameters(param.getKey(), param.getValue());
      }
    }
    if (this.partCols != null) {
      ArrayList<FieldSchema> hivePtnCols = new ArrayList<FieldSchema>();
      for (HCatFieldSchema fs : this.partCols) {
        hivePtnCols.add(HCatSchemaUtils.getFieldSchema(fs));
      }
      newTable.setPartitionKeys(hivePtnCols);
    }

    if (this.cols != null) {
      ArrayList<FieldSchema> hiveTblCols = new ArrayList<FieldSchema>();
      for (HCatFieldSchema fs : this.cols) {
        hiveTblCols.add(HCatSchemaUtils.getFieldSchema(fs));
      }
      newTable.getSd().setCols(hiveTblCols);
    }

    if (this.bucketCols != null) {
      newTable.getSd().setBucketCols(bucketCols);
      newTable.getSd().setNumBuckets(numBuckets);
    }

    if (this.sortCols != null) {
      newTable.getSd().setSortCols(sortCols);
    }

    newTable.setCreateTime((int) (System.currentTimeMillis() / 1000));
    newTable.setLastAccessTimeIsSet(false);
    return newTable;
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
  public String getTableName() {
    return this.tableName;
  }

  /**
   * Gets the cols.
   *
   * @return the cols
   */
  public List<HCatFieldSchema> getCols() {
    return this.cols;
  }

  /**
   * Gets the partition cols.
   *
   * @return the partition cols
   */
  public List<HCatFieldSchema> getPartitionCols() {
    return this.partCols;
  }

  /**
   * Gets the bucket cols.
   *
   * @return the bucket cols
   */
  public List<String> getBucketCols() {
    return this.bucketCols;
  }

  public int getNumBuckets() {
    return this.numBuckets;
  }

  /**
   * Gets the comments.
   *
   * @return the comments
   */
  public String getComments() {
    return this.comment;
  }

  /**
   * Gets the storage handler.
   *
   * @return the storage handler
   */
  public String getStorageHandler() {
    return this.storageHandler;
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
   * Gets the external.
   *
   * @return the external
   */
  public boolean getExternal() {
    return this.isExternal;
  }

  /**
   * Gets the sort cols.
   *
   * @return the sort cols
   */
  public List<Order> getSortCols() {
    return this.sortCols;
  }

  /**
   * Gets the tbl props.
   *
   * @return the tbl props
   */
  public Map<String, String> getTblProps() {
    return this.tblProps;
  }

  /**
   * Gets the file format.
   *
   * @return the file format
   */
  public String getFileFormat() {
    return this.fileFormat;
  }

  /**
   * Gets the database name.
   *
   * @return the database name
   */
  public String getDatabaseName() {
    return this.dbName;
  }
 /**
   * Gets the SerDe parameters; for example see {@link org.apache.hive.hcatalog.api.HCatCreateTableDesc.Builder#fieldsTerminatedBy(char)}
   */
  public Map<String, String> getSerdeParams() {
    return serdeParams;
  }

  @Override
  public String toString() {
    return "HCatCreateTableDesc ["
      + (tableName != null ? "tableName=" + tableName + ", " : "tableName=null")
      + (dbName != null ? "dbName=" + dbName + ", " : "dbName=null")
      + "isExternal="
      + isExternal
      + ", "
      + (comment != null ? "comment=" + comment + ", " : "comment=null")
      + (location != null ? "location=" + location + ", " : "location=null")
      + (cols != null ? "cols=" + cols + ", " : "cols=null")
      + (partCols != null ? "partCols=" + partCols + ", " : "partCols=null")
      + (bucketCols != null ? "bucketCols=" + bucketCols + ", " : "bucketCols=null")
      + "numBuckets="
      + numBuckets
      + ", "
      + (sortCols != null ? "sortCols=" + sortCols + ", " : "sortCols=null")
      + (tblProps != null ? "tblProps=" + tblProps + ", " : "tblProps=null")
      + "ifNotExists="
      + ifNotExists
      + ", "
      + (fileFormat != null ? "fileFormat=" + fileFormat + ", " : "fileFormat=null")
      + (inputformat != null ? "inputformat=" + inputformat + ", "
      : "inputformat=null")
      + (outputformat != null ? "outputformat=" + outputformat + ", "
      : "outputformat=null")
      + (serde != null ? "serde=" + serde + ", " : "serde=null")
      + (storageHandler != null ? "storageHandler=" + storageHandler
      : "storageHandler=null") 
      + ",serdeParams=" + (serdeParams == null ? "null" : serdeParams)
      + "]";
  }

  public static class Builder {

    private String tableName;
    private boolean isExternal;
    private List<HCatFieldSchema> cols;
    private List<HCatFieldSchema> partCols;
    private List<String> bucketCols;
    private List<Order> sortCols;
    private int numBuckets;
    private String comment;
    private String fileFormat;
    private String location;
    private String storageHandler;
    private Map<String, String> tblProps;
    private boolean ifNotExists;
    private String dbName;
    private Map<String, String> serdeParams;


    private Builder(String dbName, String tableName, List<HCatFieldSchema> columns) {
      this.dbName = dbName;
      this.tableName = tableName;
      this.cols = columns;
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
    public Builder partCols(List<HCatFieldSchema> partCols) {
      this.partCols = partCols;
      return this;
    }


    /**
     * Bucket cols.
     *
     * @param bucketCols the bucket cols
     * @return the builder
     */
    public Builder bucketCols(List<String> bucketCols, int buckets) {
      this.bucketCols = bucketCols;
      this.numBuckets = buckets;
      return this;
    }

    /**
     * Storage handler.
     *
     * @param storageHandler the storage handler
     * @return the builder
     */
    public Builder storageHandler(String storageHandler) {
      this.storageHandler = storageHandler;
      return this;
    }

    /**
     * Location.
     *
     * @param location the location
     * @return the builder
     */
    public Builder location(String location) {
      this.location = location;
      return this;
    }

    /**
     * Comments.
     *
     * @param comment the comment
     * @return the builder
     */
    public Builder comments(String comment) {
      this.comment = comment;
      return this;
    }

    /**
     * Checks if is table external.
     *
     * @param isExternal the is external
     * @return the builder
     */
    public Builder isTableExternal(boolean isExternal) {
      this.isExternal = isExternal;
      return this;
    }

    /**
     * Sort cols.
     *
     * @param sortCols the sort cols
     * @return the builder
     */
    public Builder sortCols(ArrayList<Order> sortCols) {
      this.sortCols = sortCols;
      return this;
    }

    /**
     * Tbl props.
     *
     * @param tblProps the tbl props
     * @return the builder
     */
    public Builder tblProps(Map<String, String> tblProps) {
      this.tblProps = tblProps;
      return this;
    }

    /**
     * File format.
     *
     * @param format the format
     * @return the builder
     */
    public Builder fileFormat(String format) {
      this.fileFormat = format;
      return this;
    }
    /**
     * See <i>row_format</i> element of CREATE_TABLE DDL for Hive.
     */
    public Builder fieldsTerminatedBy(char delimiter) {
      return serdeParam(serdeConstants.FIELD_DELIM, Character.toString(delimiter));
    }
    /**
     * See <i>row_format</i> element of CREATE_TABLE DDL for Hive.
     */
    public Builder escapeChar(char escapeChar) {
      return serdeParam(serdeConstants.ESCAPE_CHAR, Character.toString(escapeChar));
    }
    /**
     * See <i>row_format</i> element of CREATE_TABLE DDL for Hive.
     */
    public Builder collectionItemsTerminatedBy(char delimiter) {
      return serdeParam(serdeConstants.COLLECTION_DELIM, Character.toString(delimiter));
    }
    /**
     * See <i>row_format</i> element of CREATE_TABLE DDL for Hive.
     */
    public Builder mapKeysTerminatedBy(char delimiter) {
      return serdeParam(serdeConstants.MAPKEY_DELIM, Character.toString(delimiter));
    }
    /**
     * See <i>row_format</i> element of CREATE_TABLE DDL for Hive.
     */
    public Builder linesTerminatedBy(char delimiter) {
      return serdeParam(serdeConstants.LINE_DELIM, Character.toString(delimiter));
    }
    /**
     * See <i>row_format</i> element of CREATE_TABLE DDL for Hive.
     */
    public Builder nullDefinedAs(char nullChar) {
      return serdeParam(serdeConstants.SERIALIZATION_NULL_FORMAT, Character.toString(nullChar));
    }
    /**
     * used for setting arbitrary SerDe parameter
     */
    public Builder serdeParam(String paramName, String value) {
      if(serdeParams == null) {
        serdeParams = new HashMap<String, String>();
      }
      serdeParams.put(paramName, value);
      return this;
    }
    /**
     * Builds the HCatCreateTableDesc.
     *
     * @return HCatCreateTableDesc
     * @throws HCatException
     */
    public HCatCreateTableDesc build() throws HCatException {
      if (this.dbName == null) {
        LOG.info("Database name found null. Setting db to :"
          + MetaStoreUtils.DEFAULT_DATABASE_NAME);
        this.dbName = MetaStoreUtils.DEFAULT_DATABASE_NAME;
      }
      HCatCreateTableDesc desc = new HCatCreateTableDesc(this.dbName,
        this.tableName, this.cols);
      desc.ifNotExists = this.ifNotExists;
      desc.isExternal = this.isExternal;
      desc.comment = this.comment;
      desc.partCols = this.partCols;
      desc.bucketCols = this.bucketCols;
      desc.numBuckets = this.numBuckets;
      desc.location = this.location;
      desc.tblProps = this.tblProps;
      desc.sortCols = this.sortCols;
      desc.serde = null;
      if (!StringUtils.isEmpty(fileFormat)) {
        desc.fileFormat = fileFormat;
        if ("SequenceFile".equalsIgnoreCase(fileFormat)) {
          desc.inputformat = SequenceFileInputFormat.class.getName();
          desc.outputformat = SequenceFileOutputFormat.class
            .getName();
        } else if ("RCFile".equalsIgnoreCase(fileFormat)) {
          desc.inputformat = RCFileInputFormat.class.getName();
          desc.outputformat = RCFileOutputFormat.class.getName();
          desc.serde = ColumnarSerDe.class.getName();
        }
        desc.storageHandler = StringUtils.EMPTY;
      } else if (!StringUtils.isEmpty(storageHandler)) {
        desc.storageHandler = storageHandler;
      } else {
        desc.fileFormat = "TextFile";
        LOG.info("Using text file format for the table.");
        desc.inputformat = TextInputFormat.class.getName();
        LOG.info("Table input format:" + desc.inputformat);
        desc.outputformat = IgnoreKeyTextOutputFormat.class
          .getName();
        LOG.info("Table output format:" + desc.outputformat);
      }
      desc.serdeParams = this.serdeParams;
      return desc;
    }
  }
}
