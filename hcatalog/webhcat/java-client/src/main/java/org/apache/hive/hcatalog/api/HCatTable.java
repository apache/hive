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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The HCatTable is a wrapper around org.apache.hadoop.hive.metastore.api.Table.
 */
public class HCatTable {
  private static final Logger LOG = LoggerFactory.getLogger(HCatTable.class);

  public static enum Type {
    MANAGED_TABLE,
    EXTERNAL_TABLE,
    VIRTUAL_VIEW,
    INDEX_TABLE
  }

  /**
   * Attributes that can be compared between HCatTables.
   */
  public static enum TableAttribute {
    COLUMNS,
    PARTITION_COLUMNS,
    INPUT_FORMAT,
    OUTPUT_FORMAT,
    SERDE,
    SERDE_PROPERTIES,
    STORAGE_HANDLER,
    LOCATION,
    TABLE_PROPERTIES,
    STATS             // TODO: Handle replication of changes to Table-STATS.
  }

  /**
   * The default set of attributes that can be diffed between HCatTables.
   */
  public static final EnumSet<TableAttribute> DEFAULT_COMPARISON_ATTRIBUTES
      = EnumSet.of(TableAttribute.COLUMNS,
                   TableAttribute.INPUT_FORMAT,
                   TableAttribute.OUTPUT_FORMAT,
                   TableAttribute.SERDE,
                   TableAttribute.SERDE_PROPERTIES,
                   TableAttribute.STORAGE_HANDLER,
                   TableAttribute.TABLE_PROPERTIES);

  /**
   * 2 HCatTables are considered equivalent if {@code lhs.diff(rhs).equals(NO_DIFF) == true; }
   */
  public static final EnumSet<TableAttribute> NO_DIFF = EnumSet.noneOf(TableAttribute.class);

  public static final String DEFAULT_SERDE_CLASS = org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName();
  public static final String DEFAULT_INPUT_FORMAT_CLASS = org.apache.hadoop.mapred.TextInputFormat.class.getName();
  public static final String DEFAULT_OUTPUT_FORMAT_CLASS = org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat.class.getName();

  private String dbName = MetaStoreUtils.DEFAULT_DATABASE_NAME;
  private String tableName;
  private HiveConf conf;
  private String tableType;
  private boolean isExternal;
  private List<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
  private List<HCatFieldSchema> partCols = new ArrayList<HCatFieldSchema>();
  private StorageDescriptor sd;
  private String fileFormat;
  private Map<String, String> tblProps = new HashMap<String, String>();
  private String comment = "";
  private String owner;

  public HCatTable(String dbName, String tableName) {
    this.dbName = StringUtils.isBlank(dbName)? MetaStoreUtils.DEFAULT_DATABASE_NAME : dbName;
    this.tableName = tableName;
    this.sd = new StorageDescriptor();
    this.sd.setInputFormat(DEFAULT_INPUT_FORMAT_CLASS);
    this.sd.setOutputFormat(DEFAULT_OUTPUT_FORMAT_CLASS);
    this.sd.setSerdeInfo(new SerDeInfo());
    this.sd.getSerdeInfo().setSerializationLib(DEFAULT_SERDE_CLASS);
    this.sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    this.sd.getSerdeInfo().getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1"); // Default serialization format.
  }

  HCatTable(Table hiveTable) throws HCatException {
    tableName = hiveTable.getTableName();
    dbName = hiveTable.getDbName();
    tableType = hiveTable.getTableType();
    isExternal = hiveTable.getTableType().equals(TableType.EXTERNAL_TABLE.toString());
    sd = hiveTable.getSd();
    for (FieldSchema colFS : sd.getCols()) {
      cols.add(HCatSchemaUtils.getHCatFieldSchema(colFS));
    }
    partCols = new ArrayList<HCatFieldSchema>();
    for (FieldSchema colFS : hiveTable.getPartitionKeys()) {
      partCols.add(HCatSchemaUtils.getHCatFieldSchema(colFS));
    }
    if (hiveTable.getParameters() != null) {
      tblProps.putAll(hiveTable.getParameters());
    }

    if (StringUtils.isNotBlank(tblProps.get("comment"))) {
      comment = tblProps.get("comment");
    }

    owner = hiveTable.getOwner();
  }

  Table toHiveTable() throws HCatException {
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

    if (StringUtils.isNotBlank(this.comment)) {
      newTable.putToParameters("comment", comment);
    }

    newTable.setSd(sd);
    if (partCols != null) {
      ArrayList<FieldSchema> hivePtnCols = new ArrayList<FieldSchema>();
      for (HCatFieldSchema fs : partCols) {
        hivePtnCols.add(HCatSchemaUtils.getFieldSchema(fs));
      }
      newTable.setPartitionKeys(hivePtnCols);
    }

    newTable.setCreateTime((int) (System.currentTimeMillis() / 1000));
    newTable.setLastAccessTimeIsSet(false);
    try {
      // TODO: Verify that this works for systems using UGI.doAs() (e.g. Oozie).
      newTable.setOwner(owner == null? getConf().getUser() : owner);
    }
    catch (Exception exception) {
      throw new HCatException("Unable to determine owner of table (" + dbName + "." + tableName
          + ") from HiveConf.");
    }
    return newTable;
  }

  void setConf(Configuration conf) {
    if (conf instanceof HiveConf) {
      this.conf = (HiveConf)conf;
    }
    else {
      this.conf = new HiveConf(conf, getClass());
    }
  }

  HiveConf getConf() {
    if (conf == null) {
      LOG.warn("Conf hasn't been set yet. Using defaults.");
      conf = new HiveConf();
    }
    return conf;
  }

  StorageDescriptor getSd() {
    return sd;
  }

  /**
   * Gets the table name.
   *
   * @return the table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Setter for TableName.
   */
  public HCatTable tableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  /**
   * Gets the db name.
   *
   * @return the db name
   */
  public String getDbName() {
    return dbName;
  }

  /**
   * Setter for db-name.
   */
  public HCatTable dbName(String dbName) {
    this.dbName = dbName;
    return this;
  }

  /**
   * Gets the columns.
   *
   * @return the columns
   */
  public List<HCatFieldSchema> getCols() {
    return cols;
  }

  /**
   * Setter for Column schemas.
   */
  public HCatTable cols(List<HCatFieldSchema> cols) {
    if (!this.cols.equals(cols)) {
      this.cols.clear();
      this.cols.addAll(cols);
      this.sd.setCols(HCatSchemaUtils.getFieldSchemas(cols));
    }
    return this;
  }

  /**
   * Gets the part columns.
   *
   * @return the part columns
   */
  public List<HCatFieldSchema> getPartCols() {
    return partCols;
  }

  /**
   * Setter for list of partition columns.
   */
  public HCatTable partCols(List<HCatFieldSchema> partCols) {
    this.partCols = partCols;
    return this;
  }

  /**
   * Setter for individual partition columns.
   */
  public HCatTable partCol(HCatFieldSchema partCol) {
    if (this.partCols == null) {
      this.partCols = new ArrayList<HCatFieldSchema>();
    }

    this.partCols.add(partCol);
    return this;
  }

  /**
   * Gets the bucket columns.
   *
   * @return the bucket columns
   */
  public List<String> getBucketCols() {
    return this.sd.getBucketCols();
  }

  /**
   * Setter for list of bucket columns.
   */
  public HCatTable bucketCols(List<String> bucketCols) {
    this.sd.setBucketCols(bucketCols);
    return this;
  }

  /**
   * Gets the sort columns.
   *
   * @return the sort columns
   */
  public List<Order> getSortCols() {
    return this.sd.getSortCols();
  }

  /**
   * Setter for Sort-cols.
   */
  public HCatTable sortCols(List<Order> sortCols) {
    this.sd.setSortCols(sortCols);
    return this;
  }

  /**
   * Gets the number of buckets.
   *
   * @return the number of buckets
   */
  public int getNumBuckets() {
    return this.sd.getNumBuckets();
  }

  /**
   * Setter for number of buckets.
   */
  public HCatTable numBuckets(int numBuckets) {
    this.sd.setNumBuckets(numBuckets);
    return this;
  }

  /**
   * Gets the storage handler.
   *
   * @return the storage handler
   */
  public String getStorageHandler() {
    return this.tblProps.get(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE);
  }

  /**
   * Setter for StorageHandler class.
   */
  public HCatTable storageHandler(String storageHandler) throws HCatException {
    this.tblProps.put(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE,
        storageHandler);
    LOG.warn("HiveStorageHandlers can't be reliably instantiated on the client-side. " +
        "Attempting to derive Input/OutputFormat settings from StorageHandler, on best effort: ");
    try {
      HiveStorageHandler sh = HiveUtils.getStorageHandler(getConf(), storageHandler);
      this.sd.setInputFormat(sh.getInputFormatClass().getName());
      this.sd.setOutputFormat(sh.getOutputFormatClass().getName());
      this.sd.getSerdeInfo().setSerializationLib(sh.getSerDeClass().getName());
    } catch (HiveException e) {
      LOG.warn("Could not derive Input/OutputFormat and SerDe settings from storageHandler. " +
          "These values need to be set explicitly.", e);
    }

    return this;
  }

  /**
   * Gets the table props.
   *
   * @return the table props
   */
  public Map<String, String> getTblProps() {
    return tblProps;
  }

  /**
   * Setter for TableProperty map.
   */
  public HCatTable tblProps(Map<String, String> tblProps) {
    if (!this.tblProps.equals(tblProps)) {
      this.tblProps.clear();
      this.tblProps.putAll(tblProps);
    }
    return this;
  }

  /**
   * Gets the tableType.
   *
   * @return the tableType
   */
  public String getTabletype() {
    return tableType;
  }

  /**
   * Setter for table-type.
   */
  public HCatTable tableType(Type tableType) {
    this.tableType = tableType.name();
    this.isExternal = tableType.equals(Type.EXTERNAL_TABLE);
    return this;
  }

  private SerDeInfo getSerDeInfo() {
    if (!sd.isSetSerdeInfo()) {
      sd.setSerdeInfo(new SerDeInfo());
    }
    return sd.getSerdeInfo();
  }

  public HCatTable fileFormat(String fileFormat) {
    this.fileFormat = fileFormat;

    if (fileFormat.equalsIgnoreCase("sequencefile")) {
      inputFileFormat(SequenceFileInputFormat.class.getName());
      outputFileFormat(HiveSequenceFileOutputFormat.class.getName());
      serdeLib(LazySimpleSerDe.class.getName());
    }
    else
    if (fileFormat.equalsIgnoreCase("rcfile")) {
      inputFileFormat(RCFileInputFormat.class.getName());
      outputFileFormat(RCFileOutputFormat.class.getName());
      serdeLib(LazyBinaryColumnarSerDe.class.getName());
    }
    else
    if (fileFormat.equalsIgnoreCase("orcfile")) {
      inputFileFormat(OrcInputFormat.class.getName());
      outputFileFormat(OrcOutputFormat.class.getName());
      serdeLib(OrcSerde.class.getName());
    }

    return this;
  }

  public String fileFormat() {
    return fileFormat;
  }
  /**
   * Gets the input file format.
   *
   * @return the input file format
   */
  public String getInputFileFormat() {
    return sd.getInputFormat();
  }

  /**
   * Setter for InputFormat class.
   */
  public HCatTable inputFileFormat(String inputFileFormat) {
    sd.setInputFormat(inputFileFormat);
    return this;
  }

  /**
   * Gets the output file format.
   *
   * @return the output file format
   */
  public String getOutputFileFormat() {
    return sd.getOutputFormat();
  }

  /**
   * Setter for OutputFormat class.
   */
  public HCatTable outputFileFormat(String outputFileFormat) {
    this.sd.setOutputFormat(outputFileFormat);
    return this;
  }

  /**
   * Gets the serde lib.
   *
   * @return the serde lib
   */
  public String getSerdeLib() {
    return getSerDeInfo().getSerializationLib();
  }

  /**
   * Setter for SerDe class name.
   */
  public HCatTable serdeLib(String serde) {
    getSerDeInfo().setSerializationLib(serde);
    return this;
  }

  public HCatTable serdeParams(Map<String, String> serdeParams) {
    getSerDeInfo().setParameters(serdeParams);
    return this;
  }

  public HCatTable serdeParam(String paramName, String value) {
    SerDeInfo serdeInfo = getSerDeInfo();
    if (serdeInfo.getParameters() == null) {
      serdeInfo.setParameters(new HashMap<String, String>());
    }
    serdeInfo.getParameters().put(paramName, value);

    return this;
  }

  /**
   * Returns parameters such as field delimiter,etc.
   */
  public Map<String, String> getSerdeParams() {
    return getSerDeInfo().getParameters();
  }

  /**
   * Gets the location.
   *
   * @return the location
   */
  public String getLocation() {
    return sd.getLocation();
  }

  /**
   * Setter for location.
   */
  public HCatTable location(String location) {
    this.sd.setLocation(location);
    return this;
  }

  /**
   * Getter for table-owner.
   */
  public String owner() {
    return owner;
  }

  /**
   * Setter for table-owner.
   */
  public HCatTable owner(String owner) {
    this.owner = owner;
    return this;
  }

  public String comment() {
    return this.comment;
  }

  /**
   * Setter for table-level comment.
   */
  public HCatTable comment(String comment) {
    this.comment = comment;
    return this;
  }

  /**
   * See <i>row_format</i> element of CREATE_TABLE DDL for Hive.
   */
  public HCatTable fieldsTerminatedBy(char delimiter) {
    return serdeParam(serdeConstants.FIELD_DELIM, Character.toString(delimiter));
  }
  /**
   * See <i>row_format</i> element of CREATE_TABLE DDL for Hive.
   */
  public HCatTable escapeChar(char escapeChar) {
    return serdeParam(serdeConstants.ESCAPE_CHAR, Character.toString(escapeChar));
  }
  /**
   * See <i>row_format</i> element of CREATE_TABLE DDL for Hive.
   */
  public HCatTable collectionItemsTerminatedBy(char delimiter) {
    return serdeParam(serdeConstants.COLLECTION_DELIM, Character.toString(delimiter));
  }
  /**
   * See <i>row_format</i> element of CREATE_TABLE DDL for Hive.
   */
  public HCatTable mapKeysTerminatedBy(char delimiter) {
    return serdeParam(serdeConstants.MAPKEY_DELIM, Character.toString(delimiter));
  }
  /**
   * See <i>row_format</i> element of CREATE_TABLE DDL for Hive.
   */
  public HCatTable linesTerminatedBy(char delimiter) {
    return serdeParam(serdeConstants.LINE_DELIM, Character.toString(delimiter));
  }
  /**
   * See <i>row_format</i> element of CREATE_TABLE DDL for Hive.
   */
  public HCatTable nullDefinedAs(char nullChar) {
    return serdeParam(serdeConstants.SERIALIZATION_NULL_FORMAT, Character.toString(nullChar));
  }

  @Override
  public String toString() {
    return "HCatTable [ "
        + "tableName=" + tableName + ", "
        + "dbName=" + dbName + ", "
        + "tableType=" + tableType + ", "
        + "cols=" + cols + ", "
        + "partCols=" + partCols + ", "
        + "bucketCols=" + getBucketCols() + ", "
        + "numBuckets=" + getNumBuckets() + ", "
        + "sortCols=" + getSortCols() + ", "
        + "inputFormat=" + getInputFileFormat() + ", "
        + "outputFormat=" + getOutputFileFormat() + ", "
        + "storageHandler=" + getStorageHandler() + ", "
        + "serde=" + getSerdeLib() + ", "
        + "tblProps=" + getTblProps() + ", "
        + "location=" + getLocation() + ", "
        + "owner=" + owner() + " ]";

  }

  /**
   * Method to compare the attributes of 2 HCatTable instances.
   * @param rhs The other table being compared against. Can't be null.
   * @param attributesToCheck The list of TableAttributes being compared.
   * @return {@code EnumSet<TableAttribute>} containing all the attribute that differ between {@code this} and rhs.
   * Subset of {@code attributesToCheck}.
   */
  public EnumSet<TableAttribute> diff(HCatTable rhs, EnumSet<TableAttribute> attributesToCheck) {
    EnumSet<TableAttribute> theDiff = EnumSet.noneOf(TableAttribute.class);

    for (TableAttribute attribute : attributesToCheck) {

      if (attribute.equals(TableAttribute.COLUMNS)) {
        if (!rhs.getCols().containsAll(getCols()) ||
            !getCols().containsAll(rhs.getCols())) {
          theDiff.add(TableAttribute.COLUMNS);
        }
      }

      if (attribute.equals(TableAttribute.INPUT_FORMAT)) {
        if ((getInputFileFormat() == null && rhs.getInputFileFormat() != null)
            || (getInputFileFormat() != null && (rhs.getInputFileFormat() == null || !rhs.getInputFileFormat().equals(getInputFileFormat())))) {
          theDiff.add(TableAttribute.INPUT_FORMAT);
        }
      }

      if (attribute.equals(TableAttribute.OUTPUT_FORMAT)) {
        if ((getOutputFileFormat() == null && rhs.getOutputFileFormat() != null)
          || (getOutputFileFormat() != null && (rhs.getOutputFileFormat() == null || !rhs.getOutputFileFormat().equals(getOutputFileFormat())))) {
          theDiff.add(TableAttribute.OUTPUT_FORMAT);
        }
      }

      if (attribute.equals(TableAttribute.STORAGE_HANDLER)) {
        if ((getStorageHandler() == null && rhs.getStorageHandler() != null)
            || (getStorageHandler() != null && (rhs.getStorageHandler() == null || !rhs.getStorageHandler().equals(getStorageHandler())))) {
          theDiff.add(TableAttribute.STORAGE_HANDLER);
        }
      }

      if (attribute.equals(TableAttribute.SERDE)) {
        if ((getSerdeLib() == null && rhs.getSerdeLib() != null)
            || (getSerdeLib() != null && (rhs.getSerdeLib() == null || !rhs.getSerdeLib().equals(getSerdeLib())))) {
          theDiff.add(TableAttribute.SERDE);
        }
      }

      if (attribute.equals(TableAttribute.SERDE_PROPERTIES)) {
        if (!equivalent(sd.getSerdeInfo().getParameters(), rhs.sd.getSerdeInfo().getParameters())) {
          theDiff.add(TableAttribute.SERDE_PROPERTIES);
        }
      }

      if (attribute.equals(TableAttribute.TABLE_PROPERTIES)) {
        if (!equivalent(tblProps, rhs.tblProps)) {
          theDiff.add(TableAttribute.TABLE_PROPERTIES);
        }
      }

    }

    return theDiff;
  }

  /**
   * Helper method to compare 2 Map instances, for equivalence.
   * @param lhs First map to be compared.
   * @param rhs Second map to be compared.
   * @return true, if the 2 Maps contain the same entries.
   */
  private static boolean equivalent(Map<String, String> lhs, Map<String, String> rhs) {
    return lhs.size() == rhs.size() && Maps.difference(lhs, rhs).areEqual();
  }

  /**
   * Method to compare the attributes of 2 HCatTable instances.
   * Only the {@code DEFAULT_COMPARISON_ATTRIBUTES} are compared.
   * @param rhs The other table being compared against. Can't be null.
   * @return {@code EnumSet<TableAttribute>} containing all the attribute that differ between {@code this} and rhs.
   * Subset of {@code DEFAULT_COMPARISON_ATTRIBUTES}.
   */
  public EnumSet<TableAttribute> diff (HCatTable rhs) {
    return diff(rhs, DEFAULT_COMPARISON_ATTRIBUTES);
  }

  /**
   * Method to "adopt" the specified attributes from rhs into this HCatTable object.
   * @param rhs The "source" table from which attributes are to be copied from.
   * @param attributes The set of attributes to be copied from rhs. Usually the result of {@code this.diff(rhs)}.
   * @return This HCatTable
   * @throws HCatException
   */
  public HCatTable resolve(HCatTable rhs, EnumSet<TableAttribute> attributes) throws HCatException {

    if (rhs == this)
      return this;

    for (TableAttribute attribute : attributes) {

      if (attribute.equals(TableAttribute.COLUMNS)) {
        cols(rhs.cols);
      }

      if (attribute.equals(TableAttribute.INPUT_FORMAT)) {
        inputFileFormat(rhs.getInputFileFormat());
      }

      if (attribute.equals(TableAttribute.OUTPUT_FORMAT)) {
        outputFileFormat(rhs.getOutputFileFormat());
      }

      if (attribute.equals(TableAttribute.SERDE)) {
        serdeLib(rhs.getSerdeLib());
      }

      if (attribute.equals(TableAttribute.SERDE_PROPERTIES)) {
        serdeParams(rhs.getSerdeParams());
      }

      if (attribute.equals(TableAttribute.STORAGE_HANDLER)) {
        storageHandler(rhs.getStorageHandler());
      }

      if (attribute.equals(TableAttribute.TABLE_PROPERTIES)) {
        tblProps(rhs.tblProps);
      }
    }

    return this;
  }
}
