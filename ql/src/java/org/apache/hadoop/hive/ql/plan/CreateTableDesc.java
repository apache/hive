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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * CreateTableDesc.
 *
 */
@Explain(displayName = "Create Table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CreateTableDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private static Logger LOG = LoggerFactory.getLogger(CreateTableDesc.class);
  String databaseName;
  String tableName;
  boolean isExternal;
  List<FieldSchema> cols;
  List<FieldSchema> partCols;
  List<String> bucketCols;
  List<Order> sortCols;
  int numBuckets;
  String fieldDelim;
  String fieldEscape;
  String collItemDelim;
  String mapKeyDelim;
  String lineDelim;
  String nullFormat;
  String comment;
  String inputFormat;
  String outputFormat;
  String location;
  String serName;
  String storageHandler;
  Map<String, String> serdeProps;
  Map<String, String> tblProps;
  boolean ifNotExists;
  List<String> skewedColNames;
  List<List<String>> skewedColValues;
  boolean isStoredAsSubDirectories = false;
  boolean isTemporary = false;
  private boolean isMaterialization = false;
  private boolean replaceMode = false;
  private ReplicationSpec replicationSpec = null;
  private boolean isCTAS = false;
  List<SQLPrimaryKey> primaryKeys;
  List<SQLForeignKey> foreignKeys;

  public CreateTableDesc() {
  }

  public CreateTableDesc(String databaseName, String tableName, boolean isExternal, boolean isTemporary,
      List<FieldSchema> cols, List<FieldSchema> partCols,
      List<String> bucketCols, List<Order> sortCols, int numBuckets,
      String fieldDelim, String fieldEscape, String collItemDelim,
      String mapKeyDelim, String lineDelim, String comment, String inputFormat,
      String outputFormat, String location, String serName,
      String storageHandler,
      Map<String, String> serdeProps,
      Map<String, String> tblProps,
      boolean ifNotExists, List<String> skewedColNames, List<List<String>> skewedColValues,
      List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys) {

    this(tableName, isExternal, isTemporary, cols, partCols,
        bucketCols, sortCols, numBuckets, fieldDelim, fieldEscape,
        collItemDelim, mapKeyDelim, lineDelim, comment, inputFormat,
        outputFormat, location, serName, storageHandler, serdeProps,
        tblProps, ifNotExists, skewedColNames, skewedColValues, primaryKeys, foreignKeys);

    this.databaseName = databaseName;
  }

  public CreateTableDesc(String databaseName, String tableName, boolean isExternal, boolean isTemporary,
                         List<FieldSchema> cols, List<FieldSchema> partCols,
                         List<String> bucketCols, List<Order> sortCols, int numBuckets,
                         String fieldDelim, String fieldEscape, String collItemDelim,
                         String mapKeyDelim, String lineDelim, String comment, String inputFormat,
                         String outputFormat, String location, String serName,
                         String storageHandler,
                         Map<String, String> serdeProps,
                         Map<String, String> tblProps,
                         boolean ifNotExists, List<String> skewedColNames, List<List<String>> skewedColValues,
                         boolean isCTAS, List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys) {
    this(databaseName, tableName, isExternal, isTemporary, cols, partCols,
            bucketCols, sortCols, numBuckets, fieldDelim, fieldEscape,
            collItemDelim, mapKeyDelim, lineDelim, comment, inputFormat,
            outputFormat, location, serName, storageHandler, serdeProps,
            tblProps, ifNotExists, skewedColNames, skewedColValues, primaryKeys, foreignKeys);
    this.isCTAS = isCTAS;

  }


  public CreateTableDesc(String tableName, boolean isExternal, boolean isTemporary,
      List<FieldSchema> cols, List<FieldSchema> partCols,
      List<String> bucketCols, List<Order> sortCols, int numBuckets,
      String fieldDelim, String fieldEscape, String collItemDelim,
      String mapKeyDelim, String lineDelim, String comment, String inputFormat,
      String outputFormat, String location, String serName,
      String storageHandler,
      Map<String, String> serdeProps,
      Map<String, String> tblProps,
      boolean ifNotExists, List<String> skewedColNames, List<List<String>> skewedColValues,
      List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys) {
    this.tableName = tableName;
    this.isExternal = isExternal;
    this.isTemporary = isTemporary;
    this.bucketCols = new ArrayList<String>(bucketCols);
    this.sortCols = new ArrayList<Order>(sortCols);
    this.collItemDelim = collItemDelim;
    this.cols = new ArrayList<FieldSchema>(cols);
    this.comment = comment;
    this.fieldDelim = fieldDelim;
    this.fieldEscape = fieldEscape;
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.lineDelim = lineDelim;
    this.location = location;
    this.mapKeyDelim = mapKeyDelim;
    this.numBuckets = numBuckets;
    this.partCols = new ArrayList<FieldSchema>(partCols);
    this.serName = serName;
    this.storageHandler = storageHandler;
    this.serdeProps = serdeProps;
    this.tblProps = tblProps;
    this.ifNotExists = ifNotExists;
    this.skewedColNames = copyList(skewedColNames);
    this.skewedColValues = copyList(skewedColValues);
    if (primaryKeys == null) {
      this.primaryKeys = new ArrayList<SQLPrimaryKey>();
    } else {
      this.primaryKeys = new ArrayList<SQLPrimaryKey>(primaryKeys);
    }
    if (foreignKeys == null) {
      this.foreignKeys = new ArrayList<SQLForeignKey>();
    } else {
      this.foreignKeys = new ArrayList<SQLForeignKey>(foreignKeys);
    }
  }

  private static <T> List<T> copyList(List<T> copy) {
    return copy == null ? null : new ArrayList<T>(copy);
  }

  @Explain(displayName = "columns")
  public List<String> getColsString() {
    return Utilities.getFieldSchemaString(getCols());
  }

  @Explain(displayName = "partition columns")
  public List<String> getPartColsString() {
    return Utilities.getFieldSchemaString(getPartCols());
  }

  @Explain(displayName = "if not exists", displayOnlyOnTrue = true)
  public boolean getIfNotExists() {
    return ifNotExists;
  }

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  @Explain(displayName = "name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return tableName;
  }

  public String getDatabaseName(){
    return databaseName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public List<FieldSchema> getCols() {
    return cols;
  }

  public void setCols(ArrayList<FieldSchema> cols) {
    this.cols = cols;
  }

  public List<FieldSchema> getPartCols() {
    return partCols;
  }

  public void setPartCols(ArrayList<FieldSchema> partCols) {
    this.partCols = partCols;
  }

  public List<SQLPrimaryKey> getPrimaryKeys() {
    return primaryKeys;
  }

  public void setPrimaryKeys(ArrayList<SQLPrimaryKey> primaryKeys) {
    this.primaryKeys = primaryKeys;
  }

  public List<SQLForeignKey> getForeignKeys() {
    return foreignKeys;
  }

  public void setForeignKeys(ArrayList<SQLForeignKey> foreignKeys) {
    this.foreignKeys = foreignKeys;
  }

  @Explain(displayName = "bucket columns")
  public List<String> getBucketCols() {
    return bucketCols;
  }

  public void setBucketCols(ArrayList<String> bucketCols) {
    this.bucketCols = bucketCols;
  }

  @Explain(displayName = "# buckets")
  public Integer getNumBucketsExplain() {
    if (numBuckets == -1) {
      return null;
    } else {
      return numBuckets;
    }
  }

  public int getNumBuckets() {
    return numBuckets;
  }

  public void setNumBuckets(int numBuckets) {
    this.numBuckets = numBuckets;
  }

  @Explain(displayName = "field delimiter")
  public String getFieldDelim() {
    return fieldDelim;
  }

  public void setFieldDelim(String fieldDelim) {
    this.fieldDelim = fieldDelim;
  }

  @Explain(displayName = "field escape")
  public String getFieldEscape() {
    return fieldEscape;
  }

  public void setFieldEscape(String fieldEscape) {
    this.fieldEscape = fieldEscape;
  }

  @Explain(displayName = "collection delimiter")
  public String getCollItemDelim() {
    return collItemDelim;
  }

  public void setCollItemDelim(String collItemDelim) {
    this.collItemDelim = collItemDelim;
  }

  @Explain(displayName = "map key delimiter")
  public String getMapKeyDelim() {
    return mapKeyDelim;
  }

  public void setMapKeyDelim(String mapKeyDelim) {
    this.mapKeyDelim = mapKeyDelim;
  }

  @Explain(displayName = "line delimiter")
  public String getLineDelim() {
    return lineDelim;
  }

  public void setLineDelim(String lineDelim) {
    this.lineDelim = lineDelim;
  }

  @Explain(displayName = "comment")
  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  @Explain(displayName = "input format")
  public String getInputFormat() {
    return inputFormat;
  }

  public void setInputFormat(String inputFormat) {
    this.inputFormat = inputFormat;
  }

  @Explain(displayName = "output format")
  public String getOutputFormat() {
    return outputFormat;
  }

  public void setOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
  }

  @Explain(displayName = "storage handler")
  public String getStorageHandler() {
    return storageHandler;
  }

  public void setStorageHandler(String storageHandler) {
    this.storageHandler = storageHandler;
  }

  @Explain(displayName = "location")
  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  @Explain(displayName = "isExternal", displayOnlyOnTrue = true)
  public boolean isExternal() {
    return isExternal;
  }

  public void setExternal(boolean isExternal) {
    this.isExternal = isExternal;
  }

  /**
   * @return the sortCols
   */
  @Explain(displayName = "sort columns")
  public List<Order> getSortCols() {
    return sortCols;
  }

  /**
   * @param sortCols
   *          the sortCols to set
   */
  public void setSortCols(ArrayList<Order> sortCols) {
    this.sortCols = sortCols;
  }

  /**
   * @return the serDeName
   */
  @Explain(displayName = "serde name")
  public String getSerName() {
    return serName;
  }

  /**
   * @param serName
   *          the serName to set
   */
  public void setSerName(String serName) {
    this.serName = serName;
  }

  /**
   * @return the serDe properties
   */
  @Explain(displayName = "serde properties")
  public Map<String, String> getSerdeProps() {
    return serdeProps;
  }

  /**
   * @param serdeProps
   *          the serde properties to set
   */
  public void setSerdeProps(Map<String, String> serdeProps) {
    this.serdeProps = serdeProps;
  }

  /**
   * @return the table properties
   */
  @Explain(displayName = "table properties")
  public Map<String, String> getTblProps() {
    return tblProps;
  }

  /**
   * @param tblProps
   *          the table properties to set
   */
  public void setTblProps(Map<String, String> tblProps) {
    this.tblProps = tblProps;
  }

  /**
   * @return the skewedColNames
   */
  public List<String> getSkewedColNames() {
    return skewedColNames;
  }

  /**
   * @param skewedColNames the skewedColNames to set
   */
  public void setSkewedColNames(ArrayList<String> skewedColNames) {
    this.skewedColNames = skewedColNames;
  }

  /**
   * @return the skewedColValues
   */
  public List<List<String>> getSkewedColValues() {
    return skewedColValues;
  }

  /**
   * @param skewedColValues the skewedColValues to set
   */
  public void setSkewedColValues(ArrayList<List<String>> skewedColValues) {
    this.skewedColValues = skewedColValues;
  }

  public void validate(HiveConf conf)
      throws SemanticException {

    if ((this.getCols() == null) || (this.getCols().size() == 0)) {
      // for now make sure that serde exists
      if (Table.hasMetastoreBasedSchema(conf, serName) &&
              StringUtils.isEmpty(getStorageHandler())) {
        throw new SemanticException(ErrorMsg.INVALID_TBL_DDL_SERDE.getMsg());
      }
      return;
    }

    if (this.getStorageHandler() == null) {
      try {
        Class<?> origin = Class.forName(this.getOutputFormat(), true,
          Utilities.getSessionSpecifiedClassLoader());
        Class<? extends OutputFormat> replaced = HiveFileFormatUtils
          .getOutputFormatSubstitute(origin);
        if (!HiveOutputFormat.class.isAssignableFrom(replaced)) {
          throw new SemanticException(ErrorMsg.INVALID_OUTPUT_FORMAT_TYPE
            .getMsg());
        }
      } catch (ClassNotFoundException e) {
        throw new SemanticException(ErrorMsg.CLASSPATH_ERROR.getMsg(), e);
      }
    }

    List<String> colNames = ParseUtils.validateColumnNameUniqueness(this.getCols());

    if (this.getBucketCols() != null) {
      // all columns in cluster and sort are valid columns
      Iterator<String> bucketCols = this.getBucketCols().iterator();
      while (bucketCols.hasNext()) {
        String bucketCol = bucketCols.next();
        boolean found = false;
        Iterator<String> colNamesIter = colNames.iterator();
        while (colNamesIter.hasNext()) {
          String colName = colNamesIter.next();
          if (bucketCol.equalsIgnoreCase(colName)) {
            found = true;
            break;
          }
        }
        if (!found) {
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(" \'" + bucketCol + "\'"));
        }
      }
    }

    if (this.getSortCols() != null) {
      // all columns in cluster and sort are valid columns
      Iterator<Order> sortCols = this.getSortCols().iterator();
      while (sortCols.hasNext()) {
        String sortCol = sortCols.next().getCol();
        boolean found = false;
        Iterator<String> colNamesIter = colNames.iterator();
        while (colNamesIter.hasNext()) {
          String colName = colNamesIter.next();
          if (sortCol.equalsIgnoreCase(colName)) {
            found = true;
            break;
          }
        }
        if (!found) {
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(" \'" + sortCol + "\'"));
        }
      }
    }

    if (this.getPartCols() != null) {
      // there is no overlap between columns and partitioning columns
      Iterator<FieldSchema> partColsIter = this.getPartCols().iterator();
      while (partColsIter.hasNext()) {
        FieldSchema fs = partColsIter.next();
        String partCol = fs.getName();
        TypeInfo pti = null;
        try {
          pti = TypeInfoFactory.getPrimitiveTypeInfo(fs.getType());
        } catch (Exception err) {
          LOG.error("Failed to get type info", err);
        }
        if(null == pti){
          throw new SemanticException(ErrorMsg.PARTITION_COLUMN_NON_PRIMITIVE.getMsg() + " Found "
              + partCol + " of type: " + fs.getType());
        }
        Iterator<String> colNamesIter = colNames.iterator();
        while (colNamesIter.hasNext()) {
          String colName = BaseSemanticAnalyzer.unescapeIdentifier(colNamesIter.next());
          if (partCol.equalsIgnoreCase(colName)) {
            throw new SemanticException(
                ErrorMsg.COLUMN_REPEATED_IN_PARTITIONING_COLS.getMsg());
          }
        }
      }
    }

    /* Validate skewed information. */
    ValidationUtility.validateSkewedInformation(colNames, this.getSkewedColNames(),
        this.getSkewedColValues());
  }

  /**
   * @return the isStoredAsSubDirectories
   */
  public boolean isStoredAsSubDirectories() {
    return isStoredAsSubDirectories;
  }

  /**
   * @param isStoredAsSubDirectories the isStoredAsSubDirectories to set
   */
  public void setStoredAsSubDirectories(boolean isStoredAsSubDirectories) {
    this.isStoredAsSubDirectories = isStoredAsSubDirectories;
  }

  /**
   * @return the nullFormat
   */
  public String getNullFormat() {
    return nullFormat;
  }

  /**
   * Set null format string
   * @param nullFormat
   */
  public void setNullFormat(String nullFormat) {
    this.nullFormat = nullFormat;
  }

  /**
   * @return the isTemporary
   */
  @Explain(displayName = "isTemporary", displayOnlyOnTrue = true)
  public boolean isTemporary() {
    return isTemporary;
  }

  /**
   * @param isTemporary table is Temporary or not.
   */
  public void setTemporary(boolean isTemporary) {
    this.isTemporary = isTemporary;
  }

  /**
   * @return the isMaterialization
   */
  @Explain(displayName = "isMaterialization", displayOnlyOnTrue = true)
  public boolean isMaterialization() {
    return isMaterialization;
  }

  /**
   * @param isMaterialization table is a materialization or not.
   */
  public void setMaterialization(boolean isMaterialization) {
    this.isMaterialization = isMaterialization;
  }

  /**
   * @param replaceMode Determine if this CreateTable should behave like a replace-into alter instead
   */
  public void setReplaceMode(boolean replaceMode) {
    this.replaceMode = replaceMode;
  }

  /**
   * @return true if this CreateTable should behave like a replace-into alter instead
   */
  public boolean getReplaceMode() {
    return replaceMode;
  }

  /**
   * @param replicationSpec Sets the replication spec governing this create.
   * This parameter will have meaningful values only for creates happening as a result of a replication.
   */
  public void setReplicationSpec(ReplicationSpec replicationSpec) {
    this.replicationSpec = replicationSpec;
  }

  /**
   * @return what kind of replication scope this drop is running under.
   * This can result in a "CREATE/REPLACE IF NEWER THAN" kind of semantic
   */
  public ReplicationSpec getReplicationSpec(){
    if (replicationSpec == null){
      this.replicationSpec = new ReplicationSpec();
    }
    return this.replicationSpec;
  }

  public boolean isCTAS() {
    return isCTAS;
  }

  public Table toTable(HiveConf conf) throws HiveException {
    String databaseName = getDatabaseName();
    String tableName = getTableName();

    if (databaseName == null || tableName.contains(".")) {
      String[] names = Utilities.getDbTableName(tableName);
      databaseName = names[0];
      tableName = names[1];
    }

    Table tbl = new Table(databaseName, tableName);

    if (getTblProps() != null) {
      tbl.getTTable().getParameters().putAll(getTblProps());
    }

    if (getPartCols() != null) {
      tbl.setPartCols(getPartCols());
    }

    if (getNumBuckets() != -1) {
      tbl.setNumBuckets(getNumBuckets());
    }

    if (getStorageHandler() != null) {
      tbl.setProperty(
              org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE,
              getStorageHandler());
    }
    HiveStorageHandler storageHandler = tbl.getStorageHandler();

    /*
     * We use LazySimpleSerDe by default.
     *
     * If the user didn't specify a SerDe, and any of the columns are not simple
     * types, we will have to use DynamicSerDe instead.
     */
    if (getSerName() == null) {
      if (storageHandler == null) {
        LOG.info("Default to LazySimpleSerDe for table " + tableName);
        tbl.setSerializationLib(LazySimpleSerDe.class.getName());
      } else {
        String serDeClassName = storageHandler.getSerDeClass().getName();
        LOG.info("Use StorageHandler-supplied " + serDeClassName
                + " for table " + tableName);
        tbl.setSerializationLib(serDeClassName);
      }
    } else {
      // let's validate that the serde exists
      DDLTask.validateSerDe(getSerName(), conf);
      tbl.setSerializationLib(getSerName());
    }

    if (getFieldDelim() != null) {
      tbl.setSerdeParam(serdeConstants.FIELD_DELIM, getFieldDelim());
      tbl.setSerdeParam(serdeConstants.SERIALIZATION_FORMAT, getFieldDelim());
    }
    if (getFieldEscape() != null) {
      tbl.setSerdeParam(serdeConstants.ESCAPE_CHAR, getFieldEscape());
    }

    if (getCollItemDelim() != null) {
      tbl.setSerdeParam(serdeConstants.COLLECTION_DELIM, getCollItemDelim());
    }
    if (getMapKeyDelim() != null) {
      tbl.setSerdeParam(serdeConstants.MAPKEY_DELIM, getMapKeyDelim());
    }
    if (getLineDelim() != null) {
      tbl.setSerdeParam(serdeConstants.LINE_DELIM, getLineDelim());
    }
    if (getNullFormat() != null) {
      tbl.setSerdeParam(serdeConstants.SERIALIZATION_NULL_FORMAT, getNullFormat());
    }
    if (getSerdeProps() != null) {
      Iterator<Map.Entry<String, String>> iter = getSerdeProps().entrySet()
              .iterator();
      while (iter.hasNext()) {
        Map.Entry<String, String> m = iter.next();
        tbl.setSerdeParam(m.getKey(), m.getValue());
      }
    }

    if (getCols() != null) {
      tbl.setFields(getCols());
    }
    if (getBucketCols() != null) {
      tbl.setBucketCols(getBucketCols());
    }
    if (getSortCols() != null) {
      tbl.setSortCols(getSortCols());
    }
    if (getComment() != null) {
      tbl.setProperty("comment", getComment());
    }
    if (getLocation() != null) {
      tbl.setDataLocation(new Path(getLocation()));
    }

    if (getSkewedColNames() != null) {
      tbl.setSkewedColNames(getSkewedColNames());
    }
    if (getSkewedColValues() != null) {
      tbl.setSkewedColValues(getSkewedColValues());
    }

    tbl.getTTable().setTemporary(isTemporary());

    tbl.setStoredAsSubDirectories(isStoredAsSubDirectories());

    tbl.setInputFormatClass(getInputFormat());
    tbl.setOutputFormatClass(getOutputFormat());

    // only persist input/output format to metadata when it is explicitly specified.
    // Otherwise, load lazily via StorageHandler at query time.
    if (getInputFormat() != null && !getInputFormat().isEmpty()) {
      tbl.getTTable().getSd().setInputFormat(tbl.getInputFormatClass().getName());
    }
    if (getOutputFormat() != null && !getOutputFormat().isEmpty()) {
      tbl.getTTable().getSd().setOutputFormat(tbl.getOutputFormatClass().getName());
    }

    if (!Utilities.isDefaultNameNode(conf) && DDLTask.doesTableNeedLocation(tbl)) {
      // If location is specified - ensure that it is a full qualified name
      DDLTask.makeLocationQualified(tbl.getDbName(), tbl.getTTable().getSd(), tableName, conf);
    }

    if (isExternal()) {
      tbl.setProperty("EXTERNAL", "TRUE");
      tbl.setTableType(TableType.EXTERNAL_TABLE);
    }

    // If the sorted columns is a superset of bucketed columns, store this fact.
    // It can be later used to
    // optimize some group-by queries. Note that, the order does not matter as
    // long as it in the first
    // 'n' columns where 'n' is the length of the bucketed columns.
    if ((tbl.getBucketCols() != null) && (tbl.getSortCols() != null)) {
      List<String> bucketCols = tbl.getBucketCols();
      List<Order> sortCols = tbl.getSortCols();

      if ((sortCols.size() > 0) && (sortCols.size() >= bucketCols.size())) {
        boolean found = true;

        Iterator<String> iterBucketCols = bucketCols.iterator();
        while (iterBucketCols.hasNext()) {
          String bucketCol = iterBucketCols.next();
          boolean colFound = false;
          for (int i = 0; i < bucketCols.size(); i++) {
            if (bucketCol.equals(sortCols.get(i).getCol())) {
              colFound = true;
              break;
            }
          }
          if (colFound == false) {
            found = false;
            break;
          }
        }
        if (found) {
          tbl.setProperty("SORTBUCKETCOLSPREFIX", "TRUE");
        }
      }
    }
    if (getLocation() == null && !this.isCTAS) {
      if (!tbl.isPartitioned() && conf.getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
        StatsSetupConst.setBasicStatsStateForCreateTable(tbl.getTTable().getParameters(),
            StatsSetupConst.TRUE);
      }
    } else {
      StatsSetupConst.setBasicStatsStateForCreateTable(tbl.getTTable().getParameters(),
          StatsSetupConst.FALSE);
    }
    return tbl;
  }


}
