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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * CreateTableDesc.
 *
 */
@Explain(displayName = "Create Table")
public class CreateTableDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private static Log LOG = LogFactory.getLog(CreateTableDesc.class);
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

  public CreateTableDesc() {
  }

  public CreateTableDesc(String databaseName, String tableName, boolean isExternal,
      List<FieldSchema> cols, List<FieldSchema> partCols,
      List<String> bucketCols, List<Order> sortCols, int numBuckets,
      String fieldDelim, String fieldEscape, String collItemDelim,
      String mapKeyDelim, String lineDelim, String comment, String inputFormat,
      String outputFormat, String location, String serName,
      String storageHandler,
      Map<String, String> serdeProps,
      Map<String, String> tblProps,
      boolean ifNotExists, List<String> skewedColNames, List<List<String>> skewedColValues) {

    this(tableName, isExternal, cols, partCols,
        bucketCols, sortCols, numBuckets, fieldDelim, fieldEscape,
        collItemDelim, mapKeyDelim, lineDelim, comment, inputFormat,
        outputFormat, location, serName, storageHandler, serdeProps,
        tblProps, ifNotExists, skewedColNames, skewedColValues);

    this.databaseName = databaseName;
  }

  public CreateTableDesc(String tableName, boolean isExternal,
      List<FieldSchema> cols, List<FieldSchema> partCols,
      List<String> bucketCols, List<Order> sortCols, int numBuckets,
      String fieldDelim, String fieldEscape, String collItemDelim,
      String mapKeyDelim, String lineDelim, String comment, String inputFormat,
      String outputFormat, String location, String serName,
      String storageHandler,
      Map<String, String> serdeProps,
      Map<String, String> tblProps,
      boolean ifNotExists, List<String> skewedColNames, List<List<String>> skewedColValues) {
    this.tableName = tableName;
    this.isExternal = isExternal;
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

  @Explain(displayName = "name")
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
      if (StringUtils.isEmpty(this.getSerName())
          || conf.getStringCollection(ConfVars.SERDESUSINGMETASTOREFORSCHEMA.varname).contains(this.getSerName())) {
        throw new SemanticException(ErrorMsg.INVALID_TBL_DDL_SERDE.getMsg());
      }
      return;
    }

    if (this.getStorageHandler() == null) {
      try {
        Class<?> origin = Class.forName(this.getOutputFormat(), true,
          JavaUtils.getClassLoader());
        Class<? extends HiveOutputFormat> replaced = HiveFileFormatUtils
          .getOutputFormatSubstitute(origin,false);
        if (replaced == null) {
          throw new SemanticException(ErrorMsg.INVALID_OUTPUT_FORMAT_TYPE
            .getMsg());
        }
      } catch (ClassNotFoundException e) {
        throw new SemanticException(ErrorMsg.INVALID_OUTPUT_FORMAT_TYPE.getMsg());
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
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg());
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
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg());
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
          LOG.error(err);
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

}
