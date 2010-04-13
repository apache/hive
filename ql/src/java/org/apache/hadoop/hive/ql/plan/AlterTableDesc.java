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
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.exec.Utilities;

/**
 * AlterTableDesc.
 *
 */
@Explain(displayName = "Alter Table")
public class AlterTableDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * alterTableTypes.
   *
   */
  public static enum AlterTableTypes {
    RENAME, ADDCOLS, REPLACECOLS, ADDPROPS, ADDSERDE, ADDSERDEPROPS,
    ADDFILEFORMAT, ADDCLUSTERSORTCOLUMN, RENAMECOLUMN, ADDPARTITION,
    TOUCH
  };

  AlterTableTypes op;
  String oldName;
  String newName;
  ArrayList<FieldSchema> newCols;
  String serdeName;
  HashMap<String, String> props;
  String inputFormat;
  String outputFormat;
  String storageHandler;
  int numberBuckets;
  ArrayList<String> bucketColumns;
  ArrayList<Order> sortColumns;

  String oldColName;
  String newColName;
  String newColType;
  String newColComment;
  boolean first;
  String afterCol;
  boolean expectView;

  public AlterTableDesc() {
  }

  /**
   * @param tblName
   *          table name
   * @param oldColName
   *          old column name
   * @param newColName
   *          new column name
   * @param newComment
   * @param newType
   */
  public AlterTableDesc(String tblName, String oldColName, String newColName,
      String newType, String newComment, boolean first, String afterCol) {
    super();
    oldName = tblName;
    this.oldColName = oldColName;
    this.newColName = newColName;
    newColType = newType;
    newColComment = newComment;
    this.first = first;
    this.afterCol = afterCol;
    op = AlterTableTypes.RENAMECOLUMN;
  }

  /**
   * @param oldName
   *          old name of the table
   * @param newName
   *          new name of the table
   */
  public AlterTableDesc(String oldName, String newName) {
    op = AlterTableTypes.RENAME;
    this.oldName = oldName;
    this.newName = newName;
  }

  /**
   * @param name
   *          name of the table
   * @param newCols
   *          new columns to be added
   */
  public AlterTableDesc(String name, List<FieldSchema> newCols,
      AlterTableTypes alterType) {
    op = alterType;
    oldName = name;
    this.newCols = new ArrayList<FieldSchema>(newCols);
  }

  /**
   * @param alterType
   *          type of alter op
   */
  public AlterTableDesc(AlterTableTypes alterType) {
    this(alterType, false);
  }

  /**
   * @param alterType
   *          type of alter op
   */
  public AlterTableDesc(AlterTableTypes alterType, boolean expectView) {
    op = alterType;
    this.expectView = expectView;
  }

  /**
   *
   * @param name
   *          name of the table
   * @param inputFormat
   *          new table input format
   * @param outputFormat
   *          new table output format
   */
  public AlterTableDesc(String name, String inputFormat, String outputFormat,
      String serdeName, String storageHandler) {
    super();
    op = AlterTableTypes.ADDFILEFORMAT;
    oldName = name;
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.serdeName = serdeName;
    this.storageHandler = storageHandler;
  }

  public AlterTableDesc(String tableName, int numBuckets,
      List<String> bucketCols, List<Order> sortCols) {
    oldName = tableName;
    op = AlterTableTypes.ADDCLUSTERSORTCOLUMN;
    numberBuckets = numBuckets;
    bucketColumns = new ArrayList<String>(bucketCols);
    sortColumns = new ArrayList<Order>(sortCols);
  }

  @Explain(displayName = "new columns")
  public List<String> getNewColsString() {
    return Utilities.getFieldSchemaString(getNewCols());
  }

  @Explain(displayName = "type")
  public String getAlterTableTypeString() {
    switch (op) {
    case RENAME:
      return "rename";
    case ADDCOLS:
      return "add columns";
    case REPLACECOLS:
      return "replace columns";
    }

    return "unknown";
  }

  /**
   * @return the old name of the table
   */
  @Explain(displayName = "old name")
  public String getOldName() {
    return oldName;
  }

  /**
   * @param oldName
   *          the oldName to set
   */
  public void setOldName(String oldName) {
    this.oldName = oldName;
  }

  /**
   * @return the newName
   */
  @Explain(displayName = "new name")
  public String getNewName() {
    return newName;
  }

  /**
   * @param newName
   *          the newName to set
   */
  public void setNewName(String newName) {
    this.newName = newName;
  }

  /**
   * @return the op
   */
  public AlterTableTypes getOp() {
    return op;
  }

  /**
   * @param op
   *          the op to set
   */
  public void setOp(AlterTableTypes op) {
    this.op = op;
  }

  /**
   * @return the newCols
   */
  public ArrayList<FieldSchema> getNewCols() {
    return newCols;
  }

  /**
   * @param newCols
   *          the newCols to set
   */
  public void setNewCols(ArrayList<FieldSchema> newCols) {
    this.newCols = newCols;
  }

  /**
   * @return the serdeName
   */
  @Explain(displayName = "deserializer library")
  public String getSerdeName() {
    return serdeName;
  }

  /**
   * @param serdeName
   *          the serdeName to set
   */
  public void setSerdeName(String serdeName) {
    this.serdeName = serdeName;
  }

  /**
   * @return the props
   */
  @Explain(displayName = "properties")
  public HashMap<String, String> getProps() {
    return props;
  }

  /**
   * @param props
   *          the props to set
   */
  public void setProps(HashMap<String, String> props) {
    this.props = props;
  }

  /**
   * @return the input format
   */
  @Explain(displayName = "input format")
  public String getInputFormat() {
    return inputFormat;
  }

  /**
   * @param inputFormat
   *          the input format to set
   */
  public void setInputFormat(String inputFormat) {
    this.inputFormat = inputFormat;
  }

  /**
   * @return the output format
   */
  @Explain(displayName = "output format")
  public String getOutputFormat() {
    return outputFormat;
  }

  /**
   * @param outputFormat
   *          the output format to set
   */
  public void setOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
  }

  /**
   * @return the storage handler
   */
  @Explain(displayName = "storage handler")
  public String getStorageHandler() {
    return storageHandler;
  }

  /**
   * @param storageHandler
   *          the storage handler to set
   */
  public void setStorageHandler(String storageHandler) {
    this.storageHandler = storageHandler;
  }

  /**
   * @return the number of buckets
   */
  public int getNumberBuckets() {
    return numberBuckets;
  }

  /**
   * @param numberBuckets
   *          the number of buckets to set
   */
  public void setNumberBuckets(int numberBuckets) {
    this.numberBuckets = numberBuckets;
  }

  /**
   * @return the bucket columns
   */
  public ArrayList<String> getBucketColumns() {
    return bucketColumns;
  }

  /**
   * @param bucketColumns
   *          the bucket columns to set
   */
  public void setBucketColumns(ArrayList<String> bucketColumns) {
    this.bucketColumns = bucketColumns;
  }

  /**
   * @return the sort columns
   */
  public ArrayList<Order> getSortColumns() {
    return sortColumns;
  }

  /**
   * @param sortColumns
   *          the sort columns to set
   */
  public void setSortColumns(ArrayList<Order> sortColumns) {
    this.sortColumns = sortColumns;
  }

  /**
   * @return old column name
   */
  public String getOldColName() {
    return oldColName;
  }

  /**
   * @param oldColName
   *          the old column name
   */
  public void setOldColName(String oldColName) {
    this.oldColName = oldColName;
  }

  /**
   * @return new column name
   */
  public String getNewColName() {
    return newColName;
  }

  /**
   * @param newColName
   *          the new column name
   */
  public void setNewColName(String newColName) {
    this.newColName = newColName;
  }

  /**
   * @return new column type
   */
  public String getNewColType() {
    return newColType;
  }

  /**
   * @param newType
   *          new column's type
   */
  public void setNewColType(String newType) {
    newColType = newType;
  }

  /**
   * @return new column's comment
   */
  public String getNewColComment() {
    return newColComment;
  }

  /**
   * @param newComment
   *          new column's comment
   */
  public void setNewColComment(String newComment) {
    newColComment = newComment;
  }

  /**
   * @return if the column should be changed to position 0
   */
  public boolean getFirst() {
    return first;
  }

  /**
   * @param first
   *          set the column to position 0
   */
  public void setFirst(boolean first) {
    this.first = first;
  }

  /**
   * @return the column's after position
   */
  public String getAfterCol() {
    return afterCol;
  }

  /**
   * @param afterCol
   *          set the column's after position
   */
  public void setAfterCol(String afterCol) {
    this.afterCol = afterCol;
  }

  /**
   * @return whether to expect a view being altered
   */
  public boolean getExpectView() {
    return expectView;
  }

  /**
   * @param expectView
   *          set whether to expect a view being altered
   */
  public void setExpectView(boolean expectView) {
    this.expectView = expectView;
  }
}
