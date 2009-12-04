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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.exec.Utilities;

@explain(displayName="Alter Table")
public class alterTableDesc extends ddlDesc implements Serializable 
{
  private static final long serialVersionUID = 1L;
  public static enum alterTableTypes {RENAME, ADDCOLS, REPLACECOLS, ADDPROPS, ADDSERDE, ADDSERDEPROPS, ADDFILEFORMAT, ADDCLUSTERSORTCOLUMN};
    
  alterTableTypes      op;
  String               oldName;
  String               newName;
  List<FieldSchema>    newCols;
  String               serdeName;
  Map<String, String>  props;
  String	             inputFormat;
	String               outputFormat;
	int                  numberBuckets;
	List<String>         bucketColumns;
	List<Order>          sortColumns;
	
  /**
   * @param oldName old name of the table
   * @param newName new name of the table
   */
  public alterTableDesc(String oldName, String newName) {
    op = alterTableTypes.RENAME;
    this.oldName = oldName;
    this.newName = newName;
  }

  /**
   * @param name name of the table
   * @param newCols new columns to be added
   */
  public alterTableDesc(String name, List<FieldSchema> newCols, alterTableTypes alterType) {
    this.op = alterType;
    this.oldName = name;
    this.newCols = newCols;
  }
  
  /**
   * @param alterType type of alter op
   */
  public alterTableDesc(alterTableTypes alterType) {
    this.op = alterType;
  }

  /**
   * 
   * @param name name of the table
   * @param inputFormat new table input format
   * @param outputFormat new table output format 
   */
  public alterTableDesc(String name, String inputFormat, String outputFormat, String serdeName) {
	  super();
	  this.op = alterTableTypes.ADDFILEFORMAT;
	  this.oldName = name;
	  this.inputFormat = inputFormat;
	  this.outputFormat = outputFormat;
	  this.serdeName = serdeName;
  }
  
  public alterTableDesc(String tableName, int numBuckets,
      List<String> bucketCols, List<Order> sortCols) {
  	this.oldName = tableName;
  	this.op = alterTableTypes.ADDCLUSTERSORTCOLUMN;
  	this.numberBuckets = numBuckets;
  	this.bucketColumns = bucketCols;
  	this.sortColumns = sortCols;
  }

	/**
   * @return the old name of the table
   */
  @explain(displayName="old name")
  public String getOldName() {
    return oldName;
  }

  /**
   * @param oldName the oldName to set
   */
  public void setOldName(String oldName) {
    this.oldName = oldName;
  }

  /**
   * @return the newName
   */
  @explain(displayName="new name")
  public String getNewName() {
    return newName;
  }

  /**
   * @param newName the newName to set
   */
  public void setNewName(String newName) {
    this.newName = newName;
  }

  /**
   * @return the op
   */
  public alterTableTypes getOp() {
    return op;
  }

  @explain(displayName="type")
  public String getAlterTableTypeString() {
    switch(op) {
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
   * @param op the op to set
   */
  public void setOp(alterTableTypes op) {
    this.op = op;
  }

  /**
   * @return the newCols
   */
  public List<FieldSchema> getNewCols() {
    return newCols;
  }

  @explain(displayName="new columns")
  public List<String> getNewColsString() {
    return Utilities.getFieldSchemaString(getNewCols());
  }
  /**
   * @param newCols the newCols to set
   */
  public void setNewCols(List<FieldSchema> newCols) {
    this.newCols = newCols;
  }

  /**
   * @return the serdeName
   */
  @explain(displayName="deserializer library")
  public String getSerdeName() {
    return serdeName;
  }

  /**
   * @param serdeName the serdeName to set
   */
  public void setSerdeName(String serdeName) {
    this.serdeName = serdeName;
  }

  /**
   * @return the props
   */
  @explain(displayName="properties")
  public Map<String, String> getProps() {
    return props;
  }

  /**
   * @param props the props to set
   */
  public void setProps(Map<String, String> props) {
    this.props = props;
  }
  
  /**
   * @return the input format
   */
  @explain(displayName="input format")
	public String getInputFormat() {
  	return inputFormat;
  }

  /**
   * @param inputFormat the input format to set
   */
	public void setInputFormat(String inputFormat) {
  	this.inputFormat = inputFormat;
  }

  /**
   * @return the output format
   */
  @explain(displayName="output format")
	public String getOutputFormat() {
  	return outputFormat;
  }

  /**
   * @param outputFormat the output format to set
   */
	public void setOutputFormat(String outputFormat) {
  	this.outputFormat = outputFormat;
  }

	/**
	 * @return the number of buckets
	 */
	public int getNumberBuckets() {
  	return numberBuckets;
  }

	/**
	 * @param numberBuckets the number of buckets to set
	 */
	public void setNumberBuckets(int numberBuckets) {
  	this.numberBuckets = numberBuckets;
  }

	/**
	 * @return the bucket columns
	 */
	public List<String> getBucketColumns() {
  	return bucketColumns;
  }

	/**
	 * @param bucketColumns the bucket columns to set
	 */
	public void setBucketColumns(List<String> bucketColumns) {
  	this.bucketColumns = bucketColumns;
  }

	/**
	 * @return the sort columns
	 */
	public List<Order> getSortColumns() {
  	return sortColumns;
  }

	/**
	 * @param sortColumns the sort columns to set
	 */
	public void setSortColumns(List<Order> sortColumns) {
  	this.sortColumns = sortColumns;
  }

}
