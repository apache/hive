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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.plan;

import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * AlterTableDesc.
 *
 */
@Explain(displayName = "Alter Table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterTableDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * alterTableTypes.
   *
   */
  public static enum AlterTableTypes {
    RENAME("rename"), ADDCOLS("add columns"), REPLACECOLS("replace columns"),
    ADDPROPS("add props"), DROPPROPS("drop props"), ADDSERDE("add serde"), ADDSERDEPROPS("add serde props"),
    ADDFILEFORMAT("add fileformat"), ADDCLUSTERSORTCOLUMN("add cluster sort column"),
    RENAMECOLUMN("rename column"), ADDPARTITION("add partition"), TOUCH("touch"), ARCHIVE("archieve"),
    UNARCHIVE("unarchieve"), ALTERLOCATION("alter location"),
    DROPPARTITION("drop partition"), RENAMEPARTITION("rename partition"), ADDSKEWEDBY("add skew column"),
    ALTERSKEWEDLOCATION("alter skew location"), ALTERBUCKETNUM("alter bucket number"),
    ALTERPARTITION("alter partition"), COMPACT("compact"),
    TRUNCATE("truncate"), MERGEFILES("merge files"), DROPCONSTRAINT("drop constraint"), ADDCONSTRAINT("add constraint");
    ;

    private final String name;
    private AlterTableTypes(String name) { this.name = name; }
    public String getName() { return name; }

    public static final List<AlterTableTypes> nonNativeTableAllowedTypes = 
        ImmutableList.of(ADDPROPS, DROPPROPS); 
  }

  public static enum ProtectModeType {
    NO_DROP, OFFLINE, READ_ONLY, NO_DROP_CASCADE
  }

  public static final Set<AlterTableTypes> alterTableTypesWithPartialSpec =
      new HashSet<AlterTableDesc.AlterTableTypes>();

  static {
    alterTableTypesWithPartialSpec.add(AlterTableDesc.AlterTableTypes.ADDCOLS);
    alterTableTypesWithPartialSpec.add(AlterTableDesc.AlterTableTypes.REPLACECOLS);
    alterTableTypesWithPartialSpec.add(AlterTableDesc.AlterTableTypes.RENAMECOLUMN);
    alterTableTypesWithPartialSpec.add(AlterTableDesc.AlterTableTypes.ADDPROPS);
    alterTableTypesWithPartialSpec.add(AlterTableDesc.AlterTableTypes.DROPPROPS);
    alterTableTypesWithPartialSpec.add(AlterTableDesc.AlterTableTypes.ADDSERDE);
    alterTableTypesWithPartialSpec.add(AlterTableDesc.AlterTableTypes.ADDSERDEPROPS);
    alterTableTypesWithPartialSpec.add(AlterTableDesc.AlterTableTypes.ADDFILEFORMAT);
  }

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
  HashMap<String, String> partSpec;
  private String newLocation;
  boolean protectModeEnable;
  ProtectModeType protectModeType;
  Map<List<String>, String> skewedLocations;
  boolean isTurnOffSkewed = false;
  boolean isStoredAsSubDirectories = false;
  List<String> skewedColNames;
  List<List<String>> skewedColValues;
  Table table;
  boolean isDropIfExists = false;
  boolean isTurnOffSorting = false;
  boolean isCascade = false;
  EnvironmentContext environmentContext;
  String dropConstraintName;
  List<SQLPrimaryKey> primaryKeyCols;
  List<SQLForeignKey> foreignKeyCols;
  List<SQLUniqueConstraint> uniqueConstraintCols;
  List<SQLNotNullConstraint> notNullConstraintCols;
  ReplicationSpec replicationSpec;

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
  public AlterTableDesc(String tblName, HashMap<String, String> partSpec,
      String oldColName, String newColName, String newType, String newComment,
      boolean first, String afterCol, boolean isCascade) {
    super();
    oldName = tblName;
    this.partSpec = partSpec;
    this.oldColName = oldColName;
    this.newColName = newColName;
    newColType = newType;
    newColComment = newComment;
    this.first = first;
    this.afterCol = afterCol;
    op = AlterTableTypes.RENAMECOLUMN;
    this.isCascade = isCascade;
  }

  public AlterTableDesc(String tblName, HashMap<String, String> partSpec,
      String oldColName, String newColName, String newType, String newComment,
      boolean first, String afterCol, boolean isCascade, List<SQLPrimaryKey> primaryKeyCols,
      List<SQLForeignKey> foreignKeyCols, List<SQLUniqueConstraint> uniqueConstraintCols,
      List<SQLNotNullConstraint> notNullConstraintCols) {
    super();
    oldName = tblName;
    this.partSpec = partSpec;
    this.oldColName = oldColName;
    this.newColName = newColName;
    newColType = newType;
    newColComment = newComment;
    this.first = first;
    this.afterCol = afterCol;
    op = AlterTableTypes.RENAMECOLUMN;
    this.isCascade = isCascade;
    this.primaryKeyCols = primaryKeyCols;
    this.foreignKeyCols = foreignKeyCols;
    this.uniqueConstraintCols = uniqueConstraintCols;
    this.notNullConstraintCols = notNullConstraintCols;
  }

  /**
   * @param oldName
   *          old name of the table
   * @param newName
   *          new name of the table
   * @param expectView
   *          Flag to denote if current table can be a view
   * @param replicationSpec
   *          Replication specification with current event ID
   */
  public AlterTableDesc(String oldName, String newName, boolean expectView, ReplicationSpec replicationSpec) {
    op = AlterTableTypes.RENAME;
    this.oldName = oldName;
    this.newName = newName;
    this.expectView = expectView;
    this.replicationSpec = replicationSpec;
  }

  /**
   * @param name
   *          name of the table
   * @param newCols
   *          new columns to be added
   */
  public AlterTableDesc(String name, HashMap<String, String> partSpec, List<FieldSchema> newCols,
      AlterTableTypes alterType, boolean isCascade) {
    op = alterType;
    oldName = name;
    this.newCols = new ArrayList<FieldSchema>(newCols);
    this.partSpec = partSpec;
    this.isCascade = isCascade;
  }

  /**
   * @param alterType
   *          type of alter op
   * @param replicationSpec
   *          Replication specification with current event ID
   */
  public AlterTableDesc(AlterTableTypes alterType, ReplicationSpec replicationSpec) {
    op = alterType;
    this.replicationSpec = replicationSpec;
  }

  /**
   * @param alterType
   *          type of alter op
   */
  public AlterTableDesc(AlterTableTypes alterType) {
    this(alterType, null, false);
  }

  /**
   * @param alterType
   *          type of alter op
   * @param expectView
   *          Flag to denote if current table can be a view
   * @param partSpec
   *          Partition specifier with map of key and values.
   */
  public AlterTableDesc(AlterTableTypes alterType, HashMap<String, String> partSpec, boolean expectView) {
    op = alterType;
    this.partSpec = partSpec;
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
   * @param partSpec
   */
  public AlterTableDesc(String name, String inputFormat, String outputFormat,
      String serdeName, String storageHandler, HashMap<String, String> partSpec) {
    super();
    op = AlterTableTypes.ADDFILEFORMAT;
    oldName = name;
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.serdeName = serdeName;
    this.storageHandler = storageHandler;
    this.partSpec = partSpec;
  }

  public AlterTableDesc(String tableName, int numBuckets,
      List<String> bucketCols, List<Order> sortCols, HashMap<String, String> partSpec) {
    oldName = tableName;
    op = AlterTableTypes.ADDCLUSTERSORTCOLUMN;
    numberBuckets = numBuckets;
    bucketColumns = new ArrayList<String>(bucketCols);
    sortColumns = new ArrayList<Order>(sortCols);
    this.partSpec = partSpec;
  }

  public AlterTableDesc(String tableName, boolean sortingOff, HashMap<String, String> partSpec) {
    oldName = tableName;
    op = AlterTableTypes.ADDCLUSTERSORTCOLUMN;
    isTurnOffSorting = sortingOff;
    this.partSpec = partSpec;
  }

  public AlterTableDesc(String tableName, String newLocation,
      HashMap<String, String> partSpec) {
    op = AlterTableTypes.ALTERLOCATION;
    this.oldName = tableName;
    this.newLocation = newLocation;
    this.partSpec = partSpec;
  }

  public AlterTableDesc(String tableName, Map<List<String>, String> locations,
      HashMap<String, String> partSpec) {
    op = AlterTableTypes.ALTERSKEWEDLOCATION;
    this.oldName = tableName;
    this.skewedLocations = locations;
    this.partSpec = partSpec;
  }

  public AlterTableDesc(String tableName, boolean turnOffSkewed,
      List<String> skewedColNames, List<List<String>> skewedColValues) {
    oldName = tableName;
    op = AlterTableTypes.ADDSKEWEDBY;
    this.isTurnOffSkewed = turnOffSkewed;
    this.skewedColNames = new ArrayList<String>(skewedColNames);
    this.skewedColValues = new ArrayList<List<String>>(skewedColValues);
  }

  public AlterTableDesc(String tableName, HashMap<String, String> partSpec, int numBuckets) {
    op = AlterTableTypes.ALTERBUCKETNUM;
    this.oldName = tableName;
    this.partSpec = partSpec;
    this.numberBuckets = numBuckets;
  }

  public AlterTableDesc(String tableName, String dropConstraintName, ReplicationSpec replicationSpec) {
    this.oldName = tableName;
    this.dropConstraintName = dropConstraintName;
    this.replicationSpec = replicationSpec;
    op = AlterTableTypes.DROPCONSTRAINT;
  }

  public AlterTableDesc(String tableName, List<SQLPrimaryKey> primaryKeyCols,
          List<SQLForeignKey> foreignKeyCols, List<SQLUniqueConstraint> uniqueConstraintCols,
          ReplicationSpec replicationSpec) {
    this.oldName = tableName;
    this.primaryKeyCols = primaryKeyCols;
    this.foreignKeyCols = foreignKeyCols;
    this.uniqueConstraintCols = uniqueConstraintCols;
    this.replicationSpec = replicationSpec;
    op = AlterTableTypes.ADDCONSTRAINT;
  }

  public AlterTableDesc(String tableName, List<SQLPrimaryKey> primaryKeyCols,
      List<SQLForeignKey> foreignKeyCols, List<SQLUniqueConstraint> uniqueConstraintCols,
      List<SQLNotNullConstraint> notNullConstraintCols, ReplicationSpec replicationSpec) {
    this.oldName = tableName;
    this.primaryKeyCols = primaryKeyCols;
    this.foreignKeyCols = foreignKeyCols;
    this.uniqueConstraintCols = uniqueConstraintCols;
    this.notNullConstraintCols = notNullConstraintCols;
    this.replicationSpec = replicationSpec;
    op = AlterTableTypes.ADDCONSTRAINT;
  }

  @Explain(displayName = "new columns", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public List<String> getNewColsString() {
    return Utilities.getFieldSchemaString(getNewCols());
  }

  @Explain(displayName = "type", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getAlterTableTypeString() {
    return op.getName();
  }

  /**
   * @return the old name of the table
   */
  @Explain(displayName = "old name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
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
  @Explain(displayName = "new name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
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
  @Explain(displayName = "input format", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
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
  @Explain(displayName = "output format", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
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
  @Explain(displayName = "storage handler", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getStorageHandler() {
    return storageHandler;
  }

  /**
   * @param primaryKeyCols
   *          the primary key cols to set
   */
  public void setPrimaryKeyCols(List<SQLPrimaryKey> primaryKeyCols) {
    this.primaryKeyCols = primaryKeyCols;
  }

  /**
   * @return the primary key cols
   */
  public List<SQLPrimaryKey> getPrimaryKeyCols() {
    return primaryKeyCols;
  }

  /**
   * @param foreignKeyCols
   *          the foreign key cols to set
   */
  public void setForeignKeyCols(List<SQLForeignKey> foreignKeyCols) {
    this.foreignKeyCols = foreignKeyCols;
  }

  /**
   * @return the foreign key cols
   */
  public List<SQLForeignKey> getForeignKeyCols() {
    return foreignKeyCols;
  }

  /**
   * @return the unique constraint cols
   */
  public List<SQLUniqueConstraint> getUniqueConstraintCols() {
    return uniqueConstraintCols;
  }

  /**
   * @return the not null constraint cols
   */
  public List<SQLNotNullConstraint> getNotNullConstraintCols() {
    return notNullConstraintCols;
  }

  /**
   * @return the drop constraint name of the table
   */
  @Explain(displayName = "drop constraint name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getConstraintName() {
    return dropConstraintName;
  }

  /**
   * @param constraintName
   *          the dropConstraintName to set
   */
  public void setDropConstraintName(String constraintName) {
    this.dropConstraintName = constraintName;
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

  /**
   * @return part specification
   */
  public HashMap<String, String> getPartSpec() {
    return partSpec;
  }

  /**
   * @param partSpec
   */
  public void setPartSpec(HashMap<String, String> partSpec) {
    this.partSpec = partSpec;
  }

  /**
   * @return new location
   */
  public String getNewLocation() {
    return newLocation;
  }

  /**
   * @param newLocation new location
   */
  public void setNewLocation(String newLocation) {
    this.newLocation = newLocation;
  }

  public boolean isProtectModeEnable() {
    return protectModeEnable;
  }

  public void setProtectModeEnable(boolean protectModeEnable) {
    this.protectModeEnable = protectModeEnable;
  }

  public ProtectModeType getProtectModeType() {
    return protectModeType;
  }

  public void setProtectModeType(ProtectModeType protectModeType) {
    this.protectModeType = protectModeType;
  }
  /**
   * @return the skewedLocations
   */
  public Map<List<String>, String> getSkewedLocations() {
    return skewedLocations;
  }

  /**
   * @param skewedLocations the skewedLocations to set
   */
  public void setSkewedLocations(Map<List<String>, String> skewedLocations) {
    this.skewedLocations = skewedLocations;
  }

  /**
   * @return isTurnOffSorting
   */
  public boolean isTurnOffSorting() {
    return isTurnOffSorting;
  }

  /**
   * @return the turnOffSkewed
   */
  public boolean isTurnOffSkewed() {
    return isTurnOffSkewed;
  }

  /**
   * @param turnOffSkewed the turnOffSkewed to set
   */
  public void setTurnOffSkewed(boolean turnOffSkewed) {
    this.isTurnOffSkewed = turnOffSkewed;
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
  public void setSkewedColNames(List<String> skewedColNames) {
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
  public void setSkewedColValues(List<List<String>> skewedColValues) {
    this.skewedColValues = skewedColValues;
  }

  /**
   * Validate alter table description.
   *
   * @throws SemanticException
   */
  public void validate() throws SemanticException {
    if (null != table) {
      /* Validate skewed information. */
      ValidationUtility.validateSkewedInformation(
          ParseUtils.validateColumnNameUniqueness(table.getCols()), this.getSkewedColNames(),
          this.getSkewedColValues());
    }
  }

  /**
   * @return the table
   */
  public Table getTable() {
    return table;
  }

  /**
   * @param table the table to set
   */
  public void setTable(Table table) {
    this.table = table;
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
   * @param isDropIfExists the isDropIfExists to set
   */
  public void setDropIfExists(boolean isDropIfExists) {
    this.isDropIfExists = isDropIfExists;
  }

  /**
   * @return isDropIfExists
   */
  public boolean getIsDropIfExists() {
    return isDropIfExists;
  }

  /**
   * @return isCascade
   */
  public boolean getIsCascade() {
    return isCascade;
  }

  public static boolean doesAlterTableTypeSupportPartialPartitionSpec(AlterTableTypes type) {
    return alterTableTypesWithPartialSpec.contains(type);
  }

  public EnvironmentContext getEnvironmentContext() {
    return environmentContext;
  }

  public void setEnvironmentContext(EnvironmentContext environmentContext) {
    this.environmentContext = environmentContext;
  }

  /**
   * @return what kind of replication scope this alter is running under.
   * This can result in a "ALTER IF NEWER THAN" kind of semantic
   */
  public ReplicationSpec getReplicationSpec(){ return this.replicationSpec; }
}
