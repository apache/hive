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

package org.apache.hadoop.hive.ql.exec;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.ddl.table.create.CreateTableOperation;
import org.apache.hadoop.hive.ql.metadata.CheckConstraint;
import org.apache.hadoop.hive.ql.metadata.CheckConstraint.CheckConstraintCol;
import org.apache.hadoop.hive.ql.metadata.DefaultConstraint;
import org.apache.hadoop.hive.ql.metadata.DefaultConstraint.DefaultConstraintCol;
import org.apache.hadoop.hive.ql.metadata.ForeignKeyInfo;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.NotNullConstraint;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.PrimaryKeyInfo;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.UniqueConstraint;
import org.apache.hadoop.hive.ql.util.DirectionUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hive.common.util.HiveStringUtils;
import org.stringtemplate.v4.ST;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;

public class DDLPlanUtils {
  private static final String EXTERNAL = "external";
  private static final String TEMPORARY = "temporary";
  private static final String LIST_COLUMNS = "columns";
  private static final String COMMENT = "comment";
  private static final String PARTITIONS = "partitions";
  private static final String BUCKETS = "buckets";
  private static final String SKEWED = "skewedinfo";
  private static final String ROW_FORMAT = "row_format";
  private static final String LOCATION_BLOCK = "location_block";
  private static final String LOCATION = "location";
  private static final String PROPERTIES = "properties";
  private static final String TABLE_NAME = "TABLE_NAME";
  private static final String DATABASE_NAME = "DATABASE_NAME";
  private static final String DATABASE_NAME_FR = "DATABASE_NAME_FR";
  private static final String PARTITION = "PARTITION";
  private static final String COLUMN_NAME = "COLUMN_NAME";
  private static final String TBLPROPERTIES = "TABLE_PROPERTIES";
  private static final String PARTITION_NAME = "PARTITION_NAME";
  private static final String CONSTRAINT_NAME = "CONSTRAINT_NAME";
  private static final String COL_NAMES = "COLUMN_NAMES";
  private static final String CHILD_TABLE_NAME = "CHILD_TABLE_NAME";
  private static final String PARENT_TABLE_NAME = "PARENT_TABLE_NAME";
  private static final String CHILD_COL_NAME = "CHILD_COL_NAME";
  private static final String PARENT_COL_NAME = "PARENT_COL_NAME";
  private static final String CHECK_EXPRESSION = "CHECK_EXPRESSION";
  private static final String DEFAULT_VALUE = "DEFAULT_VALUE";
  private static final String COL_TYPE = "COL_TYPE";
  private static final String SQL = "SQL";
  private static final String COMMENT_SQL = "COMMENT_SQL";
  private static final String HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__";
  private static final String BASE_64_VALUE = "BASE_64";
  private static final String numNulls = "'numNulls'='";
  private static final String numDVs = "'numDVs'='";
  private static final String numTrues = "'numTrues'='";
  private static final String numFalses = "'numFalses'='";
  private static final String lowValue = "'lowValue'='";
  private static final String highValue = "'highValue'='";
  private static final String avgColLen = "'avgColLen'='";
  private static final String maxColLen = "'maxColLen'='";
  private static final String[] req = {"numRows", "rawDataSize"};
  private static final String[] explain_plans = {"EXPLAIN", "EXPLAIN CBO", "EXPLAIN VECTORIZED"};

  private static final String CREATE_DATABASE_STMT = "CREATE DATABASE IF NOT EXISTS <" + DATABASE_NAME + ">;";

  private final String CREATE_TABLE_TEMPLATE =
      "CREATE <" + TEMPORARY + "><" + EXTERNAL + ">TABLE <if(" + DATABASE_NAME + ")>`<" + DATABASE_NAME + ">`.<endif>"
          + "`<" + TABLE_NAME + ">`(\n" +
          "<" + LIST_COLUMNS + ">)\n" +
          "<" + COMMENT + ">\n" +
          "<" + PARTITIONS + ">\n" +
          "<" + BUCKETS + ">\n" +
          "<" + SKEWED + ">\n" +
          "<" + ROW_FORMAT + ">\n" +
          "<" + LOCATION_BLOCK + ">" +
          "TBLPROPERTIES (\n" +
          "<" + PROPERTIES + ">)";

  private static final String CREATE_VIEW_TEMPLATE =
      "CREATE VIEW <if(" + DATABASE_NAME + ")>`<" + DATABASE_NAME + ">`.<endif>`<" + TABLE_NAME +
          ">`<" + PARTITIONS + "> AS <" + SQL +">";

  private final String CREATE_TABLE_TEMPLATE_LOCATION = "LOCATION\n" +
      "<" + LOCATION + ">\n";

  private final Set<String> PROPERTIES_TO_IGNORE_AT_TBLPROPERTIES = Sets.union(
      ImmutableSet.of("TEMPORARY", "EXTERNAL", "comment", "SORTBUCKETCOLSPREFIX", META_TABLE_STORAGE),
      new HashSet<String>(StatsSetupConst.TABLE_PARAMS_STATS_KEYS));

  private final String ALTER_TABLE_CREATE_PARTITION = "<if(" + COMMENT_SQL + ")><" + COMMENT_SQL + "> <endif>" + "ALTER TABLE <"
      + DATABASE_NAME + ">.<" + TABLE_NAME +
      "> ADD IF NOT EXISTS PARTITION (<" + PARTITION +
      ">);";

  private final String ALTER_TABLE_UPDATE_STATISTICS_TABLE_COLUMN = "ALTER TABLE <"
      + DATABASE_NAME + ">.<" +
      TABLE_NAME + "> UPDATE STATISTICS FOR COLUMN <"
      + COLUMN_NAME + "> SET(<" + TBLPROPERTIES + "> );";

  private final String ALTER_TABLE_UPDATE_STATISTICS_PARTITION_COLUMN = "<if(" + COMMENT_SQL + ")><" + COMMENT_SQL + "> <endif>" + "ALTER TABLE <"
      + DATABASE_NAME + ">.<" + TABLE_NAME +
      "> PARTITION (<" + PARTITION_NAME +
      ">) UPDATE STATISTICS FOR COLUMN <"
      + COLUMN_NAME + "> SET(<" + TBLPROPERTIES + "> );";

  private final String ALTER_TABLE_UPDATE_STATISTICS_TABLE_BASIC = "ALTER TABLE <"
      + DATABASE_NAME + ">.<" + TABLE_NAME +
      "> UPDATE STATISTICS SET(<" + TBLPROPERTIES + "> );";

  private final String ALTER_TABLE_UPDATE_STATISTICS_PARTITION_BASIC = "<if(" + COMMENT_SQL + ")><" + COMMENT_SQL + "> <endif>" + "ALTER TABLE <"
      + DATABASE_NAME + ">.<" + TABLE_NAME + "> PARTITION (<" +
      PARTITION_NAME + ">) UPDATE STATISTICS SET(<" + TBLPROPERTIES + "> );";
  private final String ALTER_TABLE_ADD_PRIMARY_KEY = "ALTER TABLE <"
      + DATABASE_NAME + ">.<" + TABLE_NAME + "> ADD CONSTRAINT <" +
      CONSTRAINT_NAME + "> PRIMARY KEY (<" + COL_NAMES + ">) DISABLE NOVALIDATE;";

  private final String ALTER_TABLE_ADD_FOREIGN_KEY = "ALTER TABLE <"
      + DATABASE_NAME + ">.<" + CHILD_TABLE_NAME + "> ADD CONSTRAINT <"
      + CONSTRAINT_NAME + "> FOREIGN KEY (<" + CHILD_COL_NAME + ">) REFERENCES <"
      + DATABASE_NAME_FR + ">.<" + PARENT_TABLE_NAME + ">(<" + PARENT_COL_NAME + ">) DISABLE NOVALIDATE RELY;";

  private final String ALTER_TABLE_ADD_UNIQUE_CONSTRAINT = "ALTER TABLE <"
      + DATABASE_NAME + ">.<" + TABLE_NAME + "> ADD CONSTRAINT <" +
      CONSTRAINT_NAME + "> UNIQUE (<" + COLUMN_NAME + ">) DISABLE NOVALIDATE;";

  private final String ALTER_TABLE_ADD_CHECK_CONSTRAINT = "ALTER TABLE <"
      + DATABASE_NAME + ">.<" + TABLE_NAME +
      "> ADD CONSTRAINT <" + CONSTRAINT_NAME + "> CHECK (<" +
      CHECK_EXPRESSION + ">) DISABLE;";

  private final String ALTER_TABLE_ADD_NOT_NULL_CONSTRAINT = "ALTER TABLE <"
      + DATABASE_NAME + ">.<" + TABLE_NAME + "> CHANGE COLUMN < "
      + COLUMN_NAME + "> <" + COLUMN_NAME +
      "> <" + COL_TYPE + "> CONSTRAINT <" + CONSTRAINT_NAME + "> NOT NULL DISABLE;";

  private final String ALTER_TABLE_ADD_DEFAULT_CONSTRAINT = "ALTER TABLE <"
      + DATABASE_NAME + ">.<" + TABLE_NAME + "> CHANGE COLUMN < "
      + COLUMN_NAME + "> <" + COLUMN_NAME +
      "> <" + COL_TYPE + "> CONSTRAINT <" + CONSTRAINT_NAME + "> DEFAULT <" + DEFAULT_VALUE + "> DISABLE;";

  private final String EXIST_BIT_VECTORS = "-- BIT VECTORS PRESENT FOR <" + DATABASE_NAME + ">.<" + TABLE_NAME + "> " +
      "FOR COLUMN <" + COLUMN_NAME + "> BUT THEY ARE NOT SUPPORTED YET. THE BASE64 VALUE FOR THE BITVECTOR IS <" +
      BASE_64_VALUE +"> ";

  private final String EXIST_BIT_VECTORS_PARTITIONED = "-- BIT VECTORS PRESENT FOR <" + DATABASE_NAME + ">.<" +
      TABLE_NAME + "> PARTITION <" + PARTITION_NAME + "> FOR COLUMN <"
      + COLUMN_NAME + "> BUT THEY ARE NOT SUPPORTED YET.THE BASE64 VALUE FOR THE BITVECTOR IS <" +
      BASE_64_VALUE +"> ";

  /**
   * Returns the create database query for a give database name.
   *
   * @param dbNames
   * @return
   */
  public List<String> getCreateDatabaseStmt(Set<String> dbNames) {
    List<String> allDbStmt = new ArrayList<String>();
    for (String dbName : dbNames) {
      if (dbName.equals("default")) {
        continue;
      }
      ST command = new ST(CREATE_DATABASE_STMT);
      command.add(DATABASE_NAME, dbName);
      allDbStmt.add(command.render());
    }
    return allDbStmt;
  }

  public Map<String, String> getTableColumnsToType(Table tbl) {
    List<FieldSchema> fieldSchemas = tbl.getAllCols();
    Map<String, String> ret = new HashMap<String, String>();
    fieldSchemas.stream().forEach(f -> ret.put(f.getName(), f.getType()));
    return ret;
  }

  public List<String> getTableColumnNames(Table tbl) {
    List<FieldSchema> fieldSchemas = tbl.getAllCols();
    List<String> ret = new ArrayList<String>();
    fieldSchemas.stream().forEach(f -> ret.add(f.getName()));
    return ret;
  }

  public String getPartitionActualName(Partition pt) {
    Map<String, String> colTypeMap = getTableColumnsToType(pt.getTable());
    String[] partColsDef = pt.getName().split(",");
    List<String> ptParam = new ArrayList<>();
    for (String partCol : partColsDef) {
      String[] colValue = partCol.split("=");
      if (colTypeMap.get(colValue[0]).equalsIgnoreCase("string")) {
        ptParam.add(colValue[0] + "='" + colValue[1] + "'");
      } else {
        ptParam.add(colValue[0] + "=" + colValue[1]);
      }
    }
    return StringUtils.join(ptParam, ",");
  }

  public boolean checkIfDefaultPartition(String pt){
    if(pt.contains(HIVE_DEFAULT_PARTITION)){
      return true;
    }
    else {
      return false;
    }
  }

  /**
   * Creates the alter table command to add a partition to the given table.
   *
   * @param pt
   * @return
   * @throws MetaException
   */
  //TODO: Adding/Updating Stats to Default Partition Not Allowed. Need to Fix Later
  public String getAlterTableAddPartition(Partition pt) throws MetaException {
    Table tb = pt.getTable();
    ST command = new ST(ALTER_TABLE_CREATE_PARTITION);
    command.add(DATABASE_NAME, tb.getDbName());
    command.add(TABLE_NAME, tb.getTableName());
    command.add(PARTITION, getPartitionActualName(pt));
    if(checkIfDefaultPartition(pt.getName())){
      command.add(COMMENT_SQL, "--");
    }
    return command.render();
  }

  public void addLongStats(ColumnStatisticsData cd, List<String> ls) {
    if (!cd.isSetLongStats()) {
      return;
    }
    LongColumnStatsData lg = cd.getLongStats();
    ls.add(lowValue + lg.getLowValue() + "'");
    ls.add(highValue + lg.getHighValue() + "'");
    ls.add(numNulls + lg.getNumNulls() + "'");
    ls.add(numDVs + lg.getNumDVs() + "'");
    return;
  }

  public void addBooleanStats(ColumnStatisticsData cd, List<String> ls) {
    if (!cd.isSetBooleanStats()) {
      return;
    }
    BooleanColumnStatsData bd = cd.getBooleanStats();
    ls.add(numTrues + bd.getNumFalses() + "'");
    ls.add(numFalses + bd.getNumTrues() + "'");
    ls.add(numNulls + bd.getNumNulls() + "'");
    return;
  }

  public void addStringStats(ColumnStatisticsData cd, List<String> ls) {
    if (!cd.isSetStringStats()) {
      return;
    }
    StringColumnStatsData lg = cd.getStringStats();
    ls.add(avgColLen + lg.getAvgColLen() + "'");
    ls.add(maxColLen + lg.getMaxColLen() + "'");
    ls.add(numNulls + lg.getNumNulls() + "'");
    ls.add(numDVs + lg.getNumDVs() + "'");
    return;
  }

  public void addDateStats(ColumnStatisticsData cd, List<String> ls) {
    if (!cd.isSetDateStats()) {
      return;
    }
    DateColumnStatsData dt = cd.getDateStats();
    ls.add(lowValue + dt.getLowValue().getDaysSinceEpoch() + "'");
    ls.add(highValue + dt.getHighValue().getDaysSinceEpoch() + "'");
    ls.add(numNulls + dt.getNumNulls() + "'");
    ls.add(numDVs + dt.getNumDVs() + "'");
    return;
  }

  public void addBinaryStats(ColumnStatisticsData cd, List<String> ls) {
    if (!cd.isSetBinaryStats()) {
      return;
    }
    BinaryColumnStatsData bd = cd.getBinaryStats();
    ls.add(avgColLen + bd.getAvgColLen() + "'");
    ls.add(maxColLen + bd.getMaxColLen() + "'");
    ls.add(numNulls + bd.getNumNulls() + "'");
    return;
  }

  public byte[] setByteArrayToLongSize(byte[] arr) {
    byte[] temp = new byte[Math.max(arr.length, 8)];
    for (int i = 0; i < arr.length; i++) {
      temp[i] = arr[i];
    }
    for (int i = arr.length; i < 8; i++) {
      temp[i] = (byte) 0;
    }
    return temp;
  }

  public void addDecimalStats(ColumnStatisticsData cd, List<String> ls) {
    if (!cd.isSetDecimalStats()) {
      return;
    }
    DecimalColumnStatsData dc = cd.getDecimalStats();
    if(dc.isSetHighValue()) {
      byte[] highValArr = setByteArrayToLongSize(dc.getHighValue().getUnscaled());
      ls.add(highValue + ByteBuffer.wrap(highValArr).getLong() + "E" + dc.getHighValue().getScale() + "'");
    }
    if(dc.isSetLowValue()) {
      byte[] lowValArr = setByteArrayToLongSize(dc.getLowValue().getUnscaled());
      ls.add(lowValue + ByteBuffer.wrap(lowValArr).getLong() + "E" + dc.getLowValue().getScale() + "'");
    }
    ls.add(numNulls + dc.getNumNulls() + "'");
    ls.add(numDVs + dc.getNumDVs() + "'");
  }

  public void addDoubleStats(ColumnStatisticsData cd, List<String> ls) {
    if (!cd.isSetDoubleStats()) {
      return;
    }
    DoubleColumnStatsData dc = cd.getDoubleStats();
    ls.add(numNulls + dc.getNumNulls() + "'");
    ls.add(numDVs + dc.getNumDVs() + "'");
    ls.add(highValue + dc.getHighValue() + "'");
    ls.add(lowValue + dc.getLowValue() + "'");
  }

  public String checkBitVectors(ColumnStatisticsData cd) {
    if (cd.isSetDoubleStats() && cd.getDoubleStats().isSetBitVectors()) {
      return Base64.getEncoder().encodeToString(cd.getDoubleStats().getBitVectors());
    }
    if (cd.isSetBinaryStats() && cd.getBinaryStats().isSetBitVectors()) {
      return Base64.getEncoder().encodeToString(cd.getBinaryStats().getBitVectors());
    }
    if (cd.isSetStringStats() && cd.getStringStats().isSetBitVectors()) {
      return Base64.getEncoder().encodeToString(cd.getStringStats().getBitVectors());
    }
    if (cd.isSetDateStats() && cd.getDateStats().isSetBitVectors()) {
      return Base64.getEncoder().encodeToString(cd.getDateStats().getBitVectors());
    }
    if (cd.isSetLongStats() && cd.getLongStats().isSetBitVectors()) {
      return Base64.getEncoder().encodeToString(cd.getLongStats().getBitVectors());
    }
    if (cd.isSetDecimalStats() && cd.getDecimalStats().isSetBitVectors()) {
      return Base64.getEncoder().encodeToString(cd.getDecimalStats().getBitVectors());
    }
    if (cd.isSetDoubleStats() && cd.getBooleanStats().isSetBitVectors()) {
      return Base64.getEncoder().encodeToString(cd.getBooleanStats().getBitVectors());
    }
    return null;
  }

  public String addAllColStats(ColumnStatisticsData columnStatisticsData){
    List<String> temp = new ArrayList<>();
    addBinaryStats(columnStatisticsData, temp);
    addLongStats(columnStatisticsData, temp);
    addBooleanStats(columnStatisticsData, temp);
    addStringStats(columnStatisticsData, temp);
    addDateStats(columnStatisticsData, temp);
    addDoubleStats(columnStatisticsData, temp);
    addDecimalStats(columnStatisticsData, temp);
    return Joiner.on(",").join(temp);
  }

  /**
   * Parses the basic ColumnStatObject and returns the alter tables stmt for each individual column in a table.
   *
   * @param columnStatisticsData
   * @param colName
   * @param tblName
   * @param dbName
   * @return
   */
  public String getAlterTableStmtCol(ColumnStatisticsData columnStatisticsData, String colName, String tblName, String dbName) {
    ST command = new ST(ALTER_TABLE_UPDATE_STATISTICS_TABLE_COLUMN);
    command.add(DATABASE_NAME, dbName);
    command.add(TABLE_NAME, tblName);
    command.add(COLUMN_NAME, colName);
    command.add(TBLPROPERTIES, addAllColStats(columnStatisticsData));
    return command.render();
  }

  /**
   * Parses the ColumnStatistics for all the columns in a given table and adds the alter table update
   * statistics command for each column.
   *
   * @param tbl
   */
  public List<String> getAlterTableStmtTableStatsColsAll(Table tbl)
      throws HiveException {
    List<String> alterTblStmt = new ArrayList<String>();
    List<String> accessedColumns = getTableColumnNames(tbl);
    List<ColumnStatisticsObj> tableColumnStatistics = Hive.get().getTableColumnStatistics(tbl.getDbName(),
        tbl.getTableName(),
        accessedColumns,
        true);
    ColumnStatisticsObj[] columnStatisticsObj = tableColumnStatistics.toArray(new ColumnStatisticsObj[0]);
    for (int i = 0; i < columnStatisticsObj.length; i++) {
      alterTblStmt.add(getAlterTableStmtCol(columnStatisticsObj[i].getStatsData(),
          columnStatisticsObj[i].getColName(),
          tbl.getTableName(), tbl.getDbName()));
      String base64 = checkBitVectors(columnStatisticsObj[i].getStatsData());
      if (base64 != null) {
        ST command = new ST(EXIST_BIT_VECTORS);
        command.add(DATABASE_NAME, tbl.getDbName());
        command.add(TABLE_NAME, tbl.getTableName());
        command.add(COLUMN_NAME, columnStatisticsObj[i].getColName());
        command.add(BASE_64_VALUE, base64);
        alterTblStmt.add(command.render());
      }
    }
    return alterTblStmt;
  }

  /**
   * Parses the basic ColumnStatObject and returns the alter tables stmt for each individual column in a partition.
   *
   * @param columnStatisticsData
   * @param colName
   * @param tblName
   * @param ptName
   * @param dbName
   * @return
   */
  public String getAlterTableStmtPartitionColStat(ColumnStatisticsData columnStatisticsData, String colName,
      String tblName, String ptName, String dbName) {
    ST command = new ST(ALTER_TABLE_UPDATE_STATISTICS_PARTITION_COLUMN);
    command.add(DATABASE_NAME, dbName);
    command.add(TABLE_NAME, tblName);
    command.add(COLUMN_NAME, colName);
    command.add(PARTITION_NAME, ptName);
    command.add(TBLPROPERTIES, addAllColStats(columnStatisticsData));
    if(checkIfDefaultPartition(ptName)){
      command.add(COMMENT_SQL, "--");
    }
    return command.render();
  }

  /**
   * Parses the ColumnStatistics for all the columns in a given partition and adds the alter table update
   * statistics command for each column.
   *
   * @param columnStatisticsObjList
   * @param tblName
   * @param ptName
   * @param dbName
   */
  public List<String> getAlterTableStmtPartitionStatsColsAll(List<ColumnStatisticsObj> columnStatisticsObjList,
      String tblName,
      String ptName,
      String dbName) {
    List<String> alterTableStmt = new ArrayList<String>();
    ColumnStatisticsObj[] columnStatisticsObj = columnStatisticsObjList.toArray(new ColumnStatisticsObj[0]);
    for (int i = 0; i < columnStatisticsObj.length; i++) {
      alterTableStmt.add(getAlterTableStmtPartitionColStat(columnStatisticsObj[i].getStatsData(),
          columnStatisticsObj[i].getColName(),
          tblName,
          ptName,
          dbName));
      String base64 = checkBitVectors(columnStatisticsObj[i].getStatsData());
      if (base64 != null) {
        ST command = new ST(EXIST_BIT_VECTORS_PARTITIONED);
        command.add(DATABASE_NAME, dbName);
        command.add(TABLE_NAME, tblName);
        command.add(PARTITION_NAME, ptName);
        command.add(COLUMN_NAME, columnStatisticsObj[i].getColName());
        command.add(BASE_64_VALUE, base64);
        alterTableStmt.add(command.render());
      }
    }
    return alterTableStmt;
  }

  public String paramToValues(Map<String, String> parameters){
    List<String> paramsToValue = new ArrayList<>();
    for (String s : req) {
      String p = parameters.get(s);
      if (p == null) {
        p = "0";
      }
      paramsToValue.add("'" + s + "'='" + p + "'");
    }
    return Joiner.on(",").join(paramsToValue);
  }

  /**
   * Parses the basic table statistics for the given table.
   *
   * @param pt
   * @return Returns the alter table .... update statistics for partititon.
   */
  public String getAlterTableStmtPartitionStatsBasic(Partition pt) {
    Map<String, String> parameters = pt.getParameters();
    ST command = new ST(ALTER_TABLE_UPDATE_STATISTICS_PARTITION_BASIC);
    command.add(DATABASE_NAME, pt.getTable().getDbName());
    command.add(TABLE_NAME, pt.getTable().getTableName());
    command.add(PARTITION_NAME, getPartitionActualName(pt));
    command.add(TBLPROPERTIES, paramToValues(parameters));
    if(checkIfDefaultPartition(pt.getName())){
      command.add(COMMENT_SQL, "--");
    }
    return command.render();
  }

  public List<String> getDDLPlanForPartitionWithStats(Table table,
      Map<String, List<Partition>> tableToPartitionList
                                                     ) throws MetaException, HiveException {
    List<String> alterTableStmt = new ArrayList<String>();
    String tableName = table.getTableName();
    for (Partition pt : tableToPartitionList.get(tableName)) {
      alterTableStmt.add(getAlterTableAddPartition(pt));
      alterTableStmt.add(getAlterTableStmtPartitionStatsBasic(pt));
    }
    String databaseName = table.getDbName();
    List<String> partNames = new ArrayList<String>();
    //TODO : Check if only Accessed Column Statistics Can be Retrieved From the HMS.
    List<String> columnNames = getTableColumnNames(table);
    tableToPartitionList.get(tableName).stream().forEach(p -> partNames.add(p.getName()));
    Map<String, List<ColumnStatisticsObj>> partitionColStats =
        Hive.get().getPartitionColumnStatistics(databaseName,
            tableName, partNames, columnNames,
            true);
    Map<String, String> partitionToActualName = new HashMap<>();
    tableToPartitionList.get(tableName).stream().forEach(p -> partitionToActualName.put(p.getName(),
        getPartitionActualName(p)));
    for (String partitionName : partitionColStats.keySet()) {
      alterTableStmt.addAll(getAlterTableStmtPartitionStatsColsAll(partitionColStats.get(partitionName),
          tableName, partitionToActualName.get(partitionName),
          databaseName));
    }
    return alterTableStmt;
  }

  /**
   * Parses the basic table statistics for the given table.
   *
   * @param tbl
   * @return Returns the alter table .... update statistics for table
   * @throws HiveException
   */
  public String getAlterTableStmtTableStatsBasic(Table tbl) {
    Map<String, String> parameters = tbl.getParameters();
    ST command = new ST(ALTER_TABLE_UPDATE_STATISTICS_TABLE_BASIC);
    command.add(TABLE_NAME, tbl.getTableName());
    command.add(DATABASE_NAME, tbl.getDbName());
    command.add(TBLPROPERTIES, paramToValues(parameters));
    return command.render();
  }

  public String getAlterTableStmtPrimaryKeyConstraint(PrimaryKeyInfo pr) {
    if (!PrimaryKeyInfo.isPrimaryKeyInfoNotEmpty(pr)) {
      return null;
    }
    ST command = new ST(ALTER_TABLE_ADD_PRIMARY_KEY);
    command.add(TABLE_NAME, pr.getTableName());
    command.add(DATABASE_NAME, pr.getDatabaseName());
    command.add(CONSTRAINT_NAME, pr.getConstraintName());
    command.add(COL_NAMES, String.join(",", pr.getColNames().values()));
    return command.render();
  }

  public void getAlterTableStmtForeignKeyConstraint(ForeignKeyInfo fr, List<String> constraints, Set<String> allTableNames) {
    if (!ForeignKeyInfo.isForeignKeyInfoNotEmpty(fr)) {
      return;
    }
    Map<String, List<ForeignKeyInfo.ForeignKeyCol>> all = fr.getForeignKeys();
    for (String key : all.keySet()) {
      for (ForeignKeyInfo.ForeignKeyCol fkc : all.get(key)) {
        if (!allTableNames.contains(fkc.parentTableName)) {
          continue;
        }
        ST command = new ST(ALTER_TABLE_ADD_FOREIGN_KEY);
        command.add(CHILD_TABLE_NAME, fr.getChildTableName());
        command.add(DATABASE_NAME, fr.getChildDatabaseName());
        command.add(CONSTRAINT_NAME, key);
        command.add(CHILD_COL_NAME, fkc.childColName);
        command.add(DATABASE_NAME_FR, fkc.parentDatabaseName);
        command.add(PARENT_TABLE_NAME, fkc.parentTableName);
        command.add(PARENT_COL_NAME, fkc.parentColName);
        constraints.add(command.render());
      }
    }
  }

  public void getAlterTableStmtUniqueConstraint(UniqueConstraint uq, List<String> constraints) {
    if (!UniqueConstraint.isUniqueConstraintNotEmpty(uq)) {
      return;
    }
    Map<String, List<UniqueConstraint.UniqueConstraintCol>> uniqueConstraints = uq.getUniqueConstraints();
    for (String key : uniqueConstraints.keySet()) {
      ST command = new ST(ALTER_TABLE_ADD_UNIQUE_CONSTRAINT);
      command.add(DATABASE_NAME, uq.getDatabaseName());
      command.add(TABLE_NAME, uq.getTableName());
      command.add(CONSTRAINT_NAME, key);
      List<String> colNames = new ArrayList<>();
      for (UniqueConstraint.UniqueConstraintCol col : uniqueConstraints.get(key)) {
        colNames.add(col.colName);
      }
      command.add(COLUMN_NAME, Joiner.on(",").join(colNames));
      constraints.add(command.render());
    }
  }

  public void getAlterTableStmtDefaultConstraint(DefaultConstraint dc, Table tb, List<String> constraints) {
    if (!DefaultConstraint.isCheckConstraintNotEmpty(dc)) {
      return;
    }
    Map<String, String> colType = getTableColumnsToType(tb);
    Map<String, List<DefaultConstraintCol>> defaultConstraints = dc.getDefaultConstraints();
    for (String constraintName : defaultConstraints.keySet()) {
      List<DefaultConstraintCol> defaultConstraintCols = defaultConstraints.get(constraintName);
      for (DefaultConstraintCol col : defaultConstraintCols) {
        ST command = new ST(ALTER_TABLE_ADD_DEFAULT_CONSTRAINT);
        command.add(DATABASE_NAME, dc.getTableName());
        command.add(TABLE_NAME, dc.getTableName());
        command.add(COLUMN_NAME, col.colName);
        command.add(COL_TYPE, colType.get(col.colName));
        command.add(DEFAULT_VALUE, col.defaultVal);
        constraints.add(command.render());
      }
    }
  }

  public void getAlterTableStmtCheckConstraint(CheckConstraint ck, List<String> constraints) {
    if (!CheckConstraint.isCheckConstraintNotEmpty(ck)) {
      return;
    }
    Map<String, List<CheckConstraint.CheckConstraintCol>> checkConstraints = ck.getCheckConstraints();
    for (String constraintName : checkConstraints.keySet()) {
      List<CheckConstraintCol> checkConstraintCols = checkConstraints.get(constraintName);
      if (checkConstraintCols != null && checkConstraintCols.size() > 0) {
        for (CheckConstraintCol col : checkConstraintCols) {
          ST command = new ST(ALTER_TABLE_ADD_CHECK_CONSTRAINT);
          command.add(DATABASE_NAME, ck.getDatabaseName());
          command.add(TABLE_NAME, ck.getTableName());
          command.add(CONSTRAINT_NAME, constraintName);
          command.add(CHECK_EXPRESSION, col.checkExpression);
          constraints.add(command.render());
        }
      }
    }
  }


  public void getAlterTableStmtNotNullConstraint(NotNullConstraint nc, Table tb, List<String> constraints) {
    if (!NotNullConstraint.isNotNullConstraintNotEmpty(nc)) {
      return;
    }
    Map<String, String> colType = getTableColumnsToType(tb);
    Map<String, String> notNullConstraints = nc.getNotNullConstraints();
    for (String constraintName : notNullConstraints.keySet()) {
      ST command = new ST(ALTER_TABLE_ADD_NOT_NULL_CONSTRAINT);
      command.add(DATABASE_NAME, nc.getDatabaseName());
      command.add(TABLE_NAME, nc.getTableName());
      command.add(COLUMN_NAME, notNullConstraints.get(constraintName));
      command.add(COL_TYPE, colType.get(notNullConstraints.get(constraintName)));
      command.add(CONSTRAINT_NAME, constraintName);
      constraints.add(command.render());
    }
  }

  /**
   * Add all the constraints for the given table. Will populate the constraints list.
   *
   * @param tb
   */
  public List<String> populateConstraints(Table tb, Set<String> allTableNames) {
    List<String> constraints = new ArrayList<>();
    getAlterTableStmtForeignKeyConstraint(tb.getForeignKeyInfo(), constraints, allTableNames);
    getAlterTableStmtUniqueConstraint(tb.getUniqueKeyInfo(), constraints);
    getAlterTableStmtDefaultConstraint(tb.getDefaultConstraint(), tb, constraints);
    getAlterTableStmtCheckConstraint(tb.getCheckConstraint(), constraints);
    getAlterTableStmtNotNullConstraint(tb.getNotNullConstraint(), tb, constraints);
    return constraints;
  }

  public List<String> addExplainPlans(String sql){
    List<String> exp = new ArrayList<String>();
    for(String ex : explain_plans){
      exp.add(sql.replaceAll("(?i)explain ddl", ex) + ";");
    }
    return exp;
  }

  /**
   * Returns the create view statement for the given view.
   *
   * @param table
   * @return create view statement.
   */
  public String getCreateViewStmt(Table table) {
    return getCreateViewCommand(table, false) + ";";
  }

  public String getCreateViewCommand(Table table, boolean isRelative) {
    ST command = new ST(CREATE_VIEW_TEMPLATE);

    if (!isRelative) {
      command.add(DATABASE_NAME, table.getDbName());
    }
    command.add(TABLE_NAME, table.getTableName());
    command.add(PARTITIONS, getPartitionsForView(table));
    command.add(SQL, table.getViewExpandedText());

    return command.render();
  }


  public String getCreateTableCommand(Table table, boolean isRelative) {
    ST command = new ST(CREATE_TABLE_TEMPLATE);

    if (!isRelative) {
      command.add(DATABASE_NAME, table.getDbName());
    }
    command.add(TABLE_NAME, table.getTableName());
    command.add(TEMPORARY, getTemporary(table));
    command.add(EXTERNAL, getExternal(table));
    command.add(LIST_COLUMNS, getColumns(table));
    command.add(COMMENT, getComment(table));
    command.add(PARTITIONS, getPartitions(table));
    command.add(BUCKETS, getBuckets(table));
    command.add(SKEWED, getSkewed(table));
    command.add(ROW_FORMAT, getRowFormat(table));
    command.add(LOCATION_BLOCK, getLocationBlock(table));
    command.add(PROPERTIES, getProperties(table));

    return command.render();
  }

  private String getTemporary(Table table) {
    return table.isTemporary() ? "TEMPORARY " : "";
  }

  private String getExternal(Table table) {
    return table.getTableType() == TableType.EXTERNAL_TABLE ? "EXTERNAL " : "";
  }

  private String getColumns(Table table) {
    List<String> columnDescs = new ArrayList<String>();
    for (FieldSchema column : table.getCols()) {
      String columnType = formatType(TypeInfoUtils.getTypeInfoFromTypeString(column.getType()));
      String columnDesc = "  `" + column.getName() + "` " + columnType;
      if (column.getComment() != null) {
        columnDesc += " COMMENT '" + HiveStringUtils.escapeHiveCommand(column.getComment()) + "'";
      }
      columnDescs.add(columnDesc);
    }
    return StringUtils.join(columnDescs, ", \n");
  }

  /** Struct fields are identifiers, need to be put between ``. */
  private String formatType(TypeInfo typeInfo) {
    switch (typeInfo.getCategory()) {
    case PRIMITIVE:
      return typeInfo.getTypeName();
    case STRUCT:
      StringBuilder structFormattedType = new StringBuilder();

      StructTypeInfo structTypeInfo = (StructTypeInfo)typeInfo;
      for (int i = 0; i < structTypeInfo.getAllStructFieldNames().size(); i++) {
        if (structFormattedType.length() != 0) {
          structFormattedType.append(", ");
        }

        String structElementName = structTypeInfo.getAllStructFieldNames().get(i);
        String structElementType = formatType(structTypeInfo.getAllStructFieldTypeInfos().get(i));

        structFormattedType.append("`" + structElementName + "`:" + structElementType);
      }
      return "struct<" + structFormattedType.toString() + ">";
    case LIST:
      ListTypeInfo listTypeInfo = (ListTypeInfo)typeInfo;
      String elementType = formatType(listTypeInfo.getListElementTypeInfo());
      return "array<" + elementType + ">";
    case MAP:
      MapTypeInfo mapTypeInfo = (MapTypeInfo)typeInfo;
      String keyTypeInfo = mapTypeInfo.getMapKeyTypeInfo().getTypeName();
      String valueTypeInfo = formatType(mapTypeInfo.getMapValueTypeInfo());
      return "map<" + keyTypeInfo + "," + valueTypeInfo + ">";
    case UNION:
      StringBuilder unionFormattedType = new StringBuilder();

      UnionTypeInfo unionTypeInfo = (UnionTypeInfo)typeInfo;
      for (TypeInfo unionElementTypeInfo : unionTypeInfo.getAllUnionObjectTypeInfos()) {
        if (unionFormattedType.length() != 0) {
          unionFormattedType.append(", ");
        }

        String unionElementType = formatType(unionElementTypeInfo);
        unionFormattedType.append(unionElementType);
      }
      return "uniontype<" + unionFormattedType.toString() + ">";
    default:
      throw new RuntimeException("Unknown type: " + typeInfo.getCategory());
    }
  }

  private String getComment(Table table) {
    String comment = table.getProperty("comment");
    return (comment != null) ? "COMMENT '" + HiveStringUtils.escapeHiveCommand(comment) + "'" : "";
  }

  private String getPartitionsForView(Table table) {
    List<FieldSchema> partitionKeys = table.getPartCols();
    if (partitionKeys.isEmpty()) {
      return "";
    }
    List<String> partitionCols = new ArrayList<String>();
    for(String col:table.getPartColNames()) {
      partitionCols.add('`' + col + '`');
    }
    return " PARTITIONED ON (" + StringUtils.join(partitionCols, ", ") + ")";
  }

  private String getPartitions(Table table) {
    List<FieldSchema> partitionKeys = table.getPartitionKeys();
    if (partitionKeys.isEmpty()) {
      return "";
    }

    List<String> partitionDescs = new ArrayList<String>();
    for (FieldSchema partitionKey : partitionKeys) {
      String partitionDesc = "  `" + partitionKey.getName() + "` " + partitionKey.getType();
      if (partitionKey.getComment() != null) {
        partitionDesc += " COMMENT '" + HiveStringUtils.escapeHiveCommand(partitionKey.getComment()) + "'";
      }
      partitionDescs.add(partitionDesc);
    }
    return "PARTITIONED BY ( \n" + StringUtils.join(partitionDescs, ", \n") + ")";
  }

  private String getBuckets(Table table) {
    List<String> bucketCols = table.getBucketCols();
    if (bucketCols.isEmpty()) {
      return "";
    }

    String buckets = "CLUSTERED BY ( \n  " + StringUtils.join(bucketCols, ", \n  ") + ") \n";

    List<Order> sortColumns = table.getSortCols();
    if (!sortColumns.isEmpty()) {
      List<String> sortKeys = new ArrayList<String>();
      for (Order sortColumn : sortColumns) {
        String sortKeyDesc = "  " + sortColumn.getCol() + " " + DirectionUtils.codeToText(sortColumn.getOrder());
        sortKeys.add(sortKeyDesc);
      }
      buckets += "SORTED BY ( \n" + StringUtils.join(sortKeys, ", \n") + ") \n";
    }

    buckets += "INTO " + table.getNumBuckets() + " BUCKETS";
    return buckets;
  }

  private String getSkewed(Table table) {
    SkewedInfo skewedInfo = table.getSkewedInfo();
    if (skewedInfo == null || skewedInfo.getSkewedColNames().isEmpty()) {
      return "";
    }

    List<String> columnValuesList = new ArrayList<String>();
    for (List<String> columnValues : skewedInfo.getSkewedColValues()) {
      columnValuesList.add("('" + StringUtils.join(columnValues, "','") + "')");
    }

    String skewed =
        "SKEWED BY (" + StringUtils.join(skewedInfo.getSkewedColNames(), ",") + ")\n" +
            "  ON (" + StringUtils.join(columnValuesList, ",") + ")";
    if (table.isStoredAsSubDirectories()) {
      skewed += "\n  STORED AS DIRECTORIES";
    }
    return skewed;
  }

  private String getRowFormat(Table table) {
    StringBuilder rowFormat = new StringBuilder();

    StorageDescriptor sd = table.getTTable().getSd();
    SerDeInfo serdeInfo = sd.getSerdeInfo();

    rowFormat
        .append("ROW FORMAT SERDE \n")
        .append("  '" + HiveStringUtils.escapeHiveCommand(serdeInfo.getSerializationLib()) + "' \n");

    Map<String, String> serdeParams = serdeInfo.getParameters();
    if (table.getStorageHandler() == null) {
      // If serialization.format property has the default value, it will not to be included in SERDE properties
      if (Warehouse.DEFAULT_SERIALIZATION_FORMAT.equals(serdeParams.get(serdeConstants.SERIALIZATION_FORMAT))) {
        serdeParams.remove(serdeConstants.SERIALIZATION_FORMAT);
      }
      if (!serdeParams.isEmpty()) {
        appendSerdeParams(rowFormat, serdeParams);
        rowFormat.append(" \n");
      }
      rowFormat
          .append("STORED AS INPUTFORMAT \n  '" + HiveStringUtils.escapeHiveCommand(sd.getInputFormat()) + "' \n")
          .append("OUTPUTFORMAT \n  '" + HiveStringUtils.escapeHiveCommand(sd.getOutputFormat()) + "'");
    } else {
      String metaTableStorage = table.getParameters().get(META_TABLE_STORAGE);
      rowFormat.append("STORED BY \n  '" + HiveStringUtils.escapeHiveCommand(metaTableStorage) + "' \n");
      if (!serdeParams.isEmpty()) {
        appendSerdeParams(rowFormat, serdeInfo.getParameters());
      }
    }

    return rowFormat.toString();
  }

  public static void appendSerdeParams(StringBuilder builder, Map<String, String> serdeParams) {
    SortedMap<String, String> sortedSerdeParams = new TreeMap<String, String>(serdeParams);
    List<String> serdeCols = new ArrayList<String>();
    for (Entry<String, String> entry : sortedSerdeParams.entrySet()) {
      serdeCols.add("  '" + entry.getKey() + "'='" +
          HiveStringUtils.escapeUnicode(HiveStringUtils.escapeHiveCommand(entry.getValue())) + "'");
    }

    builder
        .append("WITH SERDEPROPERTIES ( \n")
        .append(StringUtils.join(serdeCols, ", \n"))
        .append(')');
  }

  private String getLocationBlock(Table table) {
    if (!CreateTableOperation.doesTableNeedLocation(table)) {
      return "";
    }

    ST locationBlock = new ST(CREATE_TABLE_TEMPLATE_LOCATION);
    StorageDescriptor sd = table.getTTable().getSd();
    locationBlock.add(LOCATION, "  '" + HiveStringUtils.escapeHiveCommand(sd.getLocation()) + "'");
    return locationBlock.render();
  }

  private String getProperties(Table table) {
    return ShowUtils.propertiesToString(table.getParameters(), PROPERTIES_TO_IGNORE_AT_TBLPROPERTIES);
  }
}