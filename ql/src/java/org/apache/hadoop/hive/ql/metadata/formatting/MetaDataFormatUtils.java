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

package org.apache.hadoop.hive.ql.metadata.formatting;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMPoolTrigger;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.ql.metadata.CheckConstraint;
import org.apache.hadoop.hive.ql.metadata.DefaultConstraint;
import org.apache.hadoop.hive.ql.metadata.ForeignKeyInfo;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.NotNullConstraint;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.PrimaryKeyInfo;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.UniqueConstraint;
import org.apache.hadoop.hive.ql.metadata.UniqueConstraint.UniqueConstraintCol;
import org.apache.hadoop.hive.ql.metadata.ForeignKeyInfo.ForeignKeyCol;
import org.apache.hadoop.hive.ql.plan.DescTableDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hive.common.util.HiveStringUtils;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;


/**
 * This class provides methods to format table and index information.
 *
 */
public final class MetaDataFormatUtils {

  public static final String FIELD_DELIM = "\t";
  public static final String LINE_DELIM = "\n";

  static final int DEFAULT_STRINGBUILDER_SIZE = 2048;
  private static final int ALIGNMENT = 20;

  private MetaDataFormatUtils() {
  }

  private static String convertToString(Decimal val) {
    if (val == null) {
      return "";
    }

    HiveDecimal result = HiveDecimal.create(new BigInteger(val.getUnscaled()), val.getScale());
    if (result != null) {
      return result.toString();
    } else {
      return "";
    }
  }

  private static String convertToString(org.apache.hadoop.hive.metastore.api.Date val) {
    if (val == null) {
      return "";
    }

    DateWritableV2 writableValue = new DateWritableV2((int) val.getDaysSinceEpoch());
    return writableValue.toString();
  }

  private static String convertToString(byte[] buf) {
    if (buf == null || buf.length == 0) {
      return "";
    }
    byte[] sub = new byte[2];
    sub[0] = buf[0];
    sub[1] = buf[1];
    return new String(sub);
  }

  static ColumnStatisticsObj getColumnStatisticsObject(String colName,
      String colType, List<ColumnStatisticsObj> colStats) {
    if (colStats != null && !colStats.isEmpty()) {
      for (ColumnStatisticsObj cso : colStats) {
        if (cso.getColName().equalsIgnoreCase(colName)
            && cso.getColType().equalsIgnoreCase(colType)) {
          return cso;
        }
      }
    }
    return null;
  }

  public static String getConstraintsInformation(PrimaryKeyInfo pkInfo, ForeignKeyInfo fkInfo,
          UniqueConstraint ukInfo, NotNullConstraint nnInfo, DefaultConstraint dInfo, CheckConstraint cInfo) {
    StringBuilder constraintsInfo = new StringBuilder(DEFAULT_STRINGBUILDER_SIZE);

    constraintsInfo.append(LINE_DELIM).append("# Constraints").append(LINE_DELIM);
    if (pkInfo != null && !pkInfo.getColNames().isEmpty()) {
      constraintsInfo.append(LINE_DELIM).append("# Primary Key").append(LINE_DELIM);
      getPrimaryKeyInformation(constraintsInfo, pkInfo);
    }
    if (fkInfo != null && !fkInfo.getForeignKeys().isEmpty()) {
      constraintsInfo.append(LINE_DELIM).append("# Foreign Keys").append(LINE_DELIM);
      getForeignKeysInformation(constraintsInfo, fkInfo);
    }
    if (ukInfo != null && !ukInfo.getUniqueConstraints().isEmpty()) {
      constraintsInfo.append(LINE_DELIM).append("# Unique Constraints").append(LINE_DELIM);
      getUniqueConstraintsInformation(constraintsInfo, ukInfo);
    }
    if (nnInfo != null && !nnInfo.getNotNullConstraints().isEmpty()) {
      constraintsInfo.append(LINE_DELIM).append("# Not Null Constraints").append(LINE_DELIM);
      getNotNullConstraintsInformation(constraintsInfo, nnInfo);
    }
    if (dInfo != null && !dInfo.getDefaultConstraints().isEmpty()) {
      constraintsInfo.append(LINE_DELIM).append("# Default Constraints").append(LINE_DELIM);
      getDefaultConstraintsInformation(constraintsInfo, dInfo);
    }
    if (cInfo != null && !cInfo.getCheckConstraints().isEmpty()) {
      constraintsInfo.append(LINE_DELIM).append("# Check Constraints").append(LINE_DELIM);
      getCheckConstraintsInformation(constraintsInfo, cInfo);
    }
    return constraintsInfo.toString();
  }

  private static void  getPrimaryKeyInformation(StringBuilder constraintsInfo,
    PrimaryKeyInfo pkInfo) {
    formatOutput("Table:", pkInfo.getDatabaseName()+"."+pkInfo.getTableName(), constraintsInfo);
    formatOutput("Constraint Name:", pkInfo.getConstraintName(), constraintsInfo);
    Map<Integer, String> colNames = pkInfo.getColNames();
    final String columnNames = "Column Names:";
    constraintsInfo.append(String.format("%-" + ALIGNMENT + "s", columnNames)).append(FIELD_DELIM);
    if (colNames != null && colNames.size() > 0) {
      formatOutput(colNames.values().toArray(new String[colNames.size()]), constraintsInfo);
    }
  }

  private static void getForeignKeyColInformation(StringBuilder constraintsInfo,
    ForeignKeyCol fkCol) {
      String[] fkcFields = new String[3];
      fkcFields[0] = "Parent Column Name:" + fkCol.parentDatabaseName +
          "."+ fkCol.parentTableName + "." + fkCol.parentColName;
      fkcFields[1] = "Column Name:" + fkCol.childColName;
      fkcFields[2] = "Key Sequence:" + fkCol.position;
      formatOutput(fkcFields, constraintsInfo);
  }

  private static void getForeignKeyRelInformation(
    StringBuilder constraintsInfo,
    String constraintName,
    List<ForeignKeyCol> fkRel) {
    formatOutput("Constraint Name:", constraintName, constraintsInfo);
    if (fkRel != null && fkRel.size() > 0) {
      for (ForeignKeyCol fkc : fkRel) {
        getForeignKeyColInformation(constraintsInfo, fkc);
      }
    }
    constraintsInfo.append(LINE_DELIM);
  }

  private static void  getForeignKeysInformation(StringBuilder constraintsInfo,
    ForeignKeyInfo fkInfo) {
    formatOutput("Table:",
                 fkInfo.getChildDatabaseName()+"."+fkInfo.getChildTableName(),
                 constraintsInfo);
    Map<String, List<ForeignKeyCol>> foreignKeys = fkInfo.getForeignKeys();
    if (foreignKeys != null && foreignKeys.size() > 0) {
      for (Map.Entry<String, List<ForeignKeyCol>> me : foreignKeys.entrySet()) {
        getForeignKeyRelInformation(constraintsInfo, me.getKey(), me.getValue());
      }
    }
  }

  private static void getUniqueConstraintColInformation(StringBuilder constraintsInfo,
      UniqueConstraintCol ukCol) {
    String[] fkcFields = new String[2];
    fkcFields[0] = "Column Name:" + ukCol.colName;
    fkcFields[1] = "Key Sequence:" + ukCol.position;
    formatOutput(fkcFields, constraintsInfo);
  }

  private static void getUniqueConstraintRelInformation(
      StringBuilder constraintsInfo,
      String constraintName,
      List<UniqueConstraintCol> ukRel) {
    formatOutput("Constraint Name:", constraintName, constraintsInfo);
    if (ukRel != null && ukRel.size() > 0) {
      for (UniqueConstraintCol ukc : ukRel) {
        getUniqueConstraintColInformation(constraintsInfo, ukc);
      }
    }
    constraintsInfo.append(LINE_DELIM);
  }

  private static void getUniqueConstraintsInformation(StringBuilder constraintsInfo,
      UniqueConstraint ukInfo) {
    formatOutput("Table:",
                 ukInfo.getDatabaseName() + "." + ukInfo.getTableName(),
                 constraintsInfo);
    Map<String, List<UniqueConstraintCol>> uniqueConstraints = ukInfo.getUniqueConstraints();
    if (uniqueConstraints != null && uniqueConstraints.size() > 0) {
      for (Map.Entry<String, List<UniqueConstraintCol>> me : uniqueConstraints.entrySet()) {
        getUniqueConstraintRelInformation(constraintsInfo, me.getKey(), me.getValue());
      }
    }
  }

  private static void getNotNullConstraintsInformation(StringBuilder constraintsInfo,
      NotNullConstraint nnInfo) {
    formatOutput("Table:",
                 nnInfo.getDatabaseName() + "." + nnInfo.getTableName(),
                 constraintsInfo);
    Map<String, String> notNullConstraints = nnInfo.getNotNullConstraints();
    if (notNullConstraints != null && notNullConstraints.size() > 0) {
      for (Map.Entry<String, String> me : notNullConstraints.entrySet()) {
        formatOutput("Constraint Name:", me.getKey(), constraintsInfo);
        formatOutput("Column Name:", me.getValue(), constraintsInfo);
        constraintsInfo.append(LINE_DELIM);
      }
    }
  }

  private static void getDefaultConstraintColInformation(StringBuilder constraintsInfo,
                                                        DefaultConstraint.DefaultConstraintCol ukCol) {
    String[] fkcFields = new String[2];
    fkcFields[0] = "Column Name:" + ukCol.colName;
    fkcFields[1] = "Default Value:" + ukCol.defaultVal;
    formatOutput(fkcFields, constraintsInfo);
  }

  private static void getCheckConstraintColInformation(StringBuilder constraintsInfo,
                                                         CheckConstraint.CheckConstraintCol ukCol) {
    String[] fkcFields = new String[2];
    fkcFields[0] = "Column Name:" + ukCol.colName;
    fkcFields[1] = "Check Value:" + ukCol.checkExpression;
    formatOutput(fkcFields, constraintsInfo);
  }

  private static void getDefaultConstraintRelInformation(
      StringBuilder constraintsInfo,
      String constraintName,
      List<DefaultConstraint.DefaultConstraintCol> ukRel) {
    formatOutput("Constraint Name:", constraintName, constraintsInfo);
    if (ukRel != null && ukRel.size() > 0) {
      for (DefaultConstraint.DefaultConstraintCol ukc : ukRel) {
        getDefaultConstraintColInformation(constraintsInfo, ukc);
      }
    }
    constraintsInfo.append(LINE_DELIM);
  }

  private static void getCheckConstraintRelInformation(
      StringBuilder constraintsInfo,
      String constraintName,
      List<CheckConstraint.CheckConstraintCol> ukRel) {
    formatOutput("Constraint Name:", constraintName, constraintsInfo);
    if (ukRel != null && ukRel.size() > 0) {
      for (CheckConstraint.CheckConstraintCol ukc : ukRel) {
        getCheckConstraintColInformation(constraintsInfo, ukc);
      }
    }
    constraintsInfo.append(LINE_DELIM);
  }

  private static void getDefaultConstraintsInformation(StringBuilder constraintsInfo,
                                                        DefaultConstraint dInfo) {
    formatOutput("Table:",
        dInfo.getDatabaseName() + "." + dInfo.getTableName(),
        constraintsInfo);
    Map<String, List<DefaultConstraint.DefaultConstraintCol>> defaultConstraints = dInfo.getDefaultConstraints();
    if (defaultConstraints != null && defaultConstraints.size() > 0) {
      for (Map.Entry<String, List<DefaultConstraint.DefaultConstraintCol>> me : defaultConstraints.entrySet()) {
        getDefaultConstraintRelInformation(constraintsInfo, me.getKey(), me.getValue());
      }
    }
  }

  private static void getCheckConstraintsInformation(StringBuilder constraintsInfo,
                                                       CheckConstraint dInfo) {
    formatOutput("Table:",
                 dInfo.getDatabaseName() + "." + dInfo.getTableName(),
                 constraintsInfo);
    Map<String, List<CheckConstraint.CheckConstraintCol>> checkConstraints = dInfo.getCheckConstraints();
    if (checkConstraints != null && checkConstraints.size() > 0) {
      for (Map.Entry<String, List<CheckConstraint.CheckConstraintCol>> me : checkConstraints.entrySet()) {
        getCheckConstraintRelInformation(constraintsInfo, me.getKey(), me.getValue());
      }
    }
  }

  public static String getPartitionInformation(Partition part) {
    StringBuilder tableInfo = new StringBuilder(DEFAULT_STRINGBUILDER_SIZE);

    // Table Metadata
    tableInfo.append(LINE_DELIM).append("# Detailed Partition Information").append(LINE_DELIM);
    getPartitionMetaDataInformation(tableInfo, part);

    // Storage information.
    if (part.getTable().getTableType() != TableType.VIRTUAL_VIEW) {
      tableInfo.append(LINE_DELIM).append("# Storage Information").append(LINE_DELIM);
      getStorageDescriptorInfo(tableInfo, part.getTPartition().getSd());
    }

    return tableInfo.toString();
  }

  public static String getTableInformation(Table table, boolean isOutputPadded) {
    StringBuilder tableInfo = new StringBuilder(DEFAULT_STRINGBUILDER_SIZE);

    // Table Metadata
    tableInfo.append(LINE_DELIM).append("# Detailed Table Information").append(LINE_DELIM);
    getTableMetaDataInformation(tableInfo, table, isOutputPadded);

    // Storage information.
    tableInfo.append(LINE_DELIM).append("# Storage Information").append(LINE_DELIM);
    getStorageDescriptorInfo(tableInfo, table.getTTable().getSd());

    if (table.isView() || table.isMaterializedView()) {
      tableInfo.append(LINE_DELIM).append("# View Information").append(LINE_DELIM);
      getViewInfo(tableInfo, table);
    }

    return tableInfo.toString();
  }

  private static void getViewInfo(StringBuilder tableInfo, Table tbl) {
    formatOutput("View Original Text:", tbl.getViewOriginalText(), tableInfo);
    formatOutput("View Expanded Text:", tbl.getViewExpandedText(), tableInfo);
    formatOutput("View Rewrite Enabled:", tbl.isRewriteEnabled() ? "Yes" : "No", tableInfo);
  }

  private static void getStorageDescriptorInfo(StringBuilder tableInfo,
      StorageDescriptor storageDesc) {

    formatOutput("SerDe Library:", storageDesc.getSerdeInfo().getSerializationLib(), tableInfo);
    formatOutput("InputFormat:", storageDesc.getInputFormat(), tableInfo);
    formatOutput("OutputFormat:", storageDesc.getOutputFormat(), tableInfo);
    formatOutput("Compressed:", storageDesc.isCompressed() ? "Yes" : "No", tableInfo);
    formatOutput("Num Buckets:", String.valueOf(storageDesc.getNumBuckets()), tableInfo);
    formatOutput("Bucket Columns:", storageDesc.getBucketCols().toString(), tableInfo);
    formatOutput("Sort Columns:", storageDesc.getSortCols().toString(), tableInfo);
    if (storageDesc.isStoredAsSubDirectories()) {// optional parameter
      formatOutput("Stored As SubDirectories:", "Yes", tableInfo);
    }

    if (null != storageDesc.getSkewedInfo()) {
      List<String> skewedColNames = sortedList(storageDesc.getSkewedInfo().getSkewedColNames());
      if ((skewedColNames != null) && (skewedColNames.size() > 0)) {
        formatOutput("Skewed Columns:", skewedColNames.toString(), tableInfo);
      }

      List<List<String>> skewedColValues = sortedList(
          storageDesc.getSkewedInfo().getSkewedColValues(), new VectorComparator<String>());
      if ((skewedColValues != null) && (skewedColValues.size() > 0)) {
        formatOutput("Skewed Values:", skewedColValues.toString(), tableInfo);
      }

      Map<List<String>, String> skewedColMap = new TreeMap<>(new VectorComparator<String>());
      skewedColMap.putAll(storageDesc.getSkewedInfo().getSkewedColValueLocationMaps());
      if ((skewedColMap!=null) && (skewedColMap.size() > 0)) {
        formatOutput("Skewed Value to Path:", skewedColMap.toString(),
            tableInfo);
        Map<List<String>, String> truncatedSkewedColMap = new TreeMap<List<String>, String>(new VectorComparator<String>());
        // walk through existing map to truncate path so that test won't mask it
        // then we can verify location is right
        Set<Entry<List<String>, String>> entries = skewedColMap.entrySet();
        for (Entry<List<String>, String> entry : entries) {
          truncatedSkewedColMap.put(entry.getKey(),
              PlanUtils.removePrefixFromWarehouseConfig(entry.getValue()));
        }
        formatOutput("Skewed Value to Truncated Path:",
            truncatedSkewedColMap.toString(), tableInfo);
      }
    }

    if (storageDesc.getSerdeInfo().getParametersSize() > 0) {
      tableInfo.append("Storage Desc Params:").append(LINE_DELIM);
      displayAllParameters(storageDesc.getSerdeInfo().getParameters(), tableInfo);
    }
  }

  private static void getTableMetaDataInformation(StringBuilder tableInfo, Table  tbl,
      boolean isOutputPadded) {
    formatOutput("Database:", tbl.getDbName(), tableInfo);
    formatOutput("OwnerType:", (tbl.getOwnerType() != null) ? tbl.getOwnerType().name() : "null", tableInfo);
    formatOutput("Owner:", tbl.getOwner(), tableInfo);
    formatOutput("CreateTime:", formatDate(tbl.getTTable().getCreateTime()), tableInfo);
    formatOutput("LastAccessTime:", formatDate(tbl.getTTable().getLastAccessTime()), tableInfo);
    formatOutput("Retention:", Integer.toString(tbl.getRetention()), tableInfo);
    if (!tbl.isView()) {
      formatOutput("Location:", tbl.getDataLocation().toString(), tableInfo);
    }
    formatOutput("Table Type:", tbl.getTableType().name(), tableInfo);

    if (tbl.getParameters().size() > 0) {
      tableInfo.append("Table Parameters:").append(LINE_DELIM);
      displayAllParameters(tbl.getParameters(), tableInfo, false, isOutputPadded);
    }
  }

  private static void getPartitionMetaDataInformation(StringBuilder tableInfo, Partition part) {
    formatOutput("Partition Value:", part.getValues().toString(), tableInfo);
    formatOutput("Database:", part.getTPartition().getDbName(), tableInfo);
    formatOutput("Table:", part.getTable().getTableName(), tableInfo);
    formatOutput("CreateTime:", formatDate(part.getTPartition().getCreateTime()), tableInfo);
    formatOutput("LastAccessTime:", formatDate(part.getTPartition().getLastAccessTime()),
        tableInfo);
    formatOutput("Location:", part.getLocation(), tableInfo);

    if (part.getTPartition().getParameters().size() > 0) {
      tableInfo.append("Partition Parameters:").append(LINE_DELIM);
      displayAllParameters(part.getTPartition().getParameters(), tableInfo);
    }
  }

  /**
   * Display key, value pairs of the parameters. The characters will be escaped
   * including unicode.
   */
  private static void displayAllParameters(Map<String, String> params, StringBuilder tableInfo) {
    displayAllParameters(params, tableInfo, true, false);
  }

  /**
   * Display key, value pairs of the parameters. The characters will be escaped
   * including unicode if escapeUnicode is true; otherwise the characters other
   * than unicode will be escaped.
   */
  private static void displayAllParameters(Map<String, String> params, StringBuilder tableInfo,
      boolean escapeUnicode, boolean isOutputPadded) {
    List<String> keys = new ArrayList<String>(params.keySet());
    Collections.sort(keys);
    for (String key : keys) {
      tableInfo.append(FIELD_DELIM); // Ensures all params are indented.
      formatOutput(key,
          escapeUnicode ? StringEscapeUtils.escapeJava(params.get(key))
              : HiveStringUtils.escapeJava(params.get(key)),
          tableInfo, isOutputPadded);
    }
  }

  static String getComment(FieldSchema col) {
    return col.getComment() != null ? col.getComment() : "";
  }

  /**
   * Compares to lists of object T as vectors
   *
   * @param <T> the base object type. Must be {@link Comparable}
   */
  private static class VectorComparator<T extends Comparable<T>>  implements Comparator<List<T>>{

    @Override
    public int compare(List<T> listA, List<T> listB) {
      for (int i = 0; i < listA.size() && i < listB.size(); i++) {
        T valA = listA.get(i);
        T valB = listB.get(i);
        if (valA != null) {
          int ret = valA.compareTo(valB);
          if (ret != 0) {
            return ret;
          }
        } else {
          if (valB != null) {
            return -1;
          }
        }
      }
      return Integer.compare(listA.size(), listB.size());
    }
  }

  /**
   * Returns a sorted version of the given list
   */
  static <T extends Comparable<T>> List<T> sortedList(List<T> list){
    if (list == null || list.size() <= 1) {
      return list;
    }
    ArrayList<T> ret = new ArrayList<>();
    ret.addAll(list);
    Collections.sort(ret);
    return ret;
  }

  /**
   * Returns a sorted version of the given list, using the provided comparator
   */
  static <T> List<T> sortedList(List<T> list, Comparator<T> comp) {
    if (list == null || list.size() <= 1) {
      return list;
    }
    ArrayList<T> ret = new ArrayList<>();
    ret.addAll(list);
    Collections.sort(ret,comp);
    return ret;
  }

  private static String formatDate(long timeInSeconds) {
    if (timeInSeconds != 0) {
      Date date = new Date(timeInSeconds * 1000);
      return date.toString();
    }
    return "UNKNOWN";
  }

  /**
   * Prints a row with the given fields into the builder
   * The last field could be a multiline field, and the extra lines should be padded
   * @param fields The fields to print
   * @param tableInfo The target builder
   * @param isLastLinePadded Is the last field could be printed in multiple lines, if contains
   *                         newlines?
   */
  static void formatOutput(String[] fields, StringBuilder tableInfo,
      boolean isLastLinePadded, boolean isFormatted) {
    if (!isFormatted) {
      for (int i = 0; i < fields.length; i++) {
        Object value = HiveStringUtils.escapeJava(fields[i]);
        if (value != null) {
          tableInfo.append(value);
        }
        tableInfo.append((i == fields.length - 1) ? LINE_DELIM : FIELD_DELIM);
      }
    } else {
      int[] paddings = new int[fields.length - 1];
      if (fields.length > 1) {
        for (int i = 0; i < fields.length - 1; i++) {
          if (fields[i] == null) {
            tableInfo.append(FIELD_DELIM);
            continue;
          }
          tableInfo.append(String.format("%-" + ALIGNMENT + "s", fields[i])).append(FIELD_DELIM);
          paddings[i] = ALIGNMENT > fields[i].length() ? ALIGNMENT : fields[i].length();
        }
      }
      if (fields.length > 0) {
        String value = fields[fields.length - 1];
        String unescapedValue = (isLastLinePadded && value != null) ? value.replaceAll("\\\\n|\\\\r|\\\\r\\\\n", "\n") : value;
        indentMultilineValue(unescapedValue, tableInfo, paddings, false);
      } else {
        tableInfo.append(LINE_DELIM);
      }
    }
  }

  /**
   * Prints a row the given fields to a formatted line
   * @param fields The fields to print
   * @param tableInfo The target builder
   */
  private static void formatOutput(String[] fields, StringBuilder tableInfo) {
    formatOutput(fields, tableInfo, false, true);
  }

  /**
   * Prints the name value pair, and if the value contains newlines, it add one more empty field
   * before the two values (Assumes, the name value pair is already indented with it)
   * @param name The field name to print
   * @param value The value to print - might contain newlines
   * @param tableInfo The target builder
   */
  private static void formatOutput(String name, String value, StringBuilder tableInfo) {
    tableInfo.append(String.format("%-" + ALIGNMENT + "s", name)).append(FIELD_DELIM);
    int colNameLength = ALIGNMENT > name.length() ? ALIGNMENT : name.length();
    indentMultilineValue(value, tableInfo, new int[] {0, colNameLength}, true);
  }

  /**
   * Prints the name value pair
   * It the output is padded then unescape the value, so it could be printed in multiple lines.
   * In this case it assumes the pair is already indented with a field delimiter
   * @param name The field name to print
   * @param value The value t print
   * @param tableInfo The target builder
   * @param isOutputPadded Should the value printed as a padded string?
   */
  static void formatOutput(String name, String value, StringBuilder tableInfo,
      boolean isOutputPadded) {
    String unescapedValue =
        (isOutputPadded && value != null) ? value.replaceAll("\\\\n|\\\\r|\\\\r\\\\n","\n"):value;
    formatOutput(name, unescapedValue, tableInfo);
  }

  public static String[] extractColumnValues(FieldSchema col) {
    return extractColumnValues(col, false, null);
  }

  public static String[] extractColumnValues(FieldSchema col, boolean isColStatsAvailable, ColumnStatisticsObj columnStatisticsObj){
    List<String> ret = new ArrayList<>();
    ret.add(col.getName());
    ret.add(col.getType());

    if (isColStatsAvailable) {
      if (columnStatisticsObj != null) {
        ColumnStatisticsData csd = columnStatisticsObj.getStatsData();
        // @formatter:off
        if (csd.isSetBinaryStats()) {
          BinaryColumnStatsData bcsd = csd.getBinaryStats();
          ret.addAll(Lists.newArrayList(  "", "",
                                          "" + bcsd.getNumNulls(), "",
                                          "" + bcsd.getAvgColLen(), "" + bcsd.getMaxColLen(),
                                          "", "",
                                          convertToString(bcsd.getBitVectors())));
        } else if (csd.isSetStringStats()) {
          StringColumnStatsData scsd = csd.getStringStats();
          ret.addAll(Lists.newArrayList(  "", "",
                                          "" + scsd.getNumNulls(), "" + scsd.getNumDVs(),
                                          "" + scsd.getAvgColLen(), "" + scsd.getMaxColLen(),
                                          "", "",
                                          convertToString(scsd.getBitVectors())));
        } else if (csd.isSetBooleanStats()) {
          BooleanColumnStatsData bcsd = csd.getBooleanStats();
          ret.addAll(Lists.newArrayList(  "", "",
                                          "" + bcsd.getNumNulls(), "",
                                          "", "",
                                          "" + bcsd.getNumTrues(), "" + bcsd.getNumFalses(),
                                          convertToString(bcsd.getBitVectors())));
        } else if (csd.isSetDecimalStats()) {
          DecimalColumnStatsData dcsd = csd.getDecimalStats();
          ret.addAll(Lists.newArrayList(  convertToString(dcsd.getLowValue()), convertToString(dcsd.getHighValue()),
                                          "" + dcsd.getNumNulls(), "" + dcsd.getNumDVs(),
                                          "", "",
                                          "", "",
                                          convertToString(dcsd.getBitVectors())));
        } else if (csd.isSetDoubleStats()) {
          DoubleColumnStatsData dcsd = csd.getDoubleStats();
          ret.addAll(Lists.newArrayList(  "" + dcsd.getLowValue(), "" + dcsd.getHighValue(),
                                          "" + dcsd.getNumNulls(), "" + dcsd.getNumDVs(),
                                          "", "",
                                          "", "",
                                          convertToString(dcsd.getBitVectors())));
        } else if (csd.isSetLongStats()) {
          LongColumnStatsData lcsd = csd.getLongStats();
          ret.addAll(Lists.newArrayList(  "" + lcsd.getLowValue(), "" + lcsd.getHighValue(),
                                          "" + lcsd.getNumNulls(), "" + lcsd.getNumDVs(),
                                          "", "",
                                          "", "",
                                          convertToString(lcsd.getBitVectors())));
        } else if (csd.isSetDateStats()) {
          DateColumnStatsData dcsd = csd.getDateStats();
          ret.addAll(Lists.newArrayList(  convertToString(dcsd.getLowValue()), convertToString(dcsd.getHighValue()),
                                          "" + dcsd.getNumNulls(), "" + dcsd.getNumDVs(),
                                          "", "",
                                          "", "",
                                          convertToString(dcsd.getBitVectors())));
        }
        // @formatter:on
      } else {
        ret.addAll(Lists.newArrayList("", "", "", "", "", "", "", "", ""));
        }
      }

    ret.add(getComment(col));
    return ret.toArray(new String[] {});
  }

  /**
   * comment indent processing for multi-line values
   * values should be indented the same amount on each line
   * if the first line comment starts indented by k,
   * the following line comments should also be indented by k
   * @param value the value to write
   * @param tableInfo the buffer to write to
   * @param columnWidths the widths of the previous columns
   * @param printNull print null as a string, or do not print anything
   */
  private static void indentMultilineValue(String value, StringBuilder tableInfo,
      int[] columnWidths, boolean printNull) {
    if (value==null) {
      if (printNull) {
        tableInfo.append(String.format("%-" + ALIGNMENT + "s", value));
      }
      tableInfo.append(LINE_DELIM);
    } else {
      String[] valueSegments = value.split("\n|\r|\r\n");
      tableInfo.append(String.format("%-" + ALIGNMENT + "s", valueSegments[0])).append(LINE_DELIM);
      for (int i = 1; i < valueSegments.length; i++) {
        printPadding(tableInfo, columnWidths);
        tableInfo.append(String.format("%-" + ALIGNMENT + "s", valueSegments[i]))
            .append(LINE_DELIM);
      }
    }
  }

  /**
   * Print the rigth padding, with the given column widths
   * @param tableInfo The buffer to write to
   * @param columnWidths The column widths
   */
  private static void printPadding(StringBuilder tableInfo, int[] columnWidths) {
    for (int columnWidth : columnWidths) {
      if (columnWidth == 0) {
        tableInfo.append(FIELD_DELIM);
      } else {
        tableInfo.append(String.format("%" + columnWidth + "s" + FIELD_DELIM, ""));
      }
    }
  }

  public static String[] getColumnsHeader(List<ColumnStatisticsObj> colStats) {
    boolean showColStats = false;
    if (colStats != null) {
      showColStats = true;
    }
    return DescTableDesc.getSchema(showColStats).split("#")[0].split(",");
  }

  public static MetaDataFormatter getFormatter(HiveConf conf) {
    if ("json".equals(conf.get(HiveConf.ConfVars.HIVE_DDL_OUTPUT_FORMAT.varname, "text"))) {
      return new JsonMetaDataFormatter();
    } else {
      return new TextMetaDataFormatter(conf.getIntVar(HiveConf.ConfVars.CLIPRETTYOUTPUTNUMCOLS), conf.getBoolVar(ConfVars.HIVE_DISPLAY_PARTITION_COLUMNS_SEPARATELY));
    }
  }

  /**
   * Interface to implement actual conversion to text or json of a resource plan.
   */
  public interface RPFormatter {
    void startRP(String rpName, Object ... kvPairs) throws IOException;
    void endRP() throws IOException;
    void startPools() throws IOException;
    void startPool(String poolName, Object ...kvPairs) throws IOException;
    void endPool() throws IOException;
    void endPools() throws IOException;
    void startTriggers() throws IOException;
    void formatTrigger(String triggerName,
        String actionExpression, String triggerExpression) throws IOException;
    void endTriggers() throws IOException;
    void startMappings() throws IOException;
    void formatMappingType(String type, List<String> names) throws IOException;
    void endMappings() throws IOException;
  }

  /**
   * A n-ary tree for the pools, each node contains a pool and its children.
   */
  private static class PoolTreeNode {
    private String nonPoolName;
    private WMPool pool;
    private final List<PoolTreeNode> children = new ArrayList<>();
    private final List<WMTrigger> triggers = new ArrayList<>();
    private final HashMap<String, List<String>> mappings = new HashMap<>();
    private boolean isDefault;

    private PoolTreeNode() {}

    private void writePoolTreeNode(RPFormatter rpFormatter) throws IOException {
      if (pool != null) {
        String path = pool.getPoolPath();
        int idx = path.lastIndexOf('.');
        if (idx != -1) {
          path = path.substring(idx + 1);
        }
        Double allocFraction = pool.getAllocFraction();
        String schedulingPolicy = pool.isSetSchedulingPolicy() ? pool.getSchedulingPolicy() : null;
        Integer parallelism = pool.getQueryParallelism();
        rpFormatter.startPool(path, "allocFraction", allocFraction,
            "schedulingPolicy", schedulingPolicy, "parallelism", parallelism);
      } else {
        rpFormatter.startPool(nonPoolName);
      }
      rpFormatter.startTriggers();
      for (WMTrigger trigger : triggers) {
        rpFormatter.formatTrigger(trigger.getTriggerName(), trigger.getActionExpression(),
            trigger.getTriggerExpression());
      }
      rpFormatter.endTriggers();
      rpFormatter.startMappings();
      for (Map.Entry<String, List<String>> mappingsOfType : mappings.entrySet()) {
        mappingsOfType.getValue().sort(String::compareTo);
        rpFormatter.formatMappingType(mappingsOfType.getKey(), mappingsOfType.getValue());
      }
      if (isDefault) {
        rpFormatter.formatMappingType("default", Lists.<String>newArrayList());
      }
      rpFormatter.endMappings();
      rpFormatter.startPools();
      for (PoolTreeNode node : children) {
        node.writePoolTreeNode(rpFormatter);
      }
      rpFormatter.endPools();
      rpFormatter.endPool();
    }

    private void sortChildren() {
      children.sort((PoolTreeNode p1, PoolTreeNode p2) -> {
        if (p2.pool == null) {
          return (p1.pool == null) ? 0 : -1;
        }
        if (p1.pool == null) {
          return 1;
        }
        return Double.compare(p2.pool.getAllocFraction(), p1.pool.getAllocFraction());
      });
      for (PoolTreeNode child : children) {
        child.sortChildren();
      }
      triggers.sort((WMTrigger t1, WMTrigger t2)
          -> t1.getTriggerName().compareTo(t2.getTriggerName()));
    }

    static PoolTreeNode makePoolTree(WMFullResourcePlan fullRp) {
      Map<String, PoolTreeNode> poolMap = new HashMap<>();
      PoolTreeNode root = new PoolTreeNode();
      for (WMPool pool : fullRp.getPools()) {
        // Create or add node for current pool.
        String path = pool.getPoolPath();
        PoolTreeNode curr = poolMap.get(path);
        if (curr == null) {
          curr = new PoolTreeNode();
          poolMap.put(path, curr);
        }
        curr.pool = pool;
        if (fullRp.getPlan().isSetDefaultPoolPath()
            && fullRp.getPlan().getDefaultPoolPath().equals(path)) {
          curr.isDefault = true;
        }

        // Add this node to the parent node.
        int ind = path.lastIndexOf('.');
        PoolTreeNode parent;
        if (ind == -1) {
          parent = root;
        } else {
          String parentPath = path.substring(0, ind);
          parent = poolMap.get(parentPath);
          if (parent == null) {
            parent = new PoolTreeNode();
            poolMap.put(parentPath, parent);
          }
        }
        parent.children.add(curr);
      }
      Map<String, WMTrigger> triggerMap = new HashMap<>();
      List<WMTrigger> unmanagedTriggers = new ArrayList<>();
      HashSet<WMTrigger> unusedTriggers = new HashSet<>();
      if (fullRp.isSetTriggers()) {
        for (WMTrigger trigger : fullRp.getTriggers()) {
          triggerMap.put(trigger.getTriggerName(), trigger);
          if (trigger.isIsInUnmanaged()) {
            unmanagedTriggers.add(trigger);
          } else {
            unusedTriggers.add(trigger);
          }
        }
      }
      if (fullRp.isSetPoolTriggers()) {
        for (WMPoolTrigger pool2Trigger : fullRp.getPoolTriggers()) {
          PoolTreeNode node = poolMap.get(pool2Trigger.getPool());
          WMTrigger trigger = triggerMap.get(pool2Trigger.getTrigger());
          if (node == null || trigger == null) {
            throw new IllegalStateException("Invalid trigger to pool: " + pool2Trigger.getPool() +
                ", " + pool2Trigger.getTrigger());
          }
          unusedTriggers.remove(trigger);
          node.triggers.add(trigger);
        }
      }
      HashMap<String, List<String>> unmanagedMappings = new HashMap<>();
      HashMap<String, List<String>> invalidMappings = new HashMap<>();
      if (fullRp.isSetMappings()) {
        for (WMMapping mapping : fullRp.getMappings()) {
          if (mapping.isSetPoolPath()) {
            PoolTreeNode destNode = poolMap.get(mapping.getPoolPath());
            addMappingToMap((destNode == null) ? invalidMappings : destNode.mappings, mapping);
          } else {
            addMappingToMap(unmanagedMappings, mapping);
          }
        }
      }

      if (!unmanagedTriggers.isEmpty() || !unmanagedMappings.isEmpty()) {
        PoolTreeNode curr = createNonPoolNode(poolMap, "unmanaged queries", root);
        curr.triggers.addAll(unmanagedTriggers);
        curr.mappings.putAll(unmanagedMappings);
      }
      // TODO: perhaps we should also summarize the triggers pointing to invalid pools.
      if (!unusedTriggers.isEmpty()) {
        PoolTreeNode curr = createNonPoolNode(poolMap, "unused triggers", root);
        curr.triggers.addAll(unusedTriggers);
      }
      if (!invalidMappings.isEmpty()) {
        PoolTreeNode curr = createNonPoolNode(poolMap, "invalid mappings", root);
        curr.mappings.putAll(invalidMappings);
      }
      return root;
    }

    private static PoolTreeNode createNonPoolNode(
        Map<String, PoolTreeNode> poolMap, String name, PoolTreeNode root) {
      PoolTreeNode result;
      do {
        name = "<" + name + ">";
        result = poolMap.get(name);
        // We expect this to never happen in practice. Can pool paths even have angled braces?
      } while (result != null);
      result = new PoolTreeNode();
      result.nonPoolName = name;
      poolMap.put(name, result);
      root.children.add(result);
      return result;
    }

    private static void addMappingToMap(HashMap<String, List<String>> map, WMMapping mapping) {
      List<String> list = map.get(mapping.getEntityType());
      if (list == null) {
        list = new ArrayList<String>();
        map.put(mapping.getEntityType(), list);
      }
      list.add(mapping.getEntityName());
    }
  }

  public static void formatFullRP(RPFormatter rpFormatter, WMFullResourcePlan fullRp)
      throws HiveException {
    try {
      WMResourcePlan plan = fullRp.getPlan();
      Integer parallelism = plan.isSetQueryParallelism() ? plan.getQueryParallelism() : null;
      String defaultPool = plan.isSetDefaultPoolPath() ? plan.getDefaultPoolPath() : null;
      rpFormatter.startRP(plan.getName(), "status", plan.getStatus().toString(),
           "parallelism", parallelism, "defaultPool", defaultPool);
      rpFormatter.startPools();

      PoolTreeNode root = PoolTreeNode.makePoolTree(fullRp);
      root.sortChildren();
      for (PoolTreeNode pool : root.children) {
        pool.writePoolTreeNode(rpFormatter);
      }

      rpFormatter.endPools();
      rpFormatter.endRP();
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }
}
