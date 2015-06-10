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

package org.apache.hadoop.hive.metastore;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.model.MTableColumnStatistics;

/**
 * This class contains conversion logic that creates Thrift stat objects from
 * JDO stat objects and plain arrays from DirectSQL.
 * It is hidden here so that we wouldn't have to look at it in elsewhere.
 */
public class StatObjectConverter {
  // JDO
  public static MTableColumnStatistics convertToMTableColumnStatistics(MTable table,
      ColumnStatisticsDesc statsDesc, ColumnStatisticsObj statsObj)
          throws NoSuchObjectException, MetaException, InvalidObjectException {
     if (statsObj == null || statsDesc == null) {
       throw new InvalidObjectException("Invalid column stats object");
     }

     MTableColumnStatistics mColStats = new MTableColumnStatistics();
     mColStats.setTable(table);
     mColStats.setDbName(statsDesc.getDbName());
     mColStats.setTableName(statsDesc.getTableName());
     mColStats.setLastAnalyzed(statsDesc.getLastAnalyzed());
     mColStats.setColName(statsObj.getColName());
     mColStats.setColType(statsObj.getColType());

     if (statsObj.getStatsData().isSetBooleanStats()) {
       BooleanColumnStatsData boolStats = statsObj.getStatsData().getBooleanStats();
       mColStats.setBooleanStats(
           boolStats.isSetNumTrues() ? boolStats.getNumTrues() : null,
           boolStats.isSetNumFalses() ? boolStats.getNumFalses() : null,
           boolStats.isSetNumNulls() ? boolStats.getNumNulls() : null);
     } else if (statsObj.getStatsData().isSetLongStats()) {
       LongColumnStatsData longStats = statsObj.getStatsData().getLongStats();
       mColStats.setLongStats(
           longStats.isSetNumNulls() ? longStats.getNumNulls() : null,
           longStats.isSetNumDVs() ? longStats.getNumDVs() : null,
           longStats.isSetLowValue() ? longStats.getLowValue() : null,
           longStats.isSetHighValue() ? longStats.getHighValue() : null);
     } else if (statsObj.getStatsData().isSetDoubleStats()) {
       DoubleColumnStatsData doubleStats = statsObj.getStatsData().getDoubleStats();
       mColStats.setDoubleStats(
           doubleStats.isSetNumNulls() ? doubleStats.getNumNulls() : null,
           doubleStats.isSetNumDVs() ? doubleStats.getNumDVs() : null,
           doubleStats.isSetLowValue() ? doubleStats.getLowValue() : null,
           doubleStats.isSetHighValue() ? doubleStats.getHighValue() : null);
     } else if (statsObj.getStatsData().isSetDecimalStats()) {
       DecimalColumnStatsData decimalStats = statsObj.getStatsData().getDecimalStats();
       String low = decimalStats.isSetLowValue() ? createJdoDecimalString(decimalStats.getLowValue()) : null;
       String high = decimalStats.isSetHighValue() ? createJdoDecimalString(decimalStats.getHighValue()) : null;
       mColStats.setDecimalStats(
           decimalStats.isSetNumNulls() ? decimalStats.getNumNulls() : null,
           decimalStats.isSetNumDVs() ? decimalStats.getNumDVs() : null,
               low, high);
     } else if (statsObj.getStatsData().isSetStringStats()) {
       StringColumnStatsData stringStats = statsObj.getStatsData().getStringStats();
       mColStats.setStringStats(
           stringStats.isSetNumNulls() ? stringStats.getNumNulls() : null,
           stringStats.isSetNumDVs() ? stringStats.getNumDVs() : null,
           stringStats.isSetMaxColLen() ? stringStats.getMaxColLen() : null,
           stringStats.isSetAvgColLen() ? stringStats.getAvgColLen() : null);
     } else if (statsObj.getStatsData().isSetBinaryStats()) {
       BinaryColumnStatsData binaryStats = statsObj.getStatsData().getBinaryStats();
       mColStats.setBinaryStats(
           binaryStats.isSetNumNulls() ? binaryStats.getNumNulls() : null,
           binaryStats.isSetMaxColLen() ? binaryStats.getMaxColLen() : null,
           binaryStats.isSetAvgColLen() ? binaryStats.getAvgColLen() : null);
     } else if (statsObj.getStatsData().isSetDateStats()) {
       DateColumnStatsData dateStats = statsObj.getStatsData().getDateStats();
       mColStats.setDateStats(
           dateStats.isSetNumNulls() ? dateStats.getNumNulls() : null,
           dateStats.isSetNumDVs() ? dateStats.getNumDVs() : null,
           dateStats.isSetLowValue() ? dateStats.getLowValue().getDaysSinceEpoch() : null,
           dateStats.isSetHighValue() ? dateStats.getHighValue().getDaysSinceEpoch() : null);
     }
     return mColStats;
  }

  public static void setFieldsIntoOldStats(
      MTableColumnStatistics mStatsObj, MTableColumnStatistics oldStatsObj) {
    if (mStatsObj.getAvgColLen() != null) {
      oldStatsObj.setAvgColLen(mStatsObj.getAvgColLen());
    }
    if (mStatsObj.getLongHighValue() != null) {
      oldStatsObj.setLongHighValue(mStatsObj.getLongHighValue());
    }
    if (mStatsObj.getLongLowValue() != null) {
      oldStatsObj.setLongLowValue(mStatsObj.getLongLowValue());
    }
    if (mStatsObj.getDoubleLowValue() != null) {
      oldStatsObj.setDoubleLowValue(mStatsObj.getDoubleLowValue());
    }
    if (mStatsObj.getDoubleHighValue() != null) {
      oldStatsObj.setDoubleHighValue(mStatsObj.getDoubleHighValue());
    }
    if (mStatsObj.getDecimalLowValue() != null) {
      oldStatsObj.setDecimalLowValue(mStatsObj.getDecimalLowValue());
    }
    if (mStatsObj.getDecimalHighValue() != null) {
      oldStatsObj.setDecimalHighValue(mStatsObj.getDecimalHighValue());
    }
    if (mStatsObj.getMaxColLen() != null) {
      oldStatsObj.setMaxColLen(mStatsObj.getMaxColLen());
    }
    if (mStatsObj.getNumDVs() != null) {
      oldStatsObj.setNumDVs(mStatsObj.getNumDVs());
    }
    if (mStatsObj.getNumFalses() != null) {
      oldStatsObj.setNumFalses(mStatsObj.getNumFalses());
    }
    if (mStatsObj.getNumTrues() != null) {
      oldStatsObj.setNumTrues(mStatsObj.getNumTrues());
    }
    if (mStatsObj.getNumNulls() != null) {
      oldStatsObj.setNumNulls(mStatsObj.getNumNulls());
    }
    oldStatsObj.setLastAnalyzed(mStatsObj.getLastAnalyzed());
  }

  public static void setFieldsIntoOldStats(
      MPartitionColumnStatistics mStatsObj, MPartitionColumnStatistics oldStatsObj) {
    if (mStatsObj.getAvgColLen() != null) {
          oldStatsObj.setAvgColLen(mStatsObj.getAvgColLen());
    }
    if (mStatsObj.getLongHighValue() != null) {
      oldStatsObj.setLongHighValue(mStatsObj.getLongHighValue());
    }
    if (mStatsObj.getDoubleHighValue() != null) {
      oldStatsObj.setDoubleHighValue(mStatsObj.getDoubleHighValue());
    }
    oldStatsObj.setLastAnalyzed(mStatsObj.getLastAnalyzed());
    if (mStatsObj.getLongLowValue() != null) {
      oldStatsObj.setLongLowValue(mStatsObj.getLongLowValue());
    }
    if (mStatsObj.getDoubleLowValue() != null) {
      oldStatsObj.setDoubleLowValue(mStatsObj.getDoubleLowValue());
    }
    if (mStatsObj.getDecimalLowValue() != null) {
      oldStatsObj.setDecimalLowValue(mStatsObj.getDecimalLowValue());
    }
    if (mStatsObj.getDecimalHighValue() != null) {
      oldStatsObj.setDecimalHighValue(mStatsObj.getDecimalHighValue());
    }
    if (mStatsObj.getMaxColLen() != null) {
      oldStatsObj.setMaxColLen(mStatsObj.getMaxColLen());
    }
    if (mStatsObj.getNumDVs() != null) {
      oldStatsObj.setNumDVs(mStatsObj.getNumDVs());
    }
    if (mStatsObj.getNumFalses() != null) {
      oldStatsObj.setNumFalses(mStatsObj.getNumFalses());
    }
    if (mStatsObj.getNumTrues() != null) {
      oldStatsObj.setNumTrues(mStatsObj.getNumTrues());
    }
    if (mStatsObj.getNumNulls() != null) {
      oldStatsObj.setNumNulls(mStatsObj.getNumNulls());
    }
  }

  public static ColumnStatisticsObj getTableColumnStatisticsObj(
      MTableColumnStatistics mStatsObj) {
    ColumnStatisticsObj statsObj = new ColumnStatisticsObj();
    statsObj.setColType(mStatsObj.getColType());
    statsObj.setColName(mStatsObj.getColName());
    String colType = mStatsObj.getColType().toLowerCase();
    ColumnStatisticsData colStatsData = new ColumnStatisticsData();

    if (colType.equals("boolean")) {
      BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
      boolStats.setNumFalses(mStatsObj.getNumFalses());
      boolStats.setNumTrues(mStatsObj.getNumTrues());
      boolStats.setNumNulls(mStatsObj.getNumNulls());
      colStatsData.setBooleanStats(boolStats);
    } else if (colType.equals("string") ||
        colType.startsWith("varchar") || colType.startsWith("char")) {
      StringColumnStatsData stringStats = new StringColumnStatsData();
      stringStats.setNumNulls(mStatsObj.getNumNulls());
      stringStats.setAvgColLen(mStatsObj.getAvgColLen());
      stringStats.setMaxColLen(mStatsObj.getMaxColLen());
      stringStats.setNumDVs(mStatsObj.getNumDVs());
      colStatsData.setStringStats(stringStats);
    } else if (colType.equals("binary")) {
      BinaryColumnStatsData binaryStats = new BinaryColumnStatsData();
      binaryStats.setNumNulls(mStatsObj.getNumNulls());
      binaryStats.setAvgColLen(mStatsObj.getAvgColLen());
      binaryStats.setMaxColLen(mStatsObj.getMaxColLen());
      colStatsData.setBinaryStats(binaryStats);
    } else if (colType.equals("bigint") || colType.equals("int") ||
        colType.equals("smallint") || colType.equals("tinyint") ||
        colType.equals("timestamp")) {
      LongColumnStatsData longStats = new LongColumnStatsData();
      longStats.setNumNulls(mStatsObj.getNumNulls());
      Long longHighValue = mStatsObj.getLongHighValue();
      if (longHighValue != null) {
        longStats.setHighValue(longHighValue);
      }
      Long longLowValue = mStatsObj.getLongLowValue();
      if (longLowValue != null) {
        longStats.setLowValue(longLowValue);
      }
      longStats.setNumDVs(mStatsObj.getNumDVs());
      colStatsData.setLongStats(longStats);
    } else if (colType.equals("double") || colType.equals("float")) {
      DoubleColumnStatsData doubleStats = new DoubleColumnStatsData();
      doubleStats.setNumNulls(mStatsObj.getNumNulls());
      Double doubleHighValue = mStatsObj.getDoubleHighValue();
      if (doubleHighValue != null) {
        doubleStats.setHighValue(doubleHighValue);
      }
      Double doubleLowValue = mStatsObj.getDoubleLowValue();
      if (doubleLowValue != null) {
        doubleStats.setLowValue(doubleLowValue);
      }
      doubleStats.setNumDVs(mStatsObj.getNumDVs());
      colStatsData.setDoubleStats(doubleStats);
    } else if (colType.startsWith("decimal")) {
      DecimalColumnStatsData decimalStats = new DecimalColumnStatsData();
      decimalStats.setNumNulls(mStatsObj.getNumNulls());
      String decimalHighValue = mStatsObj.getDecimalHighValue();
      if (decimalHighValue != null) {
        decimalStats.setHighValue(createThriftDecimal(decimalHighValue));
      }
      String decimalLowValue = mStatsObj.getDecimalLowValue();
      if (decimalLowValue != null) {
        decimalStats.setLowValue(createThriftDecimal(decimalLowValue));
      }
      decimalStats.setNumDVs(mStatsObj.getNumDVs());
      colStatsData.setDecimalStats(decimalStats);
    } else if (colType.equals("date")) {
      DateColumnStatsData dateStats = new DateColumnStatsData();
      dateStats.setNumNulls(mStatsObj.getNumNulls());
      Long highValue = mStatsObj.getLongHighValue();
      if (highValue != null) {
        dateStats.setHighValue(new Date(highValue));
      }
      Long lowValue = mStatsObj.getLongLowValue();
      if (lowValue != null) {
        dateStats.setLowValue(new Date(lowValue));
      }
      dateStats.setNumDVs(mStatsObj.getNumDVs());
      colStatsData.setDateStats(dateStats);
    }
    statsObj.setStatsData(colStatsData);
    return statsObj;
  }

  public static ColumnStatisticsDesc getTableColumnStatisticsDesc(
      MTableColumnStatistics mStatsObj) {
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc();
    statsDesc.setIsTblLevel(true);
    statsDesc.setDbName(mStatsObj.getDbName());
    statsDesc.setTableName(mStatsObj.getTableName());
    statsDesc.setLastAnalyzed(mStatsObj.getLastAnalyzed());
    return statsDesc;
  }

  public static MPartitionColumnStatistics convertToMPartitionColumnStatistics(
      MPartition partition, ColumnStatisticsDesc statsDesc, ColumnStatisticsObj statsObj)
          throws MetaException, NoSuchObjectException {
    if (statsDesc == null || statsObj == null) {
      return null;
    }

    MPartitionColumnStatistics mColStats = new MPartitionColumnStatistics();
    mColStats.setPartition(partition);
    mColStats.setDbName(statsDesc.getDbName());
    mColStats.setTableName(statsDesc.getTableName());
    mColStats.setPartitionName(statsDesc.getPartName());
    mColStats.setLastAnalyzed(statsDesc.getLastAnalyzed());
    mColStats.setColName(statsObj.getColName());
    mColStats.setColType(statsObj.getColType());

    if (statsObj.getStatsData().isSetBooleanStats()) {
      BooleanColumnStatsData boolStats = statsObj.getStatsData().getBooleanStats();
      mColStats.setBooleanStats(
          boolStats.isSetNumTrues() ? boolStats.getNumTrues() : null,
          boolStats.isSetNumFalses() ? boolStats.getNumFalses() : null,
          boolStats.isSetNumNulls() ? boolStats.getNumNulls() : null);
    } else if (statsObj.getStatsData().isSetLongStats()) {
      LongColumnStatsData longStats = statsObj.getStatsData().getLongStats();
      mColStats.setLongStats(
          longStats.isSetNumNulls() ? longStats.getNumNulls() : null,
          longStats.isSetNumDVs() ? longStats.getNumDVs() : null,
          longStats.isSetLowValue() ? longStats.getLowValue() : null,
          longStats.isSetHighValue() ? longStats.getHighValue() : null);
    } else if (statsObj.getStatsData().isSetDoubleStats()) {
      DoubleColumnStatsData doubleStats = statsObj.getStatsData().getDoubleStats();
      mColStats.setDoubleStats(
          doubleStats.isSetNumNulls() ? doubleStats.getNumNulls() : null,
          doubleStats.isSetNumDVs() ? doubleStats.getNumDVs() : null,
          doubleStats.isSetLowValue() ? doubleStats.getLowValue() : null,
          doubleStats.isSetHighValue() ? doubleStats.getHighValue() : null);
    } else if (statsObj.getStatsData().isSetDecimalStats()) {
      DecimalColumnStatsData decimalStats = statsObj.getStatsData().getDecimalStats();
      String low = decimalStats.isSetLowValue() ? createJdoDecimalString(decimalStats.getLowValue()) : null;
      String high = decimalStats.isSetHighValue() ? createJdoDecimalString(decimalStats.getHighValue()) : null;
      mColStats.setDecimalStats(
          decimalStats.isSetNumNulls() ? decimalStats.getNumNulls() : null,
          decimalStats.isSetNumDVs() ? decimalStats.getNumDVs() : null,
              low, high);
    } else if (statsObj.getStatsData().isSetStringStats()) {
      StringColumnStatsData stringStats = statsObj.getStatsData().getStringStats();
      mColStats.setStringStats(
          stringStats.isSetNumNulls() ? stringStats.getNumNulls() : null,
          stringStats.isSetNumDVs() ? stringStats.getNumDVs() : null,
          stringStats.isSetMaxColLen() ? stringStats.getMaxColLen() : null,
          stringStats.isSetAvgColLen() ? stringStats.getAvgColLen() : null);
    } else if (statsObj.getStatsData().isSetBinaryStats()) {
      BinaryColumnStatsData binaryStats = statsObj.getStatsData().getBinaryStats();
      mColStats.setBinaryStats(
          binaryStats.isSetNumNulls() ? binaryStats.getNumNulls() : null,
          binaryStats.isSetMaxColLen() ? binaryStats.getMaxColLen() : null,
          binaryStats.isSetAvgColLen() ? binaryStats.getAvgColLen() : null);
    } else if (statsObj.getStatsData().isSetDateStats()) {
      DateColumnStatsData dateStats = statsObj.getStatsData().getDateStats();
      mColStats.setDateStats(
          dateStats.isSetNumNulls() ? dateStats.getNumNulls() : null,
          dateStats.isSetNumDVs() ? dateStats.getNumDVs() : null,
          dateStats.isSetLowValue() ? dateStats.getLowValue().getDaysSinceEpoch() : null,
          dateStats.isSetHighValue() ? dateStats.getHighValue().getDaysSinceEpoch() : null);
    }
    return mColStats;
  }

  public static ColumnStatisticsObj getPartitionColumnStatisticsObj(
      MPartitionColumnStatistics mStatsObj) {
    ColumnStatisticsObj statsObj = new ColumnStatisticsObj();
    statsObj.setColType(mStatsObj.getColType());
    statsObj.setColName(mStatsObj.getColName());
    String colType = mStatsObj.getColType().toLowerCase();
    ColumnStatisticsData colStatsData = new ColumnStatisticsData();

    if (colType.equals("boolean")) {
      BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
      boolStats.setNumFalses(mStatsObj.getNumFalses());
      boolStats.setNumTrues(mStatsObj.getNumTrues());
      boolStats.setNumNulls(mStatsObj.getNumNulls());
      colStatsData.setBooleanStats(boolStats);
    } else if (colType.equals("string") ||
        colType.startsWith("varchar") || colType.startsWith("char")) {
      StringColumnStatsData stringStats = new StringColumnStatsData();
      stringStats.setNumNulls(mStatsObj.getNumNulls());
      stringStats.setAvgColLen(mStatsObj.getAvgColLen());
      stringStats.setMaxColLen(mStatsObj.getMaxColLen());
      stringStats.setNumDVs(mStatsObj.getNumDVs());
      colStatsData.setStringStats(stringStats);
    } else if (colType.equals("binary")) {
      BinaryColumnStatsData binaryStats = new BinaryColumnStatsData();
      binaryStats.setNumNulls(mStatsObj.getNumNulls());
      binaryStats.setAvgColLen(mStatsObj.getAvgColLen());
      binaryStats.setMaxColLen(mStatsObj.getMaxColLen());
      colStatsData.setBinaryStats(binaryStats);
    } else if (colType.equals("tinyint") || colType.equals("smallint") ||
        colType.equals("int") || colType.equals("bigint") ||
        colType.equals("timestamp")) {
      LongColumnStatsData longStats = new LongColumnStatsData();
      longStats.setNumNulls(mStatsObj.getNumNulls());
      if (mStatsObj.getLongHighValue() != null) {
        longStats.setHighValue(mStatsObj.getLongHighValue());
      }
      if (mStatsObj.getLongLowValue() != null) {
        longStats.setLowValue(mStatsObj.getLongLowValue());
      }
      longStats.setNumDVs(mStatsObj.getNumDVs());
      colStatsData.setLongStats(longStats);
    } else if (colType.equals("double") || colType.equals("float")) {
      DoubleColumnStatsData doubleStats = new DoubleColumnStatsData();
      doubleStats.setNumNulls(mStatsObj.getNumNulls());
      if (mStatsObj.getDoubleHighValue() != null) {
        doubleStats.setHighValue(mStatsObj.getDoubleHighValue());
      }
      if (mStatsObj.getDoubleLowValue() != null) {
        doubleStats.setLowValue(mStatsObj.getDoubleLowValue());
      }
      doubleStats.setNumDVs(mStatsObj.getNumDVs());
      colStatsData.setDoubleStats(doubleStats);
    } else if (colType.startsWith("decimal")) {
      DecimalColumnStatsData decimalStats = new DecimalColumnStatsData();
      decimalStats.setNumNulls(mStatsObj.getNumNulls());
      if (mStatsObj.getDecimalHighValue() != null) {
        decimalStats.setHighValue(createThriftDecimal(mStatsObj.getDecimalHighValue()));
      }
      if (mStatsObj.getDecimalLowValue() != null) {
        decimalStats.setLowValue(createThriftDecimal(mStatsObj.getDecimalLowValue()));
      }
      decimalStats.setNumDVs(mStatsObj.getNumDVs());
      colStatsData.setDecimalStats(decimalStats);
    } else if (colType.equals("date")) {
      DateColumnStatsData dateStats = new DateColumnStatsData();
      dateStats.setNumNulls(mStatsObj.getNumNulls());
      dateStats.setHighValue(new Date(mStatsObj.getLongHighValue()));
      dateStats.setLowValue(new Date(mStatsObj.getLongLowValue()));
      dateStats.setNumDVs(mStatsObj.getNumDVs());
      colStatsData.setDateStats(dateStats);
    }
    statsObj.setStatsData(colStatsData);
    return statsObj;
  }

  public static ColumnStatisticsDesc getPartitionColumnStatisticsDesc(
    MPartitionColumnStatistics mStatsObj) {
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc();
    statsDesc.setIsTblLevel(false);
    statsDesc.setDbName(mStatsObj.getDbName());
    statsDesc.setTableName(mStatsObj.getTableName());
    statsDesc.setPartName(mStatsObj.getPartitionName());
    statsDesc.setLastAnalyzed(mStatsObj.getLastAnalyzed());
    return statsDesc;
  }

  // SQL
  public static void fillColumnStatisticsData(String colType, ColumnStatisticsData data,
      Object llow, Object lhigh, Object dlow, Object dhigh, Object declow, Object dechigh,
      Object nulls, Object dist, Object avglen, Object maxlen, Object trues, Object falses) throws MetaException {
    colType = colType.toLowerCase();
    if (colType.equals("boolean")) {
      BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
      boolStats.setNumFalses(MetaStoreDirectSql.extractSqlLong(falses));
      boolStats.setNumTrues(MetaStoreDirectSql.extractSqlLong(trues));
      boolStats.setNumNulls(MetaStoreDirectSql.extractSqlLong(nulls));
      data.setBooleanStats(boolStats);
    } else if (colType.equals("string") ||
        colType.startsWith("varchar") || colType.startsWith("char")) {
      StringColumnStatsData stringStats = new StringColumnStatsData();
      stringStats.setNumNulls(MetaStoreDirectSql.extractSqlLong(nulls));
      stringStats.setAvgColLen(MetaStoreDirectSql.extractSqlDouble(avglen));
      stringStats.setMaxColLen(MetaStoreDirectSql.extractSqlLong(maxlen));
      stringStats.setNumDVs(MetaStoreDirectSql.extractSqlLong(dist));
      data.setStringStats(stringStats);
    } else if (colType.equals("binary")) {
      BinaryColumnStatsData binaryStats = new BinaryColumnStatsData();
      binaryStats.setNumNulls(MetaStoreDirectSql.extractSqlLong(nulls));
      binaryStats.setAvgColLen(MetaStoreDirectSql.extractSqlDouble(avglen));
      binaryStats.setMaxColLen(MetaStoreDirectSql.extractSqlLong(maxlen));
      data.setBinaryStats(binaryStats);
    } else if (colType.equals("bigint") || colType.equals("int") ||
        colType.equals("smallint") || colType.equals("tinyint") ||
        colType.equals("timestamp")) {
      LongColumnStatsData longStats = new LongColumnStatsData();
      longStats.setNumNulls(MetaStoreDirectSql.extractSqlLong(nulls));
      if (lhigh != null) {
        longStats.setHighValue(MetaStoreDirectSql.extractSqlLong(lhigh));
      }
      if (llow != null) {
        longStats.setLowValue(MetaStoreDirectSql.extractSqlLong(llow));
      }
      longStats.setNumDVs(MetaStoreDirectSql.extractSqlLong(dist));
      data.setLongStats(longStats);
    } else if (colType.equals("double") || colType.equals("float")) {
      DoubleColumnStatsData doubleStats = new DoubleColumnStatsData();
      doubleStats.setNumNulls(MetaStoreDirectSql.extractSqlLong(nulls));
      if (dhigh != null) {
        doubleStats.setHighValue(MetaStoreDirectSql.extractSqlDouble(dhigh));
      }
      if (dlow != null) {
        doubleStats.setLowValue(MetaStoreDirectSql.extractSqlDouble(dlow));
      }
      doubleStats.setNumDVs(MetaStoreDirectSql.extractSqlLong(dist));
      data.setDoubleStats(doubleStats);
    } else if (colType.startsWith("decimal")) {
      DecimalColumnStatsData decimalStats = new DecimalColumnStatsData();
      decimalStats.setNumNulls(MetaStoreDirectSql.extractSqlLong(nulls));
      if (dechigh != null) {
        decimalStats.setHighValue(createThriftDecimal((String)dechigh));
      }
      if (declow != null) {
        decimalStats.setLowValue(createThriftDecimal((String)declow));
      }
      decimalStats.setNumDVs(MetaStoreDirectSql.extractSqlLong(dist));
      data.setDecimalStats(decimalStats);
    } else if (colType.equals("date")) {
      DateColumnStatsData dateStats = new DateColumnStatsData();
      dateStats.setNumNulls(MetaStoreDirectSql.extractSqlLong(nulls));
      if (lhigh != null) {
        dateStats.setHighValue(new Date(MetaStoreDirectSql.extractSqlLong(lhigh)));
      }
      if (llow != null) {
        dateStats.setLowValue(new Date(MetaStoreDirectSql.extractSqlLong(llow)));
      }
      dateStats.setNumDVs(MetaStoreDirectSql.extractSqlLong(dist));
      data.setDateStats(dateStats);
    }
  }

  public static void fillColumnStatisticsData(String colType, ColumnStatisticsData data,
      Object llow, Object lhigh, Object dlow, Object dhigh, Object declow, Object dechigh,
      Object nulls, Object dist, Object avglen, Object maxlen, Object trues, Object falses,
      Object avgLong, Object avgDouble, Object avgDecimal, Object sumDist,
      boolean useDensityFunctionForNDVEstimation) throws MetaException {
    colType = colType.toLowerCase();
    if (colType.equals("boolean")) {
      BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
      boolStats.setNumFalses(MetaStoreDirectSql.extractSqlLong(falses));
      boolStats.setNumTrues(MetaStoreDirectSql.extractSqlLong(trues));
      boolStats.setNumNulls(MetaStoreDirectSql.extractSqlLong(nulls));
      data.setBooleanStats(boolStats);
    } else if (colType.equals("string") || colType.startsWith("varchar")
        || colType.startsWith("char")) {
      StringColumnStatsData stringStats = new StringColumnStatsData();
      stringStats.setNumNulls(MetaStoreDirectSql.extractSqlLong(nulls));
      stringStats.setAvgColLen(MetaStoreDirectSql.extractSqlDouble(avglen));
      stringStats.setMaxColLen(MetaStoreDirectSql.extractSqlLong(maxlen));
      stringStats.setNumDVs(MetaStoreDirectSql.extractSqlLong(dist));
      data.setStringStats(stringStats);
    } else if (colType.equals("binary")) {
      BinaryColumnStatsData binaryStats = new BinaryColumnStatsData();
      binaryStats.setNumNulls(MetaStoreDirectSql.extractSqlLong(nulls));
      binaryStats.setAvgColLen(MetaStoreDirectSql.extractSqlDouble(avglen));
      binaryStats.setMaxColLen(MetaStoreDirectSql.extractSqlLong(maxlen));
      data.setBinaryStats(binaryStats);
    } else if (colType.equals("bigint") || colType.equals("int") || colType.equals("smallint")
        || colType.equals("tinyint") || colType.equals("timestamp")) {
      LongColumnStatsData longStats = new LongColumnStatsData();
      longStats.setNumNulls(MetaStoreDirectSql.extractSqlLong(nulls));
      if (lhigh != null) {
        longStats.setHighValue(MetaStoreDirectSql.extractSqlLong(lhigh));
      }
      if (llow != null) {
        longStats.setLowValue(MetaStoreDirectSql.extractSqlLong(llow));
      }
      long lowerBound = MetaStoreDirectSql.extractSqlLong(dist);
      long higherBound = MetaStoreDirectSql.extractSqlLong(sumDist);
      if (useDensityFunctionForNDVEstimation && lhigh != null && llow != null && avgLong != null
          && MetaStoreDirectSql.extractSqlDouble(avgLong) != 0.0) {
        // We have estimation, lowerbound and higherbound. We use estimation if
        // it is between lowerbound and higherbound.
        long estimation = MetaStoreDirectSql
            .extractSqlLong((MetaStoreDirectSql.extractSqlLong(lhigh) - MetaStoreDirectSql
                .extractSqlLong(llow)) / MetaStoreDirectSql.extractSqlDouble(avgLong));
        if (estimation < lowerBound) {
          longStats.setNumDVs(lowerBound);
        } else if (estimation > higherBound) {
          longStats.setNumDVs(higherBound);
        } else {
          longStats.setNumDVs(estimation);
        }
      } else {
        longStats.setNumDVs(lowerBound);
      }
      data.setLongStats(longStats);
    } else if (colType.equals("double") || colType.equals("float")) {
      DoubleColumnStatsData doubleStats = new DoubleColumnStatsData();
      doubleStats.setNumNulls(MetaStoreDirectSql.extractSqlLong(nulls));
      if (dhigh != null) {
        doubleStats.setHighValue(MetaStoreDirectSql.extractSqlDouble(dhigh));
      }
      if (dlow != null) {
        doubleStats.setLowValue(MetaStoreDirectSql.extractSqlDouble(dlow));
      }
      long lowerBound = MetaStoreDirectSql.extractSqlLong(dist);
      long higherBound = MetaStoreDirectSql.extractSqlLong(sumDist);
      if (useDensityFunctionForNDVEstimation && dhigh != null && dlow != null && avgDouble != null
          && MetaStoreDirectSql.extractSqlDouble(avgDouble) != 0.0) {
        long estimation = MetaStoreDirectSql
            .extractSqlLong((MetaStoreDirectSql.extractSqlLong(dhigh) - MetaStoreDirectSql
                .extractSqlLong(dlow)) / MetaStoreDirectSql.extractSqlDouble(avgDouble));
        if (estimation < lowerBound) {
          doubleStats.setNumDVs(lowerBound);
        } else if (estimation > higherBound) {
          doubleStats.setNumDVs(higherBound);
        } else {
          doubleStats.setNumDVs(estimation);
        }
      } else {
        doubleStats.setNumDVs(lowerBound);
      }
      data.setDoubleStats(doubleStats);
    } else if (colType.startsWith("decimal")) {
      DecimalColumnStatsData decimalStats = new DecimalColumnStatsData();
      decimalStats.setNumNulls(MetaStoreDirectSql.extractSqlLong(nulls));
      Decimal low = null;
      Decimal high = null;
      BigDecimal blow = null;
      BigDecimal bhigh = null;
      if (dechigh instanceof BigDecimal) {
        bhigh = (BigDecimal) dechigh;
        high = new Decimal(ByteBuffer.wrap(bhigh.unscaledValue().toByteArray()),
            (short) bhigh.scale());
      } else if (dechigh instanceof String) {
        bhigh = new BigDecimal((String) dechigh);
        high = createThriftDecimal((String) dechigh);
      }
      decimalStats.setHighValue(high);
      if (declow instanceof BigDecimal) {
        blow = (BigDecimal) declow;
        low = new Decimal(ByteBuffer.wrap(blow.unscaledValue().toByteArray()), (short) blow.scale());
      } else if (dechigh instanceof String) {
        blow = new BigDecimal((String) declow);
        low = createThriftDecimal((String) declow);
      }
      decimalStats.setLowValue(low);
      long lowerBound = MetaStoreDirectSql.extractSqlLong(dist);
      long higherBound = MetaStoreDirectSql.extractSqlLong(sumDist);
      if (useDensityFunctionForNDVEstimation && dechigh != null && declow != null && avgDecimal != null
          && MetaStoreDirectSql.extractSqlDouble(avgDecimal) != 0.0) {
        long estimation = MetaStoreDirectSql.extractSqlLong(MetaStoreDirectSql.extractSqlLong(bhigh
            .subtract(blow).floatValue() / MetaStoreDirectSql.extractSqlDouble(avgDecimal)));
        if (estimation < lowerBound) {
          decimalStats.setNumDVs(lowerBound);
        } else if (estimation > higherBound) {
          decimalStats.setNumDVs(higherBound);
        } else {
          decimalStats.setNumDVs(estimation);
        }
      } else {
        decimalStats.setNumDVs(lowerBound);
      }
      data.setDecimalStats(decimalStats);
    }
  }

  private static Decimal createThriftDecimal(String s) {
    BigDecimal d = new BigDecimal(s);
    return new Decimal(ByteBuffer.wrap(d.unscaledValue().toByteArray()), (short)d.scale());
  }

  private static String createJdoDecimalString(Decimal d) {
    return new BigDecimal(new BigInteger(d.getUnscaled()), d.getScale()).toString();
  }

}
