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

package org.apache.hadoop.hive.metastore;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData._Fields;
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
import org.apache.hadoop.hive.metastore.api.utils.DecimalUtils;
import org.apache.hadoop.hive.metastore.columnstats.cache.DateColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DecimalColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DoubleColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.LongColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.StringColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.model.MTableColumnStatistics;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;

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
     mColStats.setCatName(statsDesc.isSetCatName() ? statsDesc.getCatName() : DEFAULT_CATALOG_NAME);
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
           longStats.isSetBitVectors() ? longStats.getBitVectors() : null,
           longStats.isSetLowValue() ? longStats.getLowValue() : null,
           longStats.isSetHighValue() ? longStats.getHighValue() : null);
     } else if (statsObj.getStatsData().isSetDoubleStats()) {
       DoubleColumnStatsData doubleStats = statsObj.getStatsData().getDoubleStats();
       mColStats.setDoubleStats(
           doubleStats.isSetNumNulls() ? doubleStats.getNumNulls() : null,
           doubleStats.isSetNumDVs() ? doubleStats.getNumDVs() : null,
           doubleStats.isSetBitVectors() ? doubleStats.getBitVectors() : null,
           doubleStats.isSetLowValue() ? doubleStats.getLowValue() : null,
           doubleStats.isSetHighValue() ? doubleStats.getHighValue() : null);
     } else if (statsObj.getStatsData().isSetDecimalStats()) {
       DecimalColumnStatsData decimalStats = statsObj.getStatsData().getDecimalStats();
       String low = decimalStats.isSetLowValue() ? DecimalUtils.createJdoDecimalString(decimalStats.getLowValue()) : null;
       String high = decimalStats.isSetHighValue() ? DecimalUtils.createJdoDecimalString(decimalStats.getHighValue()) : null;
       mColStats.setDecimalStats(
           decimalStats.isSetNumNulls() ? decimalStats.getNumNulls() : null,
           decimalStats.isSetNumDVs() ? decimalStats.getNumDVs() : null,
           decimalStats.isSetBitVectors() ? decimalStats.getBitVectors() : null,
               low, high);
     } else if (statsObj.getStatsData().isSetStringStats()) {
       StringColumnStatsData stringStats = statsObj.getStatsData().getStringStats();
       mColStats.setStringStats(
           stringStats.isSetNumNulls() ? stringStats.getNumNulls() : null,
           stringStats.isSetNumDVs() ? stringStats.getNumDVs() : null,
           stringStats.isSetBitVectors() ? stringStats.getBitVectors() : null,
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
           dateStats.isSetBitVectors() ? dateStats.getBitVectors() : null,
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
    if (mStatsObj.getBitVector() != null) {
      oldStatsObj.setBitVector(mStatsObj.getBitVector());
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
    if (mStatsObj.getBitVector() != null) {
      oldStatsObj.setBitVector(mStatsObj.getBitVector());
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
      MTableColumnStatistics mStatsObj, boolean enableBitVector) {
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
      StringColumnStatsDataInspector stringStats = new StringColumnStatsDataInspector();
      stringStats.setNumNulls(mStatsObj.getNumNulls());
      stringStats.setAvgColLen(mStatsObj.getAvgColLen());
      stringStats.setMaxColLen(mStatsObj.getMaxColLen());
      stringStats.setNumDVs(mStatsObj.getNumDVs());
      stringStats.setBitVectors((mStatsObj.getBitVector()==null||!enableBitVector)? null : mStatsObj.getBitVector());
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
      LongColumnStatsDataInspector longStats = new LongColumnStatsDataInspector();
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
      longStats.setBitVectors((mStatsObj.getBitVector()==null||!enableBitVector)? null : mStatsObj.getBitVector());
      colStatsData.setLongStats(longStats);
    } else if (colType.equals("double") || colType.equals("float")) {
      DoubleColumnStatsDataInspector doubleStats = new DoubleColumnStatsDataInspector();
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
      doubleStats.setBitVectors((mStatsObj.getBitVector()==null||!enableBitVector)? null : mStatsObj.getBitVector());
      colStatsData.setDoubleStats(doubleStats);
    } else if (colType.startsWith("decimal")) {
      DecimalColumnStatsDataInspector decimalStats = new DecimalColumnStatsDataInspector();
      decimalStats.setNumNulls(mStatsObj.getNumNulls());
      String decimalHighValue = mStatsObj.getDecimalHighValue();
      if (decimalHighValue != null) {
        decimalStats.setHighValue(DecimalUtils.createThriftDecimal(decimalHighValue));
      }
      String decimalLowValue = mStatsObj.getDecimalLowValue();
      if (decimalLowValue != null) {
        decimalStats.setLowValue(DecimalUtils.createThriftDecimal(decimalLowValue));
      }
      decimalStats.setNumDVs(mStatsObj.getNumDVs());
      decimalStats.setBitVectors((mStatsObj.getBitVector()==null||!enableBitVector)? null : mStatsObj.getBitVector());
      colStatsData.setDecimalStats(decimalStats);
    } else if (colType.equals("date")) {
      DateColumnStatsDataInspector dateStats = new DateColumnStatsDataInspector();
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
      dateStats.setBitVectors((mStatsObj.getBitVector()==null||!enableBitVector)? null : mStatsObj.getBitVector());
      colStatsData.setDateStats(dateStats);
    }
    statsObj.setStatsData(colStatsData);
    return statsObj;
  }

  public static ColumnStatisticsDesc getTableColumnStatisticsDesc(
      MTableColumnStatistics mStatsObj) {
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc();
    statsDesc.setIsTblLevel(true);
    statsDesc.setCatName(mStatsObj.getCatName());
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
    mColStats.setCatName(statsDesc.isSetCatName() ? statsDesc.getCatName() : DEFAULT_CATALOG_NAME);
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
          longStats.isSetBitVectors() ? longStats.getBitVectors() : null,
          longStats.isSetLowValue() ? longStats.getLowValue() : null,
          longStats.isSetHighValue() ? longStats.getHighValue() : null);
    } else if (statsObj.getStatsData().isSetDoubleStats()) {
      DoubleColumnStatsData doubleStats = statsObj.getStatsData().getDoubleStats();
      mColStats.setDoubleStats(
          doubleStats.isSetNumNulls() ? doubleStats.getNumNulls() : null,
          doubleStats.isSetNumDVs() ? doubleStats.getNumDVs() : null,
          doubleStats.isSetBitVectors() ? doubleStats.getBitVectors() : null,
          doubleStats.isSetLowValue() ? doubleStats.getLowValue() : null,
          doubleStats.isSetHighValue() ? doubleStats.getHighValue() : null);
    } else if (statsObj.getStatsData().isSetDecimalStats()) {
      DecimalColumnStatsData decimalStats = statsObj.getStatsData().getDecimalStats();
      String low = decimalStats.isSetLowValue() ? DecimalUtils.createJdoDecimalString(decimalStats.getLowValue()) : null;
      String high = decimalStats.isSetHighValue() ? DecimalUtils.createJdoDecimalString(decimalStats.getHighValue()) : null;
      mColStats.setDecimalStats(
          decimalStats.isSetNumNulls() ? decimalStats.getNumNulls() : null,
          decimalStats.isSetNumDVs() ? decimalStats.getNumDVs() : null,
          decimalStats.isSetBitVectors() ? decimalStats.getBitVectors() : null,
              low, high);
    } else if (statsObj.getStatsData().isSetStringStats()) {
      StringColumnStatsData stringStats = statsObj.getStatsData().getStringStats();
      mColStats.setStringStats(
          stringStats.isSetNumNulls() ? stringStats.getNumNulls() : null,
          stringStats.isSetNumDVs() ? stringStats.getNumDVs() : null,
          stringStats.isSetBitVectors() ? stringStats.getBitVectors() : null,
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
          dateStats.isSetBitVectors() ? dateStats.getBitVectors() : null,
          dateStats.isSetLowValue() ? dateStats.getLowValue().getDaysSinceEpoch() : null,
          dateStats.isSetHighValue() ? dateStats.getHighValue().getDaysSinceEpoch() : null);
    }
    return mColStats;
  }

  public static ColumnStatisticsObj getPartitionColumnStatisticsObj(
      MPartitionColumnStatistics mStatsObj, boolean enableBitVector) {
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
      StringColumnStatsDataInspector stringStats = new StringColumnStatsDataInspector();
      stringStats.setNumNulls(mStatsObj.getNumNulls());
      stringStats.setAvgColLen(mStatsObj.getAvgColLen());
      stringStats.setMaxColLen(mStatsObj.getMaxColLen());
      stringStats.setNumDVs(mStatsObj.getNumDVs());
      stringStats.setBitVectors((mStatsObj.getBitVector()==null||!enableBitVector)? null : mStatsObj.getBitVector());
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
      LongColumnStatsDataInspector longStats = new LongColumnStatsDataInspector();
      longStats.setNumNulls(mStatsObj.getNumNulls());
      if (mStatsObj.getLongHighValue() != null) {
        longStats.setHighValue(mStatsObj.getLongHighValue());
      }
      if (mStatsObj.getLongLowValue() != null) {
        longStats.setLowValue(mStatsObj.getLongLowValue());
      }
      longStats.setNumDVs(mStatsObj.getNumDVs());
      longStats.setBitVectors((mStatsObj.getBitVector()==null||!enableBitVector)? null : mStatsObj.getBitVector());
      colStatsData.setLongStats(longStats);
    } else if (colType.equals("double") || colType.equals("float")) {
      DoubleColumnStatsDataInspector doubleStats = new DoubleColumnStatsDataInspector();
      doubleStats.setNumNulls(mStatsObj.getNumNulls());
      if (mStatsObj.getDoubleHighValue() != null) {
        doubleStats.setHighValue(mStatsObj.getDoubleHighValue());
      }
      if (mStatsObj.getDoubleLowValue() != null) {
        doubleStats.setLowValue(mStatsObj.getDoubleLowValue());
      }
      doubleStats.setNumDVs(mStatsObj.getNumDVs());
      doubleStats.setBitVectors((mStatsObj.getBitVector()==null||!enableBitVector)? null : mStatsObj.getBitVector());
      colStatsData.setDoubleStats(doubleStats);
    } else if (colType.startsWith("decimal")) {
      DecimalColumnStatsDataInspector decimalStats = new DecimalColumnStatsDataInspector();
      decimalStats.setNumNulls(mStatsObj.getNumNulls());
      if (mStatsObj.getDecimalHighValue() != null) {
        decimalStats.setHighValue(DecimalUtils.createThriftDecimal(mStatsObj.getDecimalHighValue()));
      }
      if (mStatsObj.getDecimalLowValue() != null) {
        decimalStats.setLowValue(DecimalUtils.createThriftDecimal(mStatsObj.getDecimalLowValue()));
      }
      decimalStats.setNumDVs(mStatsObj.getNumDVs());
      decimalStats.setBitVectors((mStatsObj.getBitVector()==null||!enableBitVector)? null : mStatsObj.getBitVector());
      colStatsData.setDecimalStats(decimalStats);
    } else if (colType.equals("date")) {
      DateColumnStatsDataInspector dateStats = new DateColumnStatsDataInspector();
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
      dateStats.setBitVectors((mStatsObj.getBitVector()==null||!enableBitVector)? null : mStatsObj.getBitVector());
      colStatsData.setDateStats(dateStats);
    }
    statsObj.setStatsData(colStatsData);
    return statsObj;
  }

  public static ColumnStatisticsDesc getPartitionColumnStatisticsDesc(
    MPartitionColumnStatistics mStatsObj) {
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc();
    statsDesc.setIsTblLevel(false);
    statsDesc.setCatName(mStatsObj.getCatName());
    statsDesc.setDbName(mStatsObj.getDbName());
    statsDesc.setTableName(mStatsObj.getTableName());
    statsDesc.setPartName(mStatsObj.getPartitionName());
    statsDesc.setLastAnalyzed(mStatsObj.getLastAnalyzed());
    return statsDesc;
  }

  // JAVA
  public static void fillColumnStatisticsData(String colType, ColumnStatisticsData data,
      Object llow, Object lhigh, Object dlow, Object dhigh, Object declow, Object dechigh,
      Object nulls, Object dist, Object bitVector, Object avglen, Object maxlen, Object trues, Object falses) throws MetaException {
    colType = colType.toLowerCase();
    if (colType.equals("boolean")) {
      BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
      boolStats.setNumFalses(MetaStoreDirectSql.extractSqlLong(falses));
      boolStats.setNumTrues(MetaStoreDirectSql.extractSqlLong(trues));
      boolStats.setNumNulls(MetaStoreDirectSql.extractSqlLong(nulls));
      data.setBooleanStats(boolStats);
    } else if (colType.equals("string") ||
        colType.startsWith("varchar") || colType.startsWith("char")) {
      StringColumnStatsDataInspector stringStats = new StringColumnStatsDataInspector();
      stringStats.setNumNulls(MetaStoreDirectSql.extractSqlLong(nulls));
      stringStats.setAvgColLen(MetaStoreDirectSql.extractSqlDouble(avglen));
      stringStats.setMaxColLen(MetaStoreDirectSql.extractSqlLong(maxlen));
      stringStats.setNumDVs(MetaStoreDirectSql.extractSqlLong(dist));
      stringStats.setBitVectors(MetaStoreDirectSql.extractSqlBlob(bitVector));
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
      LongColumnStatsDataInspector longStats = new LongColumnStatsDataInspector();
      longStats.setNumNulls(MetaStoreDirectSql.extractSqlLong(nulls));
      if (lhigh != null) {
        longStats.setHighValue(MetaStoreDirectSql.extractSqlLong(lhigh));
      }
      if (llow != null) {
        longStats.setLowValue(MetaStoreDirectSql.extractSqlLong(llow));
      }
      longStats.setNumDVs(MetaStoreDirectSql.extractSqlLong(dist));
      longStats.setBitVectors(MetaStoreDirectSql.extractSqlBlob(bitVector));
      data.setLongStats(longStats);
    } else if (colType.equals("double") || colType.equals("float")) {
      DoubleColumnStatsDataInspector doubleStats = new DoubleColumnStatsDataInspector();
      doubleStats.setNumNulls(MetaStoreDirectSql.extractSqlLong(nulls));
      if (dhigh != null) {
        doubleStats.setHighValue(MetaStoreDirectSql.extractSqlDouble(dhigh));
      }
      if (dlow != null) {
        doubleStats.setLowValue(MetaStoreDirectSql.extractSqlDouble(dlow));
      }
      doubleStats.setNumDVs(MetaStoreDirectSql.extractSqlLong(dist));
      doubleStats.setBitVectors(MetaStoreDirectSql.extractSqlBlob(bitVector));
      data.setDoubleStats(doubleStats);
    } else if (colType.startsWith("decimal")) {
      DecimalColumnStatsDataInspector decimalStats = new DecimalColumnStatsDataInspector();
      decimalStats.setNumNulls(MetaStoreDirectSql.extractSqlLong(nulls));
      if (dechigh != null) {
        decimalStats.setHighValue(DecimalUtils.createThriftDecimal((String)dechigh));
      }
      if (declow != null) {
        decimalStats.setLowValue(DecimalUtils.createThriftDecimal((String)declow));
      }
      decimalStats.setNumDVs(MetaStoreDirectSql.extractSqlLong(dist));
      decimalStats.setBitVectors(MetaStoreDirectSql.extractSqlBlob(bitVector));
      data.setDecimalStats(decimalStats);
    } else if (colType.equals("date")) {
      DateColumnStatsDataInspector dateStats = new DateColumnStatsDataInspector();
      dateStats.setNumNulls(MetaStoreDirectSql.extractSqlLong(nulls));
      if (lhigh != null) {
        dateStats.setHighValue(new Date(MetaStoreDirectSql.extractSqlLong(lhigh)));
      }
      if (llow != null) {
        dateStats.setLowValue(new Date(MetaStoreDirectSql.extractSqlLong(llow)));
      }
      dateStats.setNumDVs(MetaStoreDirectSql.extractSqlLong(dist));
      dateStats.setBitVectors(MetaStoreDirectSql.extractSqlBlob(bitVector));
      data.setDateStats(dateStats);
    }
  }

  //DB
  public static void fillColumnStatisticsData(String colType, ColumnStatisticsData data,
      Object llow, Object lhigh, Object dlow, Object dhigh, Object declow, Object dechigh,
      Object nulls, Object dist, Object avglen, Object maxlen, Object trues, Object falses,
      Object avgLong, Object avgDouble, Object avgDecimal, Object sumDist,
      boolean useDensityFunctionForNDVEstimation, double ndvTuner) throws MetaException {
    colType = colType.toLowerCase();
    if (colType.equals("boolean")) {
      BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
      boolStats.setNumFalses(MetaStoreDirectSql.extractSqlLong(falses));
      boolStats.setNumTrues(MetaStoreDirectSql.extractSqlLong(trues));
      boolStats.setNumNulls(MetaStoreDirectSql.extractSqlLong(nulls));
      data.setBooleanStats(boolStats);
    } else if (colType.equals("string") || colType.startsWith("varchar")
        || colType.startsWith("char")) {
      StringColumnStatsDataInspector stringStats = new StringColumnStatsDataInspector();
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
      LongColumnStatsDataInspector longStats = new LongColumnStatsDataInspector();
      longStats.setNumNulls(MetaStoreDirectSql.extractSqlLong(nulls));
      if (lhigh != null) {
        longStats.setHighValue(MetaStoreDirectSql.extractSqlLong(lhigh));
      }
      if (llow != null) {
        longStats.setLowValue(MetaStoreDirectSql.extractSqlLong(llow));
      }
      long lowerBound = MetaStoreDirectSql.extractSqlLong(dist);
      long higherBound = MetaStoreDirectSql.extractSqlLong(sumDist);
      long rangeBound = Long.MAX_VALUE;
      if (lhigh != null && llow != null) {
        rangeBound = MetaStoreDirectSql.extractSqlLong(lhigh)
            - MetaStoreDirectSql.extractSqlLong(llow) + 1;
      }
      long estimation;
      if (useDensityFunctionForNDVEstimation && lhigh != null && llow != null && avgLong != null
          && MetaStoreDirectSql.extractSqlDouble(avgLong) != 0.0) {
        // We have estimation, lowerbound and higherbound. We use estimation if
        // it is between lowerbound and higherbound.
        estimation = MetaStoreDirectSql
            .extractSqlLong((MetaStoreDirectSql.extractSqlLong(lhigh) - MetaStoreDirectSql
                .extractSqlLong(llow)) / MetaStoreDirectSql.extractSqlDouble(avgLong));
        if (estimation < lowerBound) {
          estimation = lowerBound;
        } else if (estimation > higherBound) {
          estimation = higherBound;
        }
      } else {
        estimation = (long) (lowerBound + (higherBound - lowerBound) * ndvTuner);
      }
      estimation = Math.min(estimation, rangeBound);
      longStats.setNumDVs(estimation);
      data.setLongStats(longStats);
    } else if (colType.equals("date")) {
      DateColumnStatsDataInspector dateStats = new DateColumnStatsDataInspector();
      dateStats.setNumNulls(MetaStoreDirectSql.extractSqlLong(nulls));
      if (lhigh != null) {
        dateStats.setHighValue(new Date(MetaStoreDirectSql.extractSqlLong(lhigh)));
      }
      if (llow != null) {
        dateStats.setLowValue(new Date(MetaStoreDirectSql.extractSqlLong(llow)));
      }
      long lowerBound = MetaStoreDirectSql.extractSqlLong(dist);
      long higherBound = MetaStoreDirectSql.extractSqlLong(sumDist);
      long rangeBound = Long.MAX_VALUE;
      if (lhigh != null && llow != null) {
        rangeBound = MetaStoreDirectSql.extractSqlLong(lhigh)
            - MetaStoreDirectSql.extractSqlLong(llow) + 1;
      }
      long estimation;
      if (useDensityFunctionForNDVEstimation && lhigh != null && llow != null && avgLong != null
          && MetaStoreDirectSql.extractSqlDouble(avgLong) != 0.0) {
        // We have estimation, lowerbound and higherbound. We use estimation if
        // it is between lowerbound and higherbound.
        estimation = MetaStoreDirectSql
            .extractSqlLong((MetaStoreDirectSql.extractSqlLong(lhigh) - MetaStoreDirectSql
                .extractSqlLong(llow)) / MetaStoreDirectSql.extractSqlDouble(avgLong));
        if (estimation < lowerBound) {
          estimation = lowerBound;
        } else if (estimation > higherBound) {
          estimation = higherBound;
        }
      } else {
        estimation = (long) (lowerBound + (higherBound - lowerBound) * ndvTuner);
      }
      estimation = Math.min(estimation, rangeBound);
      dateStats.setNumDVs(estimation);
      data.setDateStats(dateStats);
    } else if (colType.equals("double") || colType.equals("float")) {
      DoubleColumnStatsDataInspector doubleStats = new DoubleColumnStatsDataInspector();
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
        doubleStats.setNumDVs((long) (lowerBound + (higherBound - lowerBound) * ndvTuner));
      }
      data.setDoubleStats(doubleStats);
    } else if (colType.startsWith("decimal")) {
      DecimalColumnStatsDataInspector decimalStats = new DecimalColumnStatsDataInspector();
      decimalStats.setNumNulls(MetaStoreDirectSql.extractSqlLong(nulls));
      Decimal low = null;
      Decimal high = null;
      BigDecimal blow = null;
      BigDecimal bhigh = null;
      if (dechigh instanceof BigDecimal) {
        bhigh = (BigDecimal) dechigh;
        high = DecimalUtils.getDecimal(ByteBuffer.wrap(bhigh.unscaledValue().toByteArray()),
            (short) bhigh.scale());
      } else if (dechigh instanceof String) {
        bhigh = new BigDecimal((String) dechigh);
        high = DecimalUtils.createThriftDecimal((String) dechigh);
      }
      decimalStats.setHighValue(high);
      if (declow instanceof BigDecimal) {
        blow = (BigDecimal) declow;
        low = DecimalUtils.getDecimal(ByteBuffer.wrap(blow.unscaledValue().toByteArray()), (short) blow.scale());
      } else if (dechigh instanceof String) {
        blow = new BigDecimal((String) declow);
        low = DecimalUtils.createThriftDecimal((String) declow);
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
        decimalStats.setNumDVs((long) (lowerBound + (higherBound - lowerBound) * ndvTuner));
      }
      data.setDecimalStats(decimalStats);
    }
  }

  /**
   * Set field values in oldStatObj from newStatObj
   * @param oldStatObj
   * @param newStatObj
   */
  public static void setFieldsIntoOldStats(ColumnStatisticsObj oldStatObj,
      ColumnStatisticsObj newStatObj) {
    _Fields typeNew = newStatObj.getStatsData().getSetField();
    _Fields typeOld = oldStatObj.getStatsData().getSetField();
    typeNew = typeNew == typeOld ? typeNew : null;
    switch (typeNew) {
    case BOOLEAN_STATS:
      BooleanColumnStatsData oldBooleanStatsData = oldStatObj.getStatsData().getBooleanStats();
      BooleanColumnStatsData newBooleanStatsData = newStatObj.getStatsData().getBooleanStats();
      if (newBooleanStatsData.isSetNumTrues()) {
        oldBooleanStatsData.setNumTrues(newBooleanStatsData.getNumTrues());
      }
      if (newBooleanStatsData.isSetNumFalses()) {
        oldBooleanStatsData.setNumFalses(newBooleanStatsData.getNumFalses());
      }
      if (newBooleanStatsData.isSetNumNulls()) {
        oldBooleanStatsData.setNumNulls(newBooleanStatsData.getNumNulls());
      }
      if (newBooleanStatsData.isSetBitVectors()) {
        oldBooleanStatsData.setBitVectors(newBooleanStatsData.getBitVectors());
      }
      break;
    case LONG_STATS: {
      LongColumnStatsData oldLongStatsData = oldStatObj.getStatsData().getLongStats();
      LongColumnStatsData newLongStatsData = newStatObj.getStatsData().getLongStats();
      if (newLongStatsData.isSetHighValue()) {
        oldLongStatsData.setHighValue(newLongStatsData.getHighValue());
      }
      if (newLongStatsData.isSetLowValue()) {
        oldLongStatsData.setLowValue(newLongStatsData.getLowValue());
      }
      if (newLongStatsData.isSetNumNulls()) {
        oldLongStatsData.setNumNulls(newLongStatsData.getNumNulls());
      }
      if (newLongStatsData.isSetNumDVs()) {
        oldLongStatsData.setNumDVs(newLongStatsData.getNumDVs());
      }
      if (newLongStatsData.isSetBitVectors()) {
        oldLongStatsData.setBitVectors(newLongStatsData.getBitVectors());
      }
      break;
    }
    case DOUBLE_STATS: {
      DoubleColumnStatsData oldDoubleStatsData = oldStatObj.getStatsData().getDoubleStats();
      DoubleColumnStatsData newDoubleStatsData = newStatObj.getStatsData().getDoubleStats();
      if (newDoubleStatsData.isSetHighValue()) {
        oldDoubleStatsData.setHighValue(newDoubleStatsData.getHighValue());
      }
      if (newDoubleStatsData.isSetLowValue()) {
        oldDoubleStatsData.setLowValue(newDoubleStatsData.getLowValue());
      }
      if (newDoubleStatsData.isSetNumNulls()) {
        oldDoubleStatsData.setNumNulls(newDoubleStatsData.getNumNulls());
      }
      if (newDoubleStatsData.isSetNumDVs()) {
        oldDoubleStatsData.setNumDVs(newDoubleStatsData.getNumDVs());
      }
      if (newDoubleStatsData.isSetBitVectors()) {
        oldDoubleStatsData.setBitVectors(newDoubleStatsData.getBitVectors());
      }
      break;
    }
    case STRING_STATS: {
      StringColumnStatsData oldStringStatsData = oldStatObj.getStatsData().getStringStats();
      StringColumnStatsData newStringStatsData = newStatObj.getStatsData().getStringStats();
      if (newStringStatsData.isSetMaxColLen()) {
        oldStringStatsData.setMaxColLen(newStringStatsData.getMaxColLen());
      }
      if (newStringStatsData.isSetAvgColLen()) {
        oldStringStatsData.setAvgColLen(newStringStatsData.getAvgColLen());
      }
      if (newStringStatsData.isSetNumNulls()) {
        oldStringStatsData.setNumNulls(newStringStatsData.getNumNulls());
      }
      if (newStringStatsData.isSetNumDVs()) {
        oldStringStatsData.setNumDVs(newStringStatsData.getNumDVs());
      }
      if (newStringStatsData.isSetBitVectors()) {
        oldStringStatsData.setBitVectors(newStringStatsData.getBitVectors());
      }
      break;
    }
    case BINARY_STATS:
      BinaryColumnStatsData oldBinaryStatsData = oldStatObj.getStatsData().getBinaryStats();
      BinaryColumnStatsData newBinaryStatsData = newStatObj.getStatsData().getBinaryStats();
      if (newBinaryStatsData.isSetMaxColLen()) {
        oldBinaryStatsData.setMaxColLen(newBinaryStatsData.getMaxColLen());
      }
      if (newBinaryStatsData.isSetAvgColLen()) {
        oldBinaryStatsData.setAvgColLen(newBinaryStatsData.getAvgColLen());
      }
      if (newBinaryStatsData.isSetNumNulls()) {
        oldBinaryStatsData.setNumNulls(newBinaryStatsData.getNumNulls());
      }
      if (newBinaryStatsData.isSetBitVectors()) {
        oldBinaryStatsData.setBitVectors(newBinaryStatsData.getBitVectors());
      }
      break;
    case DECIMAL_STATS: {
      DecimalColumnStatsData oldDecimalStatsData = oldStatObj.getStatsData().getDecimalStats();
      DecimalColumnStatsData newDecimalStatsData = newStatObj.getStatsData().getDecimalStats();
      if (newDecimalStatsData.isSetHighValue()) {
        oldDecimalStatsData.setHighValue(newDecimalStatsData.getHighValue());
      }
      if (newDecimalStatsData.isSetLowValue()) {
        oldDecimalStatsData.setLowValue(newDecimalStatsData.getLowValue());
      }
      if (newDecimalStatsData.isSetNumNulls()) {
        oldDecimalStatsData.setNumNulls(newDecimalStatsData.getNumNulls());
      }
      if (newDecimalStatsData.isSetNumDVs()) {
        oldDecimalStatsData.setNumDVs(newDecimalStatsData.getNumDVs());
      }
      if (newDecimalStatsData.isSetBitVectors()) {
        oldDecimalStatsData.setBitVectors(newDecimalStatsData.getBitVectors());
      }
      break;
    }
    case DATE_STATS: {
      DateColumnStatsData oldDateStatsData = oldStatObj.getStatsData().getDateStats();
      DateColumnStatsData newDateStatsData = newStatObj.getStatsData().getDateStats();
      if (newDateStatsData.isSetHighValue()) {
        oldDateStatsData.setHighValue(newDateStatsData.getHighValue());
      }
      if (newDateStatsData.isSetLowValue()) {
        oldDateStatsData.setLowValue(newDateStatsData.getLowValue());
      }
      if (newDateStatsData.isSetNumNulls()) {
        oldDateStatsData.setNumNulls(newDateStatsData.getNumNulls());
      }
      if (newDateStatsData.isSetNumDVs()) {
        oldDateStatsData.setNumDVs(newDateStatsData.getNumDVs());
      }
      if (newDateStatsData.isSetBitVectors()) {
        oldDateStatsData.setBitVectors(newDateStatsData.getBitVectors());
      }
      break;
    }
    default:
      throw new IllegalArgumentException("Unknown stats type: " + typeNew.toString());
    }
  }
}
