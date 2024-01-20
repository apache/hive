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
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;

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
import org.apache.hadoop.hive.metastore.api.Timestamp;
import org.apache.hadoop.hive.metastore.api.TimestampColumnStatsData;
import org.apache.hadoop.hive.metastore.api.utils.DecimalUtils;
import org.apache.hadoop.hive.metastore.columnstats.cache.DateColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DecimalColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DoubleColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.LongColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.StringColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.TimestampColumnStatsDataInspector;
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
      ColumnStatisticsDesc statsDesc, ColumnStatisticsObj statsObj, String engine)
          throws NoSuchObjectException, MetaException, InvalidObjectException {
     if (statsObj == null || statsDesc == null) {
       throw new InvalidObjectException("Invalid column stats object");
     }

     MTableColumnStatistics mColStats = new MTableColumnStatistics();
     mColStats.setTable(table);
     mColStats.setDbName(statsDesc.getDbName());
     mColStats.setCatName(table.getDatabase().getCatalogName());
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
           longStats.isSetHistogram() ? longStats.getHistogram() : null,
           longStats.isSetLowValue() ? longStats.getLowValue() : null,
           longStats.isSetHighValue() ? longStats.getHighValue() : null);
     } else if (statsObj.getStatsData().isSetDoubleStats()) {
       DoubleColumnStatsData doubleStats = statsObj.getStatsData().getDoubleStats();
       mColStats.setDoubleStats(
           doubleStats.isSetNumNulls() ? doubleStats.getNumNulls() : null,
           doubleStats.isSetNumDVs() ? doubleStats.getNumDVs() : null,
           doubleStats.isSetBitVectors() ? doubleStats.getBitVectors() : null,
           doubleStats.isSetHistogram() ? doubleStats.getHistogram() : null,
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
           decimalStats.isSetHistogram() ? decimalStats.getHistogram() : null,
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
           dateStats.isSetHistogram() ? dateStats.getHistogram() : null,
           dateStats.isSetLowValue() ? dateStats.getLowValue().getDaysSinceEpoch() : null,
           dateStats.isSetHighValue() ? dateStats.getHighValue().getDaysSinceEpoch() : null);
     } else if (statsObj.getStatsData().isSetTimestampStats()) {
       TimestampColumnStatsData timestampStats = statsObj.getStatsData().getTimestampStats();
       mColStats.setTimestampStats(
           timestampStats.isSetNumNulls() ? timestampStats.getNumNulls() : null,
           timestampStats.isSetNumDVs() ? timestampStats.getNumDVs() : null,
           timestampStats.isSetBitVectors() ? timestampStats.getBitVectors() : null,
           timestampStats.isSetHistogram() ? timestampStats.getHistogram() : null,
           timestampStats.isSetLowValue() ? timestampStats.getLowValue().getSecondsSinceEpoch() : null,
           timestampStats.isSetHighValue() ? timestampStats.getHighValue().getSecondsSinceEpoch() : null);
     }
     mColStats.setEngine(engine);
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
    if (mStatsObj.getHistogram() != null) {
      oldStatsObj.setHistogram(mStatsObj.getHistogram());
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
    oldStatsObj.setEngine(mStatsObj.getEngine());
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
    if (mStatsObj.getHistogram() != null) {
      oldStatsObj.setHistogram(mStatsObj.getHistogram());
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
    oldStatsObj.setEngine(mStatsObj.getEngine());
  }

  public static String getUpdatedColumnSql(MPartitionColumnStatistics mStatsObj) {
    StringBuilder setStmt = new StringBuilder();
    if (mStatsObj.getAvgColLen() != null) {
      setStmt.append("\"AVG_COL_LEN\" = ? ,");
    }
    if (mStatsObj.getLongHighValue() != null) {
      setStmt.append("\"LONG_HIGH_VALUE\" = ? ,");
    }
    if (mStatsObj.getDoubleHighValue() != null) {
      setStmt.append("\"DOUBLE_HIGH_VALUE\" = ? ,");
    }
    setStmt.append("\"LAST_ANALYZED\" = ? ,");
    if (mStatsObj.getLongLowValue() != null) {
      setStmt.append("\"LONG_LOW_VALUE\" = ? ,");
    }
    if (mStatsObj.getDoubleLowValue() != null) {
      setStmt.append("\"DOUBLE_LOW_VALUE\" = ? ,");
    }
    if (mStatsObj.getDecimalLowValue() != null) {
      setStmt.append("\"BIG_DECIMAL_LOW_VALUE\" = ? ,");
    }
    if (mStatsObj.getDecimalHighValue() != null) {
      setStmt.append("\"BIG_DECIMAL_HIGH_VALUE\" = ? ,");
    }
    if (mStatsObj.getMaxColLen() != null) {
      setStmt.append("\"MAX_COL_LEN\" = ? ,");
    }
    if (mStatsObj.getNumDVs() != null) {
      setStmt.append("\"NUM_DISTINCTS\" = ? ,");
    }
    if (mStatsObj.getBitVector() != null) {
      setStmt.append("\"BIT_VECTOR\" = ? ,");
    }
    if (mStatsObj.getHistogram() != null) {
      setStmt.append("\"HISTOGRAM\" = ? ,");
    }
    if (mStatsObj.getNumFalses() != null) {
      setStmt.append("\"NUM_FALSES\" = ? ,");
    }
    if (mStatsObj.getNumTrues() != null) {
      setStmt.append(" \"NUM_TRUES\" = ? ,");
    }
    if (mStatsObj.getNumNulls() != null) {
      setStmt.append("\"NUM_NULLS\" = ? ,");
    }
    setStmt.append("\"ENGINE\" = ? ,");
    setStmt.append("\"DB_NAME\" = ? ,");
    setStmt.append("\"TABLE_NAME\" = ? ");
    return setStmt.toString();
  }

  public static int initUpdatedColumnStatement(MPartitionColumnStatistics mStatsObj,
                                                      PreparedStatement pst) throws SQLException {
    int colIdx = 1;
    if (mStatsObj.getAvgColLen() != null) {
      pst.setObject(colIdx++, mStatsObj.getAvgColLen());
    }
    if (mStatsObj.getLongHighValue() != null) {
      pst.setObject(colIdx++, mStatsObj.getLongHighValue());
    }
    if (mStatsObj.getDoubleHighValue() != null) {
      pst.setObject(colIdx++, mStatsObj.getDoubleHighValue());
    }
    pst.setLong(colIdx++, mStatsObj.getLastAnalyzed());
    if (mStatsObj.getLongLowValue() != null) {
      pst.setObject(colIdx++, mStatsObj.getLongLowValue());
    }
    if (mStatsObj.getDoubleLowValue() != null) {
      pst.setObject(colIdx++, mStatsObj.getDoubleLowValue());
    }
    if (mStatsObj.getDecimalLowValue() != null) {
      pst.setObject(colIdx++, mStatsObj.getDecimalLowValue());
    }
    if (mStatsObj.getDecimalHighValue() != null) {
      pst.setObject(colIdx++, mStatsObj.getDecimalHighValue());
    }
    if (mStatsObj.getMaxColLen() != null) {
      pst.setObject(colIdx++, mStatsObj.getMaxColLen());
    }
    if (mStatsObj.getNumDVs() != null) {
      pst.setObject(colIdx++, mStatsObj.getNumDVs());
    }
    if (mStatsObj.getBitVector() != null) {
      pst.setObject(colIdx++, mStatsObj.getBitVector());
    }
    if (mStatsObj.getHistogram() != null) {
      pst.setObject(colIdx++, mStatsObj.getHistogram());
    }
    if (mStatsObj.getNumFalses() != null) {
      pst.setObject(colIdx++, mStatsObj.getNumFalses());
    }
    if (mStatsObj.getNumTrues() != null) {
      pst.setObject(colIdx++, mStatsObj.getNumTrues());
    }
    if (mStatsObj.getNumNulls() != null) {
      pst.setObject(colIdx++, mStatsObj.getNumNulls());
    }
    pst.setString(colIdx++, mStatsObj.getEngine());
    pst.setString(colIdx++, mStatsObj.getDbName());
    pst.setString(colIdx++, mStatsObj.getTableName());
    return colIdx;
  }

  public static ColumnStatisticsObj getTableColumnStatisticsObj(
      MTableColumnStatistics mStatsObj, boolean enableBitVector, boolean enableKll) {
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
        colType.equals("smallint") || colType.equals("tinyint")) {
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
      longStats.setHistogram((mStatsObj.getHistogram()==null||!enableKll)? null : mStatsObj.getHistogram());
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
      doubleStats.setHistogram((mStatsObj.getHistogram()==null||!enableKll)? null : mStatsObj.getHistogram());
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
      decimalStats.setHistogram((mStatsObj.getHistogram()==null||!enableKll)? null : mStatsObj.getHistogram());
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
      dateStats.setHistogram((mStatsObj.getHistogram()==null||!enableKll)? null : mStatsObj.getHistogram());
      colStatsData.setDateStats(dateStats);
    } else if (colType.equals("timestamp")) {
      TimestampColumnStatsDataInspector timestampStats = new TimestampColumnStatsDataInspector();
      timestampStats.setNumNulls(mStatsObj.getNumNulls());
      Long highValue = mStatsObj.getLongHighValue();
      if (highValue != null) {
        timestampStats.setHighValue(new Timestamp(highValue));
      }
      Long lowValue = mStatsObj.getLongLowValue();
      if (lowValue != null) {
        timestampStats.setLowValue(new Timestamp(lowValue));
      }
      timestampStats.setNumDVs(mStatsObj.getNumDVs());
      timestampStats.setBitVectors((mStatsObj.getBitVector()==null||!enableBitVector)? null : mStatsObj.getBitVector());
      timestampStats.setHistogram((mStatsObj.getHistogram()==null||!enableKll)? null : mStatsObj.getHistogram());
      colStatsData.setTimestampStats(timestampStats);
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
      MPartition partition, ColumnStatisticsDesc statsDesc, ColumnStatisticsObj statsObj, String engine)
          throws MetaException, NoSuchObjectException {
    if (statsDesc == null || statsObj == null) {
      return null;
    }

    MPartitionColumnStatistics mColStats = new MPartitionColumnStatistics();
    if (partition != null) {
      mColStats.setCatName(partition.getTable().getDatabase().getCatalogName());
      mColStats.setPartition(partition);
    } else {
      // Assume that the statsDesc has already set catalogName when partition is null
      mColStats.setCatName(statsDesc.getCatName());
    }
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
          longStats.isSetHistogram() ? longStats.getHistogram() : null,
          longStats.isSetLowValue() ? longStats.getLowValue() : null,
          longStats.isSetHighValue() ? longStats.getHighValue() : null);
    } else if (statsObj.getStatsData().isSetDoubleStats()) {
      DoubleColumnStatsData doubleStats = statsObj.getStatsData().getDoubleStats();
      mColStats.setDoubleStats(
          doubleStats.isSetNumNulls() ? doubleStats.getNumNulls() : null,
          doubleStats.isSetNumDVs() ? doubleStats.getNumDVs() : null,
          doubleStats.isSetBitVectors() ? doubleStats.getBitVectors() : null,
          doubleStats.isSetHistogram() ? doubleStats.getHistogram() : null,
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
          decimalStats.isSetHistogram() ? decimalStats.getHistogram() : null,
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
          dateStats.isSetHistogram() ? dateStats.getHistogram() : null,
          dateStats.isSetLowValue() ? dateStats.getLowValue().getDaysSinceEpoch() : null,
          dateStats.isSetHighValue() ? dateStats.getHighValue().getDaysSinceEpoch() : null);
    } else if (statsObj.getStatsData().isSetTimestampStats()) {
      TimestampColumnStatsData timestampStats = statsObj.getStatsData().getTimestampStats();
      mColStats.setTimestampStats(
          timestampStats.isSetNumNulls() ? timestampStats.getNumNulls() : null,
          timestampStats.isSetNumDVs() ? timestampStats.getNumDVs() : null,
          timestampStats.isSetBitVectors() ? timestampStats.getBitVectors() : null,
          timestampStats.isSetHistogram() ? timestampStats.getHistogram() : null,
          timestampStats.isSetLowValue() ? timestampStats.getLowValue().getSecondsSinceEpoch() : null,
          timestampStats.isSetHighValue() ? timestampStats.getHighValue().getSecondsSinceEpoch() : null);
    }
    mColStats.setEngine(engine);
    return mColStats;
  }

  public static ColumnStatisticsObj getPartitionColumnStatisticsObj(
      MPartitionColumnStatistics mStatsObj, boolean enableBitVector, boolean enableKll) {
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
        colType.equals("int") || colType.equals("bigint")) {
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
      longStats.setHistogram((mStatsObj.getHistogram()==null||!enableKll)? null : mStatsObj.getHistogram());
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
      doubleStats.setHistogram((mStatsObj.getHistogram()==null||!enableKll)? null : mStatsObj.getHistogram());
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
      decimalStats.setHistogram((mStatsObj.getHistogram()==null||!enableKll)? null : mStatsObj.getHistogram());
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
      dateStats.setHistogram((mStatsObj.getHistogram()==null||!enableKll)? null : mStatsObj.getHistogram());
      colStatsData.setDateStats(dateStats);
    } else if (colType.equals("timestamp")) {
      TimestampColumnStatsDataInspector timestampStats = new TimestampColumnStatsDataInspector();
      timestampStats.setNumNulls(mStatsObj.getNumNulls());
      Long highValue = mStatsObj.getLongHighValue();
      if (highValue != null) {
        timestampStats.setHighValue(new Timestamp(highValue));
      }
      Long lowValue = mStatsObj.getLongLowValue();
      if (lowValue != null) {
        timestampStats.setLowValue(new Timestamp(lowValue));
      }
      timestampStats.setNumDVs(mStatsObj.getNumDVs());
      timestampStats.setBitVectors((mStatsObj.getBitVector()==null||!enableBitVector)? null : mStatsObj.getBitVector());
      timestampStats.setHistogram((mStatsObj.getHistogram()==null||!enableKll)? null : mStatsObj.getHistogram());
      colStatsData.setTimestampStats(timestampStats);
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

  public static byte[] getBitVector(byte[] bytes) {
    // workaround for DN bug in persisting nulls in pg bytea column
    // instead set empty bit vector with header.
    // https://issues.apache.org/jira/browse/HIVE-17836
    if (bytes != null && bytes.length == 2 && bytes[0] == 'H' && bytes[1] == 'L') {
      return null;
    }
    return bytes;
  }

  public static byte[] getHistogram(byte[] bytes) {
    // workaround for DN bug in persisting nulls in pg bytea column
    // instead set empty bit vector with header.
    // https://issues.apache.org/jira/browse/HIVE-17836
    if (bytes != null && bytes.length == 0) {
      return null;
    }
    return bytes;
  }

  // JAVA
  public static void fillColumnStatisticsData(String colType, ColumnStatisticsData data,
      Object llow, Object lhigh, Object dlow, Object dhigh, Object declow, Object dechigh,
      Object nulls, Object dist, Object bitVector, Object histogram,
      Object avglen, Object maxlen, Object trues, Object falses) throws MetaException {
    colType = colType.toLowerCase();
    if (colType.equals("boolean")) {
      BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
      boolStats.setNumFalses(MetastoreDirectSqlUtils.extractSqlLong(falses));
      boolStats.setNumTrues(MetastoreDirectSqlUtils.extractSqlLong(trues));
      boolStats.setNumNulls(MetastoreDirectSqlUtils.extractSqlLong(nulls));
      data.setBooleanStats(boolStats);
    } else if (colType.equals("string") ||
        colType.startsWith("varchar") || colType.startsWith("char")) {
      StringColumnStatsDataInspector stringStats = new StringColumnStatsDataInspector();
      stringStats.setNumNulls(MetastoreDirectSqlUtils.extractSqlLong(nulls));
      stringStats.setAvgColLen(MetastoreDirectSqlUtils.extractSqlDouble(avglen));
      stringStats.setMaxColLen(MetastoreDirectSqlUtils.extractSqlLong(maxlen));
      stringStats.setNumDVs(MetastoreDirectSqlUtils.extractSqlLong(dist));
      stringStats.setBitVectors(getBitVector(MetastoreDirectSqlUtils.extractSqlBlob(bitVector)));
      data.setStringStats(stringStats);
    } else if (colType.equals("binary")) {
      BinaryColumnStatsData binaryStats = new BinaryColumnStatsData();
      binaryStats.setNumNulls(MetastoreDirectSqlUtils.extractSqlLong(nulls));
      binaryStats.setAvgColLen(MetastoreDirectSqlUtils.extractSqlDouble(avglen));
      binaryStats.setMaxColLen(MetastoreDirectSqlUtils.extractSqlLong(maxlen));
      data.setBinaryStats(binaryStats);
    } else if (colType.equals("bigint") || colType.equals("int") ||
        colType.equals("smallint") || colType.equals("tinyint")) {
      LongColumnStatsDataInspector longStats = new LongColumnStatsDataInspector();
      longStats.setNumNulls(MetastoreDirectSqlUtils.extractSqlLong(nulls));
      if (lhigh != null) {
        longStats.setHighValue(MetastoreDirectSqlUtils.extractSqlLong(lhigh));
      }
      if (llow != null) {
        longStats.setLowValue(MetastoreDirectSqlUtils.extractSqlLong(llow));
      }
      longStats.setNumDVs(MetastoreDirectSqlUtils.extractSqlLong(dist));
      longStats.setBitVectors(getBitVector(MetastoreDirectSqlUtils.extractSqlBlob(bitVector)));
      longStats.setHistogram(getHistogram(MetastoreDirectSqlUtils.extractSqlBlob(histogram)));
      data.setLongStats(longStats);
    } else if (colType.equals("double") || colType.equals("float")) {
      DoubleColumnStatsDataInspector doubleStats = new DoubleColumnStatsDataInspector();
      doubleStats.setNumNulls(MetastoreDirectSqlUtils.extractSqlLong(nulls));
      if (dhigh != null) {
        doubleStats.setHighValue(MetastoreDirectSqlUtils.extractSqlDouble(dhigh));
      }
      if (dlow != null) {
        doubleStats.setLowValue(MetastoreDirectSqlUtils.extractSqlDouble(dlow));
      }
      doubleStats.setNumDVs(MetastoreDirectSqlUtils.extractSqlLong(dist));
      doubleStats.setBitVectors(getBitVector(MetastoreDirectSqlUtils.extractSqlBlob(bitVector)));
      doubleStats.setHistogram(getHistogram(MetastoreDirectSqlUtils.extractSqlBlob(histogram)));
      data.setDoubleStats(doubleStats);
    } else if (colType.startsWith("decimal")) {
      DecimalColumnStatsDataInspector decimalStats = new DecimalColumnStatsDataInspector();
      decimalStats.setNumNulls(MetastoreDirectSqlUtils.extractSqlLong(nulls));
      if (dechigh != null) {
        decimalStats.setHighValue(DecimalUtils.createThriftDecimal((String)dechigh));
      }
      if (declow != null) {
        decimalStats.setLowValue(DecimalUtils.createThriftDecimal((String)declow));
      }
      decimalStats.setNumDVs(MetastoreDirectSqlUtils.extractSqlLong(dist));
      decimalStats.setBitVectors(getBitVector(MetastoreDirectSqlUtils.extractSqlBlob(bitVector)));
      decimalStats.setHistogram(getHistogram(MetastoreDirectSqlUtils.extractSqlBlob(histogram)));
      data.setDecimalStats(decimalStats);
    } else if (colType.equals("date")) {
      DateColumnStatsDataInspector dateStats = new DateColumnStatsDataInspector();
      dateStats.setNumNulls(MetastoreDirectSqlUtils.extractSqlLong(nulls));
      if (lhigh != null) {
        dateStats.setHighValue(new Date(MetastoreDirectSqlUtils.extractSqlLong(lhigh)));
      }
      if (llow != null) {
        dateStats.setLowValue(new Date(MetastoreDirectSqlUtils.extractSqlLong(llow)));
      }
      dateStats.setNumDVs(MetastoreDirectSqlUtils.extractSqlLong(dist));
      dateStats.setBitVectors(getBitVector(MetastoreDirectSqlUtils.extractSqlBlob(bitVector)));
      dateStats.setHistogram(getHistogram(MetastoreDirectSqlUtils.extractSqlBlob(histogram)));
      data.setDateStats(dateStats);
    } else if (colType.equals("timestamp")) {
      TimestampColumnStatsDataInspector timestampStats = new TimestampColumnStatsDataInspector();
      timestampStats.setNumNulls(MetastoreDirectSqlUtils.extractSqlLong(nulls));
      if (lhigh != null) {
        timestampStats.setHighValue(new Timestamp(MetastoreDirectSqlUtils.extractSqlLong(lhigh)));
      }
      if (llow != null) {
        timestampStats.setLowValue(new Timestamp(MetastoreDirectSqlUtils.extractSqlLong(llow)));
      }
      timestampStats.setNumDVs(MetastoreDirectSqlUtils.extractSqlLong(dist));
      timestampStats.setBitVectors(getBitVector(MetastoreDirectSqlUtils.extractSqlBlob(bitVector)));
      timestampStats.setHistogram(getHistogram(MetastoreDirectSqlUtils.extractSqlBlob(histogram)));
      data.setTimestampStats(timestampStats);
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
      boolStats.setNumFalses(MetastoreDirectSqlUtils.extractSqlLong(falses));
      boolStats.setNumTrues(MetastoreDirectSqlUtils.extractSqlLong(trues));
      boolStats.setNumNulls(MetastoreDirectSqlUtils.extractSqlLong(nulls));
      data.setBooleanStats(boolStats);
    } else if (colType.equals("string") || colType.startsWith("varchar")
        || colType.startsWith("char")) {
      StringColumnStatsDataInspector stringStats = new StringColumnStatsDataInspector();
      stringStats.setNumNulls(MetastoreDirectSqlUtils.extractSqlLong(nulls));
      stringStats.setAvgColLen(MetastoreDirectSqlUtils.extractSqlDouble(avglen));
      stringStats.setMaxColLen(MetastoreDirectSqlUtils.extractSqlLong(maxlen));
      stringStats.setNumDVs(MetastoreDirectSqlUtils.extractSqlLong(dist));
      data.setStringStats(stringStats);
    } else if (colType.equals("binary")) {
      BinaryColumnStatsData binaryStats = new BinaryColumnStatsData();
      binaryStats.setNumNulls(MetastoreDirectSqlUtils.extractSqlLong(nulls));
      binaryStats.setAvgColLen(MetastoreDirectSqlUtils.extractSqlDouble(avglen));
      binaryStats.setMaxColLen(MetastoreDirectSqlUtils.extractSqlLong(maxlen));
      data.setBinaryStats(binaryStats);
    } else if (colType.equals("bigint") || colType.equals("int") || colType.equals("smallint")
        || colType.equals("tinyint")) {
      LongColumnStatsDataInspector longStats = new LongColumnStatsDataInspector();
      longStats.setNumNulls(MetastoreDirectSqlUtils.extractSqlLong(nulls));
      if (lhigh != null) {
        longStats.setHighValue(MetastoreDirectSqlUtils.extractSqlLong(lhigh));
      }
      if (llow != null) {
        longStats.setLowValue(MetastoreDirectSqlUtils.extractSqlLong(llow));
      }
      long lowerBound = MetastoreDirectSqlUtils.extractSqlLong(dist);
      long higherBound = MetastoreDirectSqlUtils.extractSqlLong(sumDist);
      long rangeBound = Long.MAX_VALUE;
      if (lhigh != null && llow != null) {
        rangeBound = MetastoreDirectSqlUtils.extractSqlLong(lhigh)
            - MetastoreDirectSqlUtils.extractSqlLong(llow) + 1;
      }
      long estimation;
      if (useDensityFunctionForNDVEstimation && lhigh != null && llow != null && avgLong != null
          && MetastoreDirectSqlUtils.extractSqlDouble(avgLong) != 0.0) {
        // We have estimation, lowerbound and higherbound. We use estimation if
        // it is between lowerbound and higherbound.
        estimation = MetastoreDirectSqlUtils
            .extractSqlLong((MetastoreDirectSqlUtils.extractSqlLong(lhigh) - MetastoreDirectSqlUtils
                .extractSqlLong(llow)) / MetastoreDirectSqlUtils.extractSqlDouble(avgLong));
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
      dateStats.setNumNulls(MetastoreDirectSqlUtils.extractSqlLong(nulls));
      if (lhigh != null) {
        dateStats.setHighValue(new Date(MetastoreDirectSqlUtils.extractSqlLong(lhigh)));
      }
      if (llow != null) {
        dateStats.setLowValue(new Date(MetastoreDirectSqlUtils.extractSqlLong(llow)));
      }
      long lowerBound = MetastoreDirectSqlUtils.extractSqlLong(dist);
      long higherBound = MetastoreDirectSqlUtils.extractSqlLong(sumDist);
      long rangeBound = Long.MAX_VALUE;
      if (lhigh != null && llow != null) {
        rangeBound = MetastoreDirectSqlUtils.extractSqlLong(lhigh)
            - MetastoreDirectSqlUtils.extractSqlLong(llow) + 1;
      }
      long estimation;
      if (useDensityFunctionForNDVEstimation && lhigh != null && llow != null && avgLong != null
          && MetastoreDirectSqlUtils.extractSqlDouble(avgLong) != 0.0) {
        // We have estimation, lowerbound and higherbound. We use estimation if
        // it is between lowerbound and higherbound.
        estimation = MetastoreDirectSqlUtils
            .extractSqlLong((MetastoreDirectSqlUtils.extractSqlLong(lhigh) - MetastoreDirectSqlUtils
                .extractSqlLong(llow)) / MetastoreDirectSqlUtils.extractSqlDouble(avgLong));
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
    } else if (colType.equals("timestamp")) {
      TimestampColumnStatsDataInspector timestampStats = new TimestampColumnStatsDataInspector();
      timestampStats.setNumNulls(MetastoreDirectSqlUtils.extractSqlLong(nulls));
      if (lhigh != null) {
        timestampStats.setHighValue(new Timestamp(MetastoreDirectSqlUtils.extractSqlLong(lhigh)));
      }
      if (llow != null) {
        timestampStats.setLowValue(new Timestamp(MetastoreDirectSqlUtils.extractSqlLong(llow)));
      }
      long lowerBound = MetastoreDirectSqlUtils.extractSqlLong(dist);
      long higherBound = MetastoreDirectSqlUtils.extractSqlLong(sumDist);
      long rangeBound = Long.MAX_VALUE;
      if (lhigh != null && llow != null) {
        rangeBound = MetastoreDirectSqlUtils.extractSqlLong(lhigh)
            - MetastoreDirectSqlUtils.extractSqlLong(llow) + 1;
      }
      long estimation;
      if (useDensityFunctionForNDVEstimation && lhigh != null && llow != null && avgLong != null
          && MetastoreDirectSqlUtils.extractSqlDouble(avgLong) != 0.0) {
        // We have estimation, lowerbound and higherbound. We use estimation if
        // it is between lowerbound and higherbound.
        estimation = MetastoreDirectSqlUtils
            .extractSqlLong((MetastoreDirectSqlUtils.extractSqlLong(lhigh) - MetastoreDirectSqlUtils
                .extractSqlLong(llow)) / MetastoreDirectSqlUtils.extractSqlDouble(avgLong));
        if (estimation < lowerBound) {
          estimation = lowerBound;
        } else if (estimation > higherBound) {
          estimation = higherBound;
        }
      } else {
        estimation = (long) (lowerBound + (higherBound - lowerBound) * ndvTuner);
      }
      estimation = Math.min(estimation, rangeBound);
      timestampStats.setNumDVs(estimation);
      data.setTimestampStats(timestampStats);
    } else if (colType.equals("double") || colType.equals("float")) {
      DoubleColumnStatsDataInspector doubleStats = new DoubleColumnStatsDataInspector();
      doubleStats.setNumNulls(MetastoreDirectSqlUtils.extractSqlLong(nulls));
      if (dhigh != null) {
        doubleStats.setHighValue(MetastoreDirectSqlUtils.extractSqlDouble(dhigh));
      }
      if (dlow != null) {
        doubleStats.setLowValue(MetastoreDirectSqlUtils.extractSqlDouble(dlow));
      }
      long lowerBound = MetastoreDirectSqlUtils.extractSqlLong(dist);
      long higherBound = MetastoreDirectSqlUtils.extractSqlLong(sumDist);
      if (useDensityFunctionForNDVEstimation && dhigh != null && dlow != null && avgDouble != null
          && MetastoreDirectSqlUtils.extractSqlDouble(avgDouble) != 0.0) {
        long estimation = MetastoreDirectSqlUtils
            .extractSqlLong((MetastoreDirectSqlUtils.extractSqlLong(dhigh) - MetastoreDirectSqlUtils
                .extractSqlLong(dlow)) / MetastoreDirectSqlUtils.extractSqlDouble(avgDouble));
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
      decimalStats.setNumNulls(MetastoreDirectSqlUtils.extractSqlLong(nulls));
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
      long lowerBound = MetastoreDirectSqlUtils.extractSqlLong(dist);
      long higherBound = MetastoreDirectSqlUtils.extractSqlLong(sumDist);
      if (useDensityFunctionForNDVEstimation && dechigh != null && declow != null && avgDecimal != null
          && MetastoreDirectSqlUtils.extractSqlDouble(avgDecimal) != 0.0) {
        long estimation = MetastoreDirectSqlUtils.extractSqlLong(MetastoreDirectSqlUtils.extractSqlLong(bhigh
            .subtract(blow).floatValue() / MetastoreDirectSqlUtils.extractSqlDouble(avgDecimal)));
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
      if (newLongStatsData.isSetHistogram()) {
        newLongStatsData.setHistogram(newLongStatsData.getHistogram());
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
      if (newDoubleStatsData.isSetHistogram()) {
        oldDoubleStatsData.setHistogram(newDoubleStatsData.getHistogram());
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
      if (newDecimalStatsData.isSetHistogram()) {
        oldDecimalStatsData.setHistogram(newDecimalStatsData.getHistogram());
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
      if (newDateStatsData.isSetHistogram()) {
        oldDateStatsData.setHistogram(newDateStatsData.getHistogram());
      }
      break;
    }
    case TIMESTAMP_STATS: {
      TimestampColumnStatsData oldTimestampStatsData = oldStatObj.getStatsData().getTimestampStats();
      TimestampColumnStatsData newTimestampStatsData = newStatObj.getStatsData().getTimestampStats();
      if (newTimestampStatsData.isSetHighValue()) {
        oldTimestampStatsData.setHighValue(newTimestampStatsData.getHighValue());
      }
      if (newTimestampStatsData.isSetLowValue()) {
        oldTimestampStatsData.setLowValue(newTimestampStatsData.getLowValue());
      }
      if (newTimestampStatsData.isSetNumNulls()) {
        oldTimestampStatsData.setNumNulls(newTimestampStatsData.getNumNulls());
      }
      if (newTimestampStatsData.isSetNumDVs()) {
        oldTimestampStatsData.setNumDVs(newTimestampStatsData.getNumDVs());
      }
      if (newTimestampStatsData.isSetBitVectors()) {
        oldTimestampStatsData.setBitVectors(newTimestampStatsData.getBitVectors());
      }
      if (newTimestampStatsData.isSetHistogram()) {
        oldTimestampStatsData.setHistogram(newTimestampStatsData.getHistogram());
      }
      break;
    }
    default:
      throw new IllegalArgumentException("Unknown stats type: " + typeNew.toString());
    }
  }
}
