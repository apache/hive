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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.Timestamp;
import org.apache.hadoop.hive.metastore.api.utils.DecimalUtils;
import org.apache.hadoop.hive.metastore.columnstats.cache.DateColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DecimalColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DoubleColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.LongColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.StringColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.TimestampColumnStatsDataInspector;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ColumnStatsUpdateWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ColumnStatsUpdateTask implementation. For example, ALTER TABLE src_stat
 * UPDATE STATISTICS for column key SET ('numDVs'='1111','avgColLen'='1.111');
 * For another example, ALTER TABLE src_stat_part PARTITION(partitionId=100)
 * UPDATE STATISTICS for column value SET
 * ('maxColLen'='4444','avgColLen'='44.4');
 **/

public class ColumnStatsUpdateTask extends Task<ColumnStatsUpdateWork> {
  private static final long serialVersionUID = 1L;
  private static transient final Logger LOG = LoggerFactory
      .getLogger(ColumnStatsUpdateTask.class);

  private ColumnStatistics constructColumnStatsFromInput()
      throws SemanticException, MetaException {

    // If we are replicating the stats, we don't need to construct those again.
    if (work.getColStats() != null) {
      ColumnStatistics colStats = work.getColStats();
      LOG.debug("Got stats through replication for " +
              colStats.getStatsDesc().getDbName() + "." +
              colStats.getStatsDesc().getTableName());
      return colStats;
    }
    String dbName = work.dbName();
    String tableName = work.getTableName();
    String partName = work.getPartName();
    String colName = work.getColName();
    String columnType = work.getColType();

    ColumnStatisticsObj statsObj = new ColumnStatisticsObj();

    // grammar prohibits more than 1 column so we are guaranteed to have only 1
    // element in this lists.

    statsObj.setColName(colName);

    statsObj.setColType(columnType);

    ColumnStatisticsData statsData = new ColumnStatisticsData();

    if (columnType.equalsIgnoreCase("long") || columnType.equalsIgnoreCase("tinyint")
        || columnType.equalsIgnoreCase("smallint") || columnType.equalsIgnoreCase("int")
        || columnType.equalsIgnoreCase("bigint")) {
      LongColumnStatsDataInspector longStats = new LongColumnStatsDataInspector();
      longStats.setNumNullsIsSet(false);
      longStats.setNumDVsIsSet(false);
      longStats.setLowValueIsSet(false);
      longStats.setHighValueIsSet(false);
      Map<String, String> mapProp = work.getMapProp();
      for (Entry<String, String> entry : mapProp.entrySet()) {
        String fName = entry.getKey();
        String value = entry.getValue();
        if (fName.equals("numNulls")) {
          longStats.setNumNulls(Long.parseLong(value));
        } else if (fName.equals("numDVs")) {
          longStats.setNumDVs(Long.parseLong(value));
        } else if (fName.equals("lowValue")) {
          longStats.setLowValue(Long.parseLong(value));
        } else if (fName.equals("highValue")) {
          longStats.setHighValue(Long.parseLong(value));
        } else {
          throw new SemanticException("Unknown stat");
        }
      }
      statsData.setLongStats(longStats);
      statsObj.setStatsData(statsData);
    } else if (columnType.equalsIgnoreCase("double") || columnType.equalsIgnoreCase("float")) {
      DoubleColumnStatsDataInspector doubleStats = new DoubleColumnStatsDataInspector();
      doubleStats.setNumNullsIsSet(false);
      doubleStats.setNumDVsIsSet(false);
      doubleStats.setLowValueIsSet(false);
      doubleStats.setHighValueIsSet(false);
      Map<String, String> mapProp = work.getMapProp();
      for (Entry<String, String> entry : mapProp.entrySet()) {
        String fName = entry.getKey();
        String value = entry.getValue();
        if (fName.equals("numNulls")) {
          doubleStats.setNumNulls(Long.parseLong(value));
        } else if (fName.equals("numDVs")) {
          doubleStats.setNumDVs(Long.parseLong(value));
        } else if (fName.equals("lowValue")) {
          doubleStats.setLowValue(Double.parseDouble(value));
        } else if (fName.equals("highValue")) {
          doubleStats.setHighValue(Double.parseDouble(value));
        } else {
          throw new SemanticException("Unknown stat");
        }
      }
      statsData.setDoubleStats(doubleStats);
      statsObj.setStatsData(statsData);
    } else if (columnType.equalsIgnoreCase("string") || columnType.toLowerCase().startsWith("char")
              || columnType.toLowerCase().startsWith("varchar")) { //char(x),varchar(x) types
      StringColumnStatsDataInspector stringStats = new StringColumnStatsDataInspector();
      stringStats.setMaxColLenIsSet(false);
      stringStats.setAvgColLenIsSet(false);
      stringStats.setNumNullsIsSet(false);
      stringStats.setNumDVsIsSet(false);
      Map<String, String> mapProp = work.getMapProp();
      for (Entry<String, String> entry : mapProp.entrySet()) {
        String fName = entry.getKey();
        String value = entry.getValue();
        if (fName.equals("numNulls")) {
          stringStats.setNumNulls(Long.parseLong(value));
        } else if (fName.equals("numDVs")) {
          stringStats.setNumDVs(Long.parseLong(value));
        } else if (fName.equals("avgColLen")) {
          stringStats.setAvgColLen(Double.parseDouble(value));
        } else if (fName.equals("maxColLen")) {
          stringStats.setMaxColLen(Long.parseLong(value));
        } else {
          throw new SemanticException("Unknown stat");
        }
      }
      statsData.setStringStats(stringStats);
      statsObj.setStatsData(statsData);
    } else if (columnType.equalsIgnoreCase("boolean")) {
      BooleanColumnStatsData booleanStats = new BooleanColumnStatsData();
      booleanStats.setNumNullsIsSet(false);
      booleanStats.setNumTruesIsSet(false);
      booleanStats.setNumFalsesIsSet(false);
      Map<String, String> mapProp = work.getMapProp();
      for (Entry<String, String> entry : mapProp.entrySet()) {
        String fName = entry.getKey();
        String value = entry.getValue();
        if (fName.equals("numNulls")) {
          booleanStats.setNumNulls(Long.parseLong(value));
        } else if (fName.equals("numTrues")) {
          booleanStats.setNumTrues(Long.parseLong(value));
        } else if (fName.equals("numFalses")) {
          booleanStats.setNumFalses(Long.parseLong(value));
        } else {
          throw new SemanticException("Unknown stat");
        }
      }
      statsData.setBooleanStats(booleanStats);
      statsObj.setStatsData(statsData);
    } else if (columnType.equalsIgnoreCase("binary")) {
      BinaryColumnStatsData binaryStats = new BinaryColumnStatsData();
      binaryStats.setNumNullsIsSet(false);
      binaryStats.setAvgColLenIsSet(false);
      binaryStats.setMaxColLenIsSet(false);
      Map<String, String> mapProp = work.getMapProp();
      for (Entry<String, String> entry : mapProp.entrySet()) {
        String fName = entry.getKey();
        String value = entry.getValue();
        if (fName.equals("numNulls")) {
          binaryStats.setNumNulls(Long.parseLong(value));
        } else if (fName.equals("avgColLen")) {
          binaryStats.setAvgColLen(Double.parseDouble(value));
        } else if (fName.equals("maxColLen")) {
          binaryStats.setMaxColLen(Long.parseLong(value));
        } else {
          throw new SemanticException("Unknown stat");
        }
      }
      statsData.setBinaryStats(binaryStats);
      statsObj.setStatsData(statsData);
    } else if (columnType.toLowerCase().startsWith("decimal")) { //decimal(a,b) type
      DecimalColumnStatsDataInspector decimalStats = new DecimalColumnStatsDataInspector();
      decimalStats.setNumNullsIsSet(false);
      decimalStats.setNumDVsIsSet(false);
      decimalStats.setLowValueIsSet(false);
      decimalStats.setHighValueIsSet(false);
      Map<String, String> mapProp = work.getMapProp();
      for (Entry<String, String> entry : mapProp.entrySet()) {
        String fName = entry.getKey();
        String value = entry.getValue();
        if (fName.equals("numNulls")) {
          decimalStats.setNumNulls(Long.parseLong(value));
        } else if (fName.equals("numDVs")) {
          decimalStats.setNumDVs(Long.parseLong(value));
        } else if (fName.equals("lowValue")) {
          BigDecimal d = new BigDecimal(value);
          decimalStats.setLowValue(DecimalUtils.getDecimal(ByteBuffer.wrap(d
              .unscaledValue().toByteArray()), (short) d.scale()));
        } else if (fName.equals("highValue")) {
          BigDecimal d = new BigDecimal(value);
          decimalStats.setHighValue(DecimalUtils.getDecimal(ByteBuffer.wrap(d
              .unscaledValue().toByteArray()), (short) d.scale()));
        } else {
          throw new SemanticException("Unknown stat");
        }
      }
      statsData.setDecimalStats(decimalStats);
      statsObj.setStatsData(statsData);
    } else if (columnType.equalsIgnoreCase("date")) {
      DateColumnStatsDataInspector dateStats = new DateColumnStatsDataInspector();
      Map<String, String> mapProp = work.getMapProp();
      for (Entry<String, String> entry : mapProp.entrySet()) {
        String fName = entry.getKey();
        String value = entry.getValue();
        if (fName.equals("numNulls")) {
          dateStats.setNumNulls(Long.parseLong(value));
        } else if (fName.equals("numDVs")) {
          dateStats.setNumDVs(Long.parseLong(value));
        } else if (fName.equals("lowValue")) {
          // Date high/low value is stored as long in stats DB, but allow users to set high/low
          // value using either date format (yyyy-mm-dd) or numeric format (days since epoch)
          dateStats.setLowValue(readDateValue(value));
        } else if (fName.equals("highValue")) {
          dateStats.setHighValue(readDateValue(value));
        } else {
          throw new SemanticException("Unknown stat");
        }
      }
      statsData.setDateStats(dateStats);
      statsObj.setStatsData(statsData);
    } else if (columnType.equalsIgnoreCase("timestamp")) {
      TimestampColumnStatsDataInspector timestampStats = new TimestampColumnStatsDataInspector();
      Map<String, String> mapProp = work.getMapProp();
      for (Entry<String, String> entry : mapProp.entrySet()) {
        String fName = entry.getKey();
        String value = entry.getValue();
        if (fName.equals("numNulls")) {
          timestampStats.setNumNulls(Long.parseLong(value));
        } else if (fName.equals("numDVs")) {
          timestampStats.setNumDVs(Long.parseLong(value));
        } else if (fName.equals("lowValue")) {
          timestampStats.setLowValue(readTimestampValue(value));
        } else if (fName.equals("highValue")) {
          timestampStats.setHighValue(readTimestampValue(value));
        } else {
          throw new SemanticException("Unknown stat");
        }
      }
      statsData.setTimestampStats(timestampStats);
      statsObj.setStatsData(statsData);
    } else {
      throw new SemanticException("Unsupported type");
    }
    ColumnStatisticsDesc statsDesc = getColumnStatsDesc(dbName, tableName,
        partName, partName == null);
    ColumnStatistics colStat = new ColumnStatistics();
    colStat.setStatsDesc(statsDesc);
    colStat.addToStatsObj(statsObj);
    colStat.setEngine(Constants.HIVE_ENGINE);
    return colStat;
  }

  private ColumnStatisticsDesc getColumnStatsDesc(String dbName,
      String tableName, String partName, boolean isTblLevel) {
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc();
    statsDesc.setDbName(dbName);
    statsDesc.setTableName(tableName);
    statsDesc.setIsTblLevel(isTblLevel);
    if (!isTblLevel) {
      statsDesc.setPartName(partName);
    } else {
      statsDesc.setPartName(null);
    }
    return statsDesc;
  }

  private int persistColumnStats(Hive db) throws HiveException, MetaException, IOException {
    ColumnStatistics colStats = constructColumnStatsFromInput();
    SetPartitionsStatsRequest request =
            new SetPartitionsStatsRequest(Collections.singletonList(colStats), Constants.HIVE_ENGINE);

    // Set writeId and validWriteId list for replicated statistics. getColStats() will return
    // non-null value only during replication.
    if (work.getColStats() != null) {
      String dbName = colStats.getStatsDesc().getDbName();
      String tblName = colStats.getStatsDesc().getTableName();
      Table tbl = db.getTable(dbName, tblName);
      long writeId = work.getWriteId();
      // If it's a transactional table on source and target, we will get a valid writeId
      // associated with it.
      if (AcidUtils.isTransactionalTable(tbl)) {
        ValidWriteIdList writeIds;

        // We need a valid writeId list to update column statistics for a transactional table. We
        // do not have a valid writeId list which was used to update the column stats on the
        // source. But we know for sure that the writeId associated with the stats was valid then
        // (otherwise column stats update would have failed on the source). So use a valid
        // transaction list with only that writeId and use it to update the stats.
        writeIds = new ValidReaderWriteIdList(TableName.getDbTable(dbName, tblName), new long[0],
                                              new BitSet(), writeId);
        request.setValidWriteIdList(writeIds.toString());
        request.setWriteId(writeId);
      }
    }

    db.setPartitionColumnStatistics(request);
    return 0;
  }

  @Override
  public int execute() {
    try {
      Hive db = getHive();
      return persistColumnStats(db);
    } catch (Exception e) {
      setException(e);
      LOG.info("Failed to persist stats in metastore", e);
      return ReplUtils.handleException(work.isReplication(), e, work.getDumpDirectory(), work.getMetricCollector(),
                                       getName(), conf);
    }
  }

  @Override
  public StageType getType() {
    return StageType.COLUMNSTATS;
  }

  @Override
  public String getName() {
    return "COLUMNSTATS UPDATE TASK";
  }

  private Date readDateValue(String dateStr) {
    // try either yyyy-mm-dd, or integer representing days since epoch
    try {
      DateWritableV2 writableVal = new DateWritableV2(org.apache.hadoop.hive.common.type.Date.valueOf(dateStr));
      return new Date(writableVal.getDays());
    } catch (IllegalArgumentException err) {
      // Fallback to integer parsing
      LOG.debug("Reading date value as days since epoch: {}", dateStr);
      return new Date(Long.parseLong(dateStr));
    }
  }

  private Timestamp readTimestampValue(String timestampStr) {
    try {
      TimestampWritableV2 writableVal = new TimestampWritableV2(
          org.apache.hadoop.hive.common.type.Timestamp.valueOf(timestampStr));
      return new Timestamp(writableVal.getSeconds());
    } catch (IllegalArgumentException err) {
      LOG.debug("Reading timestamp value as seconds since epoch: {}", timestampStr);
      return new Timestamp(Long.parseLong(timestampStr));
    }
  }
}
