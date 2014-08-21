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

package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ColumnStatsDesc;
import org.apache.hadoop.hive.ql.plan.ColumnStatsUpdateWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * ColumnStatsUpdateTask implementation. For example, ALTER TABLE src_stat
 * UPDATE STATISTICS for column key SET ('numDVs'='1111','avgColLen'='1.111');
 * For another example, ALTER TABLE src_stat_part PARTITION(partitionId=100)
 * UPDATE STATISTICS for column value SET
 * ('maxColLen'='4444','avgColLen'='44.4');
 **/

public class ColumnStatsUpdateTask extends Task<ColumnStatsUpdateWork> {
  private static final long serialVersionUID = 1L;
  private static transient final Log LOG = LogFactory
      .getLog(ColumnStatsUpdateTask.class);

  @Override
  public void initialize(HiveConf conf, QueryPlan queryPlan, DriverContext ctx) {
    super.initialize(conf, queryPlan, ctx);
  }

  private ColumnStatistics constructColumnStatsFromInput()
      throws SemanticException, MetaException {

    String dbName = SessionState.get().getCurrentDatabase();
    ColumnStatsDesc desc = work.getColStats();
    String tableName = desc.getTableName();
    String partName = work.getPartName();
    List<String> colName = desc.getColName();
    List<String> colType = desc.getColType();

    ColumnStatisticsObj statsObj = new ColumnStatisticsObj();

    // grammar prohibits more than 1 column so we are guaranteed to have only 1
    // element in this lists.

    statsObj.setColName(colName.get(0));

    statsObj.setColType(colType.get(0));

    ColumnStatisticsData statsData = new ColumnStatisticsData();

    String columnType = colType.get(0);

    if (columnType.equalsIgnoreCase("long")) {
      LongColumnStatsData longStats = new LongColumnStatsData();
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
    } else if (columnType.equalsIgnoreCase("double")) {
      DoubleColumnStatsData doubleStats = new DoubleColumnStatsData();
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
    } else if (columnType.equalsIgnoreCase("string")) {
      StringColumnStatsData stringStats = new StringColumnStatsData();
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
    } else if (columnType.equalsIgnoreCase("decimal")) {
      DecimalColumnStatsData decimalStats = new DecimalColumnStatsData();
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
          decimalStats.setLowValue(new Decimal(ByteBuffer.wrap(d
              .unscaledValue().toByteArray()), (short) d.scale()));
        } else if (fName.equals("highValue")) {
          BigDecimal d = new BigDecimal(value);
          decimalStats.setHighValue(new Decimal(ByteBuffer.wrap(d
              .unscaledValue().toByteArray()), (short) d.scale()));
        } else {
          throw new SemanticException("Unknown stat");
        }
      }
      statsData.setDecimalStats(decimalStats);
      statsObj.setStatsData(statsData);
    } else {
      throw new SemanticException("Unsupported type");
    }

    ColumnStatisticsDesc statsDesc = getColumnStatsDesc(dbName, tableName,
        partName, partName == null);
    ColumnStatistics colStat = new ColumnStatistics();
    colStat.setStatsDesc(statsDesc);
    colStat.addToStatsObj(statsObj);
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

  private int persistTableStats() throws HiveException, MetaException,
      IOException {
    // Construct a column statistics object from user input
    ColumnStatistics colStats = constructColumnStatsFromInput();
    // Persist the column statistics object to the metastore
    db.updateTableColumnStatistics(colStats);
    return 0;
  }

  private int persistPartitionStats() throws HiveException, MetaException,
      IOException {
    // Construct a column statistics object from user input
    ColumnStatistics colStats = constructColumnStatsFromInput();
    // Persist the column statistics object to the metastore
    db.updatePartitionColumnStatistics(colStats);
    return 0;
  }

  @Override
  public int execute(DriverContext driverContext) {
    try {
      if (work.getColStats().isTblLevel()) {
        return persistTableStats();
      } else {
        return persistPartitionStats();
      }
    } catch (Exception e) {
      LOG.info(e);
    }
    return 1;
  }

  @Override
  public StageType getType() {
    return StageType.COLUMNSTATS;
  }

  @Override
  public String getName() {
    return "COLUMNSTATS UPDATE TASK";
  }
}
