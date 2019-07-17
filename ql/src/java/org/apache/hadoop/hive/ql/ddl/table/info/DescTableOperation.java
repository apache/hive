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

package org.apache.hadoop.hive.ql.ddl.table.info;

import java.io.DataOutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.StatObjectConverter;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.PartitionIterable;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import avro.shaded.com.google.common.collect.Lists;

/**
 * Operation process of dropping a table.
 */
public class DescTableOperation extends DDLOperation<DescTableDesc> {
  public DescTableOperation(DDLOperationContext context, DescTableDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws Exception {
    Table table = getTable();
    Partition part = getPartition(table);

    try (DataOutputStream outStream = DDLUtils.getOutputStream(new Path(desc.getResFile()), context)) {
      LOG.debug("DDLTask: got data for {}", desc.getTableName());

      List<FieldSchema> cols = new ArrayList<>();
      List<ColumnStatisticsObj> colStats = new ArrayList<>();

      Deserializer deserializer = getDeserializer(table);

      if (desc.getColumnPath() == null) {
        getColumnsNoColumnPath(table, part, cols);
      } else {
        if (desc.isFormatted()) {
          getColumnDataColPathSpecified(table, part, cols, colStats, deserializer);
        } else {
          cols.addAll(Hive.getFieldsFromDeserializer(desc.getColumnPath(), deserializer));
        }
      }
      fixDecimalColumnTypeName(cols);

      setConstraintsAndStorageHandlerInfo(table);
      handleMaterializedView(table);
      // In case the query is served by HiveServer2, don't pad it with spaces,
      // as HiveServer2 output is consumed by JDBC/ODBC clients.
      boolean isOutputPadded = !SessionState.get().isHiveServerQuery();
      context.getFormatter().describeTable(outStream, desc.getColumnPath(), desc.getTableName(), table, part, cols,
          desc.isFormatted(), desc.isExtended(), isOutputPadded, colStats);

      LOG.debug("DDLTask: written data for {}", desc.getTableName());

    } catch (SQLException e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR, desc.getTableName());
    }

    return 0;
  }

  private Table getTable() throws HiveException {
    Table table = context.getDb().getTable(desc.getTableName(), false);
    if (table == null) {
      throw new HiveException(ErrorMsg.INVALID_TABLE, desc.getTableName());
    }
    return table;
  }

  private Partition getPartition(Table table) throws HiveException {
    Partition part = null;
    if (desc.getPartitionSpec() != null) {
      part = context.getDb().getPartition(table, desc.getPartitionSpec(), false);
      if (part == null) {
        throw new HiveException(ErrorMsg.INVALID_PARTITION,
            StringUtils.join(desc.getPartitionSpec().keySet(), ','), desc.getTableName());
      }
    }
    return part;
  }

  private Deserializer getDeserializer(Table table) throws SQLException {
    Deserializer deserializer = table.getDeserializer(true);
    if (deserializer instanceof AbstractSerDe) {
      String errorMsgs = ((AbstractSerDe) deserializer).getConfigurationErrors();
      if (StringUtils.isNotEmpty(errorMsgs)) {
        throw new SQLException(errorMsgs);
      }
    }
    return deserializer;
  }

  private void getColumnsNoColumnPath(Table table, Partition partition, List<FieldSchema> cols) throws HiveException {
    cols.addAll(partition == null || table.getTableType() == TableType.VIRTUAL_VIEW ?
        table.getCols() : partition.getCols());
    if (!desc.isFormatted()) {
      cols.addAll(table.getPartCols());
    }

    if (table.isPartitioned() && partition == null) {
      // No partition specified for partitioned table, lets fetch all.
      Map<String, String> tblProps = table.getParameters() == null ?
          new HashMap<String, String>() : table.getParameters();

      Map<String, Long> valueMap = new HashMap<>();
      Map<String, Boolean> stateMap = new HashMap<>();
      for (String stat : StatsSetupConst.SUPPORTED_STATS) {
        valueMap.put(stat, 0L);
        stateMap.put(stat, true);
      }

      PartitionIterable partitions = new PartitionIterable(context.getDb(), table, null,
          MetastoreConf.getIntVar(context.getConf(), MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX));
      int numParts = 0;
      for (Partition p : partitions) {
        Map<String, String> partitionProps = p.getParameters();
        Boolean state = StatsSetupConst.areBasicStatsUptoDate(partitionProps);
        for (String stat : StatsSetupConst.SUPPORTED_STATS) {
          stateMap.put(stat, stateMap.get(stat) && state);
          if (partitionProps != null && partitionProps.get(stat) != null) {
            valueMap.put(stat, valueMap.get(stat) + Long.parseLong(partitionProps.get(stat)));
          }
        }
        numParts++;
      }
      tblProps.put(StatsSetupConst.NUM_PARTITIONS, Integer.toString(numParts));

      for (String stat : StatsSetupConst.SUPPORTED_STATS) {
        StatsSetupConst.setBasicStatsState(tblProps, Boolean.toString(stateMap.get(stat)));
        tblProps.put(stat, valueMap.get(stat).toString());
      }
      table.setParameters(tblProps);
    }
  }

  private void getColumnDataColPathSpecified(Table table, Partition part, List<FieldSchema> cols,
      List<ColumnStatisticsObj> colStats, Deserializer deserializer)
      throws SemanticException, HiveException, MetaException {
    // when column name is specified in describe table DDL, colPath will be table_name.column_name
    String colName = desc.getColumnPath().split("\\.")[1];
    List<String> colNames = Lists.newArrayList(colName.toLowerCase());

    String[] dbTab = Utilities.getDbTableName(desc.getTableName());
    if (null == part) {
      if (table.isPartitioned()) {
        Map<String, String> tableProps = table.getParameters() == null ?
            new HashMap<String, String>() : table.getParameters();
        if (table.isPartitionKey(colNames.get(0))) {
          getColumnDataForPartitionKeyColumn(table, cols, colStats, colNames, tableProps);
        } else {
          getColumnsForNotPartitionKeyColumn(cols, colStats, deserializer, colNames, dbTab, tableProps);
        }
        table.setParameters(tableProps);
      } else {
        cols.addAll(Hive.getFieldsFromDeserializer(desc.getColumnPath(), deserializer));
        colStats.addAll(
            context.getDb().getTableColumnStatistics(dbTab[0].toLowerCase(), dbTab[1].toLowerCase(), colNames, false));
      }
    } else {
      List<String> partitions = new ArrayList<String>();
      partitions.add(part.getName());
      cols.addAll(Hive.getFieldsFromDeserializer(desc.getColumnPath(), deserializer));
      List<ColumnStatisticsObj> partitionColStat = context.getDb().getPartitionColumnStatistics(dbTab[0].toLowerCase(),
          dbTab[1].toLowerCase(), partitions, colNames, false).get(part.getName());
      if (partitionColStat != null) {
        colStats.addAll(partitionColStat);
      }
    }
  }

  private void getColumnDataForPartitionKeyColumn(Table table, List<FieldSchema> cols,
      List<ColumnStatisticsObj> colStats, List<String> colNames, Map<String, String> tableProps)
      throws HiveException, MetaException {
    FieldSchema partCol = table.getPartColByName(colNames.get(0));
    cols.add(partCol);
    PartitionIterable parts = new PartitionIterable(context.getDb(), table, null,
        MetastoreConf.getIntVar(context.getConf(), MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX));
    ColumnInfo ci = new ColumnInfo(partCol.getName(),
        TypeInfoUtils.getTypeInfoFromTypeString(partCol.getType()), null, false);
    ColStatistics cs = StatsUtils.getColStatsForPartCol(ci, parts, context.getConf());
    ColumnStatisticsData data = new ColumnStatisticsData();
    ColStatistics.Range r = cs.getRange();
    StatObjectConverter.fillColumnStatisticsData(partCol.getType(), data, r == null ? null : r.minValue,
        r == null ? null : r.maxValue, r == null ? null : r.minValue, r == null ? null : r.maxValue,
        r == null ? null : r.minValue.toString(), r == null ? null : r.maxValue.toString(),
        cs.getNumNulls(), cs.getCountDistint(), null, cs.getAvgColLen(), cs.getAvgColLen(),
        cs.getNumTrues(), cs.getNumFalses());
    ColumnStatisticsObj cso = new ColumnStatisticsObj(partCol.getName(), partCol.getType(), data);
    colStats.add(cso);
    StatsSetupConst.setColumnStatsState(tableProps, colNames);
  }

  private void getColumnsForNotPartitionKeyColumn(List<FieldSchema> cols, List<ColumnStatisticsObj> colStats,
      Deserializer deserializer, List<String> colNames, String[] dbTab, Map<String, String> tableProps)
      throws HiveException {
    cols.addAll(Hive.getFieldsFromDeserializer(desc.getColumnPath(), deserializer));
    List<String> parts = context.getDb().getPartitionNames(dbTab[0].toLowerCase(), dbTab[1].toLowerCase(),
        (short) -1);
    AggrStats aggrStats = context.getDb().getAggrColStatsFor(
        dbTab[0].toLowerCase(), dbTab[1].toLowerCase(), colNames, parts, false);
    colStats.addAll(aggrStats.getColStats());
    if (parts.size() == aggrStats.getPartsFound()) {
      StatsSetupConst.setColumnStatsState(tableProps, colNames);
    } else {
      StatsSetupConst.removeColumnStatsState(tableProps, colNames);
    }
  }

  /**
   * Fix the type name of a column of type decimal w/o precision/scale specified. This makes
   * the describe table show "decimal(10,0)" instead of "decimal" even if the type stored
   * in metastore is "decimal", which is possible with previous hive.
   *
   * @param cols columns that to be fixed as such
   */
  private static void fixDecimalColumnTypeName(List<FieldSchema> cols) {
    for (FieldSchema col : cols) {
      if (serdeConstants.DECIMAL_TYPE_NAME.equals(col.getType())) {
        col.setType(DecimalTypeInfo.getQualifiedName(HiveDecimal.USER_DEFAULT_PRECISION,
            HiveDecimal.USER_DEFAULT_SCALE));
      }
    }
  }

  private void setConstraintsAndStorageHandlerInfo(Table table) throws HiveException {
    if (desc.isExtended() || desc.isFormatted()) {
      table.setPrimaryKeyInfo(context.getDb().getPrimaryKeys(table.getDbName(), table.getTableName()));
      table.setForeignKeyInfo(context.getDb().getForeignKeys(table.getDbName(), table.getTableName()));
      table.setUniqueKeyInfo(context.getDb().getUniqueConstraints(table.getDbName(), table.getTableName()));
      table.setNotNullConstraint(context.getDb().getNotNullConstraints(table.getDbName(), table.getTableName()));
      table.setDefaultConstraint(context.getDb().getDefaultConstraints(table.getDbName(), table.getTableName()));
      table.setCheckConstraint(context.getDb().getCheckConstraints(table.getDbName(), table.getTableName()));
      table.setStorageHandlerInfo(context.getDb().getStorageHandlerInfo(table));
    }
  }

  private void handleMaterializedView(Table table) throws LockException {
    if (table.isMaterializedView()) {
      String validTxnsList = context.getDb().getConf().get(ValidTxnList.VALID_TXNS_KEY);
      if (validTxnsList != null) {
        List<String> tablesUsed = new ArrayList<>(table.getCreationMetadata().getTablesUsed());
        ValidTxnWriteIdList currentTxnWriteIds =
            SessionState.get().getTxnMgr().getValidWriteIds(tablesUsed, validTxnsList);
        long defaultTimeWindow = HiveConf.getTimeVar(context.getDb().getConf(),
            HiveConf.ConfVars.HIVE_MATERIALIZED_VIEW_REWRITING_TIME_WINDOW, TimeUnit.MILLISECONDS);
        table.setOutdatedForRewriting(Hive.isOutdatedMaterializedView(table,
            currentTxnWriteIds, defaultTimeWindow, tablesUsed, false));
      }
    }
  }
}
