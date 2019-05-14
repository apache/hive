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
import java.util.Collections;
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
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.CheckConstraint;
import org.apache.hadoop.hive.ql.metadata.DefaultConstraint;
import org.apache.hadoop.hive.ql.metadata.ForeignKeyInfo;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.NotNullConstraint;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.PartitionIterable;
import org.apache.hadoop.hive.ql.metadata.PrimaryKeyInfo;
import org.apache.hadoop.hive.ql.metadata.StorageHandlerInfo;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.UniqueConstraint;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IOUtils;

/**
 * Operation process of dropping a table.
 */
public class DescTableOperation extends DDLOperation {
  private final DescTableDesc desc;

  public DescTableOperation(DDLOperationContext context, DescTableDesc desc) {
    super(context);
    this.desc = desc;
  }

  @Override
  public int execute() throws Exception {
    String colPath = desc.getColumnPath();
    String tableName = desc.getTableName();

    // describe the table - populate the output stream
    Table tbl = context.getDb().getTable(tableName, false);
    if (tbl == null) {
      throw new HiveException(ErrorMsg.INVALID_TABLE, tableName);
    }
    Partition part = null;
    if (desc.getPartSpec() != null) {
      part = context.getDb().getPartition(tbl, desc.getPartSpec(), false);
      if (part == null) {
        throw new HiveException(ErrorMsg.INVALID_PARTITION,
            StringUtils.join(desc.getPartSpec().keySet(), ','), tableName);
      }
      tbl = part.getTable();
    }

    DataOutputStream outStream = DDLUtils.getOutputStream(new Path(desc.getResFile()), context);
    try {
      LOG.debug("DDLTask: got data for {}", tableName);

      List<FieldSchema> cols = null;
      List<ColumnStatisticsObj> colStats = null;

      Deserializer deserializer = tbl.getDeserializer(true);
      if (deserializer instanceof AbstractSerDe) {
        String errorMsgs = ((AbstractSerDe) deserializer).getConfigurationErrors();
        if (errorMsgs != null && !errorMsgs.isEmpty()) {
          throw new SQLException(errorMsgs);
        }
      }

      if (colPath.equals(tableName)) {
        cols = (part == null || tbl.getTableType() == TableType.VIRTUAL_VIEW) ?
            tbl.getCols() : part.getCols();

        if (!desc.isFormatted()) {
          cols.addAll(tbl.getPartCols());
        }

        if (tbl.isPartitioned() && part == null) {
          // No partitioned specified for partitioned table, lets fetch all.
          Map<String, String> tblProps = tbl.getParameters() == null ?
              new HashMap<String, String>() : tbl.getParameters();
          Map<String, Long> valueMap = new HashMap<>();
          Map<String, Boolean> stateMap = new HashMap<>();
          for (String stat : StatsSetupConst.SUPPORTED_STATS) {
            valueMap.put(stat, 0L);
            stateMap.put(stat, true);
          }
          PartitionIterable parts = new PartitionIterable(context.getDb(), tbl, null,
              context.getConf().getIntVar(HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_MAX));
          int numParts = 0;
          for (Partition partition : parts) {
            Map<String, String> props = partition.getParameters();
            Boolean state = StatsSetupConst.areBasicStatsUptoDate(props);
            for (String stat : StatsSetupConst.SUPPORTED_STATS) {
              stateMap.put(stat, stateMap.get(stat) && state);
              if (props != null && props.get(stat) != null) {
                valueMap.put(stat, valueMap.get(stat) + Long.parseLong(props.get(stat)));
              }
            }
            numParts++;
          }
          for (String stat : StatsSetupConst.SUPPORTED_STATS) {
            StatsSetupConst.setBasicStatsState(tblProps, Boolean.toString(stateMap.get(stat)));
            tblProps.put(stat, valueMap.get(stat).toString());
          }
          tblProps.put(StatsSetupConst.NUM_PARTITIONS, Integer.toString(numParts));
          tbl.setParameters(tblProps);
        }
      } else {
        if (desc.isFormatted()) {
          // when column name is specified in describe table DDL, colPath will
          // will be table_name.column_name
          String colName = colPath.split("\\.")[1];
          String[] dbTab = Utilities.getDbTableName(tableName);
          List<String> colNames = new ArrayList<String>();
          colNames.add(colName.toLowerCase());
          if (null == part) {
            if (tbl.isPartitioned()) {
              Map<String, String> tblProps = tbl.getParameters() == null ?
                  new HashMap<String, String>() : tbl.getParameters();
              if (tbl.isPartitionKey(colNames.get(0))) {
                FieldSchema partCol = tbl.getPartColByName(colNames.get(0));
                cols = Collections.singletonList(partCol);
                PartitionIterable parts = new PartitionIterable(context.getDb(), tbl, null,
                    context.getConf().getIntVar(HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_MAX));
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
                colStats = Collections.singletonList(cso);
                StatsSetupConst.setColumnStatsState(tblProps, colNames);
              } else {
                cols = Hive.getFieldsFromDeserializer(colPath, deserializer);
                List<String> parts = context.getDb().getPartitionNames(dbTab[0].toLowerCase(), dbTab[1].toLowerCase(),
                    (short) -1);
                AggrStats aggrStats = context.getDb().getAggrColStatsFor(
                    dbTab[0].toLowerCase(), dbTab[1].toLowerCase(), colNames, parts, false);
                colStats = aggrStats.getColStats();
                if (parts.size() == aggrStats.getPartsFound()) {
                  StatsSetupConst.setColumnStatsState(tblProps, colNames);
                } else {
                  StatsSetupConst.removeColumnStatsState(tblProps, colNames);
                }
              }
              tbl.setParameters(tblProps);
            } else {
              cols = Hive.getFieldsFromDeserializer(colPath, deserializer);
              colStats = context.getDb().getTableColumnStatistics(
                  dbTab[0].toLowerCase(), dbTab[1].toLowerCase(), colNames, false);
            }
          } else {
            List<String> partitions = new ArrayList<String>();
            partitions.add(part.getName());
            cols = Hive.getFieldsFromDeserializer(colPath, deserializer);
            colStats = context.getDb().getPartitionColumnStatistics(dbTab[0].toLowerCase(),
                dbTab[1].toLowerCase(), partitions, colNames, false).get(part.getName());
          }
        } else {
          cols = Hive.getFieldsFromDeserializer(colPath, deserializer);
        }
      }
      PrimaryKeyInfo pkInfo = null;
      ForeignKeyInfo fkInfo = null;
      UniqueConstraint ukInfo = null;
      NotNullConstraint nnInfo = null;
      DefaultConstraint dInfo = null;
      CheckConstraint cInfo = null;
      StorageHandlerInfo storageHandlerInfo = null;
      if (desc.isExt() || desc.isFormatted()) {
        pkInfo = context.getDb().getPrimaryKeys(tbl.getDbName(), tbl.getTableName());
        fkInfo = context.getDb().getForeignKeys(tbl.getDbName(), tbl.getTableName());
        ukInfo = context.getDb().getUniqueConstraints(tbl.getDbName(), tbl.getTableName());
        nnInfo = context.getDb().getNotNullConstraints(tbl.getDbName(), tbl.getTableName());
        dInfo = context.getDb().getDefaultConstraints(tbl.getDbName(), tbl.getTableName());
        cInfo = context.getDb().getCheckConstraints(tbl.getDbName(), tbl.getTableName());
        storageHandlerInfo = context.getDb().getStorageHandlerInfo(tbl);
      }
      fixDecimalColumnTypeName(cols);
      // Information for materialized views
      if (tbl.isMaterializedView()) {
        final String validTxnsList = context.getDb().getConf().get(ValidTxnList.VALID_TXNS_KEY);
        if (validTxnsList != null) {
          List<String> tablesUsed = new ArrayList<>(tbl.getCreationMetadata().getTablesUsed());
          ValidTxnWriteIdList currentTxnWriteIds =
              SessionState.get().getTxnMgr().getValidWriteIds(tablesUsed, validTxnsList);
          long defaultTimeWindow = HiveConf.getTimeVar(context.getDb().getConf(),
              HiveConf.ConfVars.HIVE_MATERIALIZED_VIEW_REWRITING_TIME_WINDOW, TimeUnit.MILLISECONDS);
          tbl.setOutdatedForRewriting(Hive.isOutdatedMaterializedView(tbl,
              currentTxnWriteIds, defaultTimeWindow, tablesUsed, false));
        }
      }
      // In case the query is served by HiveServer2, don't pad it with spaces,
      // as HiveServer2 output is consumed by JDBC/ODBC clients.
      boolean isOutputPadded = !SessionState.get().isHiveServerQuery();
      context.getFormatter().describeTable(outStream, colPath, tableName, tbl, part,
          cols, desc.isFormatted(), desc.isExt(), isOutputPadded,
          colStats, pkInfo, fkInfo, ukInfo, nnInfo, dInfo, cInfo,
          storageHandlerInfo);

      LOG.debug("DDLTask: written data for {}", tableName);

    } catch (SQLException e) {
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR, tableName);
    } finally {
      IOUtils.closeStream(outStream);
    }

    return 0;
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
}
