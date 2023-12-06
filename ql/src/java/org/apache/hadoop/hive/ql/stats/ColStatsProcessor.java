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

package org.apache.hadoop.hive.ql.stats;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.FetchOperator;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ColumnStatsDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ColStatsProcessor implements IStatsProcessor {
  private static transient final Logger LOG = LoggerFactory.getLogger(ColStatsProcessor.class);

  private FetchOperator ftOp;
  private FetchWork fWork;
  private ColumnStatsDesc colStatDesc;
  private HiveConf conf;
  private boolean isStatsReliable;

  public ColStatsProcessor(ColumnStatsDesc colStats, HiveConf conf) {
    this.conf = conf;
    fWork = colStats.getFWork();
    colStatDesc = colStats;
    isStatsReliable = conf.getBoolVar(ConfVars.HIVE_STATS_RELIABLE);
  }

  @Override
  public void initialize(CompilationOpContext opContext) {
    try {
      fWork.initializeForFetch(opContext);
      JobConf job = new JobConf(conf);
      ftOp = new FetchOperator(fWork, job);
    } catch (Exception e) {
      LOG.error("Failed to initialize", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public int process(Hive db, Table tbl) throws Exception {
    return persistColumnStats(db, tbl);
  }

  private boolean constructColumnStatsFromPackedRows(Table tbl, List<ColumnStatistics> stats, long maxNumStats)
      throws HiveException, MetaException, IOException {
    String partName = null;
    List<String> colName = colStatDesc.getColName();
    List<String> colType = colStatDesc.getColType();
    boolean isTblLevel = colStatDesc.isTblLevel();

    InspectableObject packedRow;
    long numStats = 0;
    while ((packedRow = ftOp.getNextRow()) != null) {
      if (packedRow.oi.getCategory() != ObjectInspector.Category.STRUCT) {
        throw new HiveException("Unexpected object type encountered while unpacking row");
      }

      final List<ColumnStatisticsObj> statsObjs = new ArrayList<>();
      final StructObjectInspector soi = (StructObjectInspector) packedRow.oi;
      final List<? extends StructField> fields = soi.getAllStructFieldRefs();
      final List<Object> values = soi.getStructFieldsDataAsList(packedRow.o);

      // Partition columns are appended at end, we only care about stats column
      int pos = 0;
      for (int i = 0; i < colName.size(); i++) {
        String columnName = colName.get(i);
        String columnType = colType.get(i);
        PrimitiveTypeInfo typeInfo = (PrimitiveTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(columnType);
        List<ColumnStatsField> columnStatsFields = ColumnStatsType.getColumnStats(typeInfo);
        columnStatsFields = ColumnStatsType.removeDisabledStatistics(conf, columnStatsFields);
        try {
          ColumnStatisticsObj statObj = ColumnStatisticsObjTranslator.readHiveColumnStatistics(
              columnName, columnType, columnStatsFields, pos, fields, values);
          statsObjs.add(statObj);
          numStats++;
        } catch (Exception e) {
          if (isStatsReliable) {
            throw new HiveException("Statistics collection failed while (hive.stats.reliable)", e);
          } else {
            LOG.debug("Because {} is infinite or NaN, we skip stats.", columnName, e);
          }
        }
        pos += columnStatsFields.size();
      }

      if (!statsObjs.isEmpty()) {
        if (!isTblLevel) {
          List<FieldSchema> partColSchema = tbl.getPartCols();
          List<String> partVals = new ArrayList<>();
          // Iterate over partition columns to figure out partition name
          for (int i = pos; i < pos + partColSchema.size(); i++) {
            Object partVal = ((PrimitiveObjectInspector) fields.get(i).getFieldObjectInspector())
                .getPrimitiveJavaObject(values.get(i));
            partVals.add(partVal == null ? // could be null for default partition
              this.conf.getVar(ConfVars.DEFAULT_PARTITION_NAME) : partVal.toString());
          }
          partName = Warehouse.makePartName(partColSchema, partVals);
        }

        ColumnStatisticsDesc statsDesc = buildColumnStatsDesc(tbl, partName, isTblLevel);
        ColumnStatistics colStats = new ColumnStatistics();
        colStats.setStatsDesc(statsDesc);
        colStats.setStatsObj(statsObjs);
        colStats.setEngine(Constants.HIVE_ENGINE);
        stats.add(colStats);
        if (numStats >= maxNumStats) {
          return false;
        }
      }
    }
    ftOp.clearFetchContext();
    return true;
  }

  private ColumnStatisticsDesc buildColumnStatsDesc(Table table, String partName, boolean isTblLevel) {
    String dbName = table.getDbName();
    assert dbName != null;
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc();
    statsDesc.setDbName(dbName);
    statsDesc.setTableName(table.getTableName());
    statsDesc.setIsTblLevel(isTblLevel);

    if (!isTblLevel) {
      statsDesc.setPartName(partName);
    } else {
      statsDesc.setPartName(null);
    }
    return statsDesc;
  }

  public int persistColumnStats(Hive db, Table tbl) throws HiveException, MetaException, IOException {
    // Construct a column statistics object from the result

    long writeId = -1;
    ValidWriteIdList validWriteIdList = null;
    HiveTxnManager txnMgr = AcidUtils.isTransactionalTable(tbl) ? SessionState.get().getTxnMgr() : null;
    if (txnMgr != null) {
      writeId = txnMgr.getAllocatedTableWriteId(tbl.getDbName(), tbl.getTableName());
      validWriteIdList =
              AcidUtils.getTableValidWriteIdList(conf, AcidUtils.getFullTableName(tbl.getDbName(), tbl.getTableName()));
    }

    boolean done = false;
    long maxNumStats = conf.getLongVar(HiveConf.ConfVars.HIVE_STATS_MAX_NUM_STATS);
    while (!done) {
      List<ColumnStatistics> colStats = new ArrayList<>();

      long start = System. currentTimeMillis();
      done = constructColumnStatsFromPackedRows(tbl, colStats, maxNumStats);
      long end = System.currentTimeMillis();
      LOG.info("Time taken to build " + colStats.size() + " stats desc : " + ((end - start)/1000F) + " seconds.");

      // Persist the column statistics object to the metastore
      // Note, this function is shared for both table and partition column stats.
      if (colStats.isEmpty()) {
        continue;
      }
      SetPartitionsStatsRequest request = new SetPartitionsStatsRequest(colStats, Constants.HIVE_ENGINE);
      request.setNeedMerge(colStatDesc.isNeedMerge());
      if (txnMgr != null) {
        request.setWriteId(writeId);
        if (validWriteIdList != null) {
          request.setValidWriteIdList(validWriteIdList.toString());
        }
      }

      start = System. currentTimeMillis();
      if (tbl != null && tbl.isNonNative() && tbl.getStorageHandler().canSetColStatistics(tbl)) {
        boolean success = tbl.getStorageHandler().setColStatistics(tbl, colStats);
        if (!(tbl.isMaterializedView() || tbl.isView() || tbl.isTemporary())) {
          setOrRemoveColumnStatsAccurateProperty(db, tbl, colStatDesc.getColName(), success);
        }
      }
      // TODO: Write stats for native tables only (See HIVE-27421)
      db.setPartitionColumnStatistics(request);
      end = System.currentTimeMillis();
      LOG.info("Time taken to update " + colStats.size() + " stats : " + ((end - start)/1000F) + " seconds.");
    }
    return 0;
  }

  @Override
  public void setDpPartSpecs(Collection<Partition> dpPartSpecs) {
  }

  private void setOrRemoveColumnStatsAccurateProperty(Hive db, Table tbl, List<String> colNames, boolean success) throws HiveException {
    if (CollectionUtils.isEmpty(colNames) || !colStatDesc.isTblLevel()) {
      return;
    }
    EnvironmentContext environmentContext = new EnvironmentContext();
    environmentContext.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
    if (success) {
      StatsSetupConst.setColumnStatsState(tbl.getParameters(), colNames);
    } else {
      StatsSetupConst.removeColumnStatsState(tbl.getParameters(), colNames);
    }
    db.alterTable(tbl.getFullyQualifiedName(), tbl, environmentContext, false);
  }

  /**
   * Enumeration of column stats fields that can currently
   * be computed. Each one has a field name associated.
   */
  public enum ColumnStatsField {
    COLUMN_STATS_TYPE("columntype"),
    COUNT_TRUES("counttrues"),
    COUNT_FALSES("countfalses"),
    COUNT_NULLS("countnulls"),
    MIN("min"),
    MAX("max"),
    NDV("numdistinctvalues"),
    BITVECTOR("ndvbitvector"),
    KLL_SKETCH("kllsketch"),
    MAX_LENGTH("maxlength"),
    AVG_LENGTH("avglength");

    private final String fieldName;

    ColumnStatsField(String fieldName) {
      this.fieldName = fieldName;
    }

    public String getFieldName() {
      return fieldName;
    }
  }

  /**
   * Enumeration of column stats type. Each Hive primitive type maps into a single
   * column stats type, e.g., byte, short, int, and bigint types map into long
   * column type. Each column stats type has _n_ column stats fields associated
   * with it.
   */
  public enum ColumnStatsType {
    BOOLEAN(
        ImmutableList.of(
            ColumnStatsField.COLUMN_STATS_TYPE,
            ColumnStatsField.COUNT_TRUES,
            ColumnStatsField.COUNT_FALSES,
            ColumnStatsField.COUNT_NULLS)),
    LONG(
        ImmutableList.of(
            ColumnStatsField.COLUMN_STATS_TYPE,
            ColumnStatsField.MIN,
            ColumnStatsField.MAX,
            ColumnStatsField.COUNT_NULLS,
            ColumnStatsField.NDV,
            ColumnStatsField.BITVECTOR,
            ColumnStatsField.KLL_SKETCH)),
    DOUBLE(
        ImmutableList.of(
            ColumnStatsField.COLUMN_STATS_TYPE,
            ColumnStatsField.MIN,
            ColumnStatsField.MAX,
            ColumnStatsField.COUNT_NULLS,
            ColumnStatsField.NDV,
            ColumnStatsField.BITVECTOR,
            ColumnStatsField.KLL_SKETCH)),
    STRING(
        ImmutableList.of(
            ColumnStatsField.COLUMN_STATS_TYPE,
            ColumnStatsField.MAX_LENGTH,
            ColumnStatsField.AVG_LENGTH,
            ColumnStatsField.COUNT_NULLS,
            ColumnStatsField.NDV,
            ColumnStatsField.BITVECTOR)),
    BINARY(
        ImmutableList.of(
            ColumnStatsField.COLUMN_STATS_TYPE,
            ColumnStatsField.MAX_LENGTH,
            ColumnStatsField.AVG_LENGTH,
            ColumnStatsField.COUNT_NULLS)),
    DECIMAL(
        ImmutableList.of(
            ColumnStatsField.COLUMN_STATS_TYPE,
            ColumnStatsField.MIN,
            ColumnStatsField.MAX,
            ColumnStatsField.COUNT_NULLS,
            ColumnStatsField.NDV,
            ColumnStatsField.BITVECTOR,
            ColumnStatsField.KLL_SKETCH)),
    DATE(
        ImmutableList.of(
            ColumnStatsField.COLUMN_STATS_TYPE,
            ColumnStatsField.MIN,
            ColumnStatsField.MAX,
            ColumnStatsField.COUNT_NULLS,
            ColumnStatsField.NDV,
            ColumnStatsField.BITVECTOR,
            ColumnStatsField.KLL_SKETCH)),
    TIMESTAMP(
        ImmutableList.of(
            ColumnStatsField.COLUMN_STATS_TYPE,
            ColumnStatsField.MIN,
            ColumnStatsField.MAX,
            ColumnStatsField.COUNT_NULLS,
            ColumnStatsField.NDV,
            ColumnStatsField.BITVECTOR,
            ColumnStatsField.KLL_SKETCH));


    private final List<ColumnStatsField> columnStats;

    ColumnStatsType(List<ColumnStatsField> columnStats) {
      this.columnStats = columnStats;
    }

    public List<ColumnStatsField> getColumnStats() {
      return columnStats;
    }

    public static ColumnStatsType getColumnStatsType(PrimitiveTypeInfo typeInfo)
        throws SemanticException {
      switch (typeInfo.getPrimitiveCategory()) {
      case BOOLEAN:
        return BOOLEAN;
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case TIMESTAMPLOCALTZ:
        return LONG;
      case FLOAT:
      case DOUBLE:
        return DOUBLE;
      case DECIMAL:
        return DECIMAL;
      case DATE:
        return DATE;
      case TIMESTAMP:
        return TIMESTAMP;
      case STRING:
      case CHAR:
      case VARCHAR:
        return STRING;
      case BINARY:
        return BINARY;
      default:
        throw new SemanticException("Not supported type "
            + typeInfo.getTypeName() + " for statistics computation");
      }
    }

    public static List<ColumnStatsField> getColumnStats(PrimitiveTypeInfo typeInfo)
        throws SemanticException {
      return getColumnStatsType(typeInfo).getColumnStats();
    }

    public static List<ColumnStatsField> removeDisabledStatistics(HiveConf conf, List<ColumnStatsField> columnStatsFields) {
      if (!HiveStatsUtils.computeHistograms(conf)) {
        return columnStatsFields.stream()
            .filter(f -> f != ColumnStatsField.KLL_SKETCH)
            .collect(Collectors.toList());
      }
      return columnStatsFields;
    }
  }
}
