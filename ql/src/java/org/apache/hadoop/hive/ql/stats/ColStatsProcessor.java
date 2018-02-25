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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.FetchOperator;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.ColumnStatsDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.stats.ColumnStatisticsObjTranslator;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
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
      LOG.error(StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
  }

  @Override
  public int process(Hive db, Table tbl) throws Exception {
    return persistColumnStats(db, tbl);
  }

  private List<ColumnStatistics> constructColumnStatsFromPackedRows(Table tbl1) throws HiveException, MetaException, IOException {

    Table tbl = tbl1;

    String partName = null;
    List<String> colName = colStatDesc.getColName();
    List<String> colType = colStatDesc.getColType();
    boolean isTblLevel = colStatDesc.isTblLevel();

    List<ColumnStatistics> stats = new ArrayList<ColumnStatistics>();
    InspectableObject packedRow;
    while ((packedRow = ftOp.getNextRow()) != null) {
      if (packedRow.oi.getCategory() != ObjectInspector.Category.STRUCT) {
        throw new HiveException("Unexpected object type encountered while unpacking row");
      }

      List<ColumnStatisticsObj> statsObjs = new ArrayList<ColumnStatisticsObj>();
      StructObjectInspector soi = (StructObjectInspector) packedRow.oi;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      List<Object> list = soi.getStructFieldsDataAsList(packedRow.o);

      List<FieldSchema> partColSchema = tbl.getPartCols();
      // Partition columns are appended at end, we only care about stats column
      int numOfStatCols = isTblLevel ? fields.size() : fields.size() - partColSchema.size();
      assert list != null;
      for (int i = 0; i < numOfStatCols; i++) {
        StructField structField = fields.get(i);
        String columnName = colName.get(i);
        String columnType = colType.get(i);
        Object values = list.get(i);
        try {
          ColumnStatisticsObj statObj = ColumnStatisticsObjTranslator.readHiveStruct(columnName, columnType, structField, values);
          statsObjs.add(statObj);
        } catch (Exception e) {
          if (isStatsReliable) {
            throw new HiveException("Statistics collection failed while (hive.stats.reliable)", e);
          } else {
            LOG.debug("Because {} is infinite or NaN, we skip stats.", columnName, e);
          }
        }
      }

      if (!statsObjs.isEmpty()) {

        if (!isTblLevel) {
          List<String> partVals = new ArrayList<String>();
          // Iterate over partition columns to figure out partition name
          for (int i = fields.size() - partColSchema.size(); i < fields.size(); i++) {
            Object partVal = ((PrimitiveObjectInspector) fields.get(i).getFieldObjectInspector()).getPrimitiveJavaObject(list.get(i));
            partVals.add(partVal == null ? // could be null for default partition
              this.conf.getVar(ConfVars.DEFAULTPARTITIONNAME) : partVal.toString());
          }
          partName = Warehouse.makePartName(partColSchema, partVals);
        }

        ColumnStatisticsDesc statsDesc = buildColumnStatsDesc(tbl, partName, isTblLevel);
        ColumnStatistics colStats = new ColumnStatistics();
        colStats.setStatsDesc(statsDesc);
        colStats.setStatsObj(statsObjs);
        stats.add(colStats);
      }
    }
    ftOp.clearFetchContext();
    return stats;
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

    List<ColumnStatistics> colStats = constructColumnStatsFromPackedRows(tbl);
    // Persist the column statistics object to the metastore
    // Note, this function is shared for both table and partition column stats.
    if (colStats.isEmpty()) {
      return 0;
    }
    SetPartitionsStatsRequest request = new SetPartitionsStatsRequest(colStats);
    request.setNeedMerge(colStatDesc.isNeedMerge());
    db.setPartitionColumnStatistics(request);
    return 0;
  }

  @Override
  public void setDpPartSpecs(Collection<Partition> dpPartSpecs) {
  }

}