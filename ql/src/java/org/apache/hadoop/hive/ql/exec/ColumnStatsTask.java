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
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.columnstats.cache.DateColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DecimalColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DoubleColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.LongColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.StringColumnStatsDataInspector;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.AnalyzeState;
import org.apache.hadoop.hive.ql.plan.ColumnStatsWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ColumnStatsTask implementation.
 **/

public class ColumnStatsTask extends Task<ColumnStatsWork> implements Serializable {
  private static final long serialVersionUID = 1L;
  private FetchOperator ftOp;
  private static transient final Logger LOG = LoggerFactory.getLogger(ColumnStatsTask.class);

  public ColumnStatsTask() {
    super();
  }

  @Override
  public void initialize(QueryState queryState, QueryPlan queryPlan, DriverContext ctx,
      CompilationOpContext opContext) {
    super.initialize(queryState, queryPlan, ctx, opContext);
    work.initializeForFetch(opContext);
    try {
      JobConf job = new JobConf(conf);
      ftOp = new FetchOperator(work.getfWork(), job);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
  }

  private void unpackBooleanStats(ObjectInspector oi, Object o, String fName,
      ColumnStatisticsObj statsObj) {
    long v = ((LongObjectInspector) oi).get(o);
    if (fName.equals("counttrues")) {
      statsObj.getStatsData().getBooleanStats().setNumTrues(v);
    } else if (fName.equals("countfalses")) {
      statsObj.getStatsData().getBooleanStats().setNumFalses(v);
    } else if (fName.equals("countnulls")) {
      statsObj.getStatsData().getBooleanStats().setNumNulls(v);
    }
  }

  @SuppressWarnings("serial")
  class UnsupportedDoubleException extends Exception {
  }

  private void unpackDoubleStats(ObjectInspector oi, Object o, String fName,
      ColumnStatisticsObj statsObj) throws UnsupportedDoubleException {
    if (fName.equals("countnulls")) {
      long v = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getDoubleStats().setNumNulls(v);
    } else if (fName.equals("numdistinctvalues")) {
      long v = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getDoubleStats().setNumDVs(v);
    } else if (fName.equals("max")) {
      double d = ((DoubleObjectInspector) oi).get(o);
      if (Double.isInfinite(d) || Double.isNaN(d)) {
        throw new UnsupportedDoubleException();
      }
      statsObj.getStatsData().getDoubleStats().setHighValue(d);
    } else if (fName.equals("min")) {
      double d = ((DoubleObjectInspector) oi).get(o);
      if (Double.isInfinite(d) || Double.isNaN(d)) {
        throw new UnsupportedDoubleException();
      }
      statsObj.getStatsData().getDoubleStats().setLowValue(d);
    } else if (fName.equals("ndvbitvector")) {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      byte[] buf = ((BinaryObjectInspector) poi).getPrimitiveJavaObject(o);
      statsObj.getStatsData().getDoubleStats().setBitVectors(buf);
    }
  }

  private void unpackDecimalStats(ObjectInspector oi, Object o, String fName,
      ColumnStatisticsObj statsObj) {
    if (fName.equals("countnulls")) {
      long v = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getDecimalStats().setNumNulls(v);
    } else if (fName.equals("numdistinctvalues")) {
      long v = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getDecimalStats().setNumDVs(v);
    } else if (fName.equals("max")) {
      HiveDecimal d = ((HiveDecimalObjectInspector) oi).getPrimitiveJavaObject(o);
      statsObj.getStatsData().getDecimalStats().setHighValue(convertToThriftDecimal(d));
    } else if (fName.equals("min")) {
      HiveDecimal d = ((HiveDecimalObjectInspector) oi).getPrimitiveJavaObject(o);
      statsObj.getStatsData().getDecimalStats().setLowValue(convertToThriftDecimal(d));
    } else if (fName.equals("ndvbitvector")) {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      byte[] buf = ((BinaryObjectInspector) poi).getPrimitiveJavaObject(o);
      statsObj.getStatsData().getDecimalStats().setBitVectors(buf);
    }
  }

  private Decimal convertToThriftDecimal(HiveDecimal d) {
    return new Decimal(ByteBuffer.wrap(d.unscaledValue().toByteArray()), (short)d.scale());
  }

  private void unpackLongStats(ObjectInspector oi, Object o, String fName,
      ColumnStatisticsObj statsObj) {
    if (fName.equals("countnulls")) {
      long v = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getLongStats().setNumNulls(v);
    } else if (fName.equals("numdistinctvalues")) {
      long v = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getLongStats().setNumDVs(v);
    } else if (fName.equals("max")) {
      long v = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getLongStats().setHighValue(v);
    } else if (fName.equals("min")) {
      long  v = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getLongStats().setLowValue(v);
    } else if (fName.equals("ndvbitvector")) {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      byte[] buf = ((BinaryObjectInspector) poi).getPrimitiveJavaObject(o);
      statsObj.getStatsData().getLongStats().setBitVectors(buf);
    }
  }

  private void unpackStringStats(ObjectInspector oi, Object o, String fName,
      ColumnStatisticsObj statsObj) {
    if (fName.equals("countnulls")) {
      long v = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getStringStats().setNumNulls(v);
    } else if (fName.equals("numdistinctvalues")) {
      long v = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getStringStats().setNumDVs(v);
    } else if (fName.equals("avglength")) {
      double d = ((DoubleObjectInspector) oi).get(o);
      statsObj.getStatsData().getStringStats().setAvgColLen(d);
    } else if (fName.equals("maxlength")) {
      long v = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getStringStats().setMaxColLen(v);
    } else if (fName.equals("ndvbitvector")) {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      byte[] buf = ((BinaryObjectInspector) poi).getPrimitiveJavaObject(o);
      statsObj.getStatsData().getStringStats().setBitVectors(buf);
    }
  }

  private void unpackBinaryStats(ObjectInspector oi, Object o, String fName,
      ColumnStatisticsObj statsObj) {
    if (fName.equals("countnulls")) {
      long v = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getBinaryStats().setNumNulls(v);
    } else if (fName.equals("avglength")) {
      double d = ((DoubleObjectInspector) oi).get(o);
      statsObj.getStatsData().getBinaryStats().setAvgColLen(d);
    } else if (fName.equals("maxlength")) {
      long v = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getBinaryStats().setMaxColLen(v);
    }
  }

  private void unpackDateStats(ObjectInspector oi, Object o, String fName,
      ColumnStatisticsObj statsObj) {
    if (fName.equals("countnulls")) {
      long v = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getDateStats().setNumNulls(v);
    } else if (fName.equals("numdistinctvalues")) {
      long v = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getDateStats().setNumDVs(v);
    } else if (fName.equals("max")) {
      DateWritable v = ((DateObjectInspector) oi).getPrimitiveWritableObject(o);
      statsObj.getStatsData().getDateStats().setHighValue(new Date(v.getDays()));
    } else if (fName.equals("min")) {
      DateWritable v = ((DateObjectInspector) oi).getPrimitiveWritableObject(o);
      statsObj.getStatsData().getDateStats().setLowValue(new Date(v.getDays()));
    } else if (fName.equals("ndvbitvector")) {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      byte[] buf = ((BinaryObjectInspector) poi).getPrimitiveJavaObject(o);
      statsObj.getStatsData().getDateStats().setBitVectors(buf);
    }
  }

  private void unpackPrimitiveObject (ObjectInspector oi, Object o, String fieldName,
      ColumnStatisticsObj statsObj) throws UnsupportedDoubleException {
    if (o == null) {
      return;
    }
    // First infer the type of object
    if (fieldName.equals("columntype")) {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      String s = ((StringObjectInspector) poi).getPrimitiveJavaObject(o);
      ColumnStatisticsData statsData = new ColumnStatisticsData();

      if (s.equalsIgnoreCase("long")) {
        LongColumnStatsDataInspector longStats = new LongColumnStatsDataInspector();
        statsData.setLongStats(longStats);
        statsObj.setStatsData(statsData);
      } else if (s.equalsIgnoreCase("double")) {
        DoubleColumnStatsDataInspector doubleStats = new DoubleColumnStatsDataInspector();
        statsData.setDoubleStats(doubleStats);
        statsObj.setStatsData(statsData);
      } else if (s.equalsIgnoreCase("string")) {
        StringColumnStatsDataInspector stringStats = new StringColumnStatsDataInspector();
        statsData.setStringStats(stringStats);
        statsObj.setStatsData(statsData);
      } else if (s.equalsIgnoreCase("boolean")) {
        BooleanColumnStatsData booleanStats = new BooleanColumnStatsData();
        statsData.setBooleanStats(booleanStats);
        statsObj.setStatsData(statsData);
      } else if (s.equalsIgnoreCase("binary")) {
        BinaryColumnStatsData binaryStats = new BinaryColumnStatsData();
        statsData.setBinaryStats(binaryStats);
        statsObj.setStatsData(statsData);
      } else if (s.equalsIgnoreCase("decimal")) {
        DecimalColumnStatsDataInspector decimalStats = new DecimalColumnStatsDataInspector();
        statsData.setDecimalStats(decimalStats);
        statsObj.setStatsData(statsData);
      } else if (s.equalsIgnoreCase("date")) {
        DateColumnStatsDataInspector dateStats = new DateColumnStatsDataInspector();
        statsData.setDateStats(dateStats);
        statsObj.setStatsData(statsData);
      }
    } else {
      // invoke the right unpack method depending on data type of the column
      if (statsObj.getStatsData().isSetBooleanStats()) {
        unpackBooleanStats(oi, o, fieldName, statsObj);
      } else if (statsObj.getStatsData().isSetLongStats()) {
        unpackLongStats(oi, o, fieldName, statsObj);
      } else if (statsObj.getStatsData().isSetDoubleStats()) {
        unpackDoubleStats(oi,o,fieldName, statsObj);
      } else if (statsObj.getStatsData().isSetStringStats()) {
        unpackStringStats(oi, o, fieldName, statsObj);
      } else if (statsObj.getStatsData().isSetBinaryStats()) {
        unpackBinaryStats(oi, o, fieldName, statsObj);
      } else if (statsObj.getStatsData().isSetDecimalStats()) {
        unpackDecimalStats(oi, o, fieldName, statsObj);
      } else if (statsObj.getStatsData().isSetDateStats()) {
        unpackDateStats(oi, o, fieldName, statsObj);
      }
    }
  }

  private void unpackStructObject(ObjectInspector oi, Object o, String fName,
      ColumnStatisticsObj cStatsObj) throws UnsupportedDoubleException {
    if (oi.getCategory() != ObjectInspector.Category.STRUCT) {
      throw new RuntimeException("Invalid object datatype : " + oi.getCategory().toString());
    }

    StructObjectInspector soi = (StructObjectInspector) oi;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> list = soi.getStructFieldsDataAsList(o);

    for (int i = 0; i < fields.size(); i++) {
      // Get the field objectInspector, fieldName and the field object.
      ObjectInspector foi = fields.get(i).getFieldObjectInspector();
      Object f = (list == null ? null : list.get(i));
      String fieldName = fields.get(i).getFieldName();

      if (foi.getCategory() == ObjectInspector.Category.PRIMITIVE) {
        unpackPrimitiveObject(foi, f, fieldName, cStatsObj);
      } else {
        unpackStructObject(foi, f, fieldName, cStatsObj);
      }
    }
  }

  private List<ColumnStatistics> constructColumnStatsFromPackedRows(
      Hive db) throws HiveException, MetaException, IOException {

    String currentDb = SessionState.get().getCurrentDatabase();
    String tableName = work.getColStats().getTableName();
    String partName = null;
    List<String> colName = work.getColStats().getColName();
    List<String> colType = work.getColStats().getColType();
    boolean isTblLevel = work.getColStats().isTblLevel();

    List<ColumnStatistics> stats = new ArrayList<ColumnStatistics>();
    InspectableObject packedRow;
    Table tbl = db.getTable(currentDb, tableName);
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
      for (int i = 0; i < numOfStatCols; i++) {
        // Get the field objectInspector, fieldName and the field object.
        ObjectInspector foi = fields.get(i).getFieldObjectInspector();
        Object f = (list == null ? null : list.get(i));
        String fieldName = fields.get(i).getFieldName();
        ColumnStatisticsObj statsObj = new ColumnStatisticsObj();
        statsObj.setColName(colName.get(i));
        statsObj.setColType(colType.get(i));
        try {
          unpackStructObject(foi, f, fieldName, statsObj);
          statsObjs.add(statsObj);
        } catch (UnsupportedDoubleException e) {
          // due to infinity or nan.
          LOG.info("Because " + colName.get(i) + " is infinite or NaN, we skip stats.");
        }
      }

      if (!isTblLevel) {
        List<String> partVals = new ArrayList<String>();
        // Iterate over partition columns to figure out partition name
        for (int i = fields.size() - partColSchema.size(); i < fields.size(); i++) {
          Object partVal = ((PrimitiveObjectInspector)fields.get(i).getFieldObjectInspector()).
              getPrimitiveJavaObject(list.get(i));
          partVals.add(partVal == null ? // could be null for default partition
            this.conf.getVar(ConfVars.DEFAULTPARTITIONNAME) : partVal.toString());
        }
        partName = Warehouse.makePartName(partColSchema, partVals);
      }
      String [] names = Utilities.getDbTableName(currentDb, tableName);
      ColumnStatisticsDesc statsDesc = getColumnStatsDesc(names[0], names[1], partName, isTblLevel);
      ColumnStatistics colStats = new ColumnStatistics();
      colStats.setStatsDesc(statsDesc);
      colStats.setStatsObj(statsObjs);
      if (!statsObjs.isEmpty()) {
        stats.add(colStats);
      }
    }
    ftOp.clearFetchContext();
    return stats;
  }

  private ColumnStatisticsDesc getColumnStatsDesc(String dbName, String tableName,
      String partName, boolean isTblLevel)
  {
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
    // Construct a column statistics object from the result
    List<ColumnStatistics> colStats = constructColumnStatsFromPackedRows(db);
    // Persist the column statistics object to the metastore
    // Note, this function is shared for both table and partition column stats.
    if (colStats.isEmpty()) {
      return 0;
    }
    SetPartitionsStatsRequest request = new SetPartitionsStatsRequest(colStats);
    if (work.getColStats() != null && work.getColStats().getNumBitVector() > 0) {
      request.setNeedMerge(true);
    }
    db.setPartitionColumnStatistics(request);
    return 0;
  }

  @Override
  public int execute(DriverContext driverContext) {
    if (driverContext.getCtx().getExplainAnalyze() == AnalyzeState.RUNNING) {
      return 0;
    }
    try {
      Hive db = getHive();
      return persistColumnStats(db);
    } catch (Exception e) {
      LOG.error("Failed to run column stats task", e);
    }
    return 1;
  }

  @Override
  public StageType getType() {
    return StageType.COLUMNSTATS;
  }

  @Override
  public String getName() {
    return "COLUMNSTATS TASK";
  }
}
