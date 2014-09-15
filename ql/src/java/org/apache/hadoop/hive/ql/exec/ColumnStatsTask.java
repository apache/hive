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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
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
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.ColumnStatsWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

/**
 * ColumnStatsTask implementation.
 **/

public class ColumnStatsTask extends Task<ColumnStatsWork> implements Serializable {
  private static final long serialVersionUID = 1L;
  private FetchOperator ftOp;
  private static transient final Log LOG = LogFactory.getLog(ColumnStatsTask.class);

  public ColumnStatsTask() {
    super();
  }

  @Override
  public void initialize(HiveConf conf, QueryPlan queryPlan, DriverContext ctx) {
    super.initialize(conf, queryPlan, ctx);
    work.initializeForFetch();
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

  private void unpackDoubleStats(ObjectInspector oi, Object o, String fName,
      ColumnStatisticsObj statsObj) {
    if (fName.equals("countnulls")) {
      long v = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getDoubleStats().setNumNulls(v);
    } else if (fName.equals("numdistinctvalues")) {
      long v = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getDoubleStats().setNumDVs(v);
    } else if (fName.equals("max")) {
      double d = ((DoubleObjectInspector) oi).get(o);
      statsObj.getStatsData().getDoubleStats().setHighValue(d);
    } else if (fName.equals("min")) {
      double d = ((DoubleObjectInspector) oi).get(o);
      statsObj.getStatsData().getDoubleStats().setLowValue(d);
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

  private void unpackPrimitiveObject (ObjectInspector oi, Object o, String fieldName,
      ColumnStatisticsObj statsObj) {
    if (o == null) {
      return;
    }
    // First infer the type of object
    if (fieldName.equals("columntype")) {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      String s = ((StringObjectInspector) poi).getPrimitiveJavaObject(o);
      ColumnStatisticsData statsData = new ColumnStatisticsData();

      if (s.equalsIgnoreCase("long")) {
        LongColumnStatsData longStats = new LongColumnStatsData();
        statsData.setLongStats(longStats);
        statsObj.setStatsData(statsData);
      } else if (s.equalsIgnoreCase("double")) {
        DoubleColumnStatsData doubleStats = new DoubleColumnStatsData();
        statsData.setDoubleStats(doubleStats);
        statsObj.setStatsData(statsData);
      } else if (s.equalsIgnoreCase("string")) {
        StringColumnStatsData stringStats = new StringColumnStatsData();
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
        DecimalColumnStatsData decimalStats = new DecimalColumnStatsData();
        statsData.setDecimalStats(decimalStats);
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
      }
    }
  }

  private void unpackStructObject(ObjectInspector oi, Object o, String fName,
      ColumnStatisticsObj cStatsObj) {
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

  private List<ColumnStatistics> constructColumnStatsFromPackedRows() throws HiveException, MetaException, IOException {

    String dbName = SessionState.get().getCurrentDatabase();
    String tableName = work.getColStats().getTableName();
    String partName = null;
    List<String> colName = work.getColStats().getColName();
    List<String> colType = work.getColStats().getColType();
    boolean isTblLevel = work.getColStats().isTblLevel();

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

      Table tbl = db.getTable(dbName,tableName);
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
        unpackStructObject(foi, f, fieldName, statsObj);
        statsObjs.add(statsObj);
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

      ColumnStatisticsDesc statsDesc = getColumnStatsDesc(dbName, tableName, partName, isTblLevel);
      ColumnStatistics colStats = new ColumnStatistics();
      colStats.setStatsDesc(statsDesc);
      colStats.setStatsObj(statsObjs);
      stats.add(colStats);
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

  private int persistPartitionStats() throws HiveException, MetaException, IOException {

    // Fetch result of the analyze table partition (p1=c1).. compute statistics for columns ..
    // Construct a column statistics object from the result
    List<ColumnStatistics> colStats = constructColumnStatsFromPackedRows();
    // Persist the column statistics object to the metastore
    db.setPartitionColumnStatistics(new SetPartitionsStatsRequest(colStats));
    return 0;
  }

  private int persistTableStats() throws HiveException, MetaException, IOException {
    // Fetch result of the analyze table .. compute statistics for columns ..
    // Construct a column statistics object from the result
    ColumnStatistics colStats = constructColumnStatsFromPackedRows().get(0);
    // Persist the column statistics object to the metastore
    db.updateTableColumnStatistics(colStats);
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
    return "COLUMNSTATS TASK";
  }
}
