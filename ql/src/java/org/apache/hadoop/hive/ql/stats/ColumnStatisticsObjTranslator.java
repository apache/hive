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

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.columnstats.cache.DateColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DecimalColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DoubleColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.LongColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.StringColumnStatsDataInspector;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DateWritable;
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

public class ColumnStatisticsObjTranslator {

  public static ColumnStatisticsObj readHiveStruct(String columnName, String columnType, StructField structField, Object values)
      throws HiveException
  {
    // Get the field objectInspector, fieldName and the field object.
    ObjectInspector foi = structField.getFieldObjectInspector();
    Object f = values;
    String fieldName = structField.getFieldName();
    ColumnStatisticsObj statsObj = new ColumnStatisticsObj();
    statsObj.setColName(columnName);
    statsObj.setColType(columnType);
    try {
      unpackStructObject(foi, f, fieldName, statsObj);
      return statsObj;
    } catch (Exception e) {
      throw new HiveException("error calculating stats for column:" + structField.getFieldName(), e);
    }
  }

  private static void unpackBooleanStats(ObjectInspector oi, Object o, String fName, ColumnStatisticsObj statsObj) {
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
  static class UnsupportedDoubleException extends Exception {
  }

  private static void unpackDoubleStats(ObjectInspector oi, Object o, String fName, ColumnStatisticsObj statsObj) throws UnsupportedDoubleException {
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
      ;
    }
  }

  private static void unpackDecimalStats(ObjectInspector oi, Object o, String fName, ColumnStatisticsObj statsObj) {
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
      ;
    }
  }

  private static Decimal convertToThriftDecimal(HiveDecimal d) {
    return new Decimal(ByteBuffer.wrap(d.unscaledValue().toByteArray()), (short) d.scale());
  }

  private static void unpackLongStats(ObjectInspector oi, Object o, String fName, ColumnStatisticsObj statsObj) {
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
      long v = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getLongStats().setLowValue(v);
    } else if (fName.equals("ndvbitvector")) {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      byte[] buf = ((BinaryObjectInspector) poi).getPrimitiveJavaObject(o);
      statsObj.getStatsData().getLongStats().setBitVectors(buf);
      ;
    }
  }

  private static void unpackStringStats(ObjectInspector oi, Object o, String fName, ColumnStatisticsObj statsObj) {
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
      ;
    }
  }

  private static void unpackBinaryStats(ObjectInspector oi, Object o, String fName, ColumnStatisticsObj statsObj) {
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

  private static void unpackDateStats(ObjectInspector oi, Object o, String fName, ColumnStatisticsObj statsObj) {
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
      ;
    }
  }

  private static void unpackPrimitiveObject(ObjectInspector oi, Object o, String fieldName, ColumnStatisticsObj statsObj) throws UnsupportedDoubleException {
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
        unpackDoubleStats(oi, o, fieldName, statsObj);
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

  private static void unpackStructObject(ObjectInspector oi, Object o, String fName, ColumnStatisticsObj cStatsObj) throws UnsupportedDoubleException {
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
}
