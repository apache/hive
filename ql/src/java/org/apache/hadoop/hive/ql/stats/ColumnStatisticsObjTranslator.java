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
import org.apache.hadoop.hive.metastore.api.Timestamp;
import org.apache.hadoop.hive.metastore.api.utils.DecimalUtils;
import org.apache.hadoop.hive.metastore.columnstats.cache.DateColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DecimalColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DoubleColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.LongColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.StringColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.TimestampColumnStatsDataInspector;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.stats.ColStatsProcessor.ColumnStatsField;
import org.apache.hadoop.hive.ql.stats.ColStatsProcessor.ColumnStatsType;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;


public class ColumnStatisticsObjTranslator {

  public static ColumnStatisticsObj readHiveColumnStatistics(String columnName, String columnType,
      List<ColumnStatsField> columnStatsFields, int start, List<? extends StructField> fields,
      List<Object> values) throws HiveException {
    ColumnStatisticsObj statsObj = new ColumnStatisticsObj();
    statsObj.setColName(columnName);
    statsObj.setColType(columnType);

    int end = start + columnStatsFields.size();
    for (int i = start; i < end; i++) {
      ObjectInspector foi = fields.get(i).getFieldObjectInspector();
      Object v = values.get(i);
      try {
        unpackPrimitiveObject(foi, v, columnStatsFields.get(i - start), statsObj);
      } catch (Exception e) {
        throw new HiveException("Error calculating statistics for column:" + columnName, e);
      }
    }

    return statsObj;
  }

  private static void unpackBooleanStats(ObjectInspector oi, Object o,
      ColumnStatsField csf, ColumnStatisticsObj statsObj) {
    long v = ((LongObjectInspector) oi).get(o);
    switch (csf) {
    case COUNT_TRUES:
      statsObj.getStatsData().getBooleanStats().setNumTrues(v);
      break;
    case COUNT_FALSES:
      statsObj.getStatsData().getBooleanStats().setNumFalses(v);
      break;
    case COUNT_NULLS:
      statsObj.getStatsData().getBooleanStats().setNumNulls(v);
      break;
    default:
      throw new RuntimeException("Unsupported column stat for BOOLEAN : " + csf);
    }
  }

  @SuppressWarnings("serial")
  static class UnsupportedDoubleException extends Exception {
  }

  private static void unpackDoubleStats(ObjectInspector oi, Object o,
      ColumnStatsField csf, ColumnStatisticsObj statsObj) throws UnsupportedDoubleException {
    switch (csf) {
    case COUNT_NULLS:
      long cn = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getDoubleStats().setNumNulls(cn);
      break;
    case MIN:
      double min = ((DoubleObjectInspector) oi).get(o);
      if (Double.isInfinite(min) || Double.isNaN(min)) {
        throw new UnsupportedDoubleException();
      }
      statsObj.getStatsData().getDoubleStats().setLowValue(min);
      break;
    case MAX:
      double max = ((DoubleObjectInspector) oi).get(o);
      if (Double.isInfinite(max) || Double.isNaN(max)) {
        throw new UnsupportedDoubleException();
      }
      statsObj.getStatsData().getDoubleStats().setHighValue(max);
      break;
    case NDV:
      long ndv = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getDoubleStats().setNumDVs(ndv);
      break;
    case BITVECTOR:
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      byte[] buf = ((BinaryObjectInspector) poi).getPrimitiveJavaObject(o);
      statsObj.getStatsData().getDoubleStats().setBitVectors(buf);
      break;
    case KLL_SKETCH:
      PrimitiveObjectInspector poi2 = (PrimitiveObjectInspector) oi;
      byte[] buf2 = ((BinaryObjectInspector) poi2).getPrimitiveJavaObject(o);
      statsObj.getStatsData().getDoubleStats().setHistogram(buf2);
      break;
    default:
      throw new RuntimeException("Unsupported column stat for DOUBLE : " + csf);
    }
  }

  private static void unpackDecimalStats(ObjectInspector oi, Object o,
      ColumnStatsField csf, ColumnStatisticsObj statsObj) {
    switch (csf) {
    case COUNT_NULLS:
      long cn = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getDecimalStats().setNumNulls(cn);
      break;
    case MIN:
      HiveDecimal min = ((HiveDecimalObjectInspector) oi).getPrimitiveJavaObject(o);
      statsObj.getStatsData().getDecimalStats().setLowValue(convertToThriftDecimal(min));
      break;
    case MAX:
      HiveDecimal max = ((HiveDecimalObjectInspector) oi).getPrimitiveJavaObject(o);
      statsObj.getStatsData().getDecimalStats().setHighValue(convertToThriftDecimal(max));
      break;
    case NDV:
      long ndv = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getDecimalStats().setNumDVs(ndv);
      break;
    case BITVECTOR:
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      byte[] buf = ((BinaryObjectInspector) poi).getPrimitiveJavaObject(o);
      statsObj.getStatsData().getDecimalStats().setBitVectors(buf);
      break;
    case KLL_SKETCH:
      PrimitiveObjectInspector poi2 = (PrimitiveObjectInspector) oi;
      byte[] buf2 = ((BinaryObjectInspector) poi2).getPrimitiveJavaObject(o);
      statsObj.getStatsData().getDecimalStats().setHistogram(buf2);
      break;
    default:
      throw new RuntimeException("Unsupported column stat for DECIMAL : " + csf);
    }
  }

  private static Decimal convertToThriftDecimal(HiveDecimal d) {
    return DecimalUtils.getDecimal(ByteBuffer.wrap(d.unscaledValue().toByteArray()), (short) d.scale());
  }

  private static void unpackLongStats(ObjectInspector oi, Object o,
      ColumnStatsField csf, ColumnStatisticsObj statsObj) {
    switch (csf) {
    case COUNT_NULLS:
      long cn = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getLongStats().setNumNulls(cn);
      break;
    case MIN:
      long min = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getLongStats().setLowValue(min);
      break;
    case MAX:
      long max = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getLongStats().setHighValue(max);
      break;
    case NDV:
      long ndv = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getLongStats().setNumDVs(ndv);
      break;
    case BITVECTOR:
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      byte[] buf = ((BinaryObjectInspector) poi).getPrimitiveJavaObject(o);
      statsObj.getStatsData().getLongStats().setBitVectors(buf);
      break;
    case KLL_SKETCH:
      PrimitiveObjectInspector poi2 = (PrimitiveObjectInspector) oi;
      byte[] buf2 = ((BinaryObjectInspector) poi2).getPrimitiveJavaObject(o);
      statsObj.getStatsData().getLongStats().setHistogram(buf2);
      break;
    default:
      throw new RuntimeException("Unsupported column stat for LONG : " + csf);
    }
  }

  private static void unpackStringStats(ObjectInspector oi, Object o,
      ColumnStatsField csf, ColumnStatisticsObj statsObj) {
    switch (csf) {
    case COUNT_NULLS:
      long cn = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getStringStats().setNumNulls(cn);
      break;
    case NDV:
      long ndv = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getStringStats().setNumDVs(ndv);
      break;
    case BITVECTOR:
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      byte[] buf = ((BinaryObjectInspector) poi).getPrimitiveJavaObject(o);
      statsObj.getStatsData().getStringStats().setBitVectors(buf);
      break;
    case MAX_LENGTH:
      long max = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getStringStats().setMaxColLen(max);
      break;
    case AVG_LENGTH:
      double avg = ((DoubleObjectInspector) oi).get(o);
      statsObj.getStatsData().getStringStats().setAvgColLen(avg);
      break;
    default:
      throw new RuntimeException("Unsupported column stat for STRING : " + csf);
    }
  }

  private static void unpackBinaryStats(ObjectInspector oi, Object o,
      ColumnStatsField csf, ColumnStatisticsObj statsObj) {
    switch (csf) {
    case COUNT_NULLS:
      long cn = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getBinaryStats().setNumNulls(cn);
      break;
    case AVG_LENGTH:
      double avg = ((DoubleObjectInspector) oi).get(o);
      statsObj.getStatsData().getBinaryStats().setAvgColLen(avg);
      break;
    case MAX_LENGTH:
      long v = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getBinaryStats().setMaxColLen(v);
      break;
    default:
      throw new RuntimeException("Unsupported column stat for BINARY : " + csf);
    }
  }

  private static void unpackDateStats(ObjectInspector oi, Object o,
      ColumnStatsField csf, ColumnStatisticsObj statsObj) {
    switch (csf) {
    case COUNT_NULLS:
      long cn = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getDateStats().setNumNulls(cn);
      break;
    case MIN:
      DateWritableV2 min = ((DateObjectInspector) oi).getPrimitiveWritableObject(o);
      statsObj.getStatsData().getDateStats().setLowValue(new Date(min.getDays()));
      break;
    case MAX:
      DateWritableV2 max = ((DateObjectInspector) oi).getPrimitiveWritableObject(o);
      statsObj.getStatsData().getDateStats().setHighValue(new Date(max.getDays()));
      break;
    case NDV:
      long ndv = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getDateStats().setNumDVs(ndv);
      break;
    case BITVECTOR:
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      byte[] buf = ((BinaryObjectInspector) poi).getPrimitiveJavaObject(o);
      statsObj.getStatsData().getDateStats().setBitVectors(buf);
      break;
    case KLL_SKETCH:
      PrimitiveObjectInspector poi2 = (PrimitiveObjectInspector) oi;
      byte[] buf2 = ((BinaryObjectInspector) poi2).getPrimitiveJavaObject(o);
      statsObj.getStatsData().getDateStats().setHistogram(buf2);
      break;
    default:
      throw new RuntimeException("Unsupported column stat for DATE : " + csf);
    }
  }

  private static void unpackTimestampStats(ObjectInspector oi, Object o,
      ColumnStatsField csf, ColumnStatisticsObj statsObj) {
    switch (csf) {
    case COUNT_NULLS:
      long cn = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getTimestampStats().setNumNulls(cn);
      break;
    case MIN:
      TimestampWritableV2 min = ((TimestampObjectInspector) oi).getPrimitiveWritableObject(o);
      statsObj.getStatsData().getTimestampStats().setLowValue(new Timestamp(min.getSeconds()));
      break;
    case MAX:
      TimestampWritableV2 max = ((TimestampObjectInspector) oi).getPrimitiveWritableObject(o);
      statsObj.getStatsData().getTimestampStats().setHighValue(new Timestamp(max.getSeconds()));
      break;
    case NDV:
      long ndv = ((LongObjectInspector) oi).get(o);
      statsObj.getStatsData().getTimestampStats().setNumDVs(ndv);
      break;
    case BITVECTOR:
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      byte[] buf = ((BinaryObjectInspector) poi).getPrimitiveJavaObject(o);
      statsObj.getStatsData().getTimestampStats().setBitVectors(buf);
      break;
    case KLL_SKETCH:
      PrimitiveObjectInspector poi2 = (PrimitiveObjectInspector) oi;
      byte[] buf2 = ((BinaryObjectInspector) poi2).getPrimitiveJavaObject(o);
      statsObj.getStatsData().getTimestampStats().setHistogram(buf2);
      break;
    default:
      throw new RuntimeException("Unsupported column stat for TIMESTAMP : " + csf);
    }
  }

  private static void unpackPrimitiveObject(ObjectInspector oi, Object o,
      ColumnStatsField csf, ColumnStatisticsObj statsObj) throws UnsupportedDoubleException {
    if (o == null) {
      return;
    }
    // First infer the type of object
    if (csf == ColumnStatsField.COLUMN_STATS_TYPE) {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      String s = ((StringObjectInspector) poi).getPrimitiveJavaObject(o);
      ColumnStatisticsData statsData = new ColumnStatisticsData();

      if (s.equalsIgnoreCase(ColumnStatsType.LONG.toString())) {
        LongColumnStatsDataInspector longStats = new LongColumnStatsDataInspector();
        statsData.setLongStats(longStats);
        statsObj.setStatsData(statsData);
      } else if (s.equalsIgnoreCase(ColumnStatsType.DOUBLE.toString())) {
        DoubleColumnStatsDataInspector doubleStats = new DoubleColumnStatsDataInspector();
        statsData.setDoubleStats(doubleStats);
        statsObj.setStatsData(statsData);
      } else if (s.equalsIgnoreCase(ColumnStatsType.STRING.toString())) {
        StringColumnStatsDataInspector stringStats = new StringColumnStatsDataInspector();
        statsData.setStringStats(stringStats);
        statsObj.setStatsData(statsData);
      } else if (s.equalsIgnoreCase(ColumnStatsType.BOOLEAN.toString())) {
        BooleanColumnStatsData booleanStats = new BooleanColumnStatsData();
        statsData.setBooleanStats(booleanStats);
        statsObj.setStatsData(statsData);
      } else if (s.equalsIgnoreCase(ColumnStatsType.BINARY.toString())) {
        BinaryColumnStatsData binaryStats = new BinaryColumnStatsData();
        statsData.setBinaryStats(binaryStats);
        statsObj.setStatsData(statsData);
      } else if (s.equalsIgnoreCase(ColumnStatsType.DECIMAL.toString())) {
        DecimalColumnStatsDataInspector decimalStats = new DecimalColumnStatsDataInspector();
        statsData.setDecimalStats(decimalStats);
        statsObj.setStatsData(statsData);
      } else if (s.equalsIgnoreCase(ColumnStatsType.DATE.toString())) {
        DateColumnStatsDataInspector dateStats = new DateColumnStatsDataInspector();
        statsData.setDateStats(dateStats);
        statsObj.setStatsData(statsData);
      } else if (s.equalsIgnoreCase(ColumnStatsType.TIMESTAMP.toString())) {
        TimestampColumnStatsDataInspector timestampStats = new TimestampColumnStatsDataInspector();
        statsData.setTimestampStats(timestampStats);
        statsObj.setStatsData(statsData);
      }
    } else {
      // invoke the right unpack method depending on data type of the column
      if (statsObj.getStatsData().isSetBooleanStats()) {
        unpackBooleanStats(oi, o, csf, statsObj);
      } else if (statsObj.getStatsData().isSetLongStats()) {
        unpackLongStats(oi, o, csf, statsObj);
      } else if (statsObj.getStatsData().isSetDoubleStats()) {
        unpackDoubleStats(oi, o, csf, statsObj);
      } else if (statsObj.getStatsData().isSetStringStats()) {
        unpackStringStats(oi, o, csf, statsObj);
      } else if (statsObj.getStatsData().isSetBinaryStats()) {
        unpackBinaryStats(oi, o, csf, statsObj);
      } else if (statsObj.getStatsData().isSetDecimalStats()) {
        unpackDecimalStats(oi, o, csf, statsObj);
      } else if (statsObj.getStatsData().isSetDateStats()) {
        unpackDateStats(oi, o, csf, statsObj);
      } else if (statsObj.getStatsData().isSetTimestampStats()) {
        unpackTimestampStats(oi, o, csf, statsObj);
      }
    }
  }
}
