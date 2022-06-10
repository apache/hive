/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.columnstats;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.common.ndv.fm.FMSketch;
import org.apache.hadoop.hive.common.ndv.hll.HyperLogLog;
import org.apache.hadoop.hive.metastore.StatisticsTestUtils;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Timestamp;
import org.apache.hadoop.hive.metastore.api.TimestampColumnStatsData;
import org.apache.hadoop.hive.metastore.columnstats.cache.DateColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DecimalColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DoubleColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.LongColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.StringColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.TimestampColumnStatsDataInspector;
import org.apache.thrift.TException;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public class ColStatsBuilder {

  private final static Map<Class<?>, Class<?>> STATS_OBJ_TO_TYPE = ImmutableMap.<Class<?>, Class<?>>builder()
      .put(LongColumnStatsDataInspector.class, long.class)
      .put(DoubleColumnStatsDataInspector.class, double.class)
      .put(DecimalColumnStatsDataInspector.class, Decimal.class)
      .put(TimestampColumnStatsDataInspector.class, Timestamp.class)
      .put(DateColumnStatsDataInspector.class, Date.class)
      .put(StringColumnStatsDataInspector.class, String.class)
      .put(BinaryColumnStatsData.class, byte[].class)
      .build();

  private Object lowValue;
  private Object highValue;
  private Double avgColLen;
  private Long maxColLen;
  private Long numTrues;
  private Long numFalses;
  private Long numNulls;
  private Long numDVs;
  private byte[] bitVector;

  public ColStatsBuilder() {
    // empty constructor
  }

  public ColStatsBuilder numNulls(long num) {
    this.numNulls = num;
    return this;
  }

  public ColStatsBuilder numDVs(long num) {
    this.numDVs = num;
    return this;
  }

  public ColStatsBuilder numFalses(long num) {
    this.numFalses = num;
    return this;
  }

  public ColStatsBuilder numTrues(long num) {
    this.numTrues = num;
    return this;
  }

  public ColStatsBuilder avgColLen(double val) {
    this.avgColLen = val;
    return this;
  }

  public ColStatsBuilder maxColLen(long val) {
    this.maxColLen = val;
    return this;
  }

  public ColStatsBuilder lowValueDate(Date val) {
    this.lowValue = val;
    return this;
  }

  public ColStatsBuilder lowValueDecimal(Decimal val) {
    this.lowValue = val;
    return this;
  }

  public ColStatsBuilder lowValueDouble(double val) {
    this.lowValue = val;
    return this;
  }

  public ColStatsBuilder lowValueLong(long val) {
    this.lowValue = val;
    return this;
  }

  public ColStatsBuilder lowValueTimestamp(Timestamp val) {
    this.lowValue = val;
    return this;
  }

  public ColStatsBuilder highValueDate(Date val) {
    this.highValue = val;
    return this;
  }

  public ColStatsBuilder highValueDecimal(Decimal val) {
    this.highValue = val;
    return this;
  }

  public ColStatsBuilder highValueDouble(double val) {
    this.highValue = val;
    return this;
  }

  public ColStatsBuilder highValueLong(long val) {
    this.highValue = val;
    return this;
  }

  public ColStatsBuilder highValueTimestamp(Timestamp val) {
    this.highValue = val;
    return this;
  }

  public ColStatsBuilder hll(long... values) {
    HyperLogLog hll = StatisticsTestUtils.createHll(values);
    this.bitVector = hll.serialize();
    return this;
  }

  public ColStatsBuilder hll(String... values) {
    HyperLogLog hll = StatisticsTestUtils.createHll(values);
    this.bitVector = hll.serialize();
    return this;
  }

  public ColStatsBuilder fmSketch(long... values) {
    FMSketch fm = StatisticsTestUtils.createFMSketch(values);
    this.bitVector = fm.serialize();
    return this;
  }

  public ColStatsBuilder fmSketch(String... values) {
    FMSketch fm = StatisticsTestUtils.createFMSketch(values);
    this.bitVector = fm.serialize();
    return this;
  }

  public ColumnStatisticsData buildBinaryStats() {
    ColumnStatisticsData data = new ColumnStatisticsData();
    BinaryColumnStatsData stats = newColData(BinaryColumnStatsData.class);
    data.setBinaryStats(stats);
    try {
      stats.validate();
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    return data;
  }

  public ColumnStatisticsData buildBooleanStats() {
    ColumnStatisticsData data = new ColumnStatisticsData();
    BooleanColumnStatsData stats = newColData(BooleanColumnStatsData.class);
    data.setBooleanStats(stats);
    try {
      stats.validate();
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    return data;
  }

  public ColumnStatisticsData buildDateStats() {
    ColumnStatisticsData data = new ColumnStatisticsData();
    DateColumnStatsData stats = newColData(DateColumnStatsDataInspector.class);
    data.setDateStats(stats);
    try {
      stats.validate();
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    return data;
  }

  public ColumnStatisticsData buildDecimalStats() {
    ColumnStatisticsData data = new ColumnStatisticsData();
    DecimalColumnStatsData stats = newColData(DecimalColumnStatsDataInspector.class);
    data.setDecimalStats(stats);
    try {
      stats.validate();
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    return data;
  }

  public ColumnStatisticsData buildDoubleStats() {
    ColumnStatisticsData data = new ColumnStatisticsData();
    DoubleColumnStatsData stats = newColData(DoubleColumnStatsDataInspector.class);
    data.setDoubleStats(stats);
    try {
      stats.validate();
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    return data;
  }

  public ColumnStatisticsData buildLongStats() {
    ColumnStatisticsData data = new ColumnStatisticsData();
    LongColumnStatsData stats = newColData(LongColumnStatsDataInspector.class);
    data.setLongStats(stats);
    try {
      stats.validate();
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    return data;
  }

  public ColumnStatisticsData buildStringStats() {
    ColumnStatisticsData data = new ColumnStatisticsData();
    StringColumnStatsData stats = newColData(StringColumnStatsDataInspector.class);
    data.setStringStats(stats);
    try {
      stats.validate();
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    return data;
  }

  public ColumnStatisticsData buildTimestampStats() {
    ColumnStatisticsData data = new ColumnStatisticsData();
    TimestampColumnStatsData stats = newColData(TimestampColumnStatsDataInspector.class);
    data.setTimestampStats(stats);
    try {
      stats.validate();
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    return data;
  }

  private <T> T newColData(Class<T> clazz) {
    try {
      T data = clazz.getDeclaredConstructor().newInstance();
      if (numNulls != null) {
        clazz.getMethod("setNumNulls", long.class).invoke(data, numNulls);
      }
      if (numDVs != null) {
        clazz.getMethod("setNumDVs", long.class).invoke(data, numDVs);
      }
      if (bitVector != null) {
        clazz.getMethod("setBitVectors", byte[].class).invoke(data, bitVector);
      }
      if (avgColLen != null) {
        clazz.getMethod("setAvgColLen", double.class).invoke(data, avgColLen);
      }
      if (maxColLen != null) {
        clazz.getMethod("setMaxColLen", long.class).invoke(data, maxColLen);
      }
      if (numFalses != null) {
        clazz.getMethod("setNumFalses", long.class).invoke(data, numFalses);
      }
      if (numTrues != null) {
        clazz.getMethod("setNumTrues", long.class).invoke(data, numTrues);
      }
      Class<?> rawClazz = STATS_OBJ_TO_TYPE.get(clazz);
      if (lowValue != null) {
        if (rawClazz.isPrimitive()) {
          clazz.getMethod("setLowValue", rawClazz).invoke(data, lowValue);
        } else {
          clazz.getMethod("setLowValue", rawClazz).invoke(data, rawClazz.cast(lowValue));
        }
      }
      if (highValue != null) {
        if (rawClazz.isPrimitive()) {
          clazz.getMethod("setHighValue", rawClazz).invoke(data, highValue);
        } else {
          clazz.getMethod("setHighValue", rawClazz).invoke(data, rawClazz.cast(highValue));
        }
      }
      return data;
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Reflection error", e);
    }
  }
}
