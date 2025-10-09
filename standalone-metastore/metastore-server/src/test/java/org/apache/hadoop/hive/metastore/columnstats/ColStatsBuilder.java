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

import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.hadoop.hive.common.ndv.fm.FMSketch;
import org.apache.hadoop.hive.common.ndv.hll.HyperLogLog;
import org.apache.hadoop.hive.metastore.StatisticsTestUtils;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.Timestamp;
import org.apache.hadoop.hive.metastore.columnstats.cache.DateColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DecimalColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DoubleColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.LongColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.StringColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.TimestampColumnStatsDataInspector;

import java.lang.reflect.InvocationTargetException;

public class ColStatsBuilder<T> {

  private final Class<T> type;
  private T lowValue;
  private T highValue;
  private Double avgColLen;
  private Long maxColLen;
  private Long numTrues;
  private Long numFalses;
  private Long numNulls;
  private Long numDVs;
  private byte[] bitVector;
  private byte[] kll;

  public ColStatsBuilder(Class<T> type) {
    this.type = type;
  }

  public ColStatsBuilder<T> numNulls(long num) {
    this.numNulls = num;
    return this;
  }

  public ColStatsBuilder<T> numDVs(long num) {
    this.numDVs = num;
    return this;
  }

  public ColStatsBuilder<T> numFalses(long num) {
    this.numFalses = num;
    return this;
  }

  public ColStatsBuilder<T> numTrues(long num) {
    this.numTrues = num;
    return this;
  }

  public ColStatsBuilder<T> avgColLen(double val) {
    this.avgColLen = val;
    return this;
  }

  public ColStatsBuilder<T> maxColLen(long val) {
    this.maxColLen = val;
    return this;
  }

  public ColStatsBuilder<T> low(T val) {
    this.lowValue = val;
    return this;
  }

  public ColStatsBuilder<T> high(T val) {
    this.highValue = val;
    return this;
  }

  public ColStatsBuilder<T> hll(long... values) {
    HyperLogLog hll = StatisticsTestUtils.createHll(values);
    this.bitVector = hll.serialize();
    return this;
  }

  public ColStatsBuilder<T> hll(String... values) {
    HyperLogLog hll = StatisticsTestUtils.createHll(values);
    this.bitVector = hll.serialize();
    return this;
  }

  public ColStatsBuilder<T> hll(double... values) {
    HyperLogLog hll = StatisticsTestUtils.createHll(values);
    this.bitVector = hll.serialize();
    return this;
  }

  public ColStatsBuilder<T> fmSketch(long... values) {
    FMSketch fm = StatisticsTestUtils.createFMSketch(values);
    this.bitVector = fm.serialize();
    return this;
  }

  public ColStatsBuilder<T> fmSketch(String... values) {
    FMSketch fm = StatisticsTestUtils.createFMSketch(values);
    this.bitVector = fm.serialize();
    return this;
  }

  public ColStatsBuilder<T> kll(long... values) {
    KllFloatsSketch kll = StatisticsTestUtils.createKll(values);
    this.kll = kll.toByteArray();
    return this;
  }

  public ColStatsBuilder<T> kll(double... values) {
    KllFloatsSketch kll = StatisticsTestUtils.createKll(values);
    this.kll = kll.toByteArray();
    return this;
  }

  public ColumnStatisticsData build() {
    ColumnStatisticsData data = new ColumnStatisticsData();
    if (type == byte[].class) {
      data.setBinaryStats(newColData(BinaryColumnStatsData.class));
    } else if (type == Boolean.class) {
      data.setBooleanStats(newColData(BooleanColumnStatsData.class));
    } else if (type == Date.class) {
      data.setDateStats(newColData(DateColumnStatsDataInspector.class));
    } else if (type == Decimal.class) {
      data.setDecimalStats(newColData(DecimalColumnStatsDataInspector.class));
    } else if (type == double.class) {
      data.setDoubleStats(newColData(DoubleColumnStatsDataInspector.class));
    } else if (type == long.class) {
      data.setLongStats(newColData(LongColumnStatsDataInspector.class));
    } else if (type == String.class) {
      data.setStringStats(newColData(StringColumnStatsDataInspector.class));
    } else if (type == Timestamp.class) {
      data.setTimestampStats(newColData(TimestampColumnStatsDataInspector.class));
    } else {
      throw new IllegalStateException(type.getSimpleName() + " is not supported");
    }
    return data;
  }

  private <X> X newColData(Class<X> clazz) {
    try {
      X data = clazz.getDeclaredConstructor().newInstance();
      if (numNulls != null) {
        clazz.getMethod("setNumNulls", long.class).invoke(data, numNulls);
      }
      if (numDVs != null) {
        clazz.getMethod("setNumDVs", long.class).invoke(data, numDVs);
      }
      if (bitVector != null) {
        clazz.getMethod("setBitVectors", byte[].class).invoke(data, bitVector);
      }
      if (kll != null) {
        clazz.getMethod("setHistogram", byte[].class).invoke(data, kll);
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

      if (lowValue != null) {
        if (type.isPrimitive()) {
          clazz.getMethod("setLowValue", type).invoke(data, lowValue);
        } else {
          clazz.getMethod("setLowValue", type).invoke(data, type.cast(lowValue));
        }
      }
      if (highValue != null) {
        if (type.isPrimitive()) {
          clazz.getMethod("setHighValue", type).invoke(data, highValue);
        } else {
          clazz.getMethod("setHighValue", type).invoke(data, type.cast(highValue));
        }
      }
      clazz.getMethod("validate").invoke(data);
      return data;
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Reflection error", e);
    }
  }
}
