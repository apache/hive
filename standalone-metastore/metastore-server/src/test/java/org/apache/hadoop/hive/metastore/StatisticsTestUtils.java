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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.common.ndv.fm.FMSketch;
import org.apache.hadoop.hive.common.ndv.hll.HyperLogLog;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Timestamp;
import org.apache.hadoop.hive.metastore.api.TimestampColumnStatsData;
import org.apache.hadoop.hive.metastore.columnstats.cache.DateColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DecimalColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DoubleColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.LongColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.StringColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.TimestampColumnStatsDataInspector;
import org.junit.Assert;

import java.util.Collections;

public class StatisticsTestUtils {

  public static final String HIVE_ENGINE = "hive";

  private static final double epsilon = Double.MIN_VALUE;

  private StatisticsTestUtils() {
    throw new AssertionError("Suppress default constructor for non instantiation");
  }

  /*------------ TYPE-AGNOSTIC METHODS ------------*/
  /**
   * Methods creating column statistics for a given table and partition.
   * @param data the statistics data
   * @param tbl the target table
   * @param column the target column
   * @param partName the target partition
   * @return column statistics for a given table and partition.
   */
  public static ColumnStatistics createColStats(ColumnStatisticsData data, Table tbl, FieldSchema column, String partName) {
    ColumnStatisticsObj statObj = new ColumnStatisticsObj(column.getName(), column.getType(), data);
    ColumnStatistics colStats = new ColumnStatistics();
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(true, tbl.getDbName(), tbl.getTableName());
    statsDesc.setPartName(partName);
    colStats.setStatsDesc(statsDesc);
    colStats.setStatsObj(Collections.singletonList(statObj));
    colStats.setEngine(HIVE_ENGINE);
    return colStats;
  }

  /**
   * Creates an FM sketch object initialized with the given values.
   * @param values the values to be added
   * @return an FM sketch initialized with the given values.
   */
  public static FMSketch createFMSketch(long... values) {
    FMSketch fm = new FMSketch(1);
    for (long value : values) {
      fm.addToEstimator(value);
    }
    return fm;
  }

  /**
   * Creates an FM sketch object initialized with the given values.
   * @param values the values to be added
   * @return an FM sketch initialized with the given values.
   */
  public static FMSketch createFMSketch(String... values) {
    FMSketch fm = new FMSketch(1);
    for (String value : values) {
      fm.addToEstimator(value);
    }
    return fm;
  }

  /**
   * Creates an HLL object initialized with the given values.
   * @param values the values to be added
   * @return an HLL object initialized with the given values.
   */
  public static HyperLogLog createHll(long ... values) {
    HyperLogLog hll = HyperLogLog.builder().build();
    for (long value : values) {
      hll.addLong(value);
    }
    return hll;
  }

  /**
   * Creates an HLL object initialized with the given values.
   * @param values the values to be added
   * @return an HLL object initialized with the given values.
   */
  public static HyperLogLog createHll(String ... values) {
    HyperLogLog hll = HyperLogLog.builder().build();
    for (String value : values) {
      hll.addBytes(value.getBytes());
    }
    return hll;
  }

  private static void assertBitVector(Object hll, boolean isSetBitVectors, byte[] bitVector) {
    if (hll == null) {
      Assert.assertFalse("bitVector should not be set", isSetBitVectors);
      Assert.assertNull("bitVector should be non null", bitVector);
    } else {
      Assert.assertTrue("bitVector should be set", isSetBitVectors);
      byte[] bytes;
      if (hll instanceof HyperLogLog) {
        bytes = ((HyperLogLog) hll).serialize();
      } else if (hll instanceof FMSketch) {
        bytes = ((FMSketch) hll).serialize();
      } else {
        throw new IllegalArgumentException("Unsupported HLL class: " + hll.getClass().getName());
      }
      // hll are left as-is
      Assert.assertArrayEquals("hll mismatch", bytes, bitVector);
    }
  }

  /*------------ LONG-RELATED METHODS ------------*/
  /**
   * Method for creating statistics for a column of the Long type family.
   * @param numNulls the number of null values
   * @param numDVs the number of distinct values
   * @param low the low value
   * @param high the high value
   * @param hll the HLL value
   * @return a statistics data object for a column of the Long type family with the sought values.
   */
  public static ColumnStatisticsData createLongStats(long numNulls, long numDVs, Long low, Long high, Object hll) {
    ColumnStatisticsData data = new ColumnStatisticsData();
    LongColumnStatsDataInspector stats = new LongColumnStatsDataInspector();
    if (low != null) {
      stats.setLowValue(low);
    }
    if (high != null) {
      stats.setHighValue(high);
    }
    stats.setNumNulls(numNulls);
    stats.setNumDVs(numDVs);
    if (hll != null) {
      if (hll instanceof HyperLogLog) {
        stats.setBitVectors(((HyperLogLog) hll).serialize());
      } else if (hll instanceof FMSketch) {
        stats.setBitVectors(((FMSketch) hll).serialize());
      } else {
        throw new IllegalArgumentException("Unsupported HLL class: " + hll.getClass().getName());
      }
    }
    data.setLongStats(stats);
    return data;
  }

  /**
   * Methods asserting that computed statistics for a column of family type Long conform to their expected values.
   * @param statsObj the computed statistics object to be compared
   * @param numNulls the expected number of null values
   * @param numDVs the expected number of distinct values
   * @param low the expected low value
   * @param high the expected high value
   * @param hll the expected HLL value
   */
  public static void assertLongStats(ColumnStatisticsObj statsObj, Long numNulls, Long numDVs, Long low, Long high,
      Object hll) {
    Assert.assertNotNull(statsObj);

    LongColumnStatsData stats = statsObj.getStatsData().getLongStats();
    Assert.assertNotNull(stats);

    if (numNulls == null) {
      Assert.assertFalse("numNulls should not be set", stats.isSetNumNulls());
    } else {
      Assert.assertTrue("numNulls should be set", stats.isSetNumNulls());
      // number of nulls is set to the sum
      Assert.assertEquals("numNulls mismatch", (long) numNulls, stats.getNumNulls());
    }

    if (numDVs == null) {
      Assert.assertFalse("numDVs should not be set", stats.isSetNumDVs());
    } else {
      Assert.assertTrue("numDVs should be set", stats.isSetNumDVs());
      // NDV is set to the max
      Assert.assertEquals("numDVs mismatch", (long) numDVs, stats.getNumDVs());
    }

    if (low == null) {
      Assert.assertFalse("low should not be set", stats.isSetLowValue());
    } else {
      Assert.assertTrue("low should be set", stats.isSetLowValue());
      // low is set to the min
      Assert.assertEquals("low mismatch", (long) low, stats.getLowValue());
    }

    if (high == null) {
      Assert.assertFalse("high should not be set", stats.isSetHighValue());
    } else {
      Assert.assertTrue("high should be set", stats.isSetHighValue());
      // high is set to the max
      Assert.assertEquals("high mismatch", (long) high, stats.getHighValue());
    }

    assertBitVector(hll, stats.isSetBitVectors(), stats.getBitVectors());
  }

  /*------------ DOUBLE-RELATED METHODS ------------*/
  /**
   * Method for creating statistics for a column of the Double type family.
   * @param numNulls the number of null values
   * @param numDVs the number of distinct values
   * @param low the low value
   * @param high the high value
   * @param hll the HLL value
   * @return a statistics data object for a column of the Double type family with the sought values.
   */
  public static ColumnStatisticsData createDoubleStats(long numNulls, long numDVs, Double low, Double high, Object hll) {
    ColumnStatisticsData data = new ColumnStatisticsData();
    DoubleColumnStatsDataInspector stats = new DoubleColumnStatsDataInspector();
    if (low != null) {
      stats.setLowValue(low);
    }
    if (high != null) {
      stats.setHighValue(high);
    }
    stats.setNumNulls(numNulls);
    stats.setNumDVs(numDVs);
    if (hll != null) {
      if (hll instanceof HyperLogLog) {
        stats.setBitVectors(((HyperLogLog) hll).serialize());
      } else if (hll instanceof FMSketch) {
        stats.setBitVectors(((FMSketch) hll).serialize());
      } else {
        throw new IllegalArgumentException("Unsupported HLL class: " + hll.getClass().getName());
      }
    }
    data.setDoubleStats(stats);
    return data;
  }

  /**
   * Methods asserting that computed statistics for a column of family type Double conform to their expected values.
   * @param statsObj the computed statistics object to be compared
   * @param numNulls the expected number of null values
   * @param numDVs the expected number of distinct values
   * @param low the expected low value
   * @param high the expected high value
   * @param hll the expected HLL value
   */
  public static void assertDoubleStats(ColumnStatisticsObj statsObj, Long numNulls, Long numDVs, Double low, Double high,
      Object hll) {
    Assert.assertNotNull(statsObj);

    DoubleColumnStatsData stats = statsObj.getStatsData().getDoubleStats();
    Assert.assertNotNull(stats);

    if (numNulls == null) {
      Assert.assertFalse("numNulls should not be set", stats.isSetNumNulls());
    } else {
      Assert.assertTrue("numNulls should be set", stats.isSetNumNulls());
      // number of nulls is set to the sum
      Assert.assertEquals("numNulls mismatch", (long) numNulls, stats.getNumNulls());
    }

    if (numDVs == null) {
      Assert.assertFalse("numDVs should not be set", stats.isSetNumDVs());
    } else {
      Assert.assertTrue("numDVs should be set", stats.isSetNumDVs());
      // NDV is set to the max
      Assert.assertEquals("numDVs mismatch", (long) numDVs, stats.getNumDVs());
    }

    if (low == null) {
      Assert.assertFalse("low should not be set", stats.isSetLowValue());
    } else {
      Assert.assertTrue("low should be set", stats.isSetLowValue());
      // low is set to the min
      Assert.assertEquals("low mismatch", low, stats.getLowValue(), epsilon);
    }

    if (high == null) {
      Assert.assertFalse("high should not be set", stats.isSetHighValue());
    } else {
      Assert.assertTrue("high should be set", stats.isSetHighValue());
      // high is set to the max
      Assert.assertEquals("high mismatch", high, stats.getHighValue(), epsilon);
    }

    assertBitVector(hll, stats.isSetBitVectors(), stats.getBitVectors());
  }

  /*------------ DECIMAL-RELATED METHODS ------------*/
  /**
   * Method for creating statistics for a column of the Decimal type family.
   * @param numNulls the number of null values
   * @param numDVs the number of distinct values
   * @param low the low value
   * @param high the high value
   * @param hll the HLL value
   * @return a statistics data object for a column of the Decimal type family with the sought values.
   */
  public static ColumnStatisticsData createDecimalStats(long numNulls, long numDVs, Decimal low, Decimal high, Object hll) {
    ColumnStatisticsData data = new ColumnStatisticsData();
    DecimalColumnStatsDataInspector stats = new DecimalColumnStatsDataInspector();
    if (low != null) {
      stats.setLowValue(low);
    }
    if (high != null) {
      stats.setHighValue(high);
    }
    stats.setNumNulls(numNulls);
    stats.setNumDVs(numDVs);
    if (hll != null) {
      if (hll instanceof HyperLogLog) {
        stats.setBitVectors(((HyperLogLog) hll).serialize());
      } else if (hll instanceof FMSketch) {
        stats.setBitVectors(((FMSketch) hll).serialize());
      } else {
        throw new IllegalArgumentException("Unsupported HLL class: " + hll.getClass().getName());
      }
    }
    data.setDecimalStats(stats);
    return data;
  }

  /**
   * Methods asserting that computed statistics for a column of family type Decimal conform to their expected values.
   * @param statsObj the computed statistics object to be compared
   * @param numNulls the expected number of null values
   * @param numDVs the expected number of distinct values
   * @param low the expected low value
   * @param high the expected high value
   * @param hll the expected HLL value
   */
  public static void assertDecimalStats(ColumnStatisticsObj statsObj, Long numNulls, Long numDVs, Decimal low,
      Decimal high, Object hll) {
    Assert.assertNotNull(statsObj);

    DecimalColumnStatsData stats = statsObj.getStatsData().getDecimalStats();
    Assert.assertNotNull(stats);

    if (numNulls == null) {
      Assert.assertFalse("numNulls should not be set", stats.isSetNumNulls());
    } else {
      Assert.assertTrue("numNulls should be set", stats.isSetNumNulls());
      // number of nulls is set to the sum
      Assert.assertEquals("numNulls mismatch", (long) numNulls, stats.getNumNulls());
    }

    if (numDVs == null) {
      Assert.assertFalse("numDVs should not be set", stats.isSetNumDVs());
    } else {
      Assert.assertTrue("numDVs should be set", stats.isSetNumDVs());
      // NDV is set to the max
      Assert.assertEquals("numDVs mismatch", (long) numDVs, stats.getNumDVs());
    }

    if (low == null) {
      Assert.assertFalse("low should not be set", stats.isSetLowValue());
    } else {
      Assert.assertTrue("low should be set", stats.isSetLowValue());
      // low is set to the min
      Assert.assertEquals("low scale mismatch", low.getScale(), stats.getLowValue().getScale());
      Assert.assertArrayEquals("low unscaled mismatch", low.getUnscaled(), stats.getLowValue().getUnscaled());
    }

    if (high == null) {
      Assert.assertFalse("high should not be set", stats.isSetHighValue());
    } else {
      Assert.assertTrue("high should be set", stats.isSetHighValue());
      // high is set to the max
      Assert.assertEquals("high scale mismatch", high.getScale(), stats.getHighValue().getScale());
      Assert.assertArrayEquals("high unscaled mismatch", high.getUnscaled(), stats.getHighValue().getUnscaled());
    }

    assertBitVector(hll, stats.isSetBitVectors(), stats.getBitVectors());
  }

  /*------------ DATE-RELATED METHODS ------------*/
  /**
   * Method for creating statistics for a column of the Date type family.
   * @param numNulls the number of null values
   * @param numDVs the number of distinct values
   * @param low the low value
   * @param high the high value
   * @param hll the HLL value
   * @return a statistics data object for a column of the Date type family with the sought values.
   */
  public static ColumnStatisticsData createDateStats(long numNulls, long numDVs, Date low, Date high, Object hll) {
    ColumnStatisticsData data = new ColumnStatisticsData();
    DateColumnStatsDataInspector stats = new DateColumnStatsDataInspector();
    if (low != null) {
      stats.setLowValue(low);
    }
    if (high != null) {
      stats.setHighValue(high);
    }
    stats.setNumNulls(numNulls);
    stats.setNumDVs(numDVs);
    if (hll != null) {
      if (hll instanceof HyperLogLog) {
        stats.setBitVectors(((HyperLogLog) hll).serialize());
      } else if (hll instanceof FMSketch) {
        stats.setBitVectors(((FMSketch) hll).serialize());
      } else {
        throw new IllegalArgumentException("Unsupported HLL class: " + hll.getClass().getName());
      }
    }
    data.setDateStats(stats);
    return data;
  }

  /**
   * Methods asserting that computed statistics for a column of family type Date conform to their expected values.
   * @param statsObj the computed statistics object to be compared
   * @param numNulls the expected number of null values
   * @param numDVs the expected number of distinct values
   * @param low the expected low value
   * @param high the expected high value
   * @param hll the expected HLL value
   */
  public static void assertDateStats(ColumnStatisticsObj statsObj, Long numNulls, Long numDVs, Date low, Date high,
      Object hll) {
    Assert.assertNotNull(statsObj);

    DateColumnStatsData stats = statsObj.getStatsData().getDateStats();
    Assert.assertNotNull(stats);

    if (numNulls == null) {
      Assert.assertFalse("numNulls should not be set", stats.isSetNumNulls());
    } else {
      Assert.assertTrue("numNulls should be set", stats.isSetNumNulls());
      // number of nulls is set to the sum
      Assert.assertEquals("numNulls mismatch", (long) numNulls, stats.getNumNulls());
    }

    if (numDVs == null) {
      Assert.assertFalse("numDVs should not be set", stats.isSetNumDVs());
    } else {
      Assert.assertTrue("numDVs should be set", stats.isSetNumDVs());
      // NDV is set to the max
      Assert.assertEquals("numDVs mismatch", (long) numDVs, stats.getNumDVs());
    }

    if (low == null) {
      Assert.assertFalse("low should not be set", stats.isSetLowValue());
    } else {
      Assert.assertTrue("low should be set", stats.isSetLowValue());
      // low is set to the min
      Assert.assertEquals("low mismatch", low, stats.getLowValue());
    }

    if (high == null) {
      Assert.assertFalse("high should not be set", stats.isSetHighValue());
    } else {
      Assert.assertTrue("high should be set", stats.isSetHighValue());
      // high is set to the max
      Assert.assertEquals("high mismatch", high, stats.getHighValue());
    }

    assertBitVector(hll, stats.isSetBitVectors(), stats.getBitVectors());
  }

  /*------------ TIMESTAMP-RELATED METHODS ------------*/
  /**
   * Method for creating statistics for a column of the Timestamp type family.
   * @param numNulls the number of null values
   * @param numDVs the number of distinct values
   * @param low the low value
   * @param high the high value
   * @param hll the HLL value
   * @return a statistics data object for a column of the Timestamp type family with the sought values.
   */
  public static ColumnStatisticsData createTimestampStats(
      long numNulls, long numDVs, Timestamp low, Timestamp high, Object hll) {
    ColumnStatisticsData data = new ColumnStatisticsData();
    TimestampColumnStatsDataInspector stats = new TimestampColumnStatsDataInspector();
    if (low != null) {
      stats.setLowValue(low);
    }
    if (high != null) {
      stats.setHighValue(high);
    }
    stats.setNumNulls(numNulls);
    stats.setNumDVs(numDVs);
    if (hll != null) {
      if (hll instanceof HyperLogLog) {
        stats.setBitVectors(((HyperLogLog) hll).serialize());
      } else if (hll instanceof FMSketch) {
        stats.setBitVectors(((FMSketch) hll).serialize());
      } else {
        throw new IllegalArgumentException("Unsupported HLL class: " + hll.getClass().getName());
      }
    }
    data.setTimestampStats(stats);
    return data;
  }

  /**
   * Methods asserting that computed statistics for a column of family type Timestamp conform to their expected values.
   * @param statsObj the computed statistics object to be compared
   * @param numNulls the expected number of null values
   * @param numDVs the expected number of distinct values
   * @param low the expected low value
   * @param high the expected high value
   * @param hll the expected HLL value
   */
  public static void assertTimestampStats(
      ColumnStatisticsObj statsObj, Long numNulls, Long numDVs, Timestamp low, Timestamp high, Object hll) {
    Assert.assertNotNull(statsObj);

    TimestampColumnStatsData stats = statsObj.getStatsData().getTimestampStats();
    Assert.assertNotNull(stats);

    if (numNulls == null) {
      Assert.assertFalse("numNulls should not be set", stats.isSetNumNulls());
    } else {
      Assert.assertTrue("numNulls should be set", stats.isSetNumNulls());
      // number of nulls is set to the sum
      Assert.assertEquals("numNulls mismatch", (long) numNulls, stats.getNumNulls());
    }

    if (numDVs == null) {
      Assert.assertFalse("numDVs should not be set", stats.isSetNumDVs());
    } else {
      Assert.assertTrue("numDVs should be set", stats.isSetNumDVs());
      // NDV is set to the max
      Assert.assertEquals("numDVs mismatch", (long) numDVs, stats.getNumDVs());
    }

    if (low == null) {
      Assert.assertFalse("low should not be set", stats.isSetLowValue());
    } else {
      Assert.assertTrue("low should be set", stats.isSetLowValue());
      // low is set to the min
      Assert.assertEquals("low mismatch", low, stats.getLowValue());
    }

    if (high == null) {
      Assert.assertFalse("high should not be set", stats.isSetHighValue());
    } else {
      Assert.assertTrue("high should be set", stats.isSetHighValue());
      // high is set to the max
      Assert.assertEquals("high mismatch", high, stats.getHighValue());
    }

    assertBitVector(hll, stats.isSetBitVectors(), stats.getBitVectors());
  }

  /*------------ STRING-RELATED METHODS ------------*/
  /**
   * Method for creating statistics for a column of the String type family.
   * @param numNulls the number of null values
   * @param numDVs the number of distinct values
   * @param avgColLen the expected average column length value
   * @param maxColLen the expected max column length value
   * @param hll the HLL value
   * @return a statistics data object for a column of the String type family with the sought values.
   */
  public static ColumnStatisticsData createStringStats(
      long numNulls, long numDVs, Double avgColLen, Long maxColLen, Object hll) {
    ColumnStatisticsData data = new ColumnStatisticsData();
    StringColumnStatsDataInspector stats = new StringColumnStatsDataInspector();
    if (avgColLen != null) {
      stats.setAvgColLen(avgColLen);
    }
    if (maxColLen != null) {
      stats.setMaxColLen(maxColLen);
    }
    stats.setNumNulls(numNulls);
    stats.setNumDVs(numDVs);
    if (hll != null) {
      if (hll instanceof HyperLogLog) {
        stats.setBitVectors(((HyperLogLog) hll).serialize());
      } else if (hll instanceof FMSketch) {
        stats.setBitVectors(((FMSketch) hll).serialize());
      } else {
        throw new IllegalArgumentException("Unsupported HLL class: " + hll.getClass().getName());
      }
    }
    data.setStringStats(stats);
    return data;
  }

  /**
   * Methods asserting that computed statistics for a column of family type String conform to their expected values.
   * @param statsObj the computed statistics object to be compared
   * @param numNulls the expected number of null values
   * @param numDVs the expected number of distinct values
   * @param avgColLen the expected average column length value
   * @param maxColLen the expected max column length value
   * @param hll the expected HLL value
   */
  public static void assertStringStats(
      ColumnStatisticsObj statsObj, Long numNulls, Long numDVs, Double avgColLen, Long maxColLen, Object hll) {
    Assert.assertNotNull(statsObj);

    StringColumnStatsData stats = statsObj.getStatsData().getStringStats();
    Assert.assertNotNull(stats);

    if (numNulls == null) {
      Assert.assertFalse("numNulls should not be set", stats.isSetNumNulls());
    } else {
      Assert.assertTrue("numNulls should be set", stats.isSetNumNulls());
      // number of nulls is set to the sum
      Assert.assertEquals("numNulls mismatch", (long) numNulls, stats.getNumNulls());
    }

    if (numDVs == null) {
      Assert.assertFalse("numDVs should not be set", stats.isSetNumDVs());
    } else {
      Assert.assertTrue("numDVs should be set", stats.isSetNumDVs());
      // NDV is set to the max
      Assert.assertEquals("numDVs mismatch", (long) numDVs, stats.getNumDVs());
    }

    if (avgColLen == null) {
      Assert.assertFalse("avgColLen should not be set", stats.isSetAvgColLen());
    } else {
      Assert.assertTrue("avgColLen should be set", stats.isSetAvgColLen());
      // avgColLen is set to the max
      Assert.assertEquals("avgColLen mismatch", avgColLen, stats.getAvgColLen(), epsilon);
    }

    if (maxColLen == null) {
      Assert.assertFalse("maxColLen should not be set", stats.isSetMaxColLen());
    } else {
      Assert.assertTrue("maxColLen should be set", stats.isSetMaxColLen());
      // maxColLen is set to the max
      Assert.assertEquals("maxColLen mismatch", (long) maxColLen, stats.getMaxColLen());
    }

    assertBitVector(hll, stats.isSetBitVectors(), stats.getBitVectors());
  }

  /*------------ BOOLEAN-RELATED METHODS ------------*/
  /**
   * Method for creating statistics for a column of the Boolean type family.
   * @param numNulls the number of null values
   * @param numFalses the number of false values
   * @param numTrues the number of true values
   * @return a statistics data object for a column of the Boolean type family with the sought values.
   */
  public static ColumnStatisticsData createBooleanStats(long numNulls, Long numFalses, Long numTrues) {
    ColumnStatisticsData data = new ColumnStatisticsData();
    BooleanColumnStatsData stats = new BooleanColumnStatsData();
    if (numFalses != null) {
      stats.setNumFalses(numFalses);
    }
    if (numTrues != null) {
      stats.setNumTrues(numTrues);
    }
    stats.setNumNulls(numNulls);

    data.setBooleanStats(stats);
    return data;
  }

  /**
   * Methods asserting that computed statistics for a column of family type Boolean conform to their expected values.
   * @param statsObj the computed statistics object to be compared
   * @param numNulls the expected number of null values
   * @param numFalses the expected number of false values
   * @param numTrues the expected number of true values
   */
  public static void assertBooleanStats(
      ColumnStatisticsObj statsObj, Long numNulls, Long numFalses, Long numTrues) {
    Assert.assertNotNull(statsObj);

    BooleanColumnStatsData stats = statsObj.getStatsData().getBooleanStats();
    Assert.assertNotNull(stats);

    if (numNulls == null) {
      Assert.assertFalse("numNulls should not be set", stats.isSetNumNulls());
    } else {
      Assert.assertTrue("numNulls should be set", stats.isSetNumNulls());
      // number of nulls is set to the sum
      Assert.assertEquals("numNulls mismatch", (long) numNulls, stats.getNumNulls());
    }

    if (numFalses == null) {
      Assert.assertFalse("numFalses should not be set", stats.isSetNumFalses());
    } else {
      Assert.assertTrue("numFalses should be set", stats.isSetNumFalses());
      // numFalses is set to the sum
      Assert.assertEquals("numFalses mismatch", (long) numFalses, stats.getNumFalses());
    }

    if (numTrues == null) {
      Assert.assertFalse("numTrues should not be set", stats.isSetNumTrues());
    } else {
      Assert.assertTrue("numTrues should be set", stats.isSetNumTrues());
      // numTrues is set to the sum
      Assert.assertEquals("numTrues mismatch", (long) numTrues, stats.getNumTrues());
    }
  }

  /*------------ BINARY-RELATED METHODS ------------*/
  /**
   * Method for creating statistics for a column of the Binary type family.
   * @param numNulls the number of null values
   * @param avgColLen the expected average column length value
   * @param maxColLen the expected max column length value
   * @return a statistics data object for a column of the Binary type family with the sought values.
   */
  public static ColumnStatisticsData createBinaryStats(long numNulls, Double avgColLen, Long maxColLen) {
    ColumnStatisticsData data = new ColumnStatisticsData();
    BinaryColumnStatsData stats = new BinaryColumnStatsData();
    if (avgColLen != null) {
      stats.setAvgColLen(avgColLen);
    }
    if (maxColLen != null) {
      stats.setMaxColLen(maxColLen);
    }
    stats.setNumNulls(numNulls);

    data.setBinaryStats(stats);
    return data;
  }

  /**
   * Methods asserting that computed statistics for a column of family type Binary conform to their expected values.
   * @param statsObj the computed statistics object to be compared
   * @param numNulls the expected number of null values
   * @param avgColLen the expected average column length value
   * @param maxColLen the expected max column length value
   */
  public static void assertBinaryStats(
      ColumnStatisticsObj statsObj, Long numNulls, Double avgColLen, Long maxColLen) {
    Assert.assertNotNull(statsObj);

    BinaryColumnStatsData stats = statsObj.getStatsData().getBinaryStats();
    Assert.assertNotNull(stats);

    if (numNulls == null) {
      Assert.assertFalse("numNulls should not be set", stats.isSetNumNulls());
    } else {
      Assert.assertTrue("numNulls should be set", stats.isSetNumNulls());
      // number of nulls is set to the sum
      Assert.assertEquals("numNulls mismatch", (long) numNulls, stats.getNumNulls());
    }

    if (avgColLen == null) {
      Assert.assertFalse("avgColLen should not be set", stats.isSetAvgColLen());
    } else {
      Assert.assertTrue("avgColLen should be set", stats.isSetAvgColLen());
      // avgColLen is set to the max
      Assert.assertEquals("avgColLen mismatch", avgColLen, stats.getAvgColLen(), epsilon);
    }

    if (maxColLen == null) {
      Assert.assertFalse("maxColLen should not be set", stats.isSetMaxColLen());
    } else {
      Assert.assertTrue("maxColLen should be set", stats.isSetMaxColLen());
      // maxColLen is set to the max
      Assert.assertEquals("maxColLen mismatch", (long) maxColLen, stats.getMaxColLen());
    }
  }
}
