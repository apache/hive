/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.storage.jdbc.spitter;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestDecimalIntervalSplitter {
  private static final Random RANDOM = new Random(11);
  private final String lowerBound;
  private final String upperBound;
  private final int partitions;
  private final DecimalTypeInfo type;

  public TestDecimalIntervalSplitter(String lowerBound, String upperBound, int partitions, DecimalTypeInfo type) {
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.partitions = partitions;
    this.type = type;
  }

  @Parameterized.Parameters(name = "lowerBound={0}, upperBound={1}, partitions={2}, type={3}")
  public static Iterable<Object[]> generate() {
    List<Object[]> data = new ArrayList<>();
    // The bounds can be set in two ways:
    // i) Get them automatically from the database
    // ii) Let the user specify them using table properties
    // If the values come from the database then they always have the correct scale, and the result of the partitioning
    // will always have the expected scale.

    final int maxPartitions = 10000;
    // Simulates what happens when we read the bounds from a database where both have the same 
    // precision and scale.
    DecimalTypeInfo typeInDB = TypeInfoFactory.getDecimalTypeInfo(5, 3);
    for (int i = 0; i < 100; i++) {
      BigDecimal b1 = generateRandDecimal(typeInDB.getPrecision(), typeInDB.scale());
      BigDecimal b2 = generateRandDecimal(typeInDB.getPrecision(), typeInDB.scale());
      BigDecimal upperBound = b1.compareTo(b2) < 0 ? b2 : b1;
      BigDecimal lowerBound = b1.compareTo(b2) < 0 ? b1 : b2;
      int partitions = RANDOM.nextInt(maxPartitions);
      data.add(new Object[] { lowerBound.toPlainString(), upperBound.toPlainString(), partitions, typeInDB });
    }

    // Simulates what happens when a user specifies the bounds where it can basically any kind
    // of string, and we have no control over it.
    for (int i = 0; i < 100; i++) {
      int precision = RANDOM.nextInt(9) + 1;
      int scale = RANDOM.nextInt(precision);
      BigDecimal b1 = generateRandDecimal(precision, scale);
      BigDecimal b2 = generateRandDecimal(typeInDB.getPrecision(), typeInDB.scale());
      BigDecimal upperBound = b1.compareTo(b2) < 0 ? b2 : b1;
      BigDecimal lowerBound = b1.compareTo(b2) < 0 ? b1 : b2;
      int partitions = RANDOM.nextInt(maxPartitions);
      data.add(new Object[] { lowerBound.toPlainString(), upperBound.toPlainString(), partitions, typeInDB });
    }
    // Below some fixed bounds to capture explicitly certain edge cases:
    //
    // With the current implementation the last interval exceeds the specified upperbound due to rounding.
    data.add(new Object[] { "8.06500000", "93003738.88252007", 1821, typeInDB });
    // Very small bounds where the database type (mostly scale) can play a big role in the interval generation.
    data.add(new Object[] { "0.01", "0.100000000000", 1000, typeInDB });
    return data;
  }

  /**
   * Generates a pseudo random BigDecimal with the specified precision and scale.
   */
  private static BigDecimal generateRandDecimal(int precision, int scale) {
    if (precision < 0 || precision > 9) {
      throw new IllegalArgumentException("Precision " + precision + " out of bounds [0,9]");
    }
    int integerDigits = precision - scale;
    final int integerPart = integerDigits > 0 ? RANDOM.nextInt((int) Math.pow(10, integerDigits)) : 0;
    final int fractionalPart = RANDOM.nextInt((int) Math.pow(10, integerDigits));
    return new BigDecimal(String.format("%d.%3d", integerPart, fractionalPart).replace(' ', '0'));
  }

  @Test
  public void testGetIntervalsCorrectNumberOfPartitions() {
    // The splitter should generate as many partitions as requested by the user if that is possible.
    // If not possible cause the bounds are too close or the partitions requested are too many we
    // should go as close as possible
    DecimalIntervalSplitter splitter = new DecimalIntervalSplitter();
    List<MutablePair<String, String>> bounds = splitter.getIntervals(lowerBound, upperBound, partitions, type);
    BigDecimal lb = new BigDecimal(lowerBound);
    BigDecimal ub = new BigDecimal(upperBound);
    BigInteger unscaledDifference = ub.subtract(lb).unscaledValue();
    int maxPartitions = unscaledDifference.min(BigInteger.valueOf(partitions)).intValue();
    assertEquals(maxPartitions, bounds.size());
  }

  @Test
  public void testGetIntervalsCorrectScale() {
    // The bounds of each interval much have the same scale with the decimal type defined in the database.
    // If the scale is different, then rounding may appear when executing the query to the database.
    // After rounding two intervals, and subsequently two splits may become identical (or overlapping)
    // and due to that fetch the same data twice.
    DecimalIntervalSplitter splitter = new DecimalIntervalSplitter();
    List<MutablePair<String, String>> bounds = splitter.getIntervals(lowerBound, upperBound, partitions, type);
    for (MutablePair<String, String> p : bounds) {
      assertEquals(p.left, type.getScale(), new BigDecimal(p.left).scale());
      assertEquals(p.right, type.getScale(), new BigDecimal(p.right).scale());
    }
  }

  @Test
  public void testGetIntervalsInBounds() {
    DecimalIntervalSplitter splitter = new DecimalIntervalSplitter();
    List<MutablePair<String, String>> bounds = splitter.getIntervals(lowerBound, upperBound, partitions, type);
    BigDecimal lb = new BigDecimal(lowerBound);
    BigDecimal ub = new BigDecimal(upperBound);
    for (MutablePair<String, String> p : bounds) {
      Assert.assertTrue(p.left, lb.compareTo(new BigDecimal(p.left)) <= 0);
      Assert.assertTrue(p.right, ub.compareTo(new BigDecimal(p.right)) >= 0);
    }
  }

  @Test
  public void testGetIntervalsNoGaps() {
    // If there is a gap between two intervals it is problematic cause we may skip reading some data
    // which can lead to incorrect results
    DecimalIntervalSplitter splitter = new DecimalIntervalSplitter();
    List<MutablePair<String, String>> bounds = splitter.getIntervals(lowerBound, upperBound, partitions, type);
    for (int i = 0; i < bounds.size() - 1; i++) {
      MutablePair<String, String> prev = bounds.get(i);
      MutablePair<String, String> next = bounds.get(i + 1);
      BigDecimal pUpper = new BigDecimal(prev.right);
      BigDecimal nLower = new BigDecimal(next.left);
      assertEquals("[LOW, " + pUpper + "),[" + nLower + ", HIGH)", 0, pUpper.compareTo(nLower));
    }
  }

  @Test
  public void testGetIntervalsAreDistinct() {
    // Intervals must be distinct otherwise we are gonna read the same values multiple times leading
    // into erroneous duplicates in the results
    DecimalIntervalSplitter splitter = new DecimalIntervalSplitter();
    List<MutablePair<String, String>> intervals = splitter.getIntervals(lowerBound, upperBound, partitions, type);
    assertEquals(intervals.size(), intervals.stream().distinct().count());
  }

}
