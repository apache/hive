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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
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

@RunWith(Parameterized.class)
public class TestDecimalIntervalSplitter {
  private static final Random RANDOM = new Random(11);
  private final String lowerBound;
  private final String upperBound;
  private final int partitions;
  private DecimalTypeInfo type;

  public TestDecimalIntervalSplitter(String lowerBound, String upperBound, int partitions, DecimalTypeInfo type) {
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.partitions = partitions;
    this.type = type;
  }

  @Parameterized.Parameters(name = "lowerBound={0}, upperBound={1}, partitions={2}, type={3}")
  public static Iterable<Object[]> generate() {
    List<Object[]> data = new ArrayList<>();
    TypeInfo decimal = TypeInfoFactory.getDecimalTypeInfo(5, 3);
    // Lets assume the database stores decimals (5,3)
    // Initially we have two possibilities obtain values from the database or let the user specify them
    // If the values come from the database then they always have the correct scale, and the result of the partitioning
    // will always have the expected scale.

    final int maxPartitions = 10000;
    // Generate bounds with the same precision and scale.
    // Simulates what happens when we read the values from a database.
    DecimalTypeInfo typeInDB = TypeInfoFactory.getDecimalTypeInfo(5, 3);
    for (int i = 0; i < 1000; i++) {
      BigDecimal b1 = generateRandDecimal(typeInDB.getPrecision(), typeInDB.scale());
      BigDecimal b2 = generateRandDecimal(typeInDB.getPrecision(), typeInDB.scale());
      BigDecimal upperBound = b1.compareTo(b2) < 0 ? b2 : b1;
      BigDecimal lowerBound = b1.compareTo(b2) < 0 ? b1 : b2;
      int partitions = RANDOM.nextInt(maxPartitions);
      data.add(new Object[] { lowerBound.toPlainString(), upperBound.toPlainString(), partitions, typeInDB });
    }

    // Generate bounds with the different precision and scale.
    // Simulates what happens when a user specifies the bounds.
    for (int i = 0; i < 100; i++) {
      int precision = RANDOM.nextInt(9) + 1;
      int scale = RANDOM.nextInt(precision);
      BigDecimal b1 = generateRandDecimal(precision, scale);
      BigDecimal b2 = generateRandDecimal(5, 3);
      BigDecimal upperBound = b1.compareTo(b2) < 0 ? b2 : b1;
      BigDecimal lowerBound = b1.compareTo(b2) < 0 ? b1 : b2;
      int partitions = RANDOM.nextInt(maxPartitions);
      data.add(new Object[] { lowerBound.toPlainString(), upperBound.toPlainString(), partitions, typeInDB });
    }
    // Hardcoded bounds for a DECIMAL(16,8) that may lead into problems.
    // Depending on the implementation the last interval may exceed the
    // specified upperbound due to rounding.
    data.add(new Object[] { "8.06500000", "93003738.88252007", 1821, typeInDB });
    // TODO Explain why
    data.add(new Object[] { "0.01", "0.100000000000", 1000, typeInDB });
    return data;
  }

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
    DecimalIntervalSplitter splitter = new DecimalIntervalSplitter();
    List<MutablePair<String, String>> bounds = splitter.getIntervals(lowerBound, upperBound, partitions, type);
    BigDecimal lb = new BigDecimal(lowerBound);
    BigDecimal ub = new BigDecimal(upperBound);
    BigInteger unscaledDifference = ub.subtract(lb).unscaledValue();
    int maxPartitions = unscaledDifference.min(BigInteger.valueOf(partitions)).intValue();
    // Check we have the right number of partitions
    Assert.assertEquals(maxPartitions, bounds.size());
  }

  @Test
  public void testGetIntervalsCorrectScale() {
    DecimalIntervalSplitter splitter = new DecimalIntervalSplitter();
    List<MutablePair<String, String>> bounds = splitter.getIntervals(lowerBound, upperBound, partitions, type);
    for (MutablePair p : bounds) {
      String lower = (String) p.left;
      String upper = (String) p.right;
      int lowerScale = lower.substring(lower.indexOf('.') + 1).length();
      int upperScale = upper.substring(upper.indexOf('.') + 1).length();
      Assert.assertEquals(lower, type.getScale(), lowerScale);
      Assert.assertEquals(upper, type.getScale(), upperScale);
    }
  }

  @Test
  public void testGetIntervalsInBounds() {
    DecimalIntervalSplitter splitter = new DecimalIntervalSplitter();
    List<MutablePair<String, String>> bounds = splitter.getIntervals(lowerBound, upperBound, partitions, type);
    BigDecimal lb = new BigDecimal(lowerBound);
    BigDecimal ub = new BigDecimal(upperBound);
    for (MutablePair p : bounds) {
      String lower = (String) p.left;
      String upper = (String) p.right;
      Assert.assertTrue(lower, lb.compareTo(new BigDecimal(lower)) <= 0);
      Assert.assertTrue(upper, ub.compareTo(new BigDecimal(upper)) >= 0);
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
      Assert.assertTrue("[LOW, " + pUpper + "),[" + nLower + ", HIGH)", pUpper.compareTo(nLower) == 0);
    }
  }

  @Test
  public void testGetIntervalsAreDistinct() {
    // Intervals must be distinct otherwise we are gonna read the same values multiple times leading
    // into erroneous duplicates in the results
    DecimalIntervalSplitter splitter = new DecimalIntervalSplitter();
    List<MutablePair<String, String>> intervals = splitter.getIntervals(lowerBound, upperBound, partitions, type);
    Assert.assertEquals(intervals.size(), intervals.stream().distinct().count());
  }

}
