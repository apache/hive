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

  public TestDecimalIntervalSplitter(String lowerBound, String upperBound, int partitions) {
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.partitions = partitions;
  }

  @Parameterized.Parameters(name = "lowerBound={0}, upperBound={1}, partitions={2}")
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
    for (int i = 0; i < 1000; i++) {
      BigDecimal b1 = generateRandDecimal(5, 3);
      BigDecimal b2 = generateRandDecimal(5, 3);
      BigDecimal upperBound = b1.compareTo(b2) < 0 ? b2 : b1;
      BigDecimal lowerBound = b1.compareTo(b2) < 0 ? b1 : b2;
      int partitions = RANDOM.nextInt(maxPartitions);
      data.add(new Object[] { lowerBound.toPlainString(), upperBound.toPlainString(), partitions });
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
      data.add(new Object[] { lowerBound.toPlainString(), upperBound.toPlainString(), partitions });
    }

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
  public void testGetIntevalsCorrectNumberOfPartitions() {
    DecimalIntervalSplitter splitter = new DecimalIntervalSplitter();
    List<MutablePair<String, String>> bounds = splitter.getIntervals(lowerBound, upperBound, partitions);
    BigDecimal lb = new BigDecimal(lowerBound);
    BigDecimal ub = new BigDecimal(upperBound);
    BigInteger unscaledDifference = ub.subtract(lb).unscaledValue();
    int maxPartitions = unscaledDifference.min(BigInteger.valueOf(partitions)).intValue();
    // Check we have the right number of partitions
    Assert.assertEquals(maxPartitions, bounds.size());
  }

  @Test
  public void testGetIntevalsCorrectScale() {
    DecimalIntervalSplitter splitter = new DecimalIntervalSplitter();
    List<MutablePair<String, String>> bounds = splitter.getIntervals(lowerBound, upperBound, partitions);
    int lowerBoundScale = countFractionalDigits(lowerBound);
    int upperBoundScale = countFractionalDigits(upperBound);
    int expectedScale = Math.max(lowerBoundScale, upperBoundScale);
    for (MutablePair p : bounds) {
      // Check values have correct scale
      String lower = (String) p.left;
      String upper = (String) p.right;
      int lowerScale = lower.substring(lower.indexOf('.') + 1).length();
      int upperScale = upper.substring(upper.indexOf('.') + 1).length();
      Assert.assertEquals(lower, expectedScale, lowerScale);
      Assert.assertEquals(upper, expectedScale, upperScale);
    }
  }

  @Test
  public void testGetIntevalsInBounds() {
    DecimalIntervalSplitter splitter = new DecimalIntervalSplitter();
    List<MutablePair<String, String>> bounds = splitter.getIntervals(lowerBound, upperBound, partitions);
    BigDecimal lb = new BigDecimal(lowerBound);
    BigDecimal ub = new BigDecimal(upperBound);
    for (MutablePair p : bounds) {
      String lower = (String) p.left;
      String upper = (String) p.right;
      Assert.assertTrue(lower, lb.compareTo(new BigDecimal(lower)) <= 0);
      Assert.assertTrue(upper, ub.compareTo(new BigDecimal(upper)) >= 0);
    }
  }

  private static int countFractionalDigits(String decimal) {
    int fractionalStart = decimal.indexOf('.');
    return fractionalStart >= 0 ? decimal.substring(fractionalStart + 1).length() : 0;
  }
}
