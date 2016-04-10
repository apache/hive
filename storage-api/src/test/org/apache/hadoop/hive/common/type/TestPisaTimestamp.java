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

package org.apache.hadoop.hive.common.type;

import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.Random;
import org.apache.hadoop.hive.common.type.RandomTypeUtil;

import static org.junit.Assert.*;

/**
 * Test for ListColumnVector
 */
public class TestPisaTimestamp {

  private static int TEST_COUNT = 5000;

  @Test
  public void testPisaTimestampCreate() throws Exception {

    Random r = new Random(1234);

    for (int i = 0; i < TEST_COUNT; i++) {
      Timestamp randTimestamp = RandomTypeUtil.getRandTimestamp(r);
      PisaTimestamp pisaTimestamp = new PisaTimestamp(randTimestamp);
      Timestamp reconstructedTimestamp = new Timestamp(0);
      pisaTimestamp.timestampUpdate(reconstructedTimestamp);
      if (!randTimestamp.equals(reconstructedTimestamp)) {
        assertTrue(false);
      }
    }
  }

  static BigDecimal BIG_MAX_LONG = new BigDecimal(Long.MAX_VALUE);
  static BigDecimal BIG_MIN_LONG = new BigDecimal(Long.MIN_VALUE);
  static BigDecimal BIG_NANOSECONDS_PER_DAY = new BigDecimal(PisaTimestamp.NANOSECONDS_PER_DAY);

  static boolean beyondLongRange = false;

  private BigDecimal[] randomEpochDayAndNanoOfDay(Random r) {
    double randDouble = (r.nextDouble() - 0.5D) * 2.0D;
    randDouble *= PisaTimestamp.NANOSECONDS_PER_DAY;
    randDouble *= 365 * 10000;
    BigDecimal bigDecimal = new BigDecimal(randDouble);
    bigDecimal = bigDecimal.setScale(0, RoundingMode.HALF_UP);

    if (bigDecimal.compareTo(BIG_MAX_LONG) > 0 || bigDecimal.compareTo(BIG_MIN_LONG) < 0) {
      beyondLongRange = true;
    }

    BigDecimal[] divideAndRemainder = bigDecimal.divideAndRemainder(BIG_NANOSECONDS_PER_DAY);

    return new BigDecimal[] {divideAndRemainder[0], divideAndRemainder[1], bigDecimal};
  }

  private BigDecimal pisaTimestampToBig(PisaTimestamp pisaTimestamp) {
    BigDecimal bigNanoOfDay = new BigDecimal(pisaTimestamp.getNanoOfDay());

    BigDecimal bigEpochDay = new BigDecimal(pisaTimestamp.getEpochDay());
    BigDecimal result = bigEpochDay.multiply(BIG_NANOSECONDS_PER_DAY);
    result = result.add(bigNanoOfDay);
    return result;
  }

  @Test
  public void testPisaTimestampArithmetic() throws Exception {

    Random r = new Random(1234);


    for (int i = 0; i < TEST_COUNT; i++) {
      BigDecimal[] random1 = randomEpochDayAndNanoOfDay(r);
      long epochDay1 = random1[0].longValue();
      long nanoOfDay1 = random1[1].longValue();
      PisaTimestamp pisa1 = new PisaTimestamp(epochDay1, nanoOfDay1);
      BigDecimal big1 = random1[2];

      BigDecimal[] random2 = randomEpochDayAndNanoOfDay(r);
      long epochDay2 = random2[0].longValue();
      long nanoOfDay2 = random2[1].longValue();
      PisaTimestamp pisa2 = new PisaTimestamp(epochDay2, nanoOfDay2);
      BigDecimal big2 = random2[2];

      BigDecimal expectedBig;
      PisaTimestamp pisaResult = new PisaTimestamp();
      if (i % 2 == 0) {
        expectedBig = big1.add(big2);
        PisaTimestamp.add(pisa1, pisa2, pisaResult);
      } else {
        expectedBig = big1.add(big2.negate());
        PisaTimestamp.subtract(pisa1, pisa2, pisaResult);
      }
      BigDecimal resultBig = pisaTimestampToBig(pisaResult);
      assertEquals(expectedBig, resultBig);

    }
  }
}
