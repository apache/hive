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

package org.apache.hadoop.hive.ql.exec.vector;

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBase;
import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperGeneral;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hive.common.util.IntervalDayTimeUtils;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestVectorHashKeyWrapperGeneral {

  private VectorHashKeyWrapperBase.HashContext hashContext = new VectorHashKeyWrapperBase.HashContext();

  @Test
  public void testClone() {
    VectorHashKeyWrapperGeneral expected = generateKeyWrapper(120, 324, 1024, 10, 10, 456, 1024, 100);

    VectorHashKeyWrapperGeneral actual = (VectorHashKeyWrapperGeneral) expected.copyKey();

    assertEquals(expected, actual);
    assertEquals(expected.hashCode(), actual.hashCode());
  }

  @Test
  public void testCopyKeyWhenTargetHasSameDimensions() {
    VectorHashKeyWrapperGeneral expected = generateKeyWrapper(120, 324, 1024, 10, 10, 456, 1024, 100);
    VectorHashKeyWrapperGeneral source = generateKeyWrapper(120, 324, 1024, 10, 10, 456, 1024, 100);

    // will reuse this to copy kw and see if they are equal
    VectorHashKeyWrapperGeneral actual = generateKeyWrapper(120, 324, 1024, 10, 10, 456, 1024, 12403);

    assertEquals(expected, source);
    assertNotEquals(expected, actual);

    expected.copyKey(actual);

    assertEquals(expected, actual);
    assertEquals(expected.hashCode(), actual.hashCode());
    assertEquals(expected, source);
    assertEquals(expected.hashCode(), source.hashCode());
  }

  @Test
  public void testCopyKey() {
    VectorHashKeyWrapperGeneral kwExpected = generateKeyWrapper(10, 10);
    VectorHashKeyWrapperGeneral kwSource = generateKeyWrapper(10, 10);

    // will reuse this to copy kwSource and see if they are equal
    VectorHashKeyWrapperGeneral kwCopied = generateKeyWrapper(10, 327434);

    assertEquals(kwExpected, kwSource);
    assertNotEquals(kwCopied, kwSource);

    kwSource.copyKey(kwCopied);

    assertEquals(kwExpected, kwCopied);
  }

  @Test
  public void testCopyKeyDoesNotMutateTheSourceObject() {
    VectorHashKeyWrapperGeneral expected = generateKeyWrapper(10, 10);
    int expectedHashCode = expected.hashCode();

    // will reuse this to copy kw and see if they are equal
    VectorHashKeyWrapperGeneral actual = generateKeyWrapper(10, 42363546);

    expected.copyKey(actual);
    expected.setHashKey();

    assertEquals(expectedHashCode, expected.hashCode());
  }

  private VectorHashKeyWrapperGeneral generateKeyWrapper(int byteValuesCount, int seed) {
    Random random = new Random(seed);
    final int maxStringLen = 1024;
    byte[] bytes = new byte[byteValuesCount * maxStringLen];
    random.nextBytes(bytes);

    VectorHashKeyWrapperGeneral kw = new VectorHashKeyWrapperGeneral(hashContext, 0, 0, byteValuesCount,0,0, 0, 0);
    int start = 0;
    for (int i = 0; i < byteValuesCount; i++) {
      int len = random.nextInt(maxStringLen);
      kw.assignString(i, bytes, start, len);
      start += len;
    }
    kw.setHashKey();
    return kw;
  }

  private VectorHashKeyWrapperGeneral generateKeyWrapper(int longValuesCount, int doubleValuesCount, int byteValuesCount, int decimalValuesCount, int timestampValuesCount, int intervalCount, int keyCount, int seed) {
    Random random = new Random(seed);
    final int maxStringLen = 1024;
    byte[] bytes = new byte[byteValuesCount * maxStringLen];
    random.nextBytes(bytes);

    VectorHashKeyWrapperGeneral kw = new VectorHashKeyWrapperGeneral(hashContext, longValuesCount, doubleValuesCount, byteValuesCount, decimalValuesCount, timestampValuesCount, intervalCount, keyCount);
    int start = 0;
    for (int i = 0; i < byteValuesCount; i++) {
      // 10% null values
      if (random.nextInt(100) < 10) {
        kw.assignNullString(i, i);
      }
      int len = random.nextInt(maxStringLen);
      kw.assignString(i, bytes, start, len);
      start += len;
    }

    for (int i = 0; i < longValuesCount; i++) {
      kw.assignLong(i, random.nextLong());
    }

    for (int i = 0; i < doubleValuesCount; i++) {
      kw.assignDouble(i, random.nextDouble());
    }

    for (int i = 0; i < decimalValuesCount; i++) {
      kw.assignDecimal(i, new HiveDecimalWritable(random.nextInt()));
    }

    for (int i = 0; i < timestampValuesCount; i++) {
      kw.assignTimestamp(i, new Timestamp(random.nextLong()));
    }

    for (int i = 0; i < intervalCount; i++) {
      kw.assignIntervalDayTime(i, new HiveIntervalDayTime(random.nextLong(), random.nextInt(IntervalDayTimeUtils.NANOS_PER_SEC)));
    }

    kw.setHashKey();
    return kw;
  }
}
