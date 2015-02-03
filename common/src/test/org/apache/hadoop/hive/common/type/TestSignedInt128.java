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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Testcases for {@link SignedInt128}
 *
 * This code was based on code from Microsoft PolyBase.
 */
public class TestSignedInt128 {
  private SignedInt128 zero;

  private SignedInt128 one;

  private SignedInt128 two;

  private SignedInt128 negativeOne;

  private SignedInt128 negativeTwo;

  @Before
  public void setUp() throws Exception {
    zero = new SignedInt128(0);
    one = new SignedInt128(1);
    two = new SignedInt128(2);
    negativeOne = new SignedInt128(-1);
    negativeTwo = new SignedInt128(-2);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testHashCode() {
    assertTrue(one.hashCode() != two.hashCode());
    assertTrue(zero.hashCode() != one.hashCode());
    assertTrue(zero.hashCode() != two.hashCode());

    assertTrue(one.hashCode() != negativeOne.hashCode());
    assertTrue(two.hashCode() != negativeTwo.hashCode());
    assertEquals(zero.hashCode(), new SignedInt128(-0).hashCode());

    assertEquals(zero.hashCode(), new SignedInt128(0).hashCode());
    assertEquals(one.hashCode(), new SignedInt128(1).hashCode());
    assertEquals(two.hashCode(), new SignedInt128(2).hashCode());
  }

  @Test
  public void testEquals() {
    assertTrue(!one.equals(two));
    assertTrue(!zero.equals(one));
    assertTrue(!zero.equals(two));

    assertEquals(zero, new SignedInt128(0));
    assertEquals(one, new SignedInt128(1));
    assertEquals(two, new SignedInt128(2));

    assertTrue(!one.equals(negativeOne));
    assertTrue(!two.equals(negativeTwo));
    assertEquals(zero, new SignedInt128(-0));
  }

  @Test
  public void testCompareTo() {
    assertTrue(one.compareTo(two) < 0);
    assertTrue(two.compareTo(one) > 0);
    assertTrue(one.compareTo(zero) > 0);
    assertTrue(zero.compareTo(two) < 0);

    assertTrue(zero.compareTo(negativeOne) > 0);
    assertTrue(zero.compareTo(negativeTwo) > 0);
    assertTrue(one.compareTo(negativeOne) > 0);
    assertTrue(one.compareTo(negativeTwo) > 0);
    assertTrue(two.compareTo(negativeOne) > 0);
    assertTrue(two.compareTo(negativeTwo) > 0);
    assertTrue(negativeOne.compareTo(negativeTwo) > 0);
    assertTrue(negativeTwo.compareTo(negativeOne) < 0);
  }

  @Test
  public void testToFormalString() {
    assertEquals("0", zero.toFormalString());
    assertEquals("1", one.toFormalString());
    assertEquals("-1", negativeOne.toFormalString());
    assertEquals("-2", negativeTwo.toFormalString());

    assertEquals("30", new SignedInt128(30).toFormalString());
    assertEquals("680000000000",
        new SignedInt128(680000000000L).toFormalString());
    assertEquals("6800000000000",
        new SignedInt128(6800000000000L).toFormalString());
    assertEquals("68", new SignedInt128(68).toFormalString());

    assertEquals("-30", new SignedInt128(-30).toFormalString());
    assertEquals("-680000000000",
        new SignedInt128(-680000000000L).toFormalString());
    assertEquals("-6800000000000",
        new SignedInt128(-6800000000000L).toFormalString());
    assertEquals("-68", new SignedInt128(-68).toFormalString());

    assertEquals(zero, new SignedInt128("0"));
    assertEquals(one, new SignedInt128("1"));

    assertEquals(zero, new SignedInt128("-0"));
    assertEquals(negativeOne, new SignedInt128("-1"));
    assertEquals(negativeTwo, new SignedInt128("-2"));

    assertEquals(new SignedInt128(30), new SignedInt128("30"));
    assertEquals(new SignedInt128(680000000000L), new SignedInt128(
        "680000000000"));
    assertEquals(new SignedInt128(6800000000000L), new SignedInt128(
        "6800000000000"));
    assertEquals(new SignedInt128(68), new SignedInt128("68"));

    assertEquals(new SignedInt128(-30), new SignedInt128("-30"));
    assertEquals(new SignedInt128(-680000000000L), new SignedInt128(
        "-680000000000"));
    assertEquals(new SignedInt128(-6800000000000L), new SignedInt128(
        "-6800000000000"));
    assertEquals(new SignedInt128(-68), new SignedInt128("-68"));
  }

  @Test
  public void testSignedInt128() {
    assertEquals(0L, new SignedInt128().longValue());
  }

  @Test
  public void testSignedInt128SignedInt128() {
    assertEquals(1L, new SignedInt128(one).longValue());
    assertEquals(2L, new SignedInt128(two).longValue());
  }

  @Test
  public void testSignedInt128IntIntIntInt() {
    assertEquals(((long) 11) << 32L | 23L,
        new SignedInt128(23, 11, 0, 0).longValue());
  }

  @Test
  public void testZeroClear() {
    assertFalse(one.isZero());
    assertFalse(two.isZero());
    assertTrue(0L != one.longValue());
    assertTrue(0L != two.longValue());

    two.zeroClear();

    assertTrue(0L != one.longValue());
    assertEquals(0L, two.longValue());
    assertFalse(one.isZero());
    assertTrue(two.isZero());

    one.zeroClear();

    assertEquals(0L, one.longValue());
    assertEquals(0L, two.longValue());
    assertTrue(one.isZero());
    assertTrue(two.isZero());
  }

  @Test
  public void testAddDestructive() {
    one.addDestructive(two);
    assertEquals(3L, one.longValue());
    assertEquals(2L, two.longValue());

    SignedInt128 big = new SignedInt128((1L << 62) + 3L);
    SignedInt128 tmp = new SignedInt128(0L);
    for (int i = 0; i < 54; ++i) {
      tmp.addDestructive(big);
    }

    assertEquals(3 * 54, tmp.getV0());
    assertEquals(0x80000000, tmp.getV1()); // (54 % 4) << 62
    assertEquals(13, tmp.getV2()); // 54/4
    assertEquals(0, tmp.getV3());

    assertEquals((1L << 62) + 3L, big.longValue());

    SignedInt128 huge = new SignedInt128(one);
    huge.shiftLeftDestructive(125);
    SignedInt128 huge2 = new SignedInt128(one);
    huge2.shiftLeftDestructive(125);
    try {
      huge2.addDestructive(huge);
      fail();
    } catch (ArithmeticException ex) {
      // ok
    }
  }

  @Test
  public void testSubtractDestructive() {
    two.subtractDestructive(one);
    assertEquals(1L, one.longValue());
    assertEquals(1L, one.longValue());

    one.subtractDestructive(new SignedInt128(10L));
    assertEquals(-9L, one.longValue());

    SignedInt128 big = new SignedInt128((1L << 62) + (3L << 34) + 3L);
    big.shiftLeftDestructive(6);
    SignedInt128 tmp = new SignedInt128((1L << 61) + 5L);
    tmp.shiftLeftDestructive(6);

    big.subtractDestructive(tmp);
    big.subtractDestructive(tmp);

    assertEquals((3 << 6) - 2 * (5 << 6), big.getV0());
    assertEquals((3 << 8) - 1, big.getV1());
    assertEquals(0, big.getV2());
    assertEquals(0, big.getV3());
  }

  @Test
  public void testMultiplyDestructiveInt() {
    two.multiplyDestructive(1);
    assertEquals(2L, two.longValue());
    assertEquals(1L, one.longValue());
    two.multiplyDestructive(2);
    assertEquals(4L, two.longValue());

    SignedInt128 five = new SignedInt128(5);
    five.multiplyDestructive(6432346);
    assertEquals(6432346 * 5, five.getV0());
    assertEquals(0, five.getV1());
    assertEquals(0, five.getV2());
    assertEquals(0, five.getV3());

    SignedInt128 big = new SignedInt128((1L << 62) + (3L << 34) + 3L);
    big.multiplyDestructive(96);

    assertEquals(3 * 96, big.getV0());
    assertEquals(96 * (3 << 2), big.getV1());
    assertEquals(96 / 4, big.getV2());
    assertEquals(0, big.getV3());

    SignedInt128 tmp = new SignedInt128(1);
    tmp.shiftLeftDestructive(126);
    try {
      tmp.multiplyDestructive(2);
      fail();
    } catch (ArithmeticException ex) {
      // ok
    }
  }

  @Test
  public void testShiftDestructive() {
    SignedInt128 big = new SignedInt128((1L << 62) + (23L << 32) + 89L);
    big.shiftLeftDestructive(2);

    assertEquals(89 * 4, big.getV0());
    assertEquals(23 * 4, big.getV1());
    assertEquals(1, big.getV2());
    assertEquals(0, big.getV3());

    big.shiftLeftDestructive(32);

    assertEquals(0, big.getV0());
    assertEquals(89 * 4, big.getV1());
    assertEquals(23 * 4, big.getV2());
    assertEquals(1, big.getV3());

    big.shiftRightDestructive(2, true);

    assertEquals(0, big.getV0());
    assertEquals(89, big.getV1());
    assertEquals(23 + (1 << 30), big.getV2());
    assertEquals(0, big.getV3());

    big.shiftRightDestructive(32, true);

    assertEquals(89, big.getV0());
    assertEquals(23 + (1 << 30), big.getV1());
    assertEquals(0, big.getV2());
    assertEquals(0, big.getV3());

    // test rounding
    SignedInt128 tmp = new SignedInt128(17);
    assertEquals(17, tmp.getV0());
    tmp.shiftRightDestructive(1, true);
    assertEquals(9, tmp.getV0());
    tmp.shiftRightDestructive(1, false);
    assertEquals(4, tmp.getV0());
    tmp.shiftRightDestructive(1, true);
    assertEquals(2, tmp.getV0());
    tmp.shiftRightDestructive(1, true);
    assertEquals(1, tmp.getV0());
    tmp.shiftRightDestructive(1, true);
    assertEquals(1, tmp.getV0());
    tmp.shiftRightDestructive(1, false);
    assertEquals(0, tmp.getV0());
  }

  @Test
  public void testMultiplyDestructiveSignedInt128() {
    two.multiplyDestructive(one);
    assertEquals(2L, two.longValue());
    assertEquals(1L, one.longValue());
    two.multiplyDestructive(two);
    assertEquals(4L, two.longValue());

    SignedInt128 five = new SignedInt128(5);
    five.multiplyDestructive(new SignedInt128(6432346));
    assertEquals(6432346 * 5, five.getV0());
    assertEquals(0, five.getV1());
    assertEquals(0, five.getV2());
    assertEquals(0, five.getV3());

    SignedInt128 big = new SignedInt128((1L << 62) + (3L << 34) + 3L);
    big.multiplyDestructive(new SignedInt128(96));

    assertEquals(3 * 96, big.getV0());
    assertEquals(96 * (3 << 2), big.getV1());
    assertEquals(96 / 4, big.getV2());
    assertEquals(0, big.getV3());

    SignedInt128 tmp = new SignedInt128(1);
    tmp.shiftLeftDestructive(126);
    try {
      tmp.multiplyDestructive(new SignedInt128(2));
      fail();
    } catch (ArithmeticException ex) {
      // ok
    }

    SignedInt128 complicated1 = new SignedInt128(0xF9892FCA, 0x59D109AD,
        0x0534AB4C, 0);
    BigInteger bigInteger1 = complicated1.toBigIntegerSlow();
    SignedInt128 complicated2 = new SignedInt128(54234234, 9, 0, 0);
    BigInteger bigInteger2 = complicated2.toBigIntegerSlow();
    complicated1.multiplyDestructive(complicated2);
    BigInteger ans = bigInteger1.multiply(bigInteger2);
    assertEquals(ans, complicated1.toBigIntegerSlow());

    try {
      SignedInt128 complicated3 = new SignedInt128(0xF9892FCA, 0x59D109AD,
          0x0534AB4C, 0);
      complicated3.multiplyDestructive(new SignedInt128(54234234, 9845, 0, 0));
      fail();
    } catch (ArithmeticException ex) {
      // ok
    }
  }

  @Test
  public void testDivideDestructiveInt() {
    two.divideDestructive(1);
    assertEquals(1L, one.longValue());
    assertEquals(2L, two.longValue());
    one.divideDestructive(2);
    assertEquals(0L, one.longValue());
    assertEquals(2L, two.longValue());

    SignedInt128 var1 = new SignedInt128(1234234662345L);
    var1.divideDestructive(642337);
    assertEquals(1234234662345L / 642337L, var1.longValue());

    SignedInt128 complicated1 = new SignedInt128(0xF9892FCA, 0x59D109AD,
        0x0534AB4C, 0);
    BigInteger bigInteger1 = complicated1.toBigIntegerSlow();
    complicated1.divideDestructive(1534223465);
    BigInteger bigInteger2 = BigInteger.valueOf(1534223465);
    BigInteger ans = bigInteger1.divide(bigInteger2);
    assertEquals(ans, complicated1.toBigIntegerSlow());

    try {
      complicated1.divideDestructive(0);
      fail();
    } catch (ArithmeticException ex) {
      // ok
    }
  }

  @Test
  public void testDivideDestructiveSignedInt128() {
    SignedInt128 remainder = new SignedInt128();
    two.divideDestructive(one, remainder);
    assertEquals(1L, one.longValue());
    assertEquals(2L, two.longValue());
    assertEquals(zero, remainder);
    one.divideDestructive(two, remainder);
    assertEquals(0L, one.longValue());
    assertEquals(2L, two.longValue());
    assertEquals(new SignedInt128(1), remainder);

    SignedInt128 var1 = new SignedInt128(1234234662345L);
    var1.divideDestructive(new SignedInt128(642337), remainder);
    assertEquals(1234234662345L / 642337L, var1.longValue());
    assertEquals(1234234662345L % 642337L, remainder.longValue());

    SignedInt128 complicated1 = new SignedInt128(0xF9892FCA, 0x59D109AD,
        0x0534AB4C, 0x42395ADC);
    SignedInt128 complicated2 = new SignedInt128(0xF09DC19A, 0x00001234, 0, 0);
    BigInteger bigInteger1 = complicated1.toBigIntegerSlow();
    BigInteger bigInteger2 = complicated2.toBigIntegerSlow();
    complicated1.divideDestructive(complicated2, remainder);
    BigInteger ans = bigInteger1.divide(bigInteger2);
    assertEquals(ans, complicated1.toBigIntegerSlow());

    try {
      complicated1.divideDestructive(zero, remainder);
      fail();
    } catch (ArithmeticException ex) {
      // ok
    }
  }

  @Test
  public void testDivideDestructiveSignedInt128Again() {
    SignedInt128 complicated1 = new SignedInt128(0xF9892FCA, 0x59D109AD, 0, 0);
    SignedInt128 complicated2 = new SignedInt128(0xF09DC19A, 3, 0, 0);
    BigInteger bigInteger1 = complicated1.toBigIntegerSlow();
    BigInteger bigInteger2 = complicated2.toBigIntegerSlow();
    complicated1.divideDestructive(complicated2, new SignedInt128());
    BigInteger ans = bigInteger1.divide(bigInteger2);
    assertEquals(ans, complicated1.toBigIntegerSlow());
  }
}
