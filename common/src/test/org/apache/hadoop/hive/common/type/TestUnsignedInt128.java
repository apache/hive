/**
 * Copyright (c) Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.common.type;

import static org.junit.Assert.*;

import java.math.BigInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Testcases for {@link UnsignedInt128}
 *
 * This code was originally written for Microsoft PolyBase.
 */
public class TestUnsignedInt128 {
  private UnsignedInt128 zero;

  private UnsignedInt128 one;

  private UnsignedInt128 two;

  @Before
  public void setUp() throws Exception {
    zero = new UnsignedInt128(0);
    one = new UnsignedInt128(1);
    two = new UnsignedInt128(2);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testHashCode() {
    assertNotEquals(one.hashCode(), two.hashCode());
    assertNotEquals(zero.hashCode(), one.hashCode());
    assertNotEquals(zero.hashCode(), two.hashCode());

    assertEquals(zero.hashCode(), new UnsignedInt128(0).hashCode());
    assertEquals(one.hashCode(), new UnsignedInt128(1).hashCode());
    assertEquals(two.hashCode(), new UnsignedInt128(2).hashCode());
  }

  private void assertNotEquals(int a, int b) {
    assertTrue(a != b);
  }

  private void assertNotEquals(long a, long b) {
    assertTrue(a != b);
  }

  private void assertNotEquals(UnsignedInt128 a, UnsignedInt128 b) {
    assertTrue(!a.equals(b));
  }

  @Test
  public void testEquals() {
    assertNotEquals(one, two);
    assertNotEquals(zero, one);
    assertNotEquals(zero, two);

    assertEquals(zero, new UnsignedInt128(0));
    assertEquals(one, new UnsignedInt128(1));
    assertEquals(two, new UnsignedInt128(2));
  }

  @Test
  public void testCompareTo() {
    assertTrue(one.compareTo(two) < 0);
    assertTrue(two.compareTo(one) > 0);
    assertTrue(one.compareTo(zero) > 0);
    assertTrue(zero.compareTo(two) < 0);
  }

  @Test
  public void testCompareToScaleTen() {
    assertTrue(zero.compareToScaleTen(new UnsignedInt128(0), (short) 3) == 0);
    assertTrue(zero.compareToScaleTen(new UnsignedInt128(0), (short) -1) == 0);
    assertTrue(zero.compareToScaleTen(new UnsignedInt128(0), (short) 12) == 0);
    assertTrue(one.compareToScaleTen(zero, (short) 0) > 0);
    assertTrue(one.compareToScaleTen(zero, (short) 3) > 0);
    assertTrue(one.compareToScaleTen(zero, (short) -3) > 0);

    assertTrue(zero.compareToScaleTen(one, (short) 3) < 0);
    assertTrue(zero.compareToScaleTen(one, (short) 0) < 0);
    assertTrue(zero.compareToScaleTen(one, (short) -1) == 0);

    assertTrue(new UnsignedInt128(30).compareToScaleTen(new UnsignedInt128(3),
        (short) 1) == 0);
    assertTrue(new UnsignedInt128(30).compareToScaleTen(new UnsignedInt128(3),
        (short) 2) < 0);
    assertTrue(new UnsignedInt128(30).compareToScaleTen(new UnsignedInt128(3),
        (short) 0) > 0);

    assertTrue(new UnsignedInt128(680000000000L).compareToScaleTen(
        new UnsignedInt128(68), (short) 10) == 0);
    assertTrue(new UnsignedInt128(68).compareToScaleTen(new UnsignedInt128(
        680000000000L), (short) -10) == 0);
    assertTrue(new UnsignedInt128(680000000000L).compareToScaleTen(
        new UnsignedInt128(0), (short) 60) > 0);
    assertTrue(new UnsignedInt128(680000000000L).compareToScaleTen(
        new UnsignedInt128(0), (short) 30) > 0);
    assertTrue(new UnsignedInt128(680000000000L).compareToScaleTen(
        new UnsignedInt128(0), (short) 10) > 0);
    assertTrue(new UnsignedInt128(0).compareToScaleTen(new UnsignedInt128(
        680000000000L), (short) -10) < 0);
    assertTrue(new UnsignedInt128(0).compareToScaleTen(new UnsignedInt128(
        680000000000L), (short) -11) < 0);
    assertTrue(new UnsignedInt128(0).compareToScaleTen(new UnsignedInt128(
        680000000000L), (short) -12) < 0);
    assertTrue(new UnsignedInt128(0).compareToScaleTen(new UnsignedInt128(
        680000000000L), (short) -13) == 0);
    assertTrue(new UnsignedInt128(0).compareToScaleTen(new UnsignedInt128(
        680000000000L), (short) -30) == 0);

    assertTrue(new UnsignedInt128(680000000000L).compareToScaleTen(
        new UnsignedInt128(680000000001L), (short) 0) < 1);
    assertTrue(new UnsignedInt128(68000000000L).compareToScaleTen(
        new UnsignedInt128(680000000001L), (short) -1) == 0);
    assertTrue(new UnsignedInt128(68000000000L).compareToScaleTen(
        new UnsignedInt128(680000000000L), (short) -1) == 0);
    assertTrue(new UnsignedInt128(68000000000L).compareToScaleTen(
        new UnsignedInt128(679999999999L), (short) -1) == 0);

    assertTrue(new UnsignedInt128(0x10000000000000L).shiftLeftConstructive(32)
        .compareToScaleTen(
            new UnsignedInt128(0xA0000000000000L).shiftLeftConstructive(32),
            (short) -1) == 0);
    assertTrue(new UnsignedInt128(0x10000000000000L).shiftLeftConstructive(32)
        .compareToScaleTen(
            new UnsignedInt128(0xA0000000000000L).shiftLeftConstructive(32),
            (short) 1) < 0);
    assertTrue(new UnsignedInt128(0x10000000000000L).shiftLeftConstructive(32)
        .compareToScaleTen(
            new UnsignedInt128(0xA0000000000000L).shiftLeftConstructive(32),
            (short) 0) < 0);
    assertTrue(new UnsignedInt128(0x10000000000000L).shiftLeftConstructive(32)
        .compareToScaleTen(
            new UnsignedInt128(0xA0000000000000L).shiftLeftConstructive(32),
            (short) -2) > 0);
  }

  @Test
  public void testToFormalString() {
    assertEquals("0", zero.toFormalString());
    assertEquals("1", one.toFormalString());

    assertEquals("30", new UnsignedInt128(30).toFormalString());
    assertEquals("680000000000",
        new UnsignedInt128(680000000000L).toFormalString());
    assertEquals("6800000000000",
        new UnsignedInt128(6800000000000L).toFormalString());
    assertEquals("68", new UnsignedInt128(68).toFormalString());

    assertEquals(zero, new UnsignedInt128("0"));
    assertEquals(one, new UnsignedInt128("1"));

    assertEquals(new UnsignedInt128(30), new UnsignedInt128("30"));
    assertEquals(new UnsignedInt128(680000000000L), new UnsignedInt128(
        "680000000000"));
    assertEquals(new UnsignedInt128(6800000000000L), new UnsignedInt128(
        "6800000000000"));
    assertEquals(new UnsignedInt128(68), new UnsignedInt128("68"));
  }

  @Test
  public void testUnsignedInt128() {
    assertEquals(0L, new UnsignedInt128().asLong());
  }

  @Test
  public void testUnsignedInt128UnsignedInt128() {
    assertEquals(1L, new UnsignedInt128(one).asLong());
    assertEquals(2L, new UnsignedInt128(two).asLong());
  }

  @Test
  public void testUnsignedInt128IntIntIntInt() {
    assertEquals(((long) 11) << 32L | 23L,
        new UnsignedInt128(23, 11, 0, 0).asLong());
  }

  @Test
  public void testZeroClear() {
    assertFalse(one.isZero());
    assertFalse(two.isZero());
    assertNotEquals(0L, one.asLong());
    assertNotEquals(0L, two.asLong());

    two.zeroClear();

    assertNotEquals(0L, one.asLong());
    assertEquals(0L, two.asLong());
    assertFalse(one.isZero());
    assertTrue(two.isZero());

    one.zeroClear();

    assertEquals(0L, one.asLong());
    assertEquals(0L, two.asLong());
    assertTrue(one.isZero());
    assertTrue(two.isZero());
  }

  @Test
  public void testAddDestructive() {
    one.addDestructive(two);
    assertEquals(3L, one.asLong());
    assertEquals(2L, two.asLong());

    UnsignedInt128 big = new UnsignedInt128((1L << 62) + 3L);
    UnsignedInt128 tmp = new UnsignedInt128(0L);
    for (int i = 0; i < 54; ++i) {
      tmp.addDestructive(big);
    }

    assertEquals(3 * 54, tmp.getV0());
    assertEquals(0x80000000, tmp.getV1()); // (54 % 4) << 62
    assertEquals(13, tmp.getV2()); // 54/4
    assertEquals(0, tmp.getV3());

    assertEquals((1L << 62) + 3L, big.asLong());

    UnsignedInt128 huge = one.shiftLeftConstructive(127);
    UnsignedInt128 huge2 = one.shiftLeftConstructive(127);
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
    assertEquals(1L, one.asLong());
    assertEquals(1L, one.asLong());

    try {
      one.subtractDestructive(new UnsignedInt128(10L));
      fail();
    } catch (ArithmeticException ex) {
      // ok
    }

    UnsignedInt128 big = new UnsignedInt128((1L << 62) + (3L << 34) + 3L);
    big.shiftLeftDestructive(6);
    UnsignedInt128 tmp = new UnsignedInt128((1L << 61) + 5L);
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
    assertEquals(2L, two.asLong());
    assertEquals(1L, one.asLong());
    two.multiplyDestructive(2);
    assertEquals(4L, two.asLong());

    UnsignedInt128 five = new UnsignedInt128(5);
    five.multiplyDestructive(6432346);
    assertEquals(6432346 * 5, five.getV0());
    assertEquals(0, five.getV1());
    assertEquals(0, five.getV2());
    assertEquals(0, five.getV3());

    UnsignedInt128 big = new UnsignedInt128((1L << 62) + (3L << 34) + 3L);
    big.multiplyDestructive(96);

    assertEquals(3 * 96, big.getV0());
    assertEquals(96 * (3 << 2), big.getV1());
    assertEquals(96 / 4, big.getV2());
    assertEquals(0, big.getV3());

    UnsignedInt128 tmp = new UnsignedInt128(1);
    tmp.shiftLeftDestructive(126);
    tmp.multiplyDestructive(2);
    try {
      tmp.multiplyDestructive(2);
      fail();
    } catch (ArithmeticException ex) {
      // ok
    }
  }

  @Test
  public void testShiftDestructive() {
    UnsignedInt128 big = new UnsignedInt128((1L << 62) + (23L << 32) + 89L);
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
    UnsignedInt128 tmp = new UnsignedInt128(17);
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
  public void testMultiplyDestructiveUnsignedInt128() {
    two.multiplyDestructive(one);
    assertEquals(2L, two.asLong());
    assertEquals(1L, one.asLong());
    two.multiplyDestructive(two);
    assertEquals(4L, two.asLong());

    UnsignedInt128 five = new UnsignedInt128(5);
    five.multiplyDestructive(new UnsignedInt128(6432346));
    assertEquals(6432346 * 5, five.getV0());
    assertEquals(0, five.getV1());
    assertEquals(0, five.getV2());
    assertEquals(0, five.getV3());

    UnsignedInt128 big = new UnsignedInt128((1L << 62) + (3L << 34) + 3L);
    big.multiplyDestructive(new UnsignedInt128(96));

    assertEquals(3 * 96, big.getV0());
    assertEquals(96 * (3 << 2), big.getV1());
    assertEquals(96 / 4, big.getV2());
    assertEquals(0, big.getV3());

    UnsignedInt128 tmp = new UnsignedInt128(1);
    tmp.shiftLeftDestructive(126);
    tmp.multiplyDestructive(new UnsignedInt128(2));
    try {
      tmp.multiplyDestructive(new UnsignedInt128(2));
      fail();
    } catch (ArithmeticException ex) {
      // ok
    }

    UnsignedInt128 complicated1 = new UnsignedInt128(0xF9892FCA, 0x59D109AD,
        0x0534AB4C, 0);
    BigInteger bigInteger1 = complicated1.toBigIntegerSlow();
    UnsignedInt128 complicated2 = new UnsignedInt128(54234234, 9, 0, 0);
    BigInteger bigInteger2 = complicated2.toBigIntegerSlow();
    complicated1.multiplyDestructive(complicated2);
    BigInteger ans = bigInteger1.multiply(bigInteger2);
    assertEquals(ans, complicated1.toBigIntegerSlow());

    try {
      UnsignedInt128 complicated3 = new UnsignedInt128(0xF9892FCA, 0x59D109AD,
          0x0534AB4C, 0);
      complicated3
          .multiplyDestructive(new UnsignedInt128(54234234, 9845, 0, 0));
      fail();
    } catch (ArithmeticException ex) {
      // ok
    }
  }

  @Test
  public void testMultiplyScaleDownTenDestructiveScaleTen() {
    for (int scale = 0; scale < 38; ++scale) {
      UnsignedInt128 right = new UnsignedInt128(1);
      right.scaleUpTenDestructive((short) scale);
      {
        // 10000000....000
        UnsignedInt128 leftJust = new UnsignedInt128(1);
        leftJust.scaleUpTenDestructive((short) 15);
        UnsignedInt128 leftInc = leftJust.incrementConstructive();
        UnsignedInt128 leftDec = leftJust.decrementConstructive();

        if (scale + 10 <= 38) {
          leftJust.multiplyScaleDownTenDestructive(right, (short) (scale + 10));
          assertEquals("scale=" + scale, 100000L, leftJust.asLong());
          leftInc.multiplyScaleDownTenDestructive(right, (short) (scale + 10));
          assertEquals("scale=" + scale, 100000L, leftInc.asLong());
          leftDec.multiplyScaleDownTenDestructive(right, (short) (scale + 10));
          assertEquals("scale=" + scale, 100000L, leftDec.asLong());
        } else {
          leftJust.multiplyScaleDownTenDestructive(right, (short) (scale + 10));
          assertEquals("scale=" + scale, 0L, leftJust.asLong());
          leftInc.multiplyScaleDownTenDestructive(right, (short) (scale + 10));
          assertEquals("scale=" + scale, 0L, leftInc.asLong());
          leftDec.multiplyScaleDownTenDestructive(right, (short) (scale + 10));
          assertEquals("scale=" + scale, 0L, leftDec.asLong());
        }
      }

      {
        // 10000500....00
        UnsignedInt128 leftHalfJust = new UnsignedInt128(1);
        leftHalfJust.scaleUpTenDestructive((short) 6);
        leftHalfJust.addDestructive(new UnsignedInt128(5));
        leftHalfJust.scaleUpTenDestructive((short) 9);
        UnsignedInt128 leftHalfInc = leftHalfJust.incrementConstructive();
        UnsignedInt128 leftHalfDec = leftHalfJust.decrementConstructive();

        if (scale + 10 <= 38) {
          leftHalfJust.multiplyScaleDownTenDestructive(right,
              (short) (scale + 10));
          assertEquals("scale=" + scale, 100001L, leftHalfJust.asLong());
          leftHalfInc.multiplyScaleDownTenDestructive(right,
              (short) (scale + 10));
          assertEquals("scale=" + scale, 100001L, leftHalfInc.asLong());
          leftHalfDec.multiplyScaleDownTenDestructive(right,
              (short) (scale + 10));
          assertEquals("scale=" + scale, 100000L, leftHalfDec.asLong());
        } else {
          leftHalfJust.multiplyScaleDownTenDestructive(right,
              (short) (scale + 10));
          assertEquals("scale=" + scale, 0L, leftHalfJust.asLong());
          leftHalfInc.multiplyScaleDownTenDestructive(right,
              (short) (scale + 10));
          assertEquals("scale=" + scale, 0L, leftHalfInc.asLong());
          leftHalfDec.multiplyScaleDownTenDestructive(right,
              (short) (scale + 10));
          assertEquals("scale=" + scale, 0L, leftHalfDec.asLong());
        }
      }
    }
  }

  @Test
  public void testDivideDestructiveInt() {
    two.divideDestructive(1);
    assertEquals(1L, one.asLong());
    assertEquals(2L, two.asLong());
    one.divideDestructive(2);
    assertEquals(0L, one.asLong());
    assertEquals(2L, two.asLong());

    UnsignedInt128 var1 = new UnsignedInt128(1234234662345L);
    var1.divideDestructive(642337);
    assertEquals(1234234662345L / 642337L, var1.asLong());

    UnsignedInt128 complicated1 = new UnsignedInt128(0xF9892FCA, 0x59D109AD,
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
  public void testDivideDestructiveUnsignedInt128() {
    UnsignedInt128 remainder = new UnsignedInt128();
    two.divideDestructive(one, remainder);
    assertEquals(1L, one.asLong());
    assertEquals(2L, two.asLong());
    assertEquals(zero, remainder);
    one.divideDestructive(two, remainder);
    assertEquals(0L, one.asLong());
    assertEquals(2L, two.asLong());
    assertEquals(new UnsignedInt128(1), remainder);

    UnsignedInt128 var1 = new UnsignedInt128(1234234662345L);
    var1.divideDestructive(new UnsignedInt128(642337), remainder);
    assertEquals(1234234662345L / 642337L, var1.asLong());
    assertEquals(1234234662345L % 642337L, remainder.asLong());

    UnsignedInt128 complicated1 = new UnsignedInt128(0xF9892FCA, 0x59D109AD,
        0x0534AB4C, 0x42395ADC);
    UnsignedInt128 complicated2 = new UnsignedInt128(0xF09DC19A, 0x00001234, 0,
        0);
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
  public void testDivideDestructiveUnsignedInt128Again() {
    UnsignedInt128 complicated1 = new UnsignedInt128(0xF9892FCA, 0x59D109AD, 0,
        0);
    UnsignedInt128 complicated2 = new UnsignedInt128(0xF09DC19A, 3, 0, 0);
    BigInteger bigInteger1 = complicated1.toBigIntegerSlow();
    BigInteger bigInteger2 = complicated2.toBigIntegerSlow();
    complicated1.divideDestructive(complicated2, new UnsignedInt128());
    BigInteger ans = bigInteger1.divide(bigInteger2);
    assertEquals(ans, complicated1.toBigIntegerSlow());
  }
}
