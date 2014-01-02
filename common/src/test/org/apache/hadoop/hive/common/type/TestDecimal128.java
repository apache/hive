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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.hive.common.type.UnsignedInt128;

/**
 * This code was originally written for Microsoft PolyBase.
 */

public class TestDecimal128 {
  private Decimal128 zero;

  private Decimal128 one;

  private Decimal128 two;

  @Before
  public void setUp() throws Exception {
    zero = new Decimal128(0);
    one = new Decimal128(1);
    two = new Decimal128(2);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testCalculateTenThirtyEight() {
    Decimal128 ten = new Decimal128(10, (short) 0);
    Decimal128 val = new Decimal128(1, (short) 0);
    for (int i = 0; i < 38; ++i) {
      val.multiplyDestructive(ten, (short) 0);
    }
  }

  @Test
  public void testHashCode() {
    assertTrue(one.hashCode() != two.hashCode());
    assertTrue(zero.hashCode() != one.hashCode());
    assertTrue(zero.hashCode() != two.hashCode());

    assertEquals(zero.hashCode(), new Decimal128(0).hashCode());
    assertEquals(one.hashCode(), new Decimal128(1).hashCode());
    assertEquals(two.hashCode(), new Decimal128(2).hashCode());

    // scaled value might be not equal, but after scaling it should.
    Decimal128 oneScaled = new Decimal128(1L, (short) 3);
    oneScaled.changeScaleDestructive((short) 0);
    assertEquals(one.hashCode(), oneScaled.hashCode());
  }

  @Test
  public void testEquals() {
    assertTrue(!one.equals(two));
    assertTrue(!zero.equals(one));
    assertTrue(!zero.equals(two));

    assertEquals(zero, new Decimal128(0));
    assertEquals(one, new Decimal128(1));
    assertEquals(two, new Decimal128(2));

    // scaled value might be not equal, but after scaling it should.
    Decimal128 oneScaled = new Decimal128(1L, (short) 3);
    oneScaled.changeScaleDestructive((short) 0);
    assertEquals(one, oneScaled);
  }

  @Test
  public void testCompareTo() {
    assertTrue(one.compareTo(two) < 0);
    assertTrue(two.compareTo(one) > 0);
    assertTrue(one.compareTo(zero) > 0);
    assertTrue(zero.compareTo(two) < 0);

    // compare to must compare with scaling up/down.
    Decimal128 oneScaled = new Decimal128(1L, (short) 3);
    assertTrue(one.compareTo(oneScaled) == 0);

    // exact numbers (power of 2) can do the same
    Decimal128 d1 = new Decimal128(2.0d, (short) 6);
    Decimal128 d2 = new Decimal128(2.0d, (short) 3);
    assertTrue(d1.compareTo(d2) == 0);

    // but, if the value is rounded by more scaling,
    // they will be different values.
    Decimal128 d3 = new Decimal128(2.0d / 3.0d, (short) 5);
    Decimal128 d4 = new Decimal128(2.0d / 3.0d, (short) 8);
    assertTrue(d3.compareTo(d4) != 0);
  }

  @Test
  public void testText() {
    assertEquals("1", one.toFormalString());
    assertEquals(0, new Decimal128("1", (short) 0).compareTo(one));
    assertEquals("2", two.toFormalString());
    assertEquals(0, new Decimal128("2", (short) 0).compareTo(two));
    assertEquals("0", zero.toFormalString());
    assertEquals(0, new Decimal128("0", (short) 0).compareTo(zero));

    assertEquals("1.000", new Decimal128(1L, (short) 3).toFormalString());
    assertEquals(0, new Decimal128("1", (short) 3).compareTo(one));

    assertEquals("2.000000", new Decimal128(2.0d, (short) 6).toFormalString());
    assertEquals("2.000", new Decimal128(2.0d, (short) 3).toFormalString());
    assertEquals(0, new Decimal128("2.0", (short) 6).compareTo(two));
    assertEquals(0, new Decimal128("2.0", (short) 3).compareTo(two));

    assertEquals("1.3330", new Decimal128("1.333", (short) 4).toFormalString());
    assertEquals("1.333000",
        new Decimal128("1.333", (short) 6).toFormalString());
    assertEquals("1.333", new Decimal128("1.333", (short) 3).toFormalString());
    assertEquals("1.33", new Decimal128("1.333", (short) 2).toFormalString());
    assertEquals("1.33", new Decimal128("1.333", (short) 2).toFormalString());

    assertEquals("0.13330",
        new Decimal128("1333E-4", (short) 5).toFormalString());
    assertEquals("0.01333",
        new Decimal128("1333E-5", (short) 5).toFormalString());
    assertEquals("13330000.00",
        new Decimal128("1333E4", (short) 2).toFormalString());

    assertEquals("123456789012345678901234.56789", new Decimal128(
        "123456789012345678901234567.8901234E-3", (short) 5).toFormalString());
  }

  @Test
  public void testAdd() {
    Decimal128 result = new Decimal128();
    Decimal128.add(one, two, result, (short) 2);

    assertEquals(0, new Decimal128(3L, (short) 0).compareTo(result));

    Decimal128.add(two, two, result, (short) 1);

    assertEquals(0, new Decimal128(4L, (short) 0).compareTo(result));

    long l1 = 123456789012345L;
    long l2 = 987654321097L;
    long sum = l1 + l2;
    Decimal128 left = new Decimal128(l1, (short) 3);
    Decimal128 right = new Decimal128(l2, (short) 5);
    Decimal128.add(left, right, result, (short) 2);
    assertEquals(0, new Decimal128(sum, (short) 0).compareTo(result));
    Decimal128.add(right, left, result, (short) 2);
    assertEquals(0, new Decimal128(sum, (short) 0).compareTo(result));
  }

  @Test
  public void testSubtract() {
    Decimal128 result = new Decimal128();
    Decimal128.subtract(one, two, result, (short) 2);
    assertEquals(0, new Decimal128(-1L, (short) 0).compareTo(result));

    Decimal128.subtract(two, one, result, (short) 2);
    assertEquals(0, new Decimal128(1L, (short) 0).compareTo(result));

    Decimal128.subtract(two, two, result, (short) 1);
    assertEquals(0, zero.compareTo(result));
    assertEquals(0, result.getSignum());

    long l1 = 123456789012345L;
    long l2 = 987654321097L;
    long sub = l1 - l2;
    Decimal128 left = new Decimal128(l1, (short) 3);
    Decimal128 right = new Decimal128(l2, (short) 5);
    Decimal128.subtract(left, right, result, (short) 2);
    assertEquals(0, new Decimal128(sub, (short) 0).compareTo(result));
    Decimal128.subtract(right, left, result, (short) 2);
    assertEquals(0, new Decimal128(-sub, (short) 0).compareTo(result));

    Decimal128 val = new Decimal128("1.123", (short) 3);
    val.addDestructive(new Decimal128("4.321", (short) 3), (short) 3);
    assertEquals("5.444", val.toFormalString());
  }

  @Test
  public void testMultiply() {
    Decimal128 result = new Decimal128();
    Decimal128.multiply(one, two, result, (short) 2);
    assertEquals(0, two.compareTo(result));

    Decimal128.multiply(two, two, result, (short) 2);
    assertEquals(0, new Decimal128(4L, (short) 0).compareTo(result));

    long l1 = 123456789012345L;
    long l2 = 987654321097L;
    Decimal128 left = new Decimal128(l1, (short) 0);
    Decimal128 right = new Decimal128(l2, (short) 0);
    UnsignedInt128 unscaled = new UnsignedInt128(l1)
        .multiplyConstructive(new UnsignedInt128(l2));
    Decimal128 ans = new Decimal128(unscaled, (short) 0, false);
    Decimal128.multiply(left, right, result, (short) 0);
    assertEquals(0, ans.compareTo(result));
    Decimal128.multiply(right, left, result, (short) 0);
    assertEquals(0, ans.compareTo(result));

    Decimal128.multiply(new Decimal128(1.123d, (short) 10), new Decimal128(
        4.321d, (short) 10), result, (short) 10);
    assertEquals(1.123d * 4.321d, result.doubleValue(), 0.00001d);

    // because only 10 fractional digits, it's not this much accurate
    assertNotEquals(1.123d * 4.321d, result.doubleValue(), 0.00000000000000001d);

    Decimal128.multiply(new Decimal128(1.123d, (short) 2), new Decimal128(
        4.321d, (short) 2), result, (short) 2);

    // this time even more inaccurate
    assertEquals(1.123d * 4.321d, result.doubleValue(), 1.0d);
    assertNotEquals(1.123d * 4.321d, result.doubleValue(), 0.000001d);

    Decimal128 val = new Decimal128("1.123", (short) 3);
    val.multiplyDestructive(new Decimal128("4.321", (short) 3), (short) 6);
    assertEquals("4.852483", val.toFormalString());

    Decimal128 val1 = new Decimal128("1.0001", (short) 4);
    val1.multiplyDestructive(new Decimal128("1.0001", (short) 4), (short) 8);
    assertEquals("1.00020001", val1.toFormalString());

  }

  // Assert that a and b are not the same, within epsilon tolerance.
  private void assertNotEquals(double a, double b, double epsilon) {
    assertTrue(Math.abs(a - b) > epsilon);
  }

  @Test
  public void testDivide() {
    Decimal128 quotient = new Decimal128();
    Decimal128 remainder = new Decimal128();
    Decimal128.divide(two, one, quotient, remainder, (short) 2);
    assertEquals(0, quotient.compareTo(two));
    assertTrue(remainder.isZero());

    Decimal128.divide(two, two, quotient, remainder, (short) 2);
    assertEquals(0, quotient.compareTo(one));
    assertTrue(remainder.isZero());

    Decimal128 three = new Decimal128(3);
    Decimal128 four = new Decimal128(4);
    Decimal128.divide(three, four, quotient, remainder, (short) 2);
    assertEquals("0.75", quotient.toFormalString());
    assertEquals("0", remainder.toFormalString());

    Decimal128.divide(three, four, quotient, remainder, (short) 1);
    assertEquals("0.7", quotient.toFormalString());
    assertEquals("0.2", remainder.toFormalString());

    Decimal128.divide(three, four, quotient, remainder, (short) 0);
    assertEquals("0", quotient.toFormalString());
    assertEquals("3", remainder.toFormalString());
  }

  @Test
  public void testPiNewton() {

    // see http://en.wikipedia.org/wiki/Approximations_of_%CF%80
    // Below is the simple Newton's equation
    final int LOOPS = 100;
    final short SCALE = 33;
    Decimal128 current = new Decimal128(1, SCALE);
    Decimal128 multiplier = new Decimal128();
    Decimal128 dividor = new Decimal128();
    Decimal128 remainder = new Decimal128();
    Decimal128 one = new Decimal128(1);
    for (int i = LOOPS; i > 0; --i) {
      multiplier.update(i, SCALE);
      current.multiplyDestructive(multiplier, SCALE);
      dividor.update(1 + 2 * i, SCALE);
      current.divideDestructive(dividor, SCALE, remainder);
      current.addDestructive(one, SCALE);
    }
    current.multiplyDestructive(new Decimal128(2), SCALE);
    assertTrue(current.toFormalString().startsWith("3.141592653589793238"));
  }

  @Test
  public void testPiArcsine() {

    // This one uses the arcsin method. Involves more multiplications/divisions.
    // pi=Sum (3 * 2n!/(16^n * (2n+1) * n! * n!))
    // =Sum (3 * ((n+1)(n+2)...2n)/n!*16^n/(2n+1))
    // =Sum (3 / (2n+1) * (n+1)/16 * (n+2)/32... * 2n/16(n+1))
    // (note that it is split so that each term is not overflown)
    final int LOOPS = 50;
    final short SCALE = 30;
    Decimal128 total = new Decimal128(0);
    Decimal128 multiplier = new Decimal128();
    Decimal128 dividor = new Decimal128();
    Decimal128 remainder = new Decimal128();
    Decimal128 current = new Decimal128();
    for (int i = 0; i < LOOPS; ++i) {
      current.update(3, SCALE);
      dividor.update(2 * i + 1, SCALE);
      current.divideDestructive(dividor, SCALE, remainder);
      for (int j = 1; j <= i; ++j) {
        multiplier.update(i + j, SCALE);
        dividor.update(16 * j, SCALE);
        current.multiplyDestructive(multiplier, SCALE);
        current.divideDestructive(dividor, SCALE, remainder);
      }

      total.addDestructive(current, SCALE);
    }

    assertTrue(total.toFormalString().startsWith("3.141592653589793238462"));
  }

  @Test
  public void testDoubleValue() {
    Decimal128 quotient = new Decimal128();
    Decimal128 remainder = new Decimal128();

    Decimal128 three = new Decimal128(3);
    Decimal128 four = new Decimal128(9);
    Decimal128.divide(three, four, quotient, remainder, (short) 38);
    assertEquals(0.33333333333333333333333333d, quotient.doubleValue(),
        0.0000000000000000000000001d);

    Decimal128 minusThree = new Decimal128(-3);
    Decimal128.divide(minusThree, four, quotient, remainder, (short) 38);
    assertEquals(-0.33333333333333333333333333d, quotient.doubleValue(),
        0.0000000000000000000000001d);
  }

  @Test
  public void testFloatValue() {
    Decimal128 quotient = new Decimal128();
    Decimal128 remainder = new Decimal128();

    Decimal128 three = new Decimal128(3);
    Decimal128 four = new Decimal128(9);
    Decimal128.divide(three, four, quotient, remainder, (short) 38);
    assertEquals(0.3333333333333333f, quotient.floatValue(), 0.00000000001f);

    Decimal128 minusThree = new Decimal128(-3);
    Decimal128.divide(minusThree, four, quotient, remainder, (short) 38);
    assertEquals(-0.333333333333333f, quotient.floatValue(), 0.00000000001f);
  }

  @Test
  public void testSqrtAsDouble() {
    Decimal128 val1 = new Decimal128("1.00435134913958923485982394892384",
        (short) 36);
    Decimal128 val2 = new Decimal128("1.00345982739817298323423423", (short) 36);
    assertEquals(1.00217331292526d, val1.sqrtAsDouble(), 0.000000000000001d);
    assertEquals(1.00172841998127d, val2.sqrtAsDouble(), 0.000000000000001d);

  }

  @Test
  public void testPowAsDouble() {
    Decimal128 val1 = new Decimal128("1.00435134913958923485982394892384",
        (short) 36);
    assertEquals(1.004366436877081d,
        val1.powAsDouble(1.00345982739817298323423423d), 0.000000000000001d);

    Decimal128 val2 = new Decimal128("1.001", (short) 36);
    assertEquals(1.0100451202102512d, val2.powAsDouble(10), 0.000000000000001d);
  }

  @Test
  public void testPrecisionOverflow() {
    new Decimal128("1.004", (short) 3).checkPrecisionOverflow(4);

    try {
      new Decimal128("1.004", (short) 3).checkPrecisionOverflow(3);
      fail();
    } catch (ArithmeticException ex) {
    }

    try {
      new Decimal128("1.004", (short) 3).checkPrecisionOverflow(2);
      fail();
    } catch (ArithmeticException ex) {
    }

    new Decimal128("1.004", (short) 3).checkPrecisionOverflow(38);

    new Decimal128("-3322", (short) 0).checkPrecisionOverflow(4);
    try {
      new Decimal128("-3322", (short) 0).checkPrecisionOverflow(3);
      fail();
    } catch (ArithmeticException ex) {
    }

    new Decimal128("-3322", (short) 1).checkPrecisionOverflow(5);
    try {
      new Decimal128("-3322", (short) 1).checkPrecisionOverflow(4);
      fail();
    } catch (ArithmeticException ex) {
    }
  }
}
