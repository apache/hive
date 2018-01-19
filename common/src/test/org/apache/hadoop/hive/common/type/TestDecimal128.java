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
package org.apache.hadoop.hive.common.type;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * This code was based on code from Microsoft's PolyBase.
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
  public void testCalculateTenThirtySeven() {

    // find 10^37
    Decimal128 ten = new Decimal128(10, (short) 0);
    Decimal128 val = new Decimal128(1, (short) 0);
    for (int i = 0; i < 37; ++i) {
      val.multiplyDestructive(ten, (short) 0);
    }

    // verify it
    String s = val.toFormalString();
    assertEquals("10000000000000000000000000000000000000", s);
    boolean overflow = false;
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

    Decimal128 d5 = new Decimal128(12, (short) 5);
    Decimal128 d6 = new Decimal128(15, (short) 7);
    assertTrue(d5.compareTo(d6) < 0);
    assertTrue(d6.compareTo(d5) > 0);

    Decimal128 d7 = new Decimal128(15, (short) 5);
    Decimal128 d8 = new Decimal128(12, (short) 7);
    assertTrue(d7.compareTo(d8) > 0);
    assertTrue(d8.compareTo(d7) < 0);
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
    Decimal128.divide(two, one, quotient, (short) 2);
    assertEquals(0, quotient.compareTo(two));

    Decimal128.divide(two, two, quotient, (short) 2);
    assertEquals(0, quotient.compareTo(one));

    Decimal128 three = new Decimal128(3);
    Decimal128 four = new Decimal128(4);
    Decimal128.divide(three, four, quotient, (short) 2);
    assertEquals("0.75", quotient.toFormalString());

    Decimal128.divide(three, four, quotient, (short) 1);
    assertEquals("0.8", quotient.toFormalString());

    Decimal128.divide(three, four, quotient, (short) 0);
    assertEquals("1", quotient.toFormalString());

    Decimal128 two = new Decimal128(2);
    Decimal128.divide(two, three, quotient, (short) 4);
    assertEquals("0.6667", quotient.toFormalString());
  }

  @Test
  public void testRandomMultiplyDivideInverse() {
    final int N = 100000;
    final long MASK56 = 0x00FFFFFFFFFFFFL; // 56 bit mask to generate positive 56 bit longs
                                           // from random signed longs
    int seed = 897089790;
    Random rand = new Random(seed);
    long l1, l2;
    for (int i = 1; i <= N; i++) {
      l1 = rand.nextLong() & MASK56;
      l2 = rand.nextLong() & MASK56;
      verifyMultiplyDivideInverse(l1, l2);
    }
  }

  /**
   * Verify that a * b / b == a
   * for decimal division for scale 0 with integer inputs.
   *
   * Not valid if abs(a * b) >= 10**38.
   */
  private void verifyMultiplyDivideInverse(long a, long b) {
    final short scale = 0;

    // ignore zero-divide cases
    if (b == 0) {
      return;
    }
    Decimal128 decA = new Decimal128(a, scale);
    Decimal128 decB = new Decimal128(b, scale);
    decA.multiplyDestructive(decB, scale);
    decA.checkPrecisionOverflow(38); // caller must make sure product of inputs is not too big
    decA.divideDestructive(decB, scale);
    assertEquals("Error for a = " + Long.toString(a) + ", b = " + Long.toString(b),
        new Decimal128(a, scale), decA);
  }


  @Test
  public void testRandomAddSubtractInverse() {
    final int N = 1000000;
    int seed = 1427480960;
    Random rand = new Random(seed);
    long l1, l2;
    for (int i = 1; i <= N; i++) {
      l1 = rand.nextLong();
      l2 = rand.nextLong();
      verifyAddSubtractInverse(l1, l2);
    }
  }

  /**
   * Verify that (a + b) - b == a
   * for decimal add and subtract for scale 0 with long integer inputs.
   */
  private void verifyAddSubtractInverse(long a, long b) {
    final short scale = 0;
    Decimal128 decA = new Decimal128(a, scale);
    Decimal128 decB = new Decimal128(b, scale);
    decA.addDestructive(decB, scale);

    decA.subtractDestructive(decB, scale);
    assertEquals("Error for a = " + Long.toString(a) + ", b = " + Long.toString(b),
        new Decimal128(a, scale), decA);
  }

  /**
   * During earlier code testing, if we found errors, test them here as regression tests.
   */
  @Test
  public void testKnownPriorErrors() {

    // Regression test for defect reported in HIVE-6243
    long a = 213474114411690L;
    long b = 5062120663L;
    verifyMultiplyDivideInverse(a, b);

    // Regression test for defect reported in HIVE-6399
    String a2 = "-605044214913338382";  // 18 digits
    String b2 = "55269579109718297360"; // 20 digits

    // -33440539101030154945490585226577271520 is expected result
    verifyHighPrecisionMultiplySingle(a2, b2);
  }

  // Test a set of random adds at high precision.
  @Test
  public void testHighPrecisionDecimal128Add() {
    final int N = 10000;
    for (int i = 0; i < N; i++) {
      verifyHighPrecisionAddSingle();
    }
  }

  // Test one random hi-precision decimal add.
  private void verifyHighPrecisionAddSingle() {

    Decimal128 a, b, r;
    String sA, sB;

    a = new Decimal128();
    sA = makeNumericString(37);
    a.update(sA, (short) 0);
    b = new Decimal128();
    sB = makeNumericString(37);
    b.update(sB, (short) 0);

    r = new Decimal128();
    r.addDestructive(a, (short) 0);
    r.addDestructive(b, (short) 0);

    String res1 = r.toFormalString();

    // Now do the add with Java BigDecimal
    BigDecimal bdA = new BigDecimal(sA);
    BigDecimal bdB = new BigDecimal(sB);
    BigDecimal bdR = bdA.add(bdB);

    String res2 = bdR.toPlainString();

    // Compare the results
    String message = "For operation " + a.toFormalString() + " + " + b.toFormalString();
    assertEquals(message, res2, res1);
  }

  // Test a set of random subtracts at high precision.
  @Test
  public void testHighPrecisionDecimal128Subtract() {
    final int N = 10000;
    for (int i = 0; i < N; i++) {
      verifyHighPrecisionSubtractSingle();
    }
  }

  // Test one random high-precision subtract.
  private void verifyHighPrecisionSubtractSingle() {

    Decimal128 a, b, r;
    String sA, sB;

    a = new Decimal128();
    sA = makeNumericString(37);
    a.update(sA, (short) 0);
    b = new Decimal128();
    sB = makeNumericString(37);
    b.update(sB, (short) 0);

    r = new Decimal128();
    r.addDestructive(a, (short) 0);
    r.subtractDestructive(b, (short) 0);

    String res1 = r.toFormalString();

    // Now do the add with Java BigDecimal
    BigDecimal bdA = new BigDecimal(sA);
    BigDecimal bdB = new BigDecimal(sB);
    BigDecimal bdR = bdA.subtract(bdB);

    String res2 = bdR.toPlainString();

    // Compare the results
    String message = "For operation " + a.toFormalString() + " - " + b.toFormalString();
    assertEquals(message, res2, res1);
  }

  // Test a set of random multiplications at high precision.
  @Test
  public void testHighPrecisionDecimal128Multiply() {
    final int N = 10000;
    for (int i = 0; i < N; i++) {
      verifyHighPrecisionMultiplySingle();
    }
  }

  // Test a single, high-precision multiply of random inputs.
  private void verifyHighPrecisionMultiplySingle() {

    Decimal128 a, b, r;
    String sA, sB;

    Random rand = new Random();
    int aDigits = rand.nextInt(37) + 1; // number of digits in a (1..37)
    int bDigits = 38 - aDigits;         // number of digits in b (1..37)
    assertTrue(aDigits + bDigits == 38 && aDigits > 0 && bDigits > 0);

    a = new Decimal128();
    sA = makeNumericString(aDigits);
    a.update(sA, (short) 0);
    b = new Decimal128();
    sB = makeNumericString(bDigits);
    b.update(sB, (short) 0);

    r = new Decimal128();
    r.addDestructive(a, (short) 0);
    r.multiplyDestructive(b, (short) 0);

    String res1 = r.toFormalString();

    // Now do the operation with Java BigDecimal
    BigDecimal bdA = new BigDecimal(sA);
    BigDecimal bdB = new BigDecimal(sB);
    BigDecimal bdR = bdA.multiply(bdB);

    String res2 = bdR.toPlainString();

    // Compare the results
    String message = "For operation " + a.toFormalString() + " * " + b.toFormalString();
    assertEquals(message, res2, res1);
  }

  // Test a single, high-precision multiply of random inputs.
  // Arguments must be integers with optional - sign, represented as strings.
  // Arguments must have 1 to 37 digits and the number of total digits
  // must be <= 38.
  private void verifyHighPrecisionMultiplySingle(String argA, String argB) {

    Decimal128 a, b, r;
    String sA, sB;

    // verify number of digits is <= 38 and each number has 1 or more digits
    int aDigits = argA.length();
    aDigits -= argA.charAt(0) == '-' ? 1 : 0;
    int bDigits = argB.length();
    bDigits -= argB.charAt(0) == '-' ? 1 : 0;
    assertTrue(aDigits + bDigits <= 38 && aDigits > 0 && bDigits > 0);

    a = new Decimal128();
    sA = argA;
    a.update(sA, (short) 0);
    b = new Decimal128();
    sB = argB;
    b.update(sB, (short) 0);

    r = new Decimal128();
    r.addDestructive(a, (short) 0);
    r.multiplyDestructive(b, (short) 0);

    String res1 = r.toFormalString();

    // Now do the operation with Java BigDecimal
    BigDecimal bdA = new BigDecimal(sA);
    BigDecimal bdB = new BigDecimal(sB);
    BigDecimal bdR = bdA.multiply(bdB);

    String res2 = bdR.toPlainString();

    // Compare the results
    String message = "For operation " + a.toFormalString() + " * " + b.toFormalString();
    assertEquals(message, res2, res1);
  }


  // Test a set of random divisions at high precision.
  @Test
  public void testHighPrecisionDecimal128Divide() {
    final int N = 10000;
    for (int i = 0; i < N; i++) {
      verifyHighPrecisionDivideSingle();
    }
  }

  // Test a single, high-precision divide of random inputs.
  private void verifyHighPrecisionDivideSingle() {

    Decimal128 a, b, r;
    String sA, sB;

    Random rand = new Random();
    int aDigits = rand.nextInt(37) + 1; // number of digits in a (1..37)
    int bDigits = 38 - aDigits;         // number of digits in b (1..37)
    int temp;

    // make sure b will have less digits than A
    if (bDigits > aDigits) {
      temp = aDigits;
      aDigits = bDigits;
      bDigits = temp;
    }
    if (bDigits == aDigits) {
      return;
    }
    assertTrue(aDigits + bDigits == 38 && aDigits > 0 && bDigits > 0);

    a = new Decimal128();
    sA = makeNumericString(aDigits);
    a.update(sA, (short) 0);
    b = new Decimal128();
    sB = makeNumericString(bDigits);
    b.update(sB, (short) 0);
    if (b.isZero()) {

      // don't do zero-divide if one comes up at random
      return;
    }

    r = new Decimal128();
    r.addDestructive(a, (short) 0);
    r.divideDestructive(b, (short) 0);

    String res1 = r.toFormalString();

    // Now do the operation with Java BigDecimal
    BigDecimal bdA = new BigDecimal(sA);
    BigDecimal bdB = new BigDecimal(sB);
    BigDecimal bdR = bdA.divide(bdB, 0, RoundingMode.HALF_UP);

    String res2 = bdR.toPlainString();

    // Compare the results
    String message = "For operation " + a.toFormalString() + " / " + b.toFormalString();
    assertEquals(message, res2, res1);
  }

  /* Return a random number with length digits, as a string. Results may be
   * negative or positive.
   */
  private String makeNumericString(int length) {
    Random r = new Random();
    StringBuilder b = new StringBuilder();
    for(int i = 0; i < length; i++) {
      b.append(r.nextInt(10));
    }

    // choose a random sign
    String sign = r.nextInt(2) == 0 ? "-" : "";
    return sign + b.toString();
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
    Decimal128 one = new Decimal128(1);
    for (int i = LOOPS; i > 0; --i) {
      multiplier.update(i, SCALE);
      current.multiplyDestructive(multiplier, SCALE);
      dividor.update(1 + 2 * i, SCALE);
      current.divideDestructive(dividor, SCALE);
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
    Decimal128 current = new Decimal128();
    for (int i = 0; i < LOOPS; ++i) {
      current.update(3, SCALE);
      dividor.update(2 * i + 1, SCALE);
      current.divideDestructive(dividor, SCALE);
      for (int j = 1; j <= i; ++j) {
        multiplier.update(i + j, SCALE);
        dividor.update(16 * j, SCALE);
        current.multiplyDestructive(multiplier, SCALE);
        current.divideDestructive(dividor, SCALE);
      }

      total.addDestructive(current, SCALE);
    }

    assertTrue(total.toFormalString().startsWith("3.141592653589793238462"));
  }

  @Test
  public void testDoubleValue() {
    Decimal128 quotient = new Decimal128();

    Decimal128 three = new Decimal128(3);
    Decimal128 four = new Decimal128(9);
    Decimal128.divide(three, four, quotient, (short) 38);
    assertEquals(0.33333333333333333333333333d, quotient.doubleValue(),
        0.0000000000000000000000001d);

    Decimal128 minusThree = new Decimal128(-3);
    Decimal128.divide(minusThree, four, quotient, (short) 38);
    assertEquals(-0.33333333333333333333333333d, quotient.doubleValue(),
        0.0000000000000000000000001d);
  }

  @Test
  public void testFloatValue() {
    Decimal128 quotient = new Decimal128();

    Decimal128 three = new Decimal128(3);
    Decimal128 four = new Decimal128(9);
    Decimal128.divide(three, four, quotient, (short) 38);
    assertEquals(0.3333333333333333f, quotient.floatValue(), 0.00000000001f);

    Decimal128 minusThree = new Decimal128(-3);
    Decimal128.divide(minusThree, four, quotient, (short) 38);
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

    // Try the extremes of precision and scale.

    // digit  measuring stick:
    //                12345678901234567890123456789012345678
    new Decimal128("0.99999999999999999999999999999999999999", (short) 38)
      .checkPrecisionOverflow(38);

    try {
      new Decimal128("0.99999999999999999999999999999999999999", (short) 38)
        .checkPrecisionOverflow(37);
      fail();
    } catch (ArithmeticException ex) {
    }

    new Decimal128("99999999999999999999999999999999999999", (short) 0)
      .checkPrecisionOverflow(38);

    try {
      new Decimal128("99999999999999999999999999999999999999", (short) 0)
        .checkPrecisionOverflow(37);
      fail();
    } catch (ArithmeticException ex) {
    }
  }

  @Test
  public void testToLong() {
    Decimal128 d = new Decimal128("1.25", (short) 2);
    assertEquals(1, d.longValue());
    d.update("4294967295", (short) 0); // 2^32-1
    assertEquals(4294967295L, d.longValue());
    d.update("4294967296", (short) 0); // 2^32 -- needs 2 32 bit words
    assertEquals(4294967296L, d.longValue());
    d.update("-4294967295", (short) 0); // -(2^32-1)
    assertEquals(-4294967295L, d.longValue());
    d.update("-4294967296", (short) 0); // -(2^32)
    assertEquals(-4294967296L, d.longValue());
    d.update("4294967295.01", (short) 2); // 2^32-1 + .01
    assertEquals(4294967295L, d.longValue());
    d.update("4294967296.01", (short) 2); // 2^32 + .01
    assertEquals(4294967296L, d.longValue());

    // Compare long value with HiveDecimal#longValue
    d.update(37.678, (short)5);
    HiveDecimal hd = HiveDecimal.create(BigDecimal.valueOf(37.678));
    assertEquals(hd.longValue(), d.longValue());
  }

  @Test
  public void testToHiveDecimalString() {
    Decimal128 d1 = new Decimal128("4134.923076923077", (short) 15);
    assertEquals("4134.923076923077", d1.getHiveDecimalString());

    Decimal128 d2 = new Decimal128("0.00923076923", (short) 15);
    assertEquals("0.00923076923", d2.getHiveDecimalString());

    Decimal128 d3 = new Decimal128("0.00923076000", (short) 15);
    assertEquals("0.00923076", d3.getHiveDecimalString());

    Decimal128 d4 = new Decimal128("4294967296.01", (short) 15);
    assertEquals("4294967296.01", d4.getHiveDecimalString());

    Decimal128 d5 = new Decimal128("4294967296.01", (short) 2);
    assertEquals("4294967296.01", d5.getHiveDecimalString());

    Decimal128 d6 = new Decimal128();
    HiveDecimal hd1 = HiveDecimal.create(new BigInteger("42949672"));
    d6.update(hd1.bigDecimalValue());
    assertEquals(hd1.toString(), d6.getHiveDecimalString());

    Decimal128 d7 = new Decimal128();
    HiveDecimal hd2 = HiveDecimal.create(new BigDecimal("0.0"));
    d7.update(hd2.bigDecimalValue());
    assertEquals(hd2.toString(), d7.getHiveDecimalString());

    Decimal128 d8 = new Decimal128();
    HiveDecimal hd3 = HiveDecimal.create(new BigDecimal("0.00023000"));
    d8.update(hd3.bigDecimalValue());
    assertEquals(hd3.toString(), d8.getHiveDecimalString());

    Decimal128 d9 = new Decimal128();
    HiveDecimal hd4 = HiveDecimal.create(new BigDecimal("0.1"));
    d9.update(hd4.bigDecimalValue());
    assertEquals(hd4.toString(), d9.getHiveDecimalString());

    Decimal128 d10 = new Decimal128();
    HiveDecimal hd5 = HiveDecimal.create(new BigDecimal("-00.100"));
    d10.update(hd5.bigDecimalValue());
    assertEquals(hd5.toString(), d10.getHiveDecimalString());

    Decimal128 d11 = new Decimal128();
    HiveDecimal hd6 = HiveDecimal.create(new BigDecimal("00.1"));
    d11.update(hd6.bigDecimalValue());
    assertEquals(hd6.toString(), d11.getHiveDecimalString());

    Decimal128 d12 = new Decimal128(27.000, (short)3);
    HiveDecimal hd7 = HiveDecimal.create(new BigDecimal("27.000"));
    assertEquals(hd7.toString(), d12.getHiveDecimalString());
    assertEquals("27", d12.getHiveDecimalString());

    Decimal128 d13 = new Decimal128(1234123000, (short)3);
    HiveDecimal hd8 = HiveDecimal.create(new BigDecimal("1234123000"));
    assertEquals(hd8.toString(), d13.getHiveDecimalString());
    assertEquals("1234123000", d13.getHiveDecimalString());
  }

  @Test
  public void testUpdateWithScale() {
    Decimal128 d1 = new Decimal128(1234.123, (short)4);
    Decimal128 d2 = new Decimal128(0, (short)3);
    d2.update(d1, (short)3);
    assertEquals(0, d1.compareTo(d2));
  }
}
