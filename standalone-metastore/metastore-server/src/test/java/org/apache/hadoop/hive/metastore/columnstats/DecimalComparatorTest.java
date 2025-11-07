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
package org.apache.hadoop.hive.metastore.columnstats;

import com.google.common.math.BigIntegerMath;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.columnstats.DecimalComparator.Approach;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.function.Consumer;

import static org.apache.hadoop.hive.metastore.columnstats.DecimalComparator.PAD_NEG;
import static org.apache.hadoop.hive.metastore.columnstats.DecimalComparator.PAD_POS;
import static org.apache.hadoop.hive.metastore.columnstats.DecimalComparator.bitLog;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DecimalComparatorTest {

  public record ExpectApproach(Approach expected) implements DecimalComparator.ApproachInfoCallback {

    @Override
    public void accept(Approach approach) {
      assertEquals(expected, approach);
    }
  }

  public static final HexFormat FORMAT = HexFormat.ofDelimiter(" ");

  public String toStr(Decimal val) {
    return Objects.toString(new BigDecimal(new BigInteger(val.getUnscaled()), val.getScale()));
  }

  /**
   * Create a Decimal.
   * @param bigDecimal the value
   * @param scaleDrift multiply by 10^scaleDrift * 10^-scaleDrift.
   *                   The former goes to the significand, the latter to the exponent.
   *                   In case of a negative scaleDrift, the significand is divided by 10^scaleDrift.
   * @return the Decimal
   */
  public static Decimal createDecimal(BigDecimal bigDecimal, int scaleDrift) {
    BigDecimal powTen = BigDecimal.TEN.pow(Math.abs(scaleDrift));
    BigDecimal adapted = scaleDrift >= 0 ? bigDecimal.multiply(powTen) : bigDecimal.divide(powTen, RoundingMode.DOWN);
    byte[] byteArray = adapted.unscaledValue().toByteArray();
    ByteBuffer wrap = ByteBuffer.wrap(byteArray);
    return new Decimal((short) (bigDecimal.scale() + scaleDrift), wrap);
  }

  public void check(String n1, String n2, Approach expectedApproach) {
    check(n1, 0, n2, 0, expectedApproach);
  }

  public void check(String n1, int scaleDrift1, String n2, int scaleDrift2, Approach expectedApproach) {
    BigDecimal bd1 = new BigDecimal(n1), bd2 = new BigDecimal(n2);
    checkInner(bd1, scaleDrift1, bd2, scaleDrift2, expectedApproach);
    checkInner(bd2, scaleDrift2, bd1, scaleDrift1, expectedApproach);
  }

  public int normalizeCompareTo(int cmp) {
    return Integer.compare(cmp, 0);
  }

  public void checkInner(BigDecimal bd1, int scaleDrift1, BigDecimal bd2, int scaleDrift2, Approach expectedApproach) {
    Decimal d1 = createDecimal(bd1, scaleDrift1);
    Decimal d2 = createDecimal(bd2, scaleDrift2);

    int expected = normalizeCompareTo(bd1.compareTo(bd2));
    int actual = normalizeCompareTo(new DecimalComparator().compareInner(d1, d2, false,
        expectedApproach == null ? null : new ExpectApproach(expectedApproach)));
    if (expected != actual) {
      System.out.println("compareTo result was wrong for " + toStr(d1) + " and " + toStr(
          d2) + ": " + expected + ", but was " + actual);
      assertEquals("compareTo result was wrong for " + bd1 + " and " + bd2, expected, actual);
    }
  }

  @Test
  public void testDecimal() {
    interface Helper {
      void check(String input, int scaleDrift, double expectedDouble, String expectedHiveStr, String expectedJavaStr);
    }
    Helper helper = (input, scaleDrift, expectedDouble, expectedHiveStr, expectedJavaStr) -> {
      Decimal d3 = createDecimal(new BigDecimal(input), scaleDrift);
      assertEquals(expectedDouble, MetaStoreServerUtils.decimalToDouble(d3), Double.MIN_VALUE);
      assertEquals(expectedHiveStr, MetaStoreServerUtils.decimalToString(d3));
      assertEquals(expectedJavaStr, toStr(d3));
    };

    helper.check("1.2", 0, 1.2, "1.2", "1.2");
    helper.check("123.45", 0, 123.45, "123.45", "123.45");
    helper.check("123.45", 1, 123.45, "123.45", "123.450");
    helper.check("123.45", 10, 123.45, "123.45", "123.450000000000");
    helper.check("1", 2, 1, "1", "1.00");
  }

  @Test
  public void testApproachSign() {
    check("1", "-1", Approach.SIGN);
    check("123", "-123", Approach.SIGN);
    check("123", "-11.1", Approach.SIGN);
    check("123.23", "-11.1", Approach.SIGN);
    check("123.23", "-11", Approach.SIGN);
  }

  @Test
  public void testApproachSameScale() {
    check("2", "8", Approach.EQSCALE);
    check("-2", "-8", Approach.EQSCALE);
    check("20", "80", Approach.EQSCALE);
    check("50", "20", Approach.EQSCALE);
    check("-20", "-80", Approach.EQSCALE);
    check("-50", "-20", Approach.EQSCALE);
    check("1000", "1001", Approach.EQSCALE);
    check("-1000", "-1001", Approach.EQSCALE);
    check("100000000", "100000001", Approach.EQSCALE);
    check("-100000000", "-100000001", Approach.EQSCALE);

    check("10", "1", Approach.EQSCALE);
    check("1", "10", Approach.EQSCALE);
    check("-10", "-1", Approach.EQSCALE);
    check("-1", "-10", Approach.EQSCALE);

    check("-10.2", "-123.2", Approach.EQSCALE);
    check("-123.2", "-10.2", Approach.EQSCALE);

    check("-2E+1", "-8E+1", Approach.EQSCALE);
  }

  @Test
  public void testApproachBitLog() {
    // positive values
    check("3", 0, "20", -1, Approach.BITLOG_A);
    check("9.123", "8113", Approach.BITLOG_A);
    check("9123", "8.113", Approach.BITLOG_A);
    check("9123", "1.113", Approach.BITLOG_A);
    check("1123", "9.113", Approach.BITLOG_A);
    check("10.21232", "123.2", Approach.BITLOG_A);

    check("9.1", "8113.123", Approach.BITLOG_B);
    check("9123.123", "8.1", Approach.BITLOG_B);
    check("9123.123", "1.1", Approach.BITLOG_B);
    check("1123.123", "9.1", Approach.BITLOG_B);

    // negative values
    check("-9.123", "-8113", Approach.BITLOG_A);
    check("-9123", "-8.113", Approach.BITLOG_A);
    check("-9123", "-1.113", Approach.BITLOG_A);
    check("-1123", "-9.113", Approach.BITLOG_A);
    check("-10.21232", "-123.2", Approach.BITLOG_A);

    check("-9.1", "-8113.123", Approach.BITLOG_B);
    check("-9123.123", "-8.1", Approach.BITLOG_B);
    check("-9123.123", "-1.1", Approach.BITLOG_B);
    check("-1123.123", "-9.1", Approach.BITLOG_B);

    // with scale drift
    check("10.21232", 2, "123.2", 0, Approach.BITLOG_A);
    check("10.21232", 10, "123.2", 0, Approach.BITLOG_A);
    check("10.21232", 0, "123.2", 3, Approach.BITLOG_A);
    // scale (digits after dots) of n1 is 5, scale of n2 is 1,
    // with a drift of 4 the second number also has 5 digits after the dot
    check("10.21232", 0, "123.2", 4, Approach.EQSCALE);
    check("10.21232", 0, "123.2", 5, Approach.BITLOG_B);
    check("10.21232", 0, "123.2", 10, Approach.BITLOG_B);
  }

  @Test
  public void testBitLog1() {
    interface Helper {
      void accept(int expected, int input);
    }
    Helper check = (expected, input) -> assertEquals(expected,
        bitLog(BigInteger.valueOf(input).toByteArray(), input >= 0 ? PAD_POS : PAD_NEG));

    check.accept(1, 1);
    check.accept(2, 2);
    check.accept(2, 3);
    check.accept(8, 255);
    check.accept(9, 256);
    check.accept(9, 511);
    check.accept(10, 512);
    check.accept(10, 1023);
    check.accept(11, 1024);

    check.accept(1, -1);
    check.accept(2, -2);
    check.accept(2, -3);
    check.accept(8, -255);
    check.accept(9, -256);
    check.accept(9, -511);
    check.accept(10, -512);
    check.accept(10, -1023);
    check.accept(11, -1024);

    // log2(5800) is about 12.5, bitLog floor(log(...))+1
    check.accept(13, 5800);
    check.accept(13, -5800);
  }

  @Test
  public void testBitLog2() {
    interface Helper {
      void accept(int expected, String input, byte pad);
    }
    Helper check = (expected, input, pad) -> assertEquals(expected, bitLog(FORMAT.parseHex(input), pad));

    check.accept(8, "FF 16", PAD_NEG);

    check.accept(0, "00 00 00", PAD_POS);
    check.accept(1, "00 00 01", PAD_POS);
    check.accept(2, "00 00 02", PAD_POS);
    check.accept(3, "00 00 04", PAD_POS);
    check.accept(4, "00 00 08", PAD_POS);
    check.accept(5, "00 00 10", PAD_POS);
    check.accept(5, "00 00 11", PAD_POS);
    check.accept(9, "00 01 11", PAD_POS);
    check.accept(17, "01 11 11", PAD_POS);
    check.accept(17, "01 00 00", PAD_POS);
    check.accept(17, "00 00 00 01 00 00", PAD_POS);

    check.accept(1, "FF FF FF", PAD_NEG);
    check.accept(2, "FF FF FE", PAD_NEG);
    check.accept(2, "FF FF FD", PAD_NEG);
    check.accept(3, "FF FF FB", PAD_NEG);
    check.accept(4, "FF FF F7", PAD_NEG);
    check.accept(5, "FF FF EF", PAD_NEG);
    check.accept(5, "FF FF EE", PAD_NEG);
    check.accept(9, "FF FE EE", PAD_NEG);
    check.accept(17, "FE EE EE", PAD_NEG);
    check.accept(17, "FE FF FF", PAD_NEG);
    check.accept(17, "FF FF FF FE FF FF", PAD_NEG);
  }

  @Test
  public void testBitLenPostcondition() {
    // define helper which executes the asserts
    Consumer<BigInteger> check = x -> {
      int bl = bitLog(x.toByteArray(), x.signum() == -1 ? PAD_NEG : PAD_POS);
      int log = BigIntegerMath.log2(x.abs(), RoundingMode.DOWN);
      if (log < 0) {
        fail("Log may not be negative: " + x);
      }

      if (log + 1 != bl) {
        fail(x.toString());
      }
      // check inequalities
      if (!(bl - 1 <= log)) {
        fail(x.toString());
      }
      if (!(log < bl)) {
        fail(x.toString());
      }
    };

    // check all integers from -18 to 18 (inclusive)
    for (int i = -18; i <= 18; i++) {
      if (i == 0) {
        continue;
      }
      BigInteger x = BigInteger.valueOf(i);
      check.accept(x);
    }

    // check powers of two from 2^5 to 2^200 (about 60 decimal digits)
    for (int i = 5; i < 200; i++) {
      BigInteger p = BigInteger.TWO.pow(i);
      // check negative and positive powers of two: -2^p and +2^p
      for (int neg = 0; neg < 2; neg++) {
        if (neg == 1) {
          p = p.negate();
        }

        // check the neighborhood of each power of two
        for (int j = -2; j <= 2; j++) {
          BigInteger x = p.add(BigInteger.valueOf(j));
          if (x.compareTo(BigInteger.ZERO) == 0) {
            continue;
          }
          check.accept(x);
        }
      }
    }
  }

  @Test
  public void testRandomized() {
    long globalSeed = System.nanoTime();
    Random rOuter = new Random(globalSeed);

    int shift = 15;
    int minScale = -(1 << shift);
    int maxScale = (1 << shift) - 1;

    int[] count = new int[Approach.LEN];
    int[] countError = new int[Approach.LEN];

    Map<Long, Throwable> errors = new HashMap<>();
    for (int i = 0; i < 1000; i++) {
      long seed = rOuter.nextLong();
      int[] approachIdx = new int[] { Approach.UNKNOWN.ordinal() };
      try {
        randomInner(seed, minScale, maxScale, m -> {
          count[m.ordinal()] += 1;
          approachIdx[0] = m.ordinal();
        });
      } catch (Throwable t) {
        countError[approachIdx[0]] += 1;
        errors.put(seed, t);
      }
    }

    if (!errors.isEmpty()) {
      var sb = new StringBuilder();
      sb.append("The randomized test has failed for global seed ").append(globalSeed)
          .append("; some example exceptions:\n");
      int sc = 0;
      for (var entry : errors.entrySet()) {
        sb.append("\n- seed ").append(entry.getKey()).append(":\n  ");
        sb.append(ExceptionUtils.getStackTrace(entry.getValue()).replace("\n", "\n  "));
        if ((++sc) >= 10) {
          break;
        }
      }
      throw new AssertionError(sb.toString());
    }

    var bitlogUsage = Math.max(count[Approach.BITLOG_A.ordinal()], count[Approach.BITLOG_B.ordinal()]);
    if (count[Approach.FALLBACK.ordinal()] > bitlogUsage) {
      var sb = new StringBuilder();
      sb.append("BITLOG should be used more often than the fallback:\n");
      sb.append(DecimalComparatorTest.class.getSimpleName()).append(" randomized test statistics:");
      for (Approach m : Approach.values()) {
        sb.append("  ").append(m).append(": ").append(count[m.ordinal()]).append(" errors: ")
            .append(countError[m.ordinal()]).append("\n");
      }
      throw new IllegalStateException(sb.toString());
    }
  }

  private void randomInner(long seed, int minScale, int maxScale, DecimalComparator.ApproachInfoCallback aic) {
    Random r = new Random(seed);
    int len = r.nextInt(30) + 1;
    byte[] num = new byte[len];
    r.nextBytes(num);

    int scaleDrift1 = r.nextInt(10);
    int scaleDrift2 = r.nextInt(10);

    int s1 = r.nextInt(maxScale - scaleDrift1 - minScale) + minScale;
    // both scales should be somehow "close"
    int s2 = Math.clamp(s1 + (int) r.nextGaussian(0, 10), minScale, maxScale - scaleDrift2);

    byte[] num2 = Arrays.copyOf(num, num.length);
    num2[0] = (byte) r.nextInt();
    // ensure the numbers have the same sign
    num2[0] = (byte) ((num2[0] & 0x7f) | (num[0] & 0x80));

    int adapt = 0;
    for (int i = 0; i < 100; i++) {
      BigDecimal bd1 = new BigDecimal(new BigInteger(num), s1);
      BigDecimal bd2 = new BigDecimal(new BigInteger(num2), s2);

      int expected = normalizeCompareTo(bd1.compareTo(bd2));

      Decimal d1 = Objects.requireNonNull(createDecimal(bd1, scaleDrift1));
      Decimal d2 = Objects.requireNonNull(createDecimal(bd2, scaleDrift2));

      int[] approachIdx = new int[] { Approach.UNKNOWN.ordinal() };
      int actual = new DecimalComparator().compareInner(d1, d2, false, m -> approachIdx[0] = m.ordinal());
      if (aic != null) {
        aic.accept(Approach.values()[approachIdx[0]]);
      }
      if (approachIdx[0] != Approach.FALLBACK.ordinal() && expected != normalizeCompareTo(actual)) {
        String expOp = expected < 0 ? " < " : expected > 0 ? " > " : " = ";
        assertEquals(
            "compareTo result was wrong for " + bd1 + expOp + bd2 + ", approach " + Approach.values()[approachIdx[0]] + ", seed " + seed,
            expected, actual);
      }

      if (adapt == 0) {
        // in the first iteration, choose which number will get halved in every iteration
        if (bd1.signum() == 1) {
          adapt = expected > 0 ? 1 : 2;
        } else {
          adapt = expected > 0 ? 2 : 1;
        }
      }
      if (adapt == 1) {
        shiftRight(num);
      } else {
        shiftRight(num2);
      }

      // stop if the adapted number has become zero
      if (bd1.unscaledValue().bitLength() == 0 || bd2.unscaledValue().bitLength() == 0) {
        break;
      }
    }
  }

  /**
   * Perform a signed right shift.
   */
  private void shiftRight(byte[] num) {
    int carry = num[0] & 0x80;
    for (int i = 0; i < num.length; i++) {
      int nextCarry = (num[i] & 0x1) << 7;
      // use &0xff to do an unsigned (!) right-shift,
      // so that we can just OR the carry from the previous byte
      // the first carry take care so that we keep the sign of the number
      int rightShifted = (num[i] & 0xff) >> 1;
      num[i] = (byte) ((carry | rightShifted) & 0xff);
      carry = nextCarry;
    }
  }

}
