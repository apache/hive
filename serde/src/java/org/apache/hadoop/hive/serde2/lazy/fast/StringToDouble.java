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
/*
 *	Source code for the "strtod" library procedure.
 *
 * Copyright 1988-1992 Regents of the University of California
 * Permission to use, copy, modify, and distribute this
 * software and its documentation for any purpose and without
 * fee is hereby granted, provided that the above copyright
 * notice appear in all copies.  The University of California
 * makes no representations about the suitability of this
 * software for any purpose.  It is provided "as is" without
 * express or implied warranty.
 */
package org.apache.hadoop.hive.serde2.lazy.fast;

import java.nio.charset.StandardCharsets;

public class StringToDouble {
  static final int maxExponent = 511;	/* Largest possible base 10 exponent.  Any
				 * exponent larger than this will already
				 * produce underflow or overflow, so there's
				 * no need to worry about additional digits.
				 */
  static final double powersOf10[] = {	/* Table giving binary powers of 10.  Entry */
      10.,			/* is 10^2^i.  Used to convert decimal */
      100.,			/* exponents into floating-point numbers. */
      1.0e4,
      1.0e8,
      1.0e16,
      1.0e32,
      1.0e64,
      1.0e128,
      1.0e256
  };

  /*
   * Only for testing
   */
  static double strtod(String s) {
    final byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
    return strtod(utf8, 0, utf8.length);
  }

  public static double strtod(byte[] utf8, int offset, int length)
  {
    if (length == 0) {
      throw new NumberFormatException();
    }
    boolean signIsNegative = true;
    boolean expSignIsNegative = true;
    double fraction;
    int d;
    int p = offset;
    int end = offset + length;
    int c;
    int exp = 0;		/* Exponent read from "EX" field. */
    int fracExp = 0;		/* Exponent that derives from the fractional
				 * part.  Under normal circumstances, it is
				 * the negative of the number of digits in F.
				 * However, if I is very long, the last digits
				 * of I get dropped (otherwise a long I with a
				 * large negative exponent could cause an
				 * unnecessary overflow on I alone).  In this
				 * case, fracExp is incremented one for each
				 * dropped digit. */
    int mantSize;		/* Number of digits in mantissa. */
    int decPt;			/* Number of mantissa digits BEFORE decimal
				 * point. */
    int pExp;		/* Temporarily holds location of exponent
				 * in string. */

    /*
     * Strip off leading blanks and check for a sign.
     */

    while(p < end && Character.isWhitespace(utf8[p])) {
      p++;
    }
    while(end > p && Character.isWhitespace(utf8[end - 1])) {
      end--;
    }
    if (!testSimpleDecimal(utf8, p, end - p)) {
      return Double.parseDouble(new String(utf8, p, end-p, StandardCharsets.UTF_8));
    }

    if (utf8[p] == '-') {
      signIsNegative = true;
      p += 1;
    } else {
      if (utf8[p] == '+') {
        p += 1;
      }
      signIsNegative = false;
    }

    /*
     * Count the number of digits in the mantissa (including the decimal
     * point), and also locate the decimal point.
     */

    decPt = -1;
    int mantEnd = end - p;
    for (mantSize = 0; mantSize < mantEnd; mantSize += 1)
    {
      c = utf8[p];
      if (!isdigit(c)) {
        if ((c != '.') || (decPt >= 0)) {
          break;
        }
        decPt = mantSize;
      }
      p += 1;
    }

    /*
     * Now suck up the digits in the mantissa.  Use two integers to
     * collect 9 digits each (this is faster than using floating-point).
     * If the mantissa has more than 18 digits, ignore the extras, since
     * they can't affect the value anyway.
     */

    pExp  = p;
    p -= mantSize;
    if (decPt < 0) {
      decPt = mantSize;
    } else {
      mantSize -= 1;			/* One of the digits was the point. */
    }
    if (mantSize > 18) {
      fracExp = decPt - 18;
      mantSize = 18;
    } else {
      fracExp = decPt - mantSize;
    }
    if (mantSize == 0) {
      if (signIsNegative) {
        return -0.0d;
      }
      return 0.0d;
    } else {
      double frac1, frac2;
      frac1 = 0;
      for (; mantSize > 9; mantSize -= 1)
      {
        c = utf8[p];
        p += 1;
        if (c == '.') {
          c = utf8[p];
          p += 1;
        }
        frac1 = 10 * frac1 + (c - '0');
      }
      frac2 = 0;
      for (; mantSize > 0; mantSize -= 1)
      {
        c = utf8[p];
        p += 1;
        if (c == '.') {
          c = utf8[p];
          p += 1;
        }
        frac2 = 10 * frac2 + (c - '0');
      }
      fraction = (1e9d * frac1) + frac2;
    }

    /*
     * Skim off the exponent.
     */

    p = pExp;

    if (p < end) {
      if ((utf8[p] == 'E') || (utf8[p] == 'e')) {
        p += 1;
        if (p < end) {
          if (utf8[p] == '-') {
            expSignIsNegative = true;
            p += 1;
          } else {
            if (utf8[p] == '+') {
              p += 1;
            }
            expSignIsNegative = false;
          }
          while (p < end && isdigit(utf8[p])) {
            exp = exp * 10 + (utf8[p] - '0');
            p += 1;
          }
        }
      }
    }
    if (expSignIsNegative) {
      exp = fracExp - exp;
    } else {
      exp = fracExp + exp;
    }

    /*
     * Generate a floating-point number that represents the exponent.
     * Do this by processing the exponent one bit at a time to combine
     * many powers of 2 of 10. Then combine the exponent with the
     * fraction.
     */

    if (exp < 0) {
      expSignIsNegative = true;
      exp = -exp;
    } else {
      expSignIsNegative = false;
    }
    if (exp > maxExponent) {
      exp = maxExponent;
    }

    double dblExp = 1.0;
    for (d = 0; exp != 0; exp >>= 1, d += 1) {
      if ((exp & 1) == 1) {
        dblExp *= powersOf10[d];
      }
    }

    if (expSignIsNegative) {
      fraction /= dblExp;
    } else {
      fraction *= dblExp;
    }

    if (signIsNegative) {
      return -fraction;
    }
    return fraction;
  }

  private static boolean testSimpleDecimal(byte[] utf8, int off, int len) {
    if (len > 18) {
      return false;
    }
    int decimalPts = 0;
    int signs = 0;
    int nondigits = 0;
    int digits = 0;
    for (int i = off; i < len + off; i++) {
      final int c = utf8[i];
      if (c == '.') {
        decimalPts++;
      } else if (c == '-' || c == '+') {
        signs++;
      } else if (!isdigit(c)) {
        // could be exponential notations
        nondigits++;
      } else {
        digits++;
      }
    }
    // There can be up to 5e-16 error
    return (decimalPts <= 1 && signs <= 1 && nondigits == 0 && digits < 16);
  }

  private static boolean isdigit(int c) {
    return '0' <= c && c <= '9';
  }
}
