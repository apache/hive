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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;

/**
 * Math expression evaluation helper functions.
 * Some of these are referenced from ColumnUnaryFunc.txt.
 */
public class MathExpr {

  // Round using the "half-up" method used in Hive.
  public static double round(double d) {
    if (d > 0.0) {
      return (double) ((long) (d + 0.5d));
    } else {
      return (double) ((long) (d - 0.5d));
    }
  }

  public static double log2(double d) {
    return Math.log(d) / Math.log(2);
  }

  public static long abs(long v) {
    return v >= 0 ? v : -v;
  }

  public static double sign(double v) {
    return v >= 0 ? 1.0 : -1.0;
  }

  public static double sign(long v) {
    return v >= 0 ? 1.0 : -1.0;
  }

  // for casting integral types to boolean
  public static long toBool(long v) {
    return v == 0 ? 0 : 1;
  }

  // for casting floating point types to boolean
  public static long toBool(double v) {
    return v == 0.0D ? 0L : 1L;
  }

  /* Convert an integer value in miliseconds since the epoch to a timestamp value
   * for use in a long column vector, which is represented in nanoseconds since the epoch.
   */
  public static long longToTimestamp(long v) {
    return v * 1000000;
  }

  // Convert seconds since the epoch (with fraction) to nanoseconds, as a long integer.
  public static long doubleToTimestamp(double v) {
    return (long)( v * 1000000000.0);
  }

  /* Convert an integer value representing a timestamp in nanoseconds to one
   * that represents a timestamp in seconds (since the epoch).
   */
  public static long fromTimestamp(long v) {
    return v / 1000000000;
  }

  /* Convert an integer value representing a timestamp in nanoseconds to one
   * that represents a timestamp in seconds, with fraction, since the epoch.
   */
  public static double fromTimestampToDouble(long v) {
    return ((double) v) / 1000000000.0;
  }

  /* Convert a long to a string. The string is output into the argument
   * byte array, beginning at character 0. The length is returned.
   */
  public static int writeLongToUTF8(byte[] result, long i) {
    if (i == 0) {
      result[0] = '0';
      return 1;
    }

    int current = 0;

    if (i < 0) {
      result[current++] ='-';
    } else {
      // negative range is bigger than positive range, so there is no risk
      // of overflow here.
      i = -i;
    }

    long start = 1000000000000000000L;
    while (i / start == 0) {
      start /= 10;
    }

    while (start > 0) {
      result[current++] = (byte) ('0' - (i / start % 10));
      start /= 10;
    }

    return current;
  }
  
  // Convert all NaN values in vector v to NULL. Should only be used if n > 0.
  public static void NaNToNull(DoubleColumnVector v, int[] sel, boolean selectedInUse, int n) {
    NaNToNull(v, sel, selectedInUse, n, false);
  }

  // Convert all NaN, and optionally infinity values in vector v to NULL.
  // Should only be used if n > 0.
  public static void NaNToNull(
      DoubleColumnVector v, int[] sel, boolean selectedInUse, int n, boolean convertInfinity) {
    // handle repeating case
    if (v.isRepeating) {
      if ((convertInfinity && Double.isInfinite(v.vector[0])) || Double.isNaN(v.vector[0])){
        v.vector[0] = DoubleColumnVector.NULL_VALUE;
        v.isNull[0] = true;
        v.noNulls = false;
      }
      return;
    }

    if (v.noNulls) {
      if (selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          if ((convertInfinity && Double.isInfinite(v.vector[i])) || Double.isNaN(v.vector[i])) {
            v.vector[i] = DoubleColumnVector.NULL_VALUE;
            v.isNull[i] = true;
            v.noNulls = false;
          } else {
            // Must set isNull[i] to false to make sure
            // it gets initialized, in case we set noNulls to true.
            v.isNull[i] = false;
          }
        }
      } else {
        for(int i = 0; i != n; i++) {
          if ((convertInfinity && Double.isInfinite(v.vector[i])) || Double.isNaN(v.vector[i])) {
            v.vector[i] = DoubleColumnVector.NULL_VALUE;
            v.isNull[i] = true;
            v.noNulls = false;
          } else {
            v.isNull[i] = false;
          }
        }
      }
    } else {  // there are nulls, so null array entries are already initialized
      if (selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          if((convertInfinity && Double.isInfinite(v.vector[i])) || Double.isNaN(v.vector[i])) {
            v.vector[i] = DoubleColumnVector.NULL_VALUE;
            v.isNull[i] = true;
          }
        }
      } else {
        for (int i = 0; i != n; i++) {
          if((convertInfinity && Double.isInfinite(v.vector[i])) || Double.isNaN(v.vector[i])) {
            v.vector[i] = DoubleColumnVector.NULL_VALUE;
            v.isNull[i] = true;
          }
        }
      }
    }
  }
}
