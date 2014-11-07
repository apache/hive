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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.udf.generic.RoundUtils;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

/**
 * Utility functions for vector operations on decimal values.
 */
public class DecimalUtil {

  public static int compare(HiveDecimalWritable writableLeft, HiveDecimal right) {
    return writableLeft.getHiveDecimal().compareTo(right);
  }

  public static int compare(HiveDecimal left, HiveDecimalWritable writableRight) {
    return left.compareTo(writableRight.getHiveDecimal());
  }

  // Addition with overflow check. Overflow produces NULL output.
  public static void addChecked(int i, HiveDecimal left, HiveDecimal right,
      DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, left.add(right));
    } catch (ArithmeticException e) {  // catch on overflow
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void addChecked(int i, HiveDecimalWritable left, HiveDecimalWritable right,
      DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, left.getHiveDecimal().add(right.getHiveDecimal()));
    } catch (ArithmeticException e) {  // catch on overflow
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void addChecked(int i, HiveDecimalWritable left, HiveDecimal right,
      DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, left.getHiveDecimal().add(right));
    } catch (ArithmeticException e) {  // catch on overflow
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void addChecked(int i, HiveDecimal left, HiveDecimalWritable right,
      DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, left.add(right.getHiveDecimal()));
    } catch (ArithmeticException e) {  // catch on overflow
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  // Subtraction with overflow check. Overflow produces NULL output.
  public static void subtractChecked(int i, HiveDecimal left, HiveDecimal right,
      DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, left.subtract(right));
    } catch (ArithmeticException e) {  // catch on overflow
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void subtractChecked(int i, HiveDecimalWritable left, HiveDecimalWritable right,
      DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, left.getHiveDecimal().subtract(right.getHiveDecimal()));
    } catch (ArithmeticException e) {  // catch on overflow
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void subtractChecked(int i, HiveDecimalWritable left, HiveDecimal right,
      DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, left.getHiveDecimal().subtract(right));
    } catch (ArithmeticException e) {  // catch on overflow
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void subtractChecked(int i, HiveDecimal left, HiveDecimalWritable right,
      DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, left.subtract(right.getHiveDecimal()));
    } catch (ArithmeticException e) {  // catch on overflow
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  // Multiplication with overflow check. Overflow produces NULL output.
  public static void multiplyChecked(int i, HiveDecimal left, HiveDecimal right,
      DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, left.multiply(right));
    } catch (ArithmeticException e) {  // catch on overflow
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void multiplyChecked(int i, HiveDecimalWritable left, HiveDecimalWritable right,
      DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, left.getHiveDecimal().multiply(right.getHiveDecimal()));
    } catch (ArithmeticException e) {  // catch on overflow
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void multiplyChecked(int i, HiveDecimalWritable left, HiveDecimal right,
      DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, left.getHiveDecimal().multiply(right));
    } catch (ArithmeticException e) {  // catch on overflow
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void multiplyChecked(int i, HiveDecimal left, HiveDecimalWritable right,
      DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, left.multiply(right.getHiveDecimal()));
    } catch (ArithmeticException e) {  // catch on overflow
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  // Division with overflow/zero-divide check. Error produces NULL output.
  public static void divideChecked(int i, HiveDecimal left, HiveDecimal right,
      DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, left.divide(right));
    } catch (ArithmeticException e) {  // catch on error
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void divideChecked(int i, HiveDecimalWritable left, HiveDecimalWritable right,
      DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, left.getHiveDecimal().divide(right.getHiveDecimal()));
    } catch (ArithmeticException e) {  // catch on error
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void divideChecked(int i, HiveDecimalWritable left, HiveDecimal right,
      DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, left.getHiveDecimal().divide(right));
    } catch (ArithmeticException e) {  // catch on error
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void divideChecked(int i, HiveDecimal left, HiveDecimalWritable right,
      DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, left.divide(right.getHiveDecimal()));
    } catch (ArithmeticException e) {  // catch on error
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  // Modulo operator with overflow/zero-divide check.
  public static void moduloChecked(int i, HiveDecimal left, HiveDecimal right,
      DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, left.remainder(right));
    } catch (ArithmeticException e) {  // catch on error
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void moduloChecked(int i, HiveDecimalWritable left, HiveDecimalWritable right,
      DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, left.getHiveDecimal().remainder(right.getHiveDecimal()));
    } catch (ArithmeticException e) {  // catch on error
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void moduloChecked(int i, HiveDecimalWritable left, HiveDecimal right,
      DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, left.getHiveDecimal().remainder(right));
    } catch (ArithmeticException e) {  // catch on error
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void moduloChecked(int i, HiveDecimal left, HiveDecimalWritable right,
      DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, left.remainder(right.getHiveDecimal()));
    } catch (ArithmeticException e) {  // catch on error
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void floor(int i, HiveDecimal input, DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, input.setScale(0, HiveDecimal.ROUND_FLOOR));
    } catch (ArithmeticException e) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void floor(int i, HiveDecimalWritable input, DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, input.getHiveDecimal().setScale(0, HiveDecimal.ROUND_FLOOR));
    } catch (ArithmeticException e) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void ceiling(int i, HiveDecimal input, DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, input.setScale(0, HiveDecimal.ROUND_CEILING));
    } catch (ArithmeticException e) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void ceiling(int i, HiveDecimalWritable input, DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, input.getHiveDecimal().setScale(0, HiveDecimal.ROUND_CEILING));
    } catch (ArithmeticException e) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void round(int i, HiveDecimal input, int decimalPlaces, DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, RoundUtils.round(input, decimalPlaces));
    } catch (ArithmeticException e) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void round(int i, HiveDecimalWritable input, int decimalPlaces, DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, RoundUtils.round(input.getHiveDecimal(), decimalPlaces));
    } catch (ArithmeticException e) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void round(int i, HiveDecimal input, DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, RoundUtils.round(input, outputColVector.scale));
    } catch (ArithmeticException e) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void round(int i, HiveDecimalWritable input, DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, RoundUtils.round(input.getHiveDecimal(), outputColVector.scale));
    } catch (ArithmeticException e) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void sign(int i, HiveDecimal input, LongColumnVector outputColVector) {
    outputColVector.vector[i] = input.signum();
  }

  public static void sign(int i, HiveDecimalWritable input, LongColumnVector outputColVector) {
    outputColVector.vector[i] = input.getHiveDecimal().signum();
  }

  public static void abs(int i, HiveDecimal input, DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, input.abs());
    } catch (ArithmeticException e) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void abs(int i, HiveDecimalWritable input, DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, input.getHiveDecimal().abs());
    } catch (ArithmeticException e) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void negate(int i, HiveDecimal input, DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, input.negate());
    } catch (ArithmeticException e) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void negate(int i, HiveDecimalWritable input, DecimalColumnVector outputColVector) {
    try {
      outputColVector.set(i, input.getHiveDecimal().negate());
    } catch (ArithmeticException e) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }
}
