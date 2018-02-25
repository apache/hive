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
    return HiveDecimalWritable.compareTo(left, writableRight);
  }

  // Addition with overflow check. Overflow produces NULL output.
  public static void addChecked(int i, HiveDecimal left, HiveDecimal right,
      DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(left);
    decWritable.mutateAdd(right);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void addChecked(int i, HiveDecimalWritable left, HiveDecimalWritable right,
      DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(left);
    decWritable.mutateAdd(right);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void addChecked(int i, HiveDecimalWritable left, HiveDecimal right,
      DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(left);
    decWritable.mutateAdd(right);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void addChecked(int i, HiveDecimal left, HiveDecimalWritable right,
      DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(left);
    decWritable.mutateAdd(right);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  // Subtraction with overflow check. Overflow produces NULL output.
  public static void subtractChecked(int i, HiveDecimal left, HiveDecimal right,
      DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(left);
    decWritable.mutateSubtract(right);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void subtractChecked(int i, HiveDecimalWritable left, HiveDecimalWritable right,
      DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(left);
    decWritable.mutateSubtract(right);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void subtractChecked(int i, HiveDecimalWritable left, HiveDecimal right,
      DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(left);
    decWritable.mutateSubtract(right);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void subtractChecked(int i, HiveDecimal left, HiveDecimalWritable right,
      DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(left);
    decWritable.mutateSubtract(right);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  // Multiplication with overflow check. Overflow produces NULL output.
  public static void multiplyChecked(int i, HiveDecimal left, HiveDecimal right,
      DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(left);
    decWritable.mutateMultiply(right);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void multiplyChecked(int i, HiveDecimalWritable left, HiveDecimalWritable right,
      DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(left);
    decWritable.mutateMultiply(right);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void multiplyChecked(int i, HiveDecimalWritable left, HiveDecimal right,
      DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(left);
    decWritable.mutateMultiply(right);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void multiplyChecked(int i, HiveDecimal left, HiveDecimalWritable right,
      DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(left);
    decWritable.mutateMultiply(right);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  // Division with overflow/zero-divide check. Error produces NULL output.
  public static void divideChecked(int i, HiveDecimal left, HiveDecimal right,
      DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(left);
    decWritable.mutateDivide(right);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void divideChecked(int i, HiveDecimalWritable left, HiveDecimalWritable right,
      DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(left);
    decWritable.mutateDivide(right);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void divideChecked(int i, HiveDecimalWritable left, HiveDecimal right,
      DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(left);
    decWritable.mutateDivide(right);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void divideChecked(int i, HiveDecimal left, HiveDecimalWritable right,
      DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(left);
    decWritable.mutateDivide(right);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  // Modulo operator with overflow/zero-divide check.
  public static void moduloChecked(int i, HiveDecimal left, HiveDecimal right,
      DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(left);
    decWritable.mutateRemainder(right);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void moduloChecked(int i, HiveDecimalWritable left, HiveDecimalWritable right,
      DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(left);
    decWritable.mutateRemainder(right);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void moduloChecked(int i, HiveDecimalWritable left, HiveDecimal right,
      DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(left);
    decWritable.mutateRemainder(right);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void moduloChecked(int i, HiveDecimal left, HiveDecimalWritable right,
      DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(left);
    decWritable.mutateRemainder(right);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  // UNDONE: Why don't these methods take decimalPlaces?
  public static void floor(int i, HiveDecimal input, DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(input);
    decWritable.mutateSetScale(0, HiveDecimal.ROUND_FLOOR);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void floor(int i, HiveDecimalWritable input, DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(input);
    decWritable.mutateSetScale(0, HiveDecimal.ROUND_FLOOR);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void ceiling(int i, HiveDecimal input, DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(input);
    decWritable.mutateSetScale(0, HiveDecimal.ROUND_CEILING);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void ceiling(int i, HiveDecimalWritable input, DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(input);
    decWritable.mutateSetScale(0, HiveDecimal.ROUND_CEILING);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void round(int i, HiveDecimal input, int decimalPlaces, DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(input);
    decWritable.mutateSetScale(decimalPlaces, HiveDecimal.ROUND_HALF_UP);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void round(int i, HiveDecimalWritable input, int decimalPlaces, DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(input);
    decWritable.mutateSetScale(decimalPlaces, HiveDecimal.ROUND_HALF_UP);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void round(int i, HiveDecimal input, DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(input);
    decWritable.mutateSetScale(outputColVector.scale, HiveDecimal.ROUND_HALF_UP);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void round(int i, HiveDecimalWritable input, DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(input);
    decWritable.mutateSetScale(outputColVector.scale, HiveDecimal.ROUND_HALF_UP);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void bround(int i, HiveDecimalWritable input, int decimalPlaces, DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(input);
    decWritable.mutateSetScale(decimalPlaces, HiveDecimal.ROUND_HALF_EVEN);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void bround(int i, HiveDecimalWritable input, DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(input);
    decWritable.mutateSetScale(outputColVector.scale, HiveDecimal.ROUND_HALF_EVEN);
    decWritable.mutateEnforcePrecisionScale(outputColVector.precision, outputColVector.scale);
    if (!decWritable.isSet()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void sign(int i, HiveDecimal input, LongColumnVector outputColVector) {
    outputColVector.vector[i] = input.signum();
  }

  public static void sign(int i, HiveDecimalWritable input, LongColumnVector outputColVector) {
    outputColVector.vector[i] = input.signum();
  }

  public static void abs(int i, HiveDecimal input, DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(input);
    decWritable.mutateAbs();
  }

  public static void abs(int i, HiveDecimalWritable input, DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(input);
    decWritable.mutateAbs();
  }

  public static void negate(int i, HiveDecimal input, DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(input);
    decWritable.mutateNegate();
  }

  public static void negate(int i, HiveDecimalWritable input, DecimalColumnVector outputColVector) {
    HiveDecimalWritable decWritable = outputColVector.vector[i];
    decWritable.set(input);
    decWritable.mutateNegate();
  }
}
