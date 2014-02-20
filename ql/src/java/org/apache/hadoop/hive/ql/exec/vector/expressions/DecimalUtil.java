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

import org.apache.hadoop.hive.common.type.Decimal128;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.SqlMathUtil;
import org.apache.hadoop.hive.common.type.UnsignedInt128;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.udf.generic.RoundUtils;

/**
 * Utility functions for vector operations on decimal values.
 */
public class DecimalUtil {

  public static final Decimal128 DECIMAL_ONE = new Decimal128();
  private static final UnsignedInt128 scratchUInt128 = new UnsignedInt128();

  static {
    DECIMAL_ONE.update(1L, (short) 0);
  }

  // Addition with overflow check. Overflow produces NULL output.
  public static void addChecked(int i, Decimal128 left, Decimal128 right,
      DecimalColumnVector outputColVector) {
    try {
      Decimal128.add(left, right, outputColVector.vector[i], outputColVector.scale);
      outputColVector.vector[i].checkPrecisionOverflow(outputColVector.precision);
    } catch (ArithmeticException e) {  // catch on overflow
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  // Subtraction with overflow check. Overflow produces NULL output.
  public static void subtractChecked(int i, Decimal128 left, Decimal128 right,
      DecimalColumnVector outputColVector) {
    try {
      Decimal128.subtract(left, right, outputColVector.vector[i], outputColVector.scale);
      outputColVector.vector[i].checkPrecisionOverflow(outputColVector.precision);
    } catch (ArithmeticException e) {  // catch on overflow
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  // Multiplication with overflow check. Overflow produces NULL output.
  public static void multiplyChecked(int i, Decimal128 left, Decimal128 right,
      DecimalColumnVector outputColVector) {
    try {
      Decimal128.multiply(left, right, outputColVector.vector[i], outputColVector.scale);
      outputColVector.vector[i].checkPrecisionOverflow(outputColVector.precision);
    } catch (ArithmeticException e) {  // catch on overflow
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  // Division with overflow/zero-divide check. Error produces NULL output.
  public static void divideChecked(int i, Decimal128 left, Decimal128 right,
      DecimalColumnVector outputColVector) {
    try {
      Decimal128.divide(left, right, outputColVector.vector[i], outputColVector.scale);
      outputColVector.vector[i].checkPrecisionOverflow(outputColVector.precision);
    } catch (ArithmeticException e) {  // catch on error
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  // Modulo operator with overflow/zero-divide check.
  public static void moduloChecked(int i, Decimal128 left, Decimal128 right,
      DecimalColumnVector outputColVector) {
    try {
      Decimal128.modulo(left, right, outputColVector.vector[i], outputColVector.scale);
      outputColVector.vector[i].checkPrecisionOverflow(outputColVector.precision);
    } catch (ArithmeticException e) {  // catch on error
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void floor(int i, Decimal128 input, DecimalColumnVector outputColVector) {
    try {
      Decimal128 result = outputColVector.vector[i];
      result.update(input);
      result.zeroFractionPart(scratchUInt128);
      result.changeScaleDestructive(outputColVector.scale);
      if ((result.compareTo(input) != 0) && input.getSignum() < 0) {
        result.subtractDestructive(DECIMAL_ONE, outputColVector.scale);
      }
    } catch (ArithmeticException e) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void ceiling(int i, Decimal128 input, DecimalColumnVector outputColVector) {
    try {
      Decimal128 result = outputColVector.vector[i];
      result.update(input);
      result.zeroFractionPart(scratchUInt128);
      result.changeScaleDestructive(outputColVector.scale);
      if ((result.compareTo(input) != 0) && input.getSignum() > 0) {
        result.addDestructive(DECIMAL_ONE, outputColVector.scale);
      }
    } catch (ArithmeticException e) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void round(int i, Decimal128 input, DecimalColumnVector outputColVector) {
    HiveDecimal inputHD = HiveDecimal.create(input.toBigDecimal());
    HiveDecimal result = RoundUtils.round(inputHD, outputColVector.scale);
    if (result == null) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    } else {
      outputColVector.vector[i].update(result.bigDecimalValue().toPlainString(), outputColVector.scale);
    }
  }

  public static void sign(int i, Decimal128 input, LongColumnVector outputColVector) {
    outputColVector.vector[i] = input.getSignum();
  }

  public static void abs(int i, Decimal128 input, DecimalColumnVector outputColVector) {
    Decimal128 result = outputColVector.vector[i];
    try {
      result.update(input);
      result.absDestructive();
      result.changeScaleDestructive(outputColVector.scale);
    } catch (ArithmeticException e) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  public static void negate(int i, Decimal128 input, DecimalColumnVector outputColVector) {
    Decimal128 result = outputColVector.vector[i];
    try {
      result.update(input);
      result.negateDestructive();
      result.changeScaleDestructive(outputColVector.scale);
    } catch (ArithmeticException e) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }
}
