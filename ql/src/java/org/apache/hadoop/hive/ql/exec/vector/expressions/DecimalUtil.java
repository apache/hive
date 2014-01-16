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
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;

/**
 * Utility functions for vector operations on decimal values.
 */
public class DecimalUtil {

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
  // Remainder argument is necessary to match up with the Decimal128.divide() interface.
  // It will be discarded so just pass in a dummy argument.
  public static void divideChecked(int i, Decimal128 left, Decimal128 right,
      DecimalColumnVector outputColVector, Decimal128 remainder) {
    try {
      Decimal128.divide(left, right, outputColVector.vector[i], remainder, outputColVector.scale);
      outputColVector.vector[i].checkPrecisionOverflow(outputColVector.precision);
    } catch (ArithmeticException e) {  // catch on error
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }

  // Modulo operator with overflow/zero-divide check.
  // Quotient argument is necessary to match up with Decimal128.divide() interface.
  // It will be discarded so just pass in a dummy argument.
  public static void moduloChecked(int i, Decimal128 left, Decimal128 right,
      DecimalColumnVector outputColVector, Decimal128 quotient) {
    try {
      Decimal128.divide(left, right, quotient, outputColVector.vector[i], outputColVector.scale);
      outputColVector.vector[i].checkPrecisionOverflow(outputColVector.precision);
    } catch (ArithmeticException e) {  // catch on error
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    }
  }
}
