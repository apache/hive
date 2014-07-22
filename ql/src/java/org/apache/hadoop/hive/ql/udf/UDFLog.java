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

package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FuncLogWithBaseDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FuncLogWithBaseLongToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncLnDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncLnLongToDouble;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

/**
 * UDFLog.
 */
@Description(name = "log",
             value = "_FUNC_([b], x) - Returns the logarithm of x with base b",
             extended = "Example:\n"
                        + "  > SELECT _FUNC_(13, 13) FROM src LIMIT 1;\n"
                        + "  1")
@VectorizedExpressions({FuncLogWithBaseLongToDouble.class, FuncLogWithBaseDoubleToDouble.class,
                         FuncLnLongToDouble.class, FuncLnDoubleToDouble.class})
public class UDFLog extends UDFMath {

  private final DoubleWritable result = new DoubleWritable();

  /**
   * Returns the logarithm of "a" with base "base".
   */
  public DoubleWritable evaluate(DoubleWritable base, DoubleWritable a) {
    if (a == null || base == null) {
      return null;
    }
    return log(base.get(), a.get());
  }

  /**
   * Get the logarithm of the given decimal with the given base.
   */
  public DoubleWritable evaluate(DoubleWritable base, HiveDecimalWritable writable) {
    if (base == null || writable == null) {
      return null;
    }
    double d = writable.getHiveDecimal().bigDecimalValue().doubleValue();
    return log(base.get(), d);
  }

  /**
   * Get the logarithm of input with the given decimal as the base.
   */
  public DoubleWritable evaluate(HiveDecimalWritable base, DoubleWritable d) {
    if (base == null || d == null) {
      return null;
    }

    double b = base.getHiveDecimal().bigDecimalValue().doubleValue();
    return log(b, d.get());
  }

  /**
   * Get the logarithm of the given decimal input with the given decimal base.
   */
  public DoubleWritable evaluate(HiveDecimalWritable baseWritable, HiveDecimalWritable writable) {
    if (baseWritable == null || writable == null) {
      return null;
    }

    double base = baseWritable.getHiveDecimal().bigDecimalValue().doubleValue();
    double d = writable.getHiveDecimal().bigDecimalValue().doubleValue();
    return log(base, d);
  }

  /**
   * Returns the natural logarithm of "a".
   */
  @Override
  protected DoubleWritable doEvaluate(DoubleWritable a) {
    if (a.get() <= 0.0) {
      return null;
    } else {
      result.set(Math.log(a.get()));
      return result;
    }
  }

  private DoubleWritable log(double base, double input) {
    if (base <= 1.0 || input <= 0.0) {
      return null;
    }
    result.set(Math.log(input) / Math.log(base));
    return result;
  }

}
