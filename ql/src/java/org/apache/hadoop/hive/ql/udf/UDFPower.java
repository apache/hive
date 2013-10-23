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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FuncPowerDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FuncPowerLongToDouble;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * UDFPower.
 *
 */
@Description(name = "power,pow",
    value = "_FUNC_(x1, x2) - raise x1 to the power of x2",
    extended = "Example:\n"
    + "  > SELECT _FUNC_(2, 3) FROM src LIMIT 1;\n" + "  8")
@VectorizedExpressions({FuncPowerLongToDouble.class, FuncPowerDoubleToDouble.class})
public class UDFPower extends UDF {
  private final DoubleWritable resultDouble = new DoubleWritable();
  private final HiveDecimalWritable resultHiveDecimal = new HiveDecimalWritable();

  public UDFPower() {
  }

  /**
   * Raise a to the power of b.
   */
  public DoubleWritable evaluate(DoubleWritable a, DoubleWritable b) {
    if (a == null || b == null) {
      return null;
    } else {
      resultDouble.set(Math.pow(a.get(), b.get()));
      return resultDouble;
    }
  }

  /**
   * Raise a to the power of b.
   */
  public DoubleWritable evaluate(DoubleWritable a, IntWritable b) {
    if (a == null || b == null) {
      return null;
    } else {
      resultDouble.set(Math.pow(a.get(), b.get()));
      return resultDouble;
    }
  }

  /**
   * Raise a to the power of b
   */
  public HiveDecimalWritable evaluate(HiveDecimalWritable a, IntWritable b) {
    if (a == null || b == null) {
      return null;
    }

    HiveDecimal dec = a.getHiveDecimal().pow(b.get());
    if (dec == null) {
      return null;
    }

    resultHiveDecimal.set(dec);
    return resultHiveDecimal;
  }

}
