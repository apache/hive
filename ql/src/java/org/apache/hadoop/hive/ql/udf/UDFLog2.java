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
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncLog2DoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncLog2LongToDouble;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

/**
 * UDFLog2.
 *
 */
@Description(name = "log2",
    value = "_FUNC_(x) - Returns the logarithm of x with base 2",
    extended = "Example:\n"
    + "  > SELECT _FUNC_(2) FROM src LIMIT 1;\n" + "  1")
@VectorizedExpressions({FuncLog2LongToDouble.class, FuncLog2DoubleToDouble.class})
public class UDFLog2 extends UDF {
  private static double log2 = Math.log(2.0);

  private final DoubleWritable result = new DoubleWritable();

  public UDFLog2() {
  }

  /**
   * Returns the logarithm of "a" with base 2.
   */
  public DoubleWritable evaluate(DoubleWritable a) {
    if (a == null || a.get() <= 0.0) {
      return null;
    } else {
      result.set(Math.log(a.get()) / log2);
      return result;
    }
  }

}
