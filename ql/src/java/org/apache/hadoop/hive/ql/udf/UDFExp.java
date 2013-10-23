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
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncExpDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncExpLongToDouble;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

/**
 * UDFExp.
 *
 */
@Description(name = "exp",
    value = "_FUNC_(x) - Returns e to the power of x",
    extended = "Example:\n "
    + "  > SELECT _FUNC_(0) FROM src LIMIT 1;\n" + "  1")
@VectorizedExpressions({FuncExpDoubleToDouble.class, FuncExpLongToDouble.class})
public class UDFExp extends UDF {
  private final DoubleWritable result = new DoubleWritable();

  public UDFExp() {
  }

  /**
   * Raise e (the base of natural logarithm) to the power of a.
   */
  public DoubleWritable evaluate(DoubleWritable a) {
    if (a == null) {
      return null;
    } else {
      result.set(Math.exp(a.get()));
      return result;
    }
  }

}
