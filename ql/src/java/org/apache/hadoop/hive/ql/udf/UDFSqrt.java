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

package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncSqrtDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncSqrtLongToDouble;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

/**
 * Implementation of the SQRT UDF found in many databases.
 */
@Description(name = "sqrt",
             value = "_FUNC_(x) - returns the square root of x",
             extended = "Example:\n "
                        + "  > SELECT _FUNC_(4) FROM src LIMIT 1;\n"
                        + "  2")
@VectorizedExpressions({FuncSqrtLongToDouble.class, FuncSqrtDoubleToDouble.class})
public class UDFSqrt extends UDFMath {

  private final DoubleWritable result = new DoubleWritable();

  /**
   * Return NULL for negative inputs; otherwise, return the square root.
   */
  @Override
  protected DoubleWritable doEvaluate(DoubleWritable a) {
    if (a.get() < 0) {
      return null;
    } else {
      result.set(Math.sqrt(a.get()));
      return result;
    }
  }

}
