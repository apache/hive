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
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncLog10DoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncLog10LongToDouble;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

/**
 * UDFLog10.
 */
@Description(name = "log10",
             value = "_FUNC_(x) - Returns the logarithm of x with base 10",
             extended = "Example:\n"
                        + "  > SELECT _FUNC_(10) FROM src LIMIT 1;\n"
                        + "  1")
@VectorizedExpressions({FuncLog10LongToDouble.class, FuncLog10DoubleToDouble.class})
public class UDFLog10 extends UDFMath {

  private final DoubleWritable result = new DoubleWritable();

  /**
   * Returns the logarithm of "a" with base 10.
   */
  @Override
  protected DoubleWritable doEvaluate(DoubleWritable a) {
    if (a.get() <= 0.0) {
      return null;
    } else {
      result.set(Math.log10(a.get()));
      return result;
    }
  }

}
