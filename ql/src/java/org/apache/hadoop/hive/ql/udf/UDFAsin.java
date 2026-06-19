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
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncASinDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncASinLongToDouble;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

/**
 * UDFAsin.
 */
@Description(name = "asin",
             value = "_FUNC_(x) - returns the arc sine of x if -1<=x<=1 or NULL otherwise",
             extended = "Example:\n"
                        + "  > SELECT _FUNC_(0) FROM src LIMIT 1;\n"
                        + "  0\n"
                        + "  > SELECT _FUNC_(2) FROM src LIMIT 1;\n"
                        + "  NULL")
@VectorizedExpressions({FuncASinLongToDouble.class, FuncASinDoubleToDouble.class})
public class UDFAsin extends UDFMath {

  private final DoubleWritable result = new DoubleWritable();

  /**
   * Take Arc Sine of a in radians.
   */
  @Override
  protected DoubleWritable doEvaluate(DoubleWritable a) {
    double d = a.get();
    if (d < -1 || d > 1) {
      return null;
    } else {
      result.set(StrictMath.asin(d));
      return result;
    }
  }

}
