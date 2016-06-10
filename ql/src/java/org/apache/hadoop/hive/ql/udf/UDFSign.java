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
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncSignDecimalToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncSignDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncSignLongToDouble;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.IntWritable;

@Description(name = "sign",
             value = "_FUNC_(x) - returns the sign of x )",
             extended = "Example:\n "
                        + "  > SELECT _FUNC_(40) FROM src LIMIT 1;\n"
                        + "  1"
)
@VectorizedExpressions({FuncSignLongToDouble.class, FuncSignDoubleToDouble.class, FuncSignDecimalToLong.class})
public class UDFSign extends UDF {

  private final DoubleWritable result = new DoubleWritable();
  private final IntWritable intWritable = new IntWritable();

  /**
   * Take sign of a
   */
  public DoubleWritable evaluate(DoubleWritable a) {
    if (a == null) {
      return null;
    }
    if (a.get() == 0) {
      result.set(0);
    } else if (a.get() > 0) {
      result.set(1);
    } else {
      result.set(-1);
    }
    return result;
  }

  /**
   * Get the sign of the decimal input
   *
   * @param dec decimal input
   *
   * @return -1, 0, or 1 representing the sign of the input decimal
   */
  public IntWritable evaluate(HiveDecimalWritable dec) {
    if (dec == null || dec.getHiveDecimal() == null) {
      return null;
    }

    intWritable.set(dec.getHiveDecimal().signum());
    return intWritable;
  }

}
