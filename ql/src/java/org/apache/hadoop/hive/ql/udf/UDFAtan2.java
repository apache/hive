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
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * UDFAtan2.
 */
@Description(name = "atan2",
             value = "_FUNC_(x, y) -  Returns the angle in radians between the x-axis of a plane \n"
                     + "and the point given by the coordinates (x, y), as if computed by "
                     + "java.lang.Math._FUNC_",
             extended = "Example:\n"
                        + "  > SELECT _FUNC_(13, 13) FROM src LIMIT 1;\n"
                        + "  0.7853981633974483")
public class UDFAtan2 extends UDF {

  private final DoubleWritable result = new DoubleWritable();

  /**
   * Returns the angle in radians between the x-axis and y-axis point.
   */
  public DoubleWritable evaluate(DoubleWritable x, DoubleWritable y) {
    if (x == null || y == null) {
      return null;
    }
    double res = Math.atan2(x.get(), y.get());
    result.set(res);
    return result;
  }
}
