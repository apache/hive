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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

@description(
    name = "log",
    value = "_FUNC_([b], x) - Returns the logarithm of x with base b",
    extended = "Example:\n" +
        "  > SELECT _FUNC_(13, 13) FROM src LIMIT 1;\n" +
        "  1"
    )
public class UDFLog extends UDF {

  private static Log LOG = LogFactory.getLog(UDFLog.class.getName());

  DoubleWritable result = new DoubleWritable();
  public UDFLog() {
  }

  /**
   * Returns the natural logarithm of "a".
   */
  public DoubleWritable evaluate(DoubleWritable a)  {
    if (a == null || a.get() <= 0.0) {
      return null;
    } else {
      result.set(Math.log(a.get()));
      return result;
    }
  }

  /**
   * Returns the logarithm of "a" with base "base".
   */
  public DoubleWritable evaluate(DoubleWritable base, DoubleWritable a)  {
    if (a == null || a.get() <= 0.0 || base == null || base.get() <= 1.0) {
      return null;
    } else {
      result.set(Math.log(a.get())/Math.log(base.get()));
      return result;
    }
  }

}
