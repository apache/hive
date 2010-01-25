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
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

@Description(name = "acos", value = "_FUNC_(x) - returns the arc cosine of x if -1<=x<=1 or "
    + "NULL otherwise", extended = "Example:\n"
    + "  > SELECT _FUNC_(1) FROM src LIMIT 1;\n" + "  0\n"
    + "  > SELECT _FUNC_(2) FROM src LIMIT 1;\n" + "  NULL")
public class UDFAcos extends UDF {

  private static Log LOG = LogFactory.getLog(UDFAcos.class.getName());

  DoubleWritable result = new DoubleWritable();

  public UDFAcos() {
  }

  /**
   * Take Arc Cosine of a in radians.
   */
  public DoubleWritable evaluate(DoubleWritable a) {
    if (a == null) {
      return null;
    } else {
      result.set(Math.acos(a.get()));
      return result;
    }
  }

}
