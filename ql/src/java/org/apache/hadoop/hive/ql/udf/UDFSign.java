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
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

@Description(
    name = "sign",
    value = "_FUNC_(x) - returns the sign of x )",
    extended = "Example:\n " +
    		"  > SELECT _FUNC_(40) FROM src LIMIT 1;\n" +
    		"  1"
    )
public class UDFSign extends UDF {

  @SuppressWarnings("unused")
  private static Log LOG = LogFactory.getLog(UDFSign.class.getName());
  DoubleWritable result = new DoubleWritable();

  public UDFSign() {
  }

  /**
   * Take sign of a
   */
  public DoubleWritable evaluate(DoubleWritable a)  {
    if (a == null) {
      return null;
    }
    if (a.get()==0) {
      result.set(0);
    }
    else if (a.get()>0) {
      result.set(1);
    }
    else {
      result.set(-1);
    }
    return result;
  }

}
