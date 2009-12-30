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
import org.apache.hadoop.io.BooleanWritable;

@description(
    name = "and",
    value = "a _FUNC_ b - Logical and",
    extended = "Example:\n" +
        "  > SELECT * FROM srcpart WHERE src.hr=12 _FUNC_ " +
        "src.hr='2008-04-08' LIMIT 1;\n" +
        "  27      val_27  2008-04-08      12"
)
public class UDFOPAnd extends UDF {

  private static Log LOG = LogFactory.getLog("org.apache.hadoop.hive.ql.udf.UDFOPAnd");

  BooleanWritable result = new BooleanWritable();
  public UDFOPAnd() {
  }

  // Three-value Boolean: NULL stands for unknown
  public BooleanWritable evaluate(BooleanWritable a, BooleanWritable b)  {
    
    if ((a != null && a.get() == false) || (b != null && b.get() == false)) {
      result.set(false);
      return result;
    }
    if ((a != null && a.get() == true) && (b != null && b.get() == true)) {
      result.set(true);
      return result;
    }
    return null;
  }
}
