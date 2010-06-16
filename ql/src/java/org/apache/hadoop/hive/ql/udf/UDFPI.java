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

@Description (
  name="PI",
  value="_FUNC_() - returns pi ",
  extended = "Example:\n"+
     " > SELECT _FUNC_() FROM src LIMIT 1;\n" +
     " 3.14159..."
   )
public class UDFPI extends UDF {
  @SuppressWarnings("unused")
  private static Log LOG = LogFactory.getLog(UDFPI.class.getName() );
  DoubleWritable result = new DoubleWritable();

  public UDFPI() {
    result.set(Math.PI);
  }

  public DoubleWritable evaluate() {
    return result;
  }
}
