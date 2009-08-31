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
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

@description(
    name = "div",
    value = "a _FUNC_ b - Divide a by b rounded to the long integer",
    extended = "Example:\n" +
        "  > SELECT 3 _FUNC_ 2 FROM src LIMIT 1;\n" +
        "  1"
)
public class UDFOPLongDivide extends UDF {

  private static Log LOG = LogFactory.getLog("org.apache.hadoop.hive.ql.udf.UDFOPLongDivide");

  protected LongWritable longWritable = new LongWritable();
  
  public LongWritable evaluate(LongWritable a, LongWritable b)  {
    // LOG.info("Get input " + a.getClass() + ":" + a + " " + b.getClass() + ":" + b);
    if ((a == null) || (b == null))
      return null;

    longWritable.set((long)a.get()/b.get());
    return longWritable;
  }
}
