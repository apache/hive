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
import org.apache.hadoop.hive.ql.exec.vector.expressions.FuncBin;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFBin.
 *
 */
@Description(name = "bin",
    value = "_FUNC_(n) - returns n in binary",
    extended = "n is a BIGINT. Returns NULL if n is NULL.\n"
    + "Example:\n" + "  > SELECT _FUNC_(13) FROM src LIMIT 1\n" + "  '1101'")
@VectorizedExpressions({FuncBin.class})
public class UDFBin extends UDF {
  private final Text result = new Text();
  private final byte[] value = new byte[64];

  public Text evaluate(LongWritable n) {
    if (n == null) {
      return null;
    }

    long num = n.get();
    // Extract the bits of num into value[] from right to left
    int len = 0;
    do {
      len++;
      value[value.length - len] = (byte) ('0' + (num & 1));
      num >>>= 1;
    } while (num != 0);

    result.set(value, value.length - len, len);
    return result;
  }
}
