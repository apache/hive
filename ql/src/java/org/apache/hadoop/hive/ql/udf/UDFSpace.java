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

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

@description(name = "space", value = "_FUNC_(n) - returns n spaces", extended = "Example:\n "
    + "  > SELECT _FUNC_(2) FROM src LIMIT 1;\n" + "  '  '")
public class UDFSpace extends UDF {
  private final Text result = new Text();

  public Text evaluate(IntWritable n) {
    if (n == null) {
      return null;
    }

    int len = n.get();
    if (len < 0) {
      len = 0;
    }

    if (result.getBytes().length >= len) {
      result.set(result.getBytes(), 0, len);
    } else {
      byte[] spaces = new byte[len];
      Arrays.fill(spaces, (byte) ' ');
      result.set(spaces);
    }

    return result;
  }
}
