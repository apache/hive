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
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFChr converts an integer into its ASCII equivalent.
 *
 */
@Description(name = "char", value = "_FUNC_(str) - convert n where n : [0, 256) into the ascii equivalent as a varchar." +
    "If n is less than 0 return the empty string. If n > 256, return _FUNC_(n % 256).",
    extended = "Example:\n"
    + "  > SELECT _FUNC_('48') FROM src LIMIT 1;\n" + "  '0'\n"
    + "  > SELECT _FUNC_('65') FROM src LIMIT 1;\n" + "  'A'"
)
public class UDFChr extends UDF {
  private final Text result = new Text();

  final String nulString = String.valueOf('\u0000');

  public Text evaluate(LongWritable n) {
    if (n == null) {
      return null;
    }
    return evaluateInternal(n.get());
  }

  public Text evaluate(DoubleWritable n) {
    if (n == null) {
      return null;
    }
    return evaluateInternal(n.get());
  }

  private Text evaluateInternal(long n) {
    if (n == 0L) {
      result.set(nulString);
      return result;
    }
    if (n < 0L) {
      result.set("");
      return result;
    }

    // Should only down-cast if within valid range.
    return evaluateInternal((short) n);
  }

  private Text evaluateInternal(double n) {
    if (n == 0.0d) {
      result.set(nulString);
      return result;
    }
    if (n < 0.0d) {
      result.set("");
      return result;
    }

    // Should only down-cast and elimination precision if within valid range.
    return evaluateInternal((short) n);
  }

  private Text evaluateInternal(short n) {
    if (n > 255) {
      n = (short) (n % 256);
    }
    if (n == 0) {
      result.set(nulString);
      return result;
    }
    if (n < 0) {
      result.set("");
      return result;
    }

    result.set(String.valueOf((char) n));

    return result;
  }
}
