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
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FuncHex;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringUnaryUDF.IUDFUnaryString;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFHex.
 *
 */
@Description(name = "hex",
    value = "_FUNC_(n, bin, or str) - Convert the argument to hexadecimal ",
    extended = "If the argument is a string, returns two hex digits for each "
    + "character in the string.\n"
    + "If the argument is a number or binary, returns the hexadecimal representation.\n"
    + "Example:\n"
    + "  > SELECT _FUNC_(17) FROM src LIMIT 1;\n"
    + "  'H1'\n"
    + "  > SELECT _FUNC_('Facebook') FROM src LIMIT 1;\n"
    + "  '46616365626F6F6B'")
@VectorizedExpressions({FuncHex.class})
public class UDFHex extends UDF implements IUDFUnaryString {
  private final Text result = new Text();
  private byte[] value = new byte[16];

  /**
   * Convert num to hex.
   *
   */
  private Text evaluate(long num) {
    // Extract the hex digits of num into value[] from right to left
    int len = 0;
    do {
      len++;
      value[value.length - len] = (byte) Character.toUpperCase(Character
          .forDigit((int) (num & 0xF), 16));
      num >>>= 4;
    } while (num != 0);

    result.set(value, value.length - len, len);
    return result;
  }

  public Text evaluate(LongWritable n) {
    if (n == null) {
      return null;
    }
    return evaluate(n.get());
  }

  public Text evaluate(IntWritable n) {
    if (n == null) {
      return null;
    }
    return evaluate(n.get());
  }

  /**
   * Convert every character in s to two hex digits.
   *
   */
  public Text evaluate(Text s) {
    if (s == null) {
      return null;
    }

    byte[] str = s.getBytes();
    return evaluate(str, s.getLength());
  }

  /**
   * Convert bytes to a hex string
   */
  public Text evaluate(BytesWritable b){
    if (b == null) {
      return null;
    }

    byte[] bytes = b.getBytes();
    return evaluate(bytes, b.getLength());
  }

  private Text evaluate(byte[] bytes, int length){
    if (value.length < length * 2) {
      value = new byte[length * 2];
    }

    for (int i = 0; i < length; i++) {
      value[i * 2] = (byte) Character.toUpperCase(Character.forDigit(
          (bytes[i] & 0xF0) >>> 4, 16));
      value[i * 2 + 1] = (byte) Character.toUpperCase(Character.forDigit(
          bytes[i] & 0x0F, 16));
    }
    result.set(value, 0, length*2);
    return result;
  }
}
