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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFMd5.
 *
 */
@Description(name = "md5",
    value = "_FUNC_(str or bin) - Calculates an MD5 128-bit checksum for the string or binary.",
    extended = "The value is returned as a string of 32 hex digits, or NULL if the argument was NULL.\n"
    + "Example:\n"
    + "  > SELECT _FUNC_('ABC');\n"
    + "  '902fbdd2b1df0c4f70b4a5d23525e932'\n"
    + "  > SELECT _FUNC_(binary('ABC'));\n"
    + "  '902fbdd2b1df0c4f70b4a5d23525e932'")
public class UDFMd5 extends UDF {

  private final Text result = new Text();

  /**
   * Convert String to md5
   */
  public Text evaluate(Text n) {
    if (n == null) {
      return null;
    }

    String str = n.toString();
    String md5Hex = DigestUtils.md5Hex(str);

    result.set(md5Hex);
    return result;
  }

  /**
   * Convert bytes to md5
   */
  public Text evaluate(BytesWritable b) {
    if (b == null) {
      return null;
    }

    byte[] bytes = copyBytes(b);
    String md5Hex = DigestUtils.md5Hex(bytes);

    result.set(md5Hex);
    return result;
  }

  protected byte[] copyBytes(BytesWritable b) {
    int size = b.getLength();
    byte[] result = new byte[size];
    System.arraycopy(b.getBytes(), 0, result, 0, size);
    return result;
  }
}
