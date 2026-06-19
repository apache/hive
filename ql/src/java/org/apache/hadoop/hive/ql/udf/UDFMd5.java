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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;
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
  private final MessageDigest digest;

  public UDFMd5() {
    try {
      digest = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Convert String to md5
   */
  public Text evaluate(Text n) {
    if (n == null) {
      return null;
    }

    digest.reset();
    digest.update(n.getBytes(), 0, n.getLength());
    byte[] md5Bytes = digest.digest();
    String md5Hex = Hex.encodeHexString(md5Bytes);

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

    digest.reset();
    digest.update(b.getBytes(), 0, b.getLength());
    byte[] md5Bytes = digest.digest();
    String md5Hex = Hex.encodeHexString(md5Bytes);

    result.set(md5Hex);
    return result;
  }
}
