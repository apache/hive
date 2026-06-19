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
 * UDFSha.
 *
 */
@Description(name = "sha1,sha",
    value = "_FUNC_(str or bin) - Calculates the SHA-1 digest for string or binary "
    + "and returns the value as a hex string.",
    extended = "Example:\n"
    + "  > SELECT _FUNC_('ABC');\n"
    + "  '3c01bdbb26f358bab27f267924aa2c9a03fcfdb8'\n"
    + "  > SELECT _FUNC_(binary('ABC'));\n"
    + "  '3c01bdbb26f358bab27f267924aa2c9a03fcfdb8'")
public class UDFSha1 extends UDF {

  private final Text result = new Text();
  private final MessageDigest digest;

  public UDFSha1() {
    try {
      digest = MessageDigest.getInstance("SHA");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Convert String to SHA-1
   */
  public Text evaluate(Text n) {
    if (n == null) {
      return null;
    }

    digest.reset();
    digest.update(n.getBytes(), 0, n.getLength());
    byte[] shaBytes = digest.digest();
    String shaHex = Hex.encodeHexString(shaBytes);

    result.set(shaHex);
    return result;
  }

  /**
   * Convert bytes to SHA-1
   */
  public Text evaluate(BytesWritable b) {
    if (b == null) {
      return null;
    }

    digest.reset();
    digest.update(b.getBytes(), 0, b.getLength());
    byte[] shaBytes = digest.digest();
    String shaHex = Hex.encodeHexString(shaBytes);

    result.set(shaHex);
    return result;
  }
}
