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
import org.apache.hadoop.io.Text;

/**
 * UDFUnhex.
 *
 */
@Description(name = "unhex",
    value = "_FUNC_(str) - Converts hexadecimal argument to binary",
    extended = "Performs the inverse operation of HEX(str). That is, it interprets\n"
    + "each pair of hexadecimal digits in the argument as a number and\n"
    + "converts it to the byte representation of the number. The\n"
    + "resulting characters are returned as a binary string.\n\n"
    + "Example:\n"
    + "> SELECT DECODE(UNHEX('4D7953514C'), 'UTF-8') from src limit 1;\n"
    + "'MySQL'\n\n"
    + "The characters in the argument string must be legal hexadecimal\n"
    + "digits: '0' .. '9', 'A' .. 'F', 'a' .. 'f'. If UNHEX() encounters\n"
    + "any nonhexadecimal digits in the argument, it returns NULL. Also,\n"
    + "if there are an odd number of characters a leading 0 is appended.")
public class UDFUnhex extends UDF {

  /**
   * Convert every two hex digits in s into a byte.
   */
  public byte[] evaluate(Text s) {
    if (s == null) {
      return null;
    }

    int len = s.getLength();
    if (len == 0) {
      return new byte[0];
    }

    byte[] textBytes = s.getBytes();

    // (len + 1) / 2 ensures right size for odd lengths
    byte[] result = new byte[(len + 1) / 2];

    int i = 0;
    int resIdx = 0;

    // If length is odd, the first character acts as the first byte avoiding adding "0" prefix
    if (len % 2 != 0) {
      int val = decodeHexChar(textBytes[i++]);
      if (val == -1) {
        return null;
      }
      result[resIdx++] = (byte) val;
    }

    while (i < len) {
      int high = decodeHexChar(textBytes[i++]);
      int low  = decodeHexChar(textBytes[i++]);

      if (high == -1 || low == -1) {
        return null;
      }

      result[resIdx++] = (byte) ((high << 4) | low);
    }

    return result;
  }

  private int decodeHexChar(byte b) {
    if (b >= '0' && b <= '9') {
      return b - '0';
    }
    if (b >= 'a' && b <= 'f') {
      return b - 'a' + 10;
    }
    if (b >= 'A' && b <= 'F') {
      return b - 'A' + 10;
    }
    return -1;
  }
}
