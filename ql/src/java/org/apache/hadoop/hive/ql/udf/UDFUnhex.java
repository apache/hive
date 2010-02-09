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
import org.apache.hadoop.io.Text;

/**
 * UDFUnhex.
 *
 */
@Description(name = "unhex",
    value = "_FUNC_(str) - Converts hexadecimal argument to string",
    extended = "Performs the inverse operation of HEX(str). That is, it interprets\n"
    + "each pair of hexadecimal digits in the argument as a number and\n"
    + "converts it to the character represented by the number. The\n"
    + "resulting characters are returned as a binary string.\n\n"
    + "Example:\n"
    + "> SELECT UNHEX('4D7953514C') from src limit 1;\n"
    + "'MySQL'\n"
    + "> SELECT UNHEX(HEX('string')) from src limit 1;\n"
    + "'string'\n"
    + "> SELECT HEX(UNHEX('1267')) from src limit 1;\n"
    + "'1267'\n\n"
    + "The characters in the argument string must be legal hexadecimal\n"
    + "digits: '0' .. '9', 'A' .. 'F', 'a' .. 'f'. If UNHEX() encounters\n"
    + "any nonhexadecimal digits in the argument, it returns NULL. Also,\n"
    + "if there are an odd number of characters a leading 0 is appended.")
public class UDFUnhex extends UDF {

  /**
   * Convert every two hex digits in s into.
   * 
   */
  public Text evaluate(Text s) {
    if (s == null) {
      return null;
    }

    // append a leading 0 if needed
    String str;
    if (s.getLength() % 2 == 1) {
      str = "0" + s.toString();
    } else {
      str = s.toString();
    }

    byte[] result = new byte[str.length() / 2];
    for (int i = 0; i < str.length(); i += 2) {
      try {
        result[i / 2] = ((byte) Integer.parseInt(str.substring(i, i + 2), 16));
      } catch (NumberFormatException e) {
        // invalid character present, return null
        return null;
      }
    }

    return new Text(result);
  }
}
