/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.common.cli;

public class EscapeCRLFHelper {

  private static final char CARRIAGE_RETURN = '\r';
  private static final char LINE_FEED = '\n';

  public EscapeCRLFHelper() {
  }

  /*
   * Substitute for any carriage return or line feed characters in line with the escaped
   * 2-character sequences \r or \n.
   *
   * @param line  the string for the CRLF substitution.
   * @return If there were no replacements, then just return line.  Otherwise, a new String with
   *         escaped CRLF.
   */
  public static String escapeCRLF(String line) {

    StringBuilder sb = null;
    int lastNonCRLFIndex = 0;
    int index = 0;
    final int length = line.length();
    while (index < length) {
      char ch = line.charAt(index);
      if (ch == CARRIAGE_RETURN || ch == LINE_FEED) {
        if (sb == null) {

          // We defer allocation until we really need it since in the common case there is
          // no CRLF substitution.
          sb = new StringBuilder();
        }
        if (lastNonCRLFIndex < index) {

          // Copy an intervening non-CRLF characters up to but not including current 'index'.
          sb.append(line.substring(lastNonCRLFIndex, index));
        }
        lastNonCRLFIndex = ++index;
        if (ch == CARRIAGE_RETURN) {
          sb.append("\\r");
        } else {
          sb.append("\\n");
        }
      } else {
        index++;
      }
    }
    if (sb == null) {

      // No CRLF substitution -- return original line.
      return line;
    } else {
      if (lastNonCRLFIndex < index) {

        // Copy an intervening non-CRLF characters up to but not including current 'index'.
        sb.append(line.substring(lastNonCRLFIndex, index));
      }
      return sb.toString();
    }
  }
}
