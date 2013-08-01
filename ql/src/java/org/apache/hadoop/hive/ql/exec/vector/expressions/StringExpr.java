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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

/**
 * String expression evaluation helper functions.
 */
public class StringExpr {

  /* Compare two strings from two byte arrays each
   * with their own start position and length.
   * Use lexicographic unsigned byte value order.
   * This is what's used for UTF-8 sort order.
   * Return negative value if arg1 < arg2, 0 if arg1 = arg2,
   * positive if arg1 > arg2.
   */
  public static int compare(byte[] arg1, int start1, int len1, byte[] arg2, int start2, int len2) {
    for (int i = 0; i < len1 && i < len2; i++) {
      int b1 = arg1[i + start1] & 0xff;
      int b2 = arg2[i + start2] & 0xff;
      if (b1 != b2) {
        return b1 - b2;
      }
    }
    return len1 - len2;
  }
}
