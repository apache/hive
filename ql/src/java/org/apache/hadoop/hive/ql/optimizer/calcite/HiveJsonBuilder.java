/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.JsonBuilder;

public class HiveJsonBuilder extends JsonBuilder {
  /** Maps control characters (0 ... 31) to JSON escaped strings.
   * Tab, newline, form feed and carriage return are mapped to '\t', '\n',
   * '\f', 'r' respectively; others are mapped to '\\u00xx' for some 'xx'. */

  //TODO: This is copied from Calcite. Remove it after upgrading to at least 1.31
  private static final ImmutableList<String> ESCAPED =
      ImmutableList.of("\\u0000",
          "\\u0001",
          "\\u0002",
          "\\u0003",
          "\\u0004",
          "\\u0005",
          "\\u0006",
          "\\u0007",
          "\\u0008",
          "\\t", // tab, ASCII 9 x09
          "\\n", // newline, ASCII 10 x0A
          "\\u000B",
          "\\f", // form feed, ASCII 12 x0C
          "\\r", // carriage return, ASCII 13 x0D
          "\\u000E",
          "\\u000F",
          "\\u0010",
          "\\u0011",
          "\\u0012",
          "\\u0013",
          "\\u0014",
          "\\u0015",
          "\\u0016",
          "\\u0017",
          "\\u0018",
          "\\u0019",
          "\\u001A",
          "\\u001B",
          "\\u001C",
          "\\u001D",
          "\\u001E",
          "\\u001F");

  public void append(StringBuilder buf, int indent, Object o) {
    if (o instanceof String) {
      appendString(buf, (String) o);
    } else {
      super.append(buf, indent, o);
    }
  }

  private static void appendString(StringBuilder buf, String s) {
    buf.append('"');
    final int n = s.length();
    for (int i = 0; i < n; i++) {
      char c = s.charAt(i);
      if (c < 32) {
        buf.append(ESCAPED.get(c));
      } else if (c == '\\') {
        buf.append("\\\\");
      } else if (c == '"') {
        buf.append("\\\"");
      } else {
        buf.append(c);
      }
    }
    buf.append('"');
  }
}
