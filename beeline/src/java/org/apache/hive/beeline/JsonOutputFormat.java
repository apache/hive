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

/*
 * This source file is based on code taken from SQLLine 1.4.0
 * See SQLLine notice in LICENSE
 */
package org.apache.hive.beeline;

import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of {@link OutputFormat} that formats rows as JSON.
 */
public class JsonOutputFormat extends AbstractOutputFormat {
  private static final Map<Character, String> ESCAPING_MAP = new HashMap<Character, String>();

  static {
    ESCAPING_MAP.put('\\', "\\\\");
    ESCAPING_MAP.put('\"', "\\\"");
    ESCAPING_MAP.put('\b', "\\b");
    ESCAPING_MAP.put('\f', "\\f");
    ESCAPING_MAP.put('\n', "\\n");
    ESCAPING_MAP.put('\r', "\\r");
    ESCAPING_MAP.put('\t', "\\t");
    ESCAPING_MAP.put('/', "\\/");
    ESCAPING_MAP.put('\u0000', "\\u0000");
    ESCAPING_MAP.put('\u0001', "\\u0001");
    ESCAPING_MAP.put('\u0002', "\\u0002");
    ESCAPING_MAP.put('\u0003', "\\u0003");
    ESCAPING_MAP.put('\u0004', "\\u0004");
    ESCAPING_MAP.put('\u0005', "\\u0005");
    ESCAPING_MAP.put('\u0006', "\\u0006");
    ESCAPING_MAP.put('\u0007', "\\u0007");
    // ESCAPING_MAP.put('\u0008', "\\u0008");
    // covered by ESCAPING_MAP.put('\b', "\\b");
    // ESCAPING_MAP.put('\u0009', "\\u0009");
    // covered by ESCAPING_MAP.put('\t', "\\t");
    // ESCAPING_MAP.put((char) 10, "\\u000A");
    // covered by ESCAPING_MAP.put('\n', "\\n");
    ESCAPING_MAP.put('\u000B', "\\u000B");
    // ESCAPING_MAP.put('\u000C', "\\u000C");
    // covered by ESCAPING_MAP.put('\f', "\\f");
    // ESCAPING_MAP.put((char) 13, "\\u000D");
    // covered by ESCAPING_MAP.put('\r', "\\r");
    ESCAPING_MAP.put('\u000E', "\\u000E");
    ESCAPING_MAP.put('\u000F', "\\u000F");
    ESCAPING_MAP.put('\u0010', "\\u0010");
    ESCAPING_MAP.put('\u0011', "\\u0011");
    ESCAPING_MAP.put('\u0012', "\\u0012");
    ESCAPING_MAP.put('\u0013', "\\u0013");
    ESCAPING_MAP.put('\u0014', "\\u0014");
    ESCAPING_MAP.put('\u0015', "\\u0015");
    ESCAPING_MAP.put('\u0016', "\\u0016");
    ESCAPING_MAP.put('\u0017', "\\u0017");
    ESCAPING_MAP.put('\u0018', "\\u0018");
    ESCAPING_MAP.put('\u0019', "\\u0019");
    ESCAPING_MAP.put('\u001A', "\\u001A");
    ESCAPING_MAP.put('\u001B', "\\u001B");
    ESCAPING_MAP.put('\u001C', "\\u001C");
    ESCAPING_MAP.put('\u001D', "\\u001D");
    ESCAPING_MAP.put('\u001E', "\\u001E");
    ESCAPING_MAP.put('\u001F', "\\u001F");
  }

  private int[] columnTypes;

  public JsonOutputFormat(BeeLine beeLine) {
    super(beeLine);
  }

  @Override
  void printHeader(Rows.Row header) {
    beeLine.output("{\"resultset\":[");
  }

  @Override
  void printFooter(Rows.Row header) {
    beeLine.output("]}");
  }

  @Override
  void printRow(Rows rows, Rows.Row header, Rows.Row row) {
    String[] head = header.values;
    String[] vals = row.values;
    StringBuilder sb = new StringBuilder("{");
    for (int i = 0; (i < head.length) && (i < vals.length); i++) {
      if (columnTypes == null) {
        initColumnTypes(rows, header);
      }
      sb.append("\"").append(head[i]).append("\":");
      setJsonValue(sb, vals[i], columnTypes[i]);
      sb.append((i < head.length - 1) && (i < vals.length - 1) ? "," : "");
    }
    sb.append(rows.hasNext() ? "}," : "}");
    beeLine.output(sb.toString());
  }

  private void setJsonValue(StringBuilder sb, String value, int columnTypeId) {
    if (value == null) {
      sb.append(value);
      return;
    }
    switch (columnTypeId) {
    case Types.TINYINT:
    case Types.SMALLINT:
    case Types.INTEGER:
    case Types.BIGINT:
    case Types.REAL:
    case Types.FLOAT:
    case Types.DOUBLE:
    case Types.DECIMAL:
    case Types.NUMERIC:
      sb.append(value);
      return;
    case Types.NULL:
      sb.append(value.toLowerCase());
      return;
    case Types.BOOLEAN:
      // JSON requires true and false, not TRUE and FALSE
      sb.append(value.equalsIgnoreCase("TRUE"));
      return;
    }
    sb.append("\"");
    for (int i = 0; i < value.length(); i++) {
      if (ESCAPING_MAP.get(value.charAt(i)) != null) {
        sb.append(ESCAPING_MAP.get(value.charAt(i)));
      } else {
        sb.append(value.charAt(i));
      }
    }
    sb.append("\"");
  }

  private void initColumnTypes(Rows rows, Rows.Row header) {
    columnTypes = new int[header.values.length];
    for (int j = 0; j < header.values.length; j++) {
      try {
        columnTypes[j] = rows.rsMeta.getColumnType(j + 1);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }
}