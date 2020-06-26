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
 * This source file is based on code taken from SQLLine 1.9
 * See SQLLine notice in LICENSE
 */
package org.apache.hive.beeline;

import java.sql.SQLException;
import java.sql.Types;

import com.fasterxml.jackson.core.io.JsonStringEncoder;

/**
 * OutputFormat for standard JSON format.
 *
 */ 
public class JSONOutputFormat extends AbstractOutputFormat {
  private final BeeLine beeLine;
  private int[] columnTypes;
  private JsonStringEncoder jse = JsonStringEncoder.getInstance();

  /**
   * @param beeLine
   */
  JSONOutputFormat(BeeLine beeLine){ 
    this.beeLine = beeLine;
  }

  @Override 
  void printHeader(Rows.Row header) {
    beeLine.output("{\"resultset\":[");
  }

  @Override void printFooter(Rows.Row header) {
    beeLine.output("]}");
  }

  @Override void printRow(Rows rows, Rows.Row header, Rows.Row row) {
    String[] head = header.values;
    String[] vals = row.values;
    StringBuilder buf = new StringBuilder("{");
    for (int i = 0; (i < head.length) && (i < vals.length); i++) {
      if (columnTypes == null) {
        initColumnTypes(rows, header);
      }
      buf.append("\"").append(head[i]).append("\":");
      setJsonValue(buf, vals[i], columnTypes[i]);
      buf.append((i < head.length - 1) && (i < vals.length - 1) ? "," : "");
    }
    buf.append(rows.hasNext() ? "}," : "}");
    beeLine.output(buf.toString());
  }

  private void setJsonValue(StringBuilder buf, String value, int columnTypeId) {
    if (value == null) {
      buf.append(value);
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
      buf.append(value);
      return;
    case Types.NULL:
      buf.append("null");
      return;
    case Types.BOOLEAN:
      buf.append(value.equalsIgnoreCase("TRUE"));
      return;
    }
    buf.append("\"" + new String(jse.quoteAsString(value)) + "\"");
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
