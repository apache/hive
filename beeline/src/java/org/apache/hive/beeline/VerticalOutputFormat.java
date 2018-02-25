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
 * This source file is based on code taken from SQLLine 1.0.2
 * See SQLLine notice in LICENSE
 */
package org.apache.hive.beeline;

/**
 * OutputFormat for vertical column name: value format.
 *
 */
class VerticalOutputFormat implements OutputFormat {
  private final BeeLine beeLine;

  /**
   * @param beeLine
   */
  VerticalOutputFormat(BeeLine beeLine) {
    this.beeLine = beeLine;
  }

  public int print(Rows rows) {
    int count = 0;
    Rows.Row header = (Rows.Row) rows.next();
    while (rows.hasNext()) {
      printRow(rows, header, (Rows.Row) rows.next());
      count++;
    }
    return count;
  }

  public void printRow(Rows rows, Rows.Row header, Rows.Row row) {
    String[] head = header.values;
    String[] vals = row.values;
    int headwidth = 0;
    for (int i = 0; i < head.length && i < vals.length; i++) {
      headwidth = Math.max(headwidth, head[i].length());
    }

    headwidth += 2;

    for (int i = 0; i < head.length && i < vals.length; i++) {
      beeLine.output(beeLine.getColorBuffer().bold(
          beeLine.getColorBuffer().pad(head[i], headwidth).getMono())
          .append(vals[i] == null ? "" : vals[i]));
    }
    beeLine.output(""); // spacing
  }
}