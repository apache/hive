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

/*
 * This source file is based on code taken from SQLLine 1.0.2
 * See SQLLine notice in LICENSE
 */
package org.apache.hive.beeline;

class XMLAttributeOutputFormat extends AbstractOutputFormat {
  private final BeeLine beeLine;
  private final StringBuilder buf = new StringBuilder();

  /**
   * @param beeLine
   */
  XMLAttributeOutputFormat(BeeLine beeLine) {
    this.beeLine = beeLine;
  }

  @Override
  public void printHeader(Rows.Row header) {
    beeLine.output("<resultset>");
  }


  @Override
  public void printFooter(Rows.Row header) {
    beeLine.output("</resultset>");
  }

  @Override
  public void printRow(Rows rows, Rows.Row header, Rows.Row row) {
    String[] head = header.values;
    String[] vals = row.values;

    buf.setLength(0);
    buf.append("  <result");

    for (int i = 0; i < head.length && i < vals.length; i++) {
      buf.append(' ')
        .append(head[i])
        .append("=\"")
        .append(BeeLine.xmlattrencode(vals[i]))
        .append('"');
    }

    buf.append("/>");
    beeLine.output(buf.toString());
  }
}