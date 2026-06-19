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
 * OutputFormat for values separated by a delimiter.
 *
 * Note this does not handle escaping of the quote char.
 * The new SeparatedValuesOutputFormat supports that. The formats supported by
 * this class are deprecated.
 *
 */
class DeprecatedSeparatedValuesOutputFormat implements OutputFormat {

  private final BeeLine beeLine;
  private char separator;

  public DeprecatedSeparatedValuesOutputFormat(BeeLine beeLine, char separator) {
    this.beeLine = beeLine;
    setSeparator(separator);
  }

  @Override
  public int print(Rows rows) {
    int count = 0;
    while (rows.hasNext()) {
      if (count == 0 && !beeLine.getOpts().getShowHeader()) {
        rows.next();
        count++;
        continue;
      }
      printRow(rows, (Rows.Row) rows.next());
      count++;
    }
    return count - 1; // sans header row
  }

  public void printRow(Rows rows, Rows.Row row) {
    String[] vals = row.values;
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < vals.length; i++) {
      buf.append(buf.length() == 0 ? "" : "" + getSeparator())
          .append('\'')
          .append(vals[i] == null ? "" : vals[i])
          .append('\'');
    }
    beeLine.output(buf.toString());
  }

  public void setSeparator(char separator) {
    this.separator = separator;
  }

  public char getSeparator() {
    return this.separator;
  }
}
