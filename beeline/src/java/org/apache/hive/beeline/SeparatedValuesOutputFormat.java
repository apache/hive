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

import java.io.IOException;
import java.io.StringWriter;

import org.apache.hadoop.io.IOUtils;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

/**
 * OutputFormat for values separated by a delimiter.
 */
class SeparatedValuesOutputFormat implements OutputFormat {
  /**
   *
   */
  private final BeeLine beeLine;
  private CsvPreference csvPreference;

  SeparatedValuesOutputFormat(BeeLine beeLine, char separator) {
    this.beeLine = beeLine;
    csvPreference = new CsvPreference.Builder('"', separator, "").build();
  }

  private void updateCsvPreference() {
    if (beeLine.getOpts().getOutputFormat().equals("dsv")) {
      // check whether delimiter changed by user
      char curDel = (char) csvPreference.getDelimiterChar();
      char newDel = beeLine.getOpts().getDelimiterForDSV();
      // if delimiter changed, rebuild the csv preference
      if (newDel != curDel) {
        // "" is passed as the end of line symbol in following function, as
        // beeline itself adds newline
        csvPreference = new CsvPreference.Builder('"', newDel, "").build();
      }
    }
  }

  @Override
  public int print(Rows rows) {
    updateCsvPreference();

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

  private String getFormattedStr(String[] vals) {
    StringWriter strWriter = new StringWriter();
    CsvListWriter writer = new CsvListWriter(strWriter, csvPreference);
    if (vals.length > 0) {
      try {
        writer.write(vals);
      } catch (IOException e) {
        beeLine.error(e);
      } finally {
        IOUtils.closeStream(writer);
      }
    }
    return strWriter.toString();
  }

  public void printRow(Rows rows, Rows.Row row) {
    String[] vals = row.values;
    String formattedStr = getFormattedStr(vals);
    beeLine.output(formattedStr);
  }
}
