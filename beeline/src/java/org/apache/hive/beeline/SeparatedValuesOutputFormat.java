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
  public final static String DISABLE_QUOTING_FOR_SV = "disable.quoting.for.sv";
  private final BeeLine beeLine;
  private CsvPreference quotedCsvPreference;
  private CsvPreference unquotedCsvPreference;

  SeparatedValuesOutputFormat(BeeLine beeLine, char separator) {
    this.beeLine = beeLine;
    unquotedCsvPreference = new CsvPreference.Builder('\0', separator, "").build();
    quotedCsvPreference = new CsvPreference.Builder('"', separator, "").build();
  }

  private void updateCsvPreference() {
    if (beeLine.getOpts().getOutputFormat().equals("dsv")) {
      // check whether delimiter changed by user
      char curDel = (char) getCsvPreference().getDelimiterChar();
      char newDel = beeLine.getOpts().getDelimiterForDSV();
      // if delimiter changed, rebuild the csv preference
      if (newDel != curDel) {
        // "" is passed as the end of line symbol in following function, as
        // beeline itself adds newline
        if (isQuotingDisabled()) {
          unquotedCsvPreference = new CsvPreference.Builder('\0', newDel, "").build();
        } else {
          quotedCsvPreference = new CsvPreference.Builder('"', newDel, "").build();
        }
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
      printRow((Rows.Row) rows.next());
      count++;
    }
    return count - 1; // sans header row
  }

  private String getFormattedStr(String[] vals) {
    StringWriter strWriter = new StringWriter();
    CsvListWriter writer = new CsvListWriter(strWriter, getCsvPreference());
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

  private void printRow(Rows.Row row) {
    String[] vals = row.values;
    String formattedStr = getFormattedStr(vals);
    beeLine.output(formattedStr);
  }

  private boolean isQuotingDisabled() {
    String quotingDisabledStr = System.getProperty(SeparatedValuesOutputFormat.DISABLE_QUOTING_FOR_SV);
    if (quotingDisabledStr == null || quotingDisabledStr.isEmpty()) {
      // default is disabling the double quoting for separated value
      return true;
    }
    String parsedOptionStr = quotingDisabledStr.toLowerCase();
    if (parsedOptionStr.equals("false") || parsedOptionStr.equals("true")) {
      return Boolean.valueOf(parsedOptionStr);
    } else {
      beeLine.error("System Property disable.quoting.for.sv is now " + parsedOptionStr
          + " which only accepts boolean value");
      return true;
    }
  }

  private CsvPreference getCsvPreference() {
    if (isQuotingDisabled()) {
      return unquotedCsvPreference;
    } else {
      return quotedCsvPreference;
    }
  }
}
