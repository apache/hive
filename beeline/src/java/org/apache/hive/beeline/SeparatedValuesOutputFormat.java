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

import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.supercsv.encoder.CsvEncoder;
import org.supercsv.encoder.DefaultCsvEncoder;
import org.supercsv.encoder.SelectiveCsvEncoder;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

/**
 * OutputFormat for values separated by a configurable delimiter
 */
class SeparatedValuesOutputFormat implements OutputFormat {

  public final static String DSV_OPT_OUTPUT_FORMAT = "dsv";
  public final static String DISABLE_QUOTING_FOR_SV = "disable.quoting.for.sv";
  private final static char DEFAULT_QUOTE_CHAR = '"';
  private final BeeLine beeLine;
  private final StringBuilderWriter buffer;
  private final char defaultSeparator;

  SeparatedValuesOutputFormat(BeeLine beeLine, char separator) {
    this.beeLine = beeLine;
    this.defaultSeparator = separator;
    this.buffer = new StringBuilderWriter();
  }

  private CsvPreference getCsvPreference() {
    char separator = this.defaultSeparator;
    char quoteChar = DEFAULT_QUOTE_CHAR;
    CsvEncoder encoder;

    if (DSV_OPT_OUTPUT_FORMAT.equals(beeLine.getOpts().getOutputFormat())) {
      separator = beeLine.getOpts().getDelimiterForDSV();
    }

    if (isQuotingDisabled()) {
      quoteChar = '\0';
      encoder = new SelectiveCsvEncoder();
    } else {
      encoder = new DefaultCsvEncoder();
    }

    return new CsvPreference.Builder(quoteChar, separator, StringUtils.EMPTY).useEncoder(encoder).build();
  }

  @Override
  public int print(Rows rows) {
    CsvPreference csvPreference = getCsvPreference();
    CsvListWriter writer = new CsvListWriter(this.buffer, csvPreference);
    int count = 0;

    Rows.Row labels = (Rows.Row) rows.next();
    if (beeLine.getOpts().getShowHeader()) {
      fillBuffer(writer, labels);
      String line = getLine(this.buffer);
      beeLine.output(line);
    }

    while (rows.hasNext()) {
      fillBuffer(writer, (Rows.Row) rows.next());
      String line = getLine(this.buffer);
      beeLine.output(line);
      count++;
    }

    return count;
  }

  /**
   * Fills the class's internal buffer with a DSV line
   */
  private void fillBuffer(CsvListWriter writer, Rows.Row row) {
    String[] vals = row.values;

    try {
      writer.write(vals);
      writer.flush();
    } catch (Exception e) {
      beeLine.error(e);
    }
  }

  private String getLine(StringBuilderWriter buf) {
    String line = buf.toString();
    buf.getBuilder().setLength(0);
    return line;
  }

  /**
   * Default is disabling the double quoting for separated value
   */
  private boolean isQuotingDisabled() {
    Boolean quotingDisabled = Boolean.TRUE;
    String quotingDisabledStr = System.getProperty(SeparatedValuesOutputFormat.DISABLE_QUOTING_FOR_SV);

    if (StringUtils.isNotBlank(quotingDisabledStr)) {
      quotingDisabled = BooleanUtils.toBooleanObject(quotingDisabledStr);

      if (quotingDisabled == null) {
        beeLine.error("System Property " + SeparatedValuesOutputFormat.DISABLE_QUOTING_FOR_SV + " is now "
          + quotingDisabledStr + " which only accepts boolean values");
        quotingDisabled = Boolean.TRUE;
      }
    }
    return quotingDisabled;
  }
}
