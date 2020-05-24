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
 * OutputFormat for a pretty, table-like format.
 *
 */
class TableOutputFormat implements OutputFormat {
  private final BeeLine beeLine;
  private final StringBuilder sb = new StringBuilder();

  /**
   * @param beeLine
   */
  TableOutputFormat(BeeLine beeLine) {
    this.beeLine = beeLine;
  }

  @Override
  public int print(Rows rows) {
    int index = 0;
    ColorBuffer header = null;
    ColorBuffer headerCols = null;
    final int width = beeLine.getOpts().getMaxWidth() - 4;

    // normalize the columns sizes
    rows.normalizeWidths();

    for (; rows.hasNext();) {
      Rows.Row row = (Rows.Row) rows.next();
      ColorBuffer cbuf = getOutputString(rows, row);
      if (beeLine.getOpts().getTruncateTable()) {
        cbuf = cbuf.truncate(width);
      }

      if (index == 0)  {
        sb.setLength(0);
        for (int j = 0; j < row.sizes.length; j++) {
          for (int k = 0; k < row.sizes[j]; k++) {
            sb.append('-');
          }
          if (j < row.sizes.length - 1) {
              sb.append("-+-");
          }
        }

        headerCols = cbuf;
        header = beeLine.getColorBuffer().green(sb.toString());
        if (beeLine.getOpts().getTruncateTable()) {
          header = header.truncate(headerCols.getVisibleLength());
        }
      }

      if (beeLine.getOpts().getShowHeader()) {
        if (index == 0 ||
            (index - 1 > 0 && ((index - 1) % beeLine.getOpts().getHeaderInterval() == 0))
           ) {
          printRow(header, true);
          printRow(headerCols, false);
          printRow(header, true);
        }
      } else if (index == 0) {
          printRow(header, true);
      }

      if (index != 0) {
        printRow(cbuf, false);
      }
      index++;
    }

    if (header != null) {
      printRow(header, true);
    }

    return index - 1;
  }

  void printRow(ColorBuffer cbuff, boolean header) {
    if (header) {
      beeLine.output(beeLine.getColorBuffer().green("+-").append(cbuff).green("-+"));
    } else {
      beeLine.output(beeLine.getColorBuffer().green("| ").append(cbuff).green(" |"));
    }
  }

  public ColorBuffer getOutputString(Rows rows, Rows.Row row) {
    return getOutputString(rows, row, " | ");
  }


  ColorBuffer getOutputString(Rows rows, Rows.Row row, String delim) {
    ColorBuffer buf = beeLine.getColorBuffer();

    for (int i = 0; i < row.values.length; i++) {
      if (buf.getVisibleLength() > 0) {
        buf.green(delim);
      }

      ColorBuffer v;

      if (row.isMeta) {
        v = beeLine.getColorBuffer().center(row.values[i], row.sizes[i]);
        if (rows.isPrimaryKey(i)) {
          buf.cyan(v.getMono());
        } else {
          buf.bold(v.getMono());
        }
      } else {
        v = beeLine.getColorBuffer().pad(row.values[i], row.sizes[i]);
        if (rows.isPrimaryKey(i)) {
          buf.cyan(v.getMono());
        } else {
          buf.append(v.getMono());
        }
      }
    }

    if (row.deleted) {
      buf = beeLine.getColorBuffer().red(buf.getMono());
    } else if (row.updated) {
      buf = beeLine.getColorBuffer().blue(buf.getMono());
    } else if (row.inserted) {
      buf = beeLine.getColorBuffer().green(buf.getMono());
    }
    return buf;
  }
}
