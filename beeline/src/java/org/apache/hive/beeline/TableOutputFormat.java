/*
 *  Copyright (c) 2002,2003,2004,2005 Marc Prud'hommeaux
 *  All rights reserved.
 *
 *
 *  Redistribution and use in source and binary forms,
 *  with or without modification, are permitted provided
 *  that the following conditions are met:
 *
 *  Redistributions of source code must retain the above
 *  copyright notice, this list of conditions and the following
 *  disclaimer.
 *  Redistributions in binary form must reproduce the above
 *  copyright notice, this list of conditions and the following
 *  disclaimer in the documentation and/or other materials
 *  provided with the distribution.
 *  Neither the name of the <ORGANIZATION> nor the names
 *  of its contributors may be used to endorse or promote
 *  products derived from this software without specific
 *  prior written permission.
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS
 *  AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 *  WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 *  INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 *  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 *  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
 *  OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 *  OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 *  IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 *  ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *  This software is hosted by SourceForge.
 *  SourceForge is a trademark of VA Linux Systems, Inc.
 */

/*
 * This source file is based on code taken from SQLLine 1.0.2
 * The license above originally appeared in src/sqlline/SqlLine.java
 * http://sqlline.sourceforge.net/
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
      cbuf = cbuf.truncate(width);

      if (index == 0)  {
        sb.setLength(0);
        for (int j = 0; j < row.sizes.length; j++) {
          for (int k = 0; k < row.sizes[j]; k++) {
            sb.append('-');
          }
          sb.append("-+-");
        }

        headerCols = cbuf;
        header = beeLine.getColorBuffer()
            .green(sb.toString())
            .truncate(headerCols.getVisibleLength());
      }

      if (index == 0 ||
          (beeLine.getOpts().getHeaderInterval() > 0
              && index % beeLine.getOpts().getHeaderInterval() == 0
              && beeLine.getOpts().getShowHeader())) {
        printRow(header, true);
        printRow(headerCols, false);
        printRow(header, true);
      }

      if (index != 0) {
        printRow(cbuf, false);
      }
      index++;
    }

    if (header != null && beeLine.getOpts().getShowHeader()) {
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