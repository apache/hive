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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;

/**
 * Rows implementation which returns rows incrementally from result set
 * without any buffering.
 */
public class IncrementalRows extends Rows {
  private final ResultSet rs;
  private final Row labelRow;
  private final Row maxRow;
  private Row nextRow;
  private boolean endOfResult;
  private boolean normalizingWidths;


  IncrementalRows(BeeLine beeLine, ResultSet rs) throws SQLException {
    super(beeLine, rs);
    this.rs = rs;

    labelRow = new Row(rsMeta.getColumnCount());
    maxRow = new Row(rsMeta.getColumnCount());
    int maxWidth = beeLine.getOpts().getMaxColumnWidth();

    // pre-compute normalization so we don't have to deal
    // with SQLExceptions later
    for (int i = 0; i < maxRow.sizes.length; ++i) {
      // normalized display width is based on maximum of display size
      // and label size
      maxRow.sizes[i] = Math.max(
          maxRow.sizes[i],
          rsMeta.getColumnDisplaySize(i + 1));
      maxRow.sizes[i] = Math.min(maxWidth, maxRow.sizes[i]);
    }

    nextRow = labelRow;
    endOfResult = false;
  }


  public boolean hasNext() {
    if (endOfResult) {
      return false;
    }

    if (nextRow == null) {
      try {
        if (rs.next()) {
          nextRow = new Row(labelRow.sizes.length, rs);

          if (normalizingWidths) {
            // perform incremental normalization
            nextRow.sizes = labelRow.sizes;
          }
        } else {
          endOfResult = true;
        }
      } catch (SQLException ex) {
        throw new RuntimeException(ex.toString());
      }
    }
    return (nextRow != null);
  }

  public Object next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Object ret = nextRow;
    nextRow = null;
    return ret;
  }

  @Override
  void normalizeWidths() {
    // normalize label row
    labelRow.sizes = maxRow.sizes;
    // and remind ourselves to perform incremental normalization
    // for each row as it is produced
    normalizingWidths = true;
  }
}
