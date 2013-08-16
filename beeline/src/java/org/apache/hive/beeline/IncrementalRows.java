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
