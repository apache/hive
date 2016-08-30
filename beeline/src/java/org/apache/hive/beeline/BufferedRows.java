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
import java.util.Iterator;
import java.util.LinkedList;

import com.google.common.base.Optional;


/**
 * Rows implementation which buffers all rows in a linked list.
 */
class BufferedRows extends Rows {
  private final LinkedList<Row> list;
  private final Iterator<Row> iterator;
  private int maxColumnWidth;

  BufferedRows(BeeLine beeLine, ResultSet rs) throws SQLException {
    this(beeLine, rs, Optional.<Integer> absent());
  }

  BufferedRows(BeeLine beeLine, ResultSet rs, Optional<Integer> limit) throws SQLException {
    super(beeLine, rs);
    list = new LinkedList<Row>();
    int count = rsMeta.getColumnCount();
    list.add(new Row(count));

    int numRowsBuffered = 0;
    if (limit.isPresent()) {
      while (limit.get() > numRowsBuffered && rs.next()) {
        this.list.add(new Row(count, rs));
        numRowsBuffered++;
      }
    } else {
      while (rs.next()) {
        this.list.add(new Row(count, rs));
      }
    }
    iterator = list.iterator();
    maxColumnWidth = beeLine.getOpts().getMaxColumnWidth();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Object next() {
    return iterator.next();
  }

  @Override
  public String toString(){
    return list.toString();
  }

  @Override
  void normalizeWidths() {
    int[] max = null;
    for (Row row : list) {
      if (max == null) {
        max = new int[row.values.length];
      }
      for (int j = 0; j < max.length; j++) {
        // if the max column width is too large, reset it to max allowed Column width
        max[j] = Math.min(Math.max(max[j], row.sizes[j] + 1), maxColumnWidth);
      }
    }
    for (Row row : list) {
      row.sizes = max;
    }
  }
}
