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
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Rows implementation which buffers all rows in a linked list.
 */
class BufferedRows extends Rows {
  private final LinkedList<Row> list;
  private final Iterator<Row> iterator;

  BufferedRows(BeeLine beeLine, ResultSet rs) throws SQLException {
    super(beeLine, rs);
    list = new LinkedList<Row>();
    int count = rsMeta.getColumnCount();
    list.add(new Row(count));
    while (rs.next()) {
      list.add(new Row(count, rs));
    }
    iterator = list.iterator();
  }

  public boolean hasNext() {
    return iterator.hasNext();
  }

  public Object next() {
    return iterator.next();
  }

  @Override
  void normalizeWidths() {
    int[] max = null;
    for (Row row : list) {
      if (max == null) {
        max = new int[row.values.length];
      }
      for (int j = 0; j < max.length; j++) {
        max[j] = Math.max(max[j], row.sizes[j] + 1);
      }
    }
    for (Row row : list) {
      row.sizes = max;
    }
  }

}
