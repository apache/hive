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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Iterator;

/**
 * Abstract base class representing a set of rows to be displayed.
 */
abstract class Rows implements Iterator {
  private final BeeLine beeLine;
  final ResultSetMetaData rsMeta;
  final Boolean[] primaryKeys;
  final NumberFormat numberFormat;

  Rows(BeeLine beeLine, ResultSet rs) throws SQLException {
    this.beeLine = beeLine;
    rsMeta = rs.getMetaData();
    int count = rsMeta.getColumnCount();
    primaryKeys = new Boolean[count];
    if (beeLine.getOpts().getNumberFormat().equals("default")) {
      numberFormat = null;
    } else {
      numberFormat = new DecimalFormat(beeLine.getOpts().getNumberFormat());
    }
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * Update all of the rows to have the same size, set to the
   * maximum length of each column in the Rows.
   */
  abstract void normalizeWidths();

  /**
   * Return whether the specified column (0-based index) is
   * a primary key. Since this method depends on whether the
   * JDBC driver property implements {@link ResultSetMetaData#getTableName} (many do not), it
   * is not reliable for all databases.
   */
  boolean isPrimaryKey(int col) {
    if (primaryKeys[col] != null) {
      return primaryKeys[col].booleanValue();
    }

    try {
      // this doesn't always work, since some JDBC drivers (e.g.,
      // Oracle's) return a blank string from getTableName.
      String table = rsMeta.getTableName(col + 1);
      String column = rsMeta.getColumnName(col + 1);

      if (table == null || table.length() == 0 ||
          column == null || column.length() == 0) {
        return (primaryKeys[col] = new Boolean(false)).booleanValue();
      }

      ResultSet pks = beeLine.getDatabaseConnection().getDatabaseMetaData().getPrimaryKeys(
          beeLine.getDatabaseConnection().getDatabaseMetaData().getConnection().getCatalog(), null, table);

      try {
        while (pks.next()) {
          if (column.equalsIgnoreCase(
              pks.getString("COLUMN_NAME"))) {
            return (primaryKeys[col] = new Boolean(true)).booleanValue();
          }
        }
      } finally {
        pks.close();
      }

      return (primaryKeys[col] = new Boolean(false)).booleanValue();
    } catch (SQLException sqle) {
      return (primaryKeys[col] = new Boolean(false)).booleanValue();
    }
  }


  class Row {
    final String[] values;
    final boolean isMeta;
    boolean deleted;
    boolean inserted;
    boolean updated;
    int[] sizes;

    Row(int size) throws SQLException {
      isMeta = true;
      values = new String[size];
      sizes = new int[size];
      for (int i = 0; i < size; i++) {
        values[i] = rsMeta.getColumnLabel(i + 1);
        sizes[i] = values[i] == null ? 1 : values[i].length();
      }

      deleted = false;
      updated = false;
      inserted = false;
    }


    Row(int size, ResultSet rs) throws SQLException {
      isMeta = false;
      values = new String[size];
      sizes = new int[size];

      try {
        deleted = rs.rowDeleted();
      } catch (Throwable t) {
      }
      try {
        updated = rs.rowUpdated();
      } catch (Throwable t) {
      }
      try {
        inserted = rs.rowInserted();
      } catch (Throwable t) {
      }

      for (int i = 0; i < size; i++) {
        if (numberFormat != null) {
          Object o = rs.getObject(i + 1);
          if (o == null) {
            values[i] = null;
          }  else if (o instanceof Number) {
            values[i] = numberFormat.format(o);
          } else {
            values[i] = o.toString();
          }
        } else {
          values[i] = rs.getString(i + 1);
        }
        sizes[i] = values[i] == null ? 1 : values[i].length();
      }
    }
  }
}
