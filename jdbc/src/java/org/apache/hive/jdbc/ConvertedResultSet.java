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

package org.apache.hive.jdbc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;

import org.apache.hadoop.hive.serde2.thrift.Type;

import org.apache.hive.service.rpc.thrift.TRowSet;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.TypeDescriptor;

/**
 * ConvertedResultSet
 */
public class ConvertedResultSet implements RowSet {

  private final ArrayList<Object[]> rows;
  private final int numColumns;

  private interface ConvertFunc {
    Object convert(Object o);
  }

  static final int nTypes = Type.values().length;
  static final ConvertFunc convertFuncs [] = new ConvertFunc [nTypes];
  static {
    // These mirror the conversions done in HiveBaseResultSet
    convertFuncs[Type.BINARY_TYPE.ordinal()] =
        o -> (o instanceof String) ? ((String)o).getBytes() : 0;
    convertFuncs[Type.TIMESTAMP_TYPE.ordinal()] =
        o -> Timestamp.valueOf((String)o);
    convertFuncs[Type.TIMESTAMPLOCALTZ_TYPE.ordinal()] =
        o -> TimestampTZUtil.parse((String)o);
    convertFuncs[Type.DECIMAL_TYPE.ordinal()] =
        o -> new BigDecimal((String)o);
    convertFuncs[Type.DATE_TYPE.ordinal()] =
        o -> Date.valueOf((String) o);
    convertFuncs[Type. INTERVAL_YEAR_MONTH_TYPE.ordinal()] =
        o -> HiveIntervalYearMonth.valueOf((String) o);
    convertFuncs[Type. INTERVAL_DAY_TIME_TYPE.ordinal()] =
        o -> HiveIntervalDayTime.valueOf((String) o);
  }

  public ConvertedResultSet(RowSet rowSet, TableSchema schema) {
    rows = new ArrayList<Object[]>();
    numColumns = rowSet.numColumns();
    Iterator<Object[]> srcIter = rowSet.iterator();
    final int nCols = schema.getSize();
    ConvertFunc [] colConvert = new ConvertFunc[nCols];
    for (int i = 0; i < nCols; ++i) {
      colConvert[i] = convertFuncs[schema.getColumnDescriptorAt(i).getType().ordinal()];
    }

    while (srcIter.hasNext()) {
      Object [] srcRow = srcIter.next();
      Object [] dstRow = new Object[srcRow.length];
      for (int i = 0; i < nCols; ++i) {
        if (colConvert[i] != null && srcRow[i] != null) {
          dstRow[i] = colConvert[i].convert(srcRow[i]);
        } else {
          dstRow[i] = srcRow[i];
        }
      }
      rows.add(dstRow);
    }
  }

  @Override
  public ConvertedResultSet addRow(Object[] fields) {
    throw new UnsupportedOperationException("addRow");
  }

  @Override
  public int numColumns() {
    return numColumns;
  }

  @Override
  public int numRows() {
    return rows.size();
  }

  public ConvertedResultSet extractSubset(int maxRows) {
    throw new UnsupportedOperationException("extractSubset");
  }

  @Override
  public long getStartOffset() {
    throw new UnsupportedOperationException("getStartOffset");
  }

  @Override
  public void setStartOffset(long startOffset) {
    throw new UnsupportedOperationException("setStartOffset");
  }

  @Override
  public TRowSet toTRowSet() {
    throw new UnsupportedOperationException("toTRowSet");
  }

  @Override
  public Iterator<Object[]> iterator() {
    return new Iterator<Object[]>() {

      final Iterator<Object[]> iterator = rows.iterator();

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Object[] next() {
        return iterator.next();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove");
      }
    };
  }
}
