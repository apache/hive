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
import java.util.function.UnaryOperator;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;

import org.apache.hadoop.hive.serde2.thrift.Type;

import org.apache.hive.service.rpc.thrift.TRowSet;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;

/**
 * ConvertedResultSet
 */
public class ConvertedResultSet implements RowSet {

  private final List<Object[]> rows;
  private final int numColumns;

  static final Map<Type, UnaryOperator<Object>> convertFuncs = new EnumMap<Type, UnaryOperator<Object>>(Type.class) {{
    put(Type.BINARY_TYPE, o -> (o instanceof String) ? ((String) o).getBytes() : 0);
    put(Type.TIMESTAMP_TYPE, o -> Timestamp.valueOf((String) o));
    put(Type.TIMESTAMPLOCALTZ_TYPE, o -> TimestampTZUtil.parse((String) o));
    put(Type.DECIMAL_TYPE, o -> new BigDecimal((String) o));
    put(Type.DATE_TYPE, o -> Date.valueOf((String) o));
    put(Type.INTERVAL_YEAR_MONTH_TYPE, o -> HiveIntervalYearMonth.valueOf((String) o));
    put(Type.INTERVAL_DAY_TIME_TYPE, o -> HiveIntervalDayTime.valueOf((String) o));
  }};


  public ConvertedResultSet(RowSet rowSet, TableSchema schema) {
    rows = new ArrayList<>();
    numColumns = rowSet.numColumns();
    Iterator<Object[]> srcIter = rowSet.iterator();
    final int nCols = schema.getSize();
    
    UnaryOperator[] colConvert = new UnaryOperator[nCols];
    for (int i = 0; i < nCols; ++i) {
      colConvert[i] = convertFuncs.get(schema.getColumnDescriptorAt(i).getType());
    }
    while (srcIter.hasNext()) {
      Object[] srcRow = srcIter.next();
      Object[] dstRow = new Object[srcRow.length];
      for (int i = 0; i < nCols; ++i) {
        if (colConvert[i] != null && srcRow[i] != null) {
          dstRow[i] = colConvert[i].apply(srcRow[i]);
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
    };
  }
}
