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
package org.apache.hive.beeline;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestBufferedRows {
  private String[][] mockRowData = {
      { "key1", "aaa" },
      { "key2", "bbbbb" },
      { "key3",
          "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
              + "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
              + "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
              + "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
              + "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
              + "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
              + "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc" },
      { "key4", "ddddddddddddddd" }
  };
  private BeeLineOpts mockBeeLineOpts;
  private BeeLine mockBeeline;
  private ResultSet mockResultSet;
  private MockRow mockRow;

  @Test
  public void testNormalizeWidths() throws SQLException {
    setupMockData();

    BufferedRows bfRows = new BufferedRows(mockBeeline, mockResultSet);
    bfRows.normalizeWidths();
    while (bfRows.hasNext()) {
      Rows.Row row = (Rows.Row) bfRows.next();
      for (int colSize : row.sizes) {
        Assert.assertTrue(colSize <= mockBeeLineOpts.getMaxColumnWidth());
      }
    }
  }

  private void setupMockData() throws SQLException {
    // Mock BeeLine
    mockBeeline = mock(BeeLine.class);
    // Mock BeeLineOpts
    mockBeeLineOpts = mock(BeeLineOpts.class);
    when(mockBeeLineOpts.getMaxColumnWidth()).thenReturn(BeeLineOpts.DEFAULT_MAX_COLUMN_WIDTH);
    when(mockBeeLineOpts.getNumberFormat()).thenReturn("default");
    when(mockBeeLineOpts.getNullString()).thenReturn("NULL");
    when(mockBeeline.getOpts()).thenReturn(mockBeeLineOpts);

    // MockResultSet
    mockResultSet = mock(ResultSet.class);

    ResultSetMetaData mockResultSetMetaData = mock(ResultSetMetaData.class);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(2);
    when(mockResultSetMetaData.getColumnLabel(1)).thenReturn("Key");
    when(mockResultSetMetaData.getColumnLabel(2)).thenReturn("Value");
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);

    mockRow = new MockRow();
    // returns true as long as there is more data in mockResultData array
    when(mockResultSet.next()).thenAnswer(new Answer<Boolean>() {
      private int mockRowDataIndex = 0;

      @Override
      public Boolean answer(InvocationOnMock invocation) {
        if (mockRowDataIndex < mockRowData.length) {
          mockRow.setCurrentRowData(mockRowData[mockRowDataIndex]);
          mockRowDataIndex++;
          return true;
        } else {
          return false;
        }
      }
    });

    when(mockResultSet.getObject(anyInt())).thenAnswer(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        int index = ((Integer) args[0]).intValue();
        return mockRow.getColumn(index);
      }
    });

    when(mockResultSet.getString(anyInt())).thenAnswer(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        int index = ((Integer) args[0]).intValue();
        return mockRow.getColumn(index);
      }
    });
  }

  static class MockRow {
    String[] rowData;

    public void setCurrentRowData(String[] rowData) {
      this.rowData = rowData;
    }

    public String getColumn(int idx) {
      return rowData[idx - 1];
    }
  }
}
