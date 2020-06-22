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

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestJSONOutputFormat {

  private final Object[][] mockRowData = {
      {"aaa", true, null, Double.valueOf(3.14), "\\/\b\f\n\r\t"}
  };
  private TestJSONOutputFormat.BeelineMock mockBeeline;
  private ResultSet mockResultSet;
  private MockRow mockRow;

  @Before
  public void setupMockData() throws SQLException {
    mockBeeline = new TestJSONOutputFormat.BeelineMock();
    mockResultSet = mock(ResultSet.class);

    ResultSetMetaData mockResultSetMetaData = mock(ResultSetMetaData.class);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(5);
    when(mockResultSetMetaData.getColumnLabel(1)).thenReturn("string");
    when(mockResultSetMetaData.getColumnLabel(2)).thenReturn("boolean");
    when(mockResultSetMetaData.getColumnLabel(3)).thenReturn("null");
    when(mockResultSetMetaData.getColumnLabel(4)).thenReturn("double");
    when(mockResultSetMetaData.getColumnLabel(5)).thenReturn("special symbols");
    when(mockResultSetMetaData.getColumnType(1)).thenReturn(Types.VARCHAR);
    when(mockResultSetMetaData.getColumnType(2)).thenReturn(Types.BOOLEAN);
    when(mockResultSetMetaData.getColumnType(3)).thenReturn(Types.NULL);
    when(mockResultSetMetaData.getColumnType(4)).thenReturn(Types.DOUBLE);
    when(mockResultSetMetaData.getColumnType(5)).thenReturn(Types.VARCHAR);
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);

    mockRow = new MockRow();
    // returns true as long as there is more data in mockResultData array
    when(mockResultSet.next()).thenAnswer(new Answer<Boolean>() {
      private int mockRowDataIndex = 0;

      @Override
      public Boolean answer(final InvocationOnMock invocation) {
        if (mockRowDataIndex < mockRowData.length) {
          mockRow.setCurrentRowData(mockRowData[mockRowDataIndex]);
          mockRowDataIndex++;
          return true;
        } else {
          return false;
        }
      }
    });

    when(mockResultSet.getObject(anyInt())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(final InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        int index = ((Integer) args[0]);
        return mockRow.getColumn(index);
      }
    });
  }

  /**
   * Test printing output data with JsonOutputFormat
   */
  @Test
  public final void testPrint() throws SQLException {
    setupMockData();
    BufferedRows bfRows = new BufferedRows(mockBeeline, mockResultSet);
    JSONOutputFormat instance = new JSONOutputFormat(mockBeeline);
    instance.print(bfRows);
    ArrayList<String> actualOutput = mockBeeline.getLines();
    ArrayList<String> expectedOutput = new ArrayList<>(6);
    expectedOutput.add("{\"resultset\":[");
    expectedOutput.add("{\"string\":\"aaa\"," + "\"boolean\":true," + "\"null\":null," + "\"double\":3.14,"
        + "\"special symbols\":\"\\\\\\/\\b\\f\\n\\r\\t\"}");
    expectedOutput.add("]}");
    assertArrayEquals(expectedOutput.toArray(), actualOutput.toArray());
  }

  public class BeelineMock extends BeeLine {

    private ArrayList<String> lines = new ArrayList<>();

    @Override
    final void output(final ColorBuffer msg, boolean newline, PrintStream out) {
      lines.add(msg.getMono());
      super.output(msg, newline, out);
    }

    private ArrayList<String> getLines() {
      return lines;
    }
  }

  static class MockRow {
    Object[] rowData;

    public void setCurrentRowData(Object[] rowData) {
      this.rowData = rowData;
    }

    public Object getColumn(int idx) {
      return rowData[idx - 1];
    }
  }
}