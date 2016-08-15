/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.beeline;

import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.mockito.Matchers;
import static org.mockito.Mockito.when;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.mockito.Mockito.mock;

public class TestTableOutputFormat {

  public class BeelineMock extends BeeLine {

    private String lastPrintedLine;

    @Override
    final void output(final ColorBuffer msg, boolean newline, PrintStream out) {
      lastPrintedLine = msg.getMono();
      super.output(msg, newline, out);
    }

    private String getLastPrintedLine() {
      return lastPrintedLine;
    }
  }

  private final String[][] mockRowData = {
    {"key1", "aaa"},
    {"key2", "bbbbb"},
    {"key3", "ccccccccccccccccccccccccccc"},
    {"key4", "ddddddddddddddd"}
  };
  private BeelineMock mockBeeline;
  private ResultSet mockResultSet;
  private TestBufferedRows.MockRow mockRow;

  /**
   * Test of print method, of class TableOutputFormat. There was an empty extra column after the
   * last one.
   */
  @Test
  public final void testPrint() throws SQLException {
    setupMockData();
    BufferedRows bfRows = new BufferedRows(mockBeeline, mockResultSet);
    TableOutputFormat instance = new TableOutputFormat(mockBeeline);
    String expResult = "+-------+------------------------------+";
    instance.print(bfRows);
    String outPutResults = mockBeeline.getLastPrintedLine();
    assertEquals(expResult, outPutResults);
  }

  private void setupMockData() throws SQLException {
    mockBeeline = new BeelineMock();
    mockResultSet = mock(ResultSet.class);

    ResultSetMetaData mockResultSetMetaData = mock(ResultSetMetaData.class);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(2);
    when(mockResultSetMetaData.getColumnLabel(1)).thenReturn("Key");
    when(mockResultSetMetaData.getColumnLabel(2)).thenReturn("Value");
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);

    mockRow = new TestBufferedRows.MockRow();
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

    when(mockResultSet.getString(Matchers.anyInt())).thenAnswer(new Answer<String>() {
      @Override
      public String answer(final InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        int index = ((Integer) args[0]);
        return mockRow.getColumn(index);
      }
    });
  }
}
