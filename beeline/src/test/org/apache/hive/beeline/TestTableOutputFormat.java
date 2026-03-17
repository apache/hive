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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

import static org.mockito.Mockito.when;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.mockito.Mockito.mock;

public class TestTableOutputFormat {

  private static final String GREEN = "\033[1;32m";

  private final String[][] mockRowData = {
    {"key1", "aaa"},
    {"key2", "bbbbb"},
    {"key3", "ccccccccccccccccccccccccccc"},
    {"key4", "ddddddddddddddd"}
  };

  private BeelineMock mockBeeline;
  private ResultSet mockResultSet;
  private ResultSetMetaData mockResultSetMetaData;
  private TestBufferedRows.MockRow mockRow;

  /**
   * Test of print method, of class TableOutputFormat. There was an empty extra column after the
   * last one.
   */
  @Test
  public final void testPrint() throws SQLException {
    String EXP_LAST_LINE = "+-------+------------------------------+";

    setupMockData();
    BufferedRows bfRows = new BufferedRows(mockBeeline, mockResultSet);
    TableOutputFormat instance = new TableOutputFormat(mockBeeline);

    instance.print(bfRows);

    String outPutResults = mockBeeline.getLastPrintedLine();
    assertEquals(EXP_LAST_LINE, outPutResults);
  }

  /**
   * If the DatabaseConnection doesn't provide metadata, there is nothing to color
   */
  @Test
  public void testColoringWithNoTableMetadata() throws SQLException {
    setupMockData();

    BufferedRows bfRows = new BufferedRows(mockBeeline, mockResultSet);
    TableOutputFormat instance = new TableOutputFormat(mockBeeline);

    instance.print(bfRows);

    List<String> allPrintedLines = mockBeeline.getAllPrintedLines();
    assertColumnHasColor(allPrintedLines, 1, GREEN);
    assertColumnHasColor(allPrintedLines, 2, GREEN);
  }

  /**
   * Default behavior: coloring is disabled
   */
  @Test
  public void testMonoTableOutputFormat() throws SQLException {
    setupMockData();
    mockBeeline.getOpts().setColor(false);
    BufferedRows bfRows = new BufferedRows(mockBeeline, mockResultSet);
    TableOutputFormat instance = new TableOutputFormat(mockBeeline);

    instance.print(bfRows);

    List<String> allPrintedLines = mockBeeline.getAllPrintedLines();
    for (String line : allPrintedLines) {
      assertFalse(line.contains("\033["));
    }
  }


  private boolean cellHasColor(List<String> table, int row, int col, String color) {
    String rowText = table.get(row);
    String split = rowText.split("\\|")[col];
    return split.contains(color);
  }

  private void assertColumnHasColor(List<String> allPrintedLines, int col, String color) {
    assertTrue(cellHasColor(allPrintedLines, 1, col, color));
    assertTrue(cellHasColor(allPrintedLines, 3, col, color));
    assertTrue(cellHasColor(allPrintedLines, 4, col, color));
    assertTrue(cellHasColor(allPrintedLines, 5, col, color));
    assertTrue(cellHasColor(allPrintedLines, 6, col, color));
  }

  private void setupMockData() throws SQLException {
    mockBeeline = new BeelineMock();
    mockBeeline.getOpts().setColor(true);
    mockResultSet = mock(ResultSet.class);

    mockResultSetMetaData = mock(ResultSetMetaData.class);
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

    when(mockResultSet.getObject(anyInt())).thenAnswer((Answer<String>) invocation -> {
      Object[] args = invocation.getArguments();
      int index = ((Integer) args[0]);
      return mockRow.getColumn(index);
    });

    when(mockResultSet.getString(anyInt())).thenAnswer((Answer<String>) invocation -> {
      Object[] args = invocation.getArguments();
      int index = ((Integer) args[0]);
      return mockRow.getColumn(index);
    });
  }
}
