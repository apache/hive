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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.mockito.Mockito.mock;

public class TestTableOutputFormat {

  private static final String CYAN = "\033[1;36m";
  private static final String GREEN = "\033[1;32m";

  private final String[][] mockRowData = {
    {"key1", "aaa"},
    {"key2", "bbbbb"},
    {"key3", "ccccccccccccccccccccccccccc"},
    {"key4", "ddddddddddddddd"}
  };

  private BeelineMock mockBeeline;
  private DatabaseConnection dbConnection;
  private Connection mockSqlConnection;
  private DatabaseMetaData mockDatabaseMetaData;
  private ResultSet mockResultSet;
  private ResultSetMetaData mockResultSetMetaData;
  private ResultSet mockMetadataResultSet;
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
    when(mockResultSetMetaData.getTableName(anyInt())).thenReturn(null);

    BufferedRows bfRows = new BufferedRows(mockBeeline, mockResultSet);
    TableOutputFormat instance = new TableOutputFormat(mockBeeline);

    instance.print(bfRows);

    List<String> allPrintedLines = mockBeeline.getAllPrintedLines();
    assertColumnHasColor(allPrintedLines, 1, GREEN);
    assertColumnHasColor(allPrintedLines, 2, GREEN);
  }

  /**
   * The primary key has one column. It should be CYAN. The second column should be green
   */
  @Test
  public void testSingleColumnPrimaryKey() throws SQLException {
    setupMockData();
    mockBeeline.getOpts().setColor(true);

    BufferedRows bfRows = new BufferedRows(mockBeeline, mockResultSet);
    TableOutputFormat instance = new TableOutputFormat(mockBeeline);

    instance.print(bfRows);

    List<String> allPrintedLines = mockBeeline.getAllPrintedLines();

    // PK column is CYAN
    assertColumnHasColor(allPrintedLines, 1, CYAN);
    // non-PK column is green
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

  /**
   * No primary key column - all columns should be green
   */
  @Test
  public void testColoredWithoutPrimaryKey() throws SQLException {
    setupMockData();
    mockBeeline.getOpts().setColor(true);
    when(mockMetadataResultSet.next()).thenReturn(false);

    BufferedRows bfRows = new BufferedRows(mockBeeline, mockResultSet);
    TableOutputFormat instance = new TableOutputFormat(mockBeeline);

    instance.print(bfRows);

    List<String> allPrintedLines = mockBeeline.getAllPrintedLines();

    assertColumnHasColor(allPrintedLines, 1, GREEN);
    assertColumnHasColor(allPrintedLines, 2, GREEN);
  }

  /**
   * The output contains one single table. And both columns are part of the PK
   */
  @Test
  public void testColoredWithPrimaryKeyHasMultipleColumns() throws SQLException {
    setupMockData();
    mockBeeline.getOpts().setColor(true);
    when(mockMetadataResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);

    BufferedRows bfRows = new BufferedRows(mockBeeline, mockResultSet);
    TableOutputFormat instance = new TableOutputFormat(mockBeeline);

    instance.print(bfRows);

    List<String> allPrintedLines = mockBeeline.getAllPrintedLines();

    // PK column are CYAN
    assertColumnHasColor(allPrintedLines, 1, CYAN);
    assertColumnHasColor(allPrintedLines, 2, CYAN);
  }

  /**
   * The output contains two tables. Both columns are PKs
   */
  @Test
  public void testColoredWithMultipleTablesWithPrimaryKeys() throws SQLException {
    setupMockData();
    mockBeeline.getOpts().setColor(true);

    when(mockResultSetMetaData.getTableName(eq(1))).thenReturn("Table");
    when(mockResultSetMetaData.getTableName(eq(2))).thenReturn("Table2");

    DatabaseMetaData databaseMetaData2 = mock(DatabaseMetaData.class);
    ResultSet resultSet2 = mock(ResultSet.class);
    when(resultSet2.next()).thenReturn(true).thenReturn(false);
    when(resultSet2.getString(eq("COLUMN_NAME"))).thenReturn("Value");
    when(databaseMetaData2.getPrimaryKeys(anyString(), isNull(), eq("Table2"))).thenReturn(resultSet2);
    when(databaseMetaData2.getConnection()).thenReturn(mockSqlConnection);
    when(dbConnection.getDatabaseMetaData()).thenReturn(mockDatabaseMetaData, databaseMetaData2);

    BufferedRows bfRows = new BufferedRows(mockBeeline, mockResultSet);
    TableOutputFormat instance = new TableOutputFormat(mockBeeline);

    instance.print(bfRows);

    List<String> allPrintedLines = mockBeeline.getAllPrintedLines();

    // PK column are CYAN
    assertColumnHasColor(allPrintedLines, 1, CYAN);
    assertColumnHasColor(allPrintedLines, 2, CYAN);
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

    mockSqlConnection = mock(Connection.class);
    when(mockSqlConnection.getCatalog()).thenReturn("Catalog");

    mockResultSetMetaData = mock(ResultSetMetaData.class);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(2);
    when(mockResultSetMetaData.getColumnLabel(1)).thenReturn("Key");
    when(mockResultSetMetaData.getColumnLabel(2)).thenReturn("Value");
    when(mockResultSetMetaData.getTableName(anyInt())).thenReturn("Table");
    when(mockResultSetMetaData.getColumnName(eq(1))).thenReturn("Key");
    when(mockResultSetMetaData.getColumnName(eq(2))).thenReturn("Value");
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);

    mockMetadataResultSet = mock(ResultSet.class);
    when(mockMetadataResultSet.next()).thenReturn(true).thenReturn(false);
    when(mockMetadataResultSet.getString(eq("COLUMN_NAME"))).thenReturn("Key", "Value");

    mockDatabaseMetaData = mock(DatabaseMetaData.class);
    when(mockDatabaseMetaData.getPrimaryKeys(anyString(), isNull(), anyString())).thenReturn(mockMetadataResultSet);

    dbConnection = mock(DatabaseConnection.class);
    when(dbConnection.getDatabaseMetaData()).thenReturn(mockDatabaseMetaData);
    when(mockDatabaseMetaData.getConnection()).thenReturn(mockSqlConnection);

    mockBeeline.getDatabaseConnections().setConnection(dbConnection);

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
