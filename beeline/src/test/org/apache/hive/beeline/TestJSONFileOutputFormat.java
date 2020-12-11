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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import org.junit.Test;
import org.junit.Before;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestJSONFileOutputFormat {
  private final String[][] mockRowData = {
    {"aaa","1","3.14","true","","SGVsbG8sIFdvcmxkIQ"},
    {"bbb","2","2.718","false","Null","RWFzdGVyCgllZ2cu"}
  };
  
  public ResultSet mockResultSet;
  public TestBufferedRows.MockRow mockRow;
  
  @Test
  public final void testPrint() throws SQLException {
	BeeLine mockBeeline = spy(BeeLine.class);
	ArgumentCaptor<String> captureOutput = ArgumentCaptor.forClass(String.class);
	Mockito.doNothing().when(mockBeeline).output(captureOutput.capture());
    BufferedRows bfRows = new BufferedRows(mockBeeline, mockResultSet);
    JSONFileOutputFormat instance = new JSONFileOutputFormat(mockBeeline);
	instance.print(bfRows);
    String expResult = "{\"String\":\"aaa\",\"Int\":1,\"Decimal\":3.14,\"Bool\":true,\"Null\":null,\"Binary\":\"SGVsbG8sIFdvcmxkIQ\"}\n{\"String\":\"bbb\",\"Int\":2,\"Decimal\":2.718,\"Bool\":false,\"Null\":null,\"Binary\":\"RWFzdGVyCgllZ2cu\"}";
    assertEquals(expResult, captureOutput.getValue());
  }
  
  @Before
  public void setupMockData() throws SQLException {
	mockResultSet = mock(ResultSet.class);
    ResultSetMetaData mockResultSetMetaData = mock(ResultSetMetaData.class);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(6);
    when(mockResultSetMetaData.getColumnLabel(1)).thenReturn("String");
    when(mockResultSetMetaData.getColumnLabel(2)).thenReturn("Int");
    when(mockResultSetMetaData.getColumnLabel(3)).thenReturn("Decimal");
    when(mockResultSetMetaData.getColumnLabel(4)).thenReturn("Bool");
    when(mockResultSetMetaData.getColumnLabel(5)).thenReturn("Null");
    when(mockResultSetMetaData.getColumnLabel(6)).thenReturn("Binary");

    when(mockResultSetMetaData.getColumnType(1)).thenReturn(Types.VARCHAR);
    when(mockResultSetMetaData.getColumnType(2)).thenReturn(Types.INTEGER);
    when(mockResultSetMetaData.getColumnType(3)).thenReturn(Types.DECIMAL);
    when(mockResultSetMetaData.getColumnType(4)).thenReturn(Types.BOOLEAN);
    when(mockResultSetMetaData.getColumnType(5)).thenReturn(Types.NULL);
    when(mockResultSetMetaData.getColumnType(6)).thenReturn(Types.BINARY);

    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);

    mockRow = new TestBufferedRows.MockRow();
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

    when(mockResultSet.getObject(anyInt())).thenAnswer(new Answer<String>() {
      @Override
      public String answer(final InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        int index = ((Integer) args[0]);
        return mockRow.getColumn(index);
      }
    });

    when(mockResultSet.getString(anyInt())).thenAnswer(new Answer<String>() {
      @Override
      public String answer(final InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        int index = ((Integer) args[0]);
        return mockRow.getColumn(index);
      }
    });
  }
}
