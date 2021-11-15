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

package org.apache.hadoop.hive.metastore.tools.schematool;

import org.apache.hadoop.hive.metastore.Warehouse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * Testing SchemaToolTaskDrop.
 */
public class TestSchemaToolTaskDrop {

  private SchemaToolTaskDrop uut;

  private Statement stmtMock;

  private final InputStream stdin = System.in;

  @Before
  public void setUp() throws Exception {
    uut = new SchemaToolTaskDrop();
    uut.schemaTool = mock(MetastoreSchemaTool.class);
  }

  @After
  public void tearDown() throws Exception {
    System.setIn(stdin);
  }

  private void mockPromptWith(String answer) {
    InputStream in = new ByteArrayInputStream((answer + "\n").getBytes());
    System.setIn(in);
  }

  private void setUpTwoDatabases() throws Exception {
    Connection connMock = mock(Connection.class);
    stmtMock = mock(Statement.class);

    // Return two databases: `mydb` and `default`
    ResultSet databasesResult = mock(ResultSet.class);
    when(databasesResult.next()).thenReturn(true, true, false);
    when(databasesResult.getString(anyInt())).thenReturn("mydb", Warehouse.DEFAULT_DATABASE_NAME);

    // Return two tables: `table1` and `table2`
    ResultSet tablesResult = mock(ResultSet.class);
    when(tablesResult.next()).thenReturn(true, true, false);
    when(tablesResult.getString(anyInt())).thenReturn("table1", "table2");

    when(stmtMock.executeQuery(anyString())).thenReturn(databasesResult, tablesResult);
    when(connMock.createStatement()).thenReturn(stmtMock);

    when(uut.schemaTool.getConnectionToMetastore(anyBoolean())).thenReturn(connMock);
  }

  @Test
  public void testExecutePromptYes() throws Exception {
    setUpTwoDatabases();
    mockPromptWith("y");

    uut.execute();

    Mockito.verify(stmtMock).execute("DROP DATABASE `mydb` CASCADE");
    Mockito.verify(stmtMock).execute(String.format("DROP TABLE `%s`.`table1`", Warehouse.DEFAULT_DATABASE_NAME));
    Mockito.verify(stmtMock).execute(String.format("DROP TABLE `%s`.`table2`", Warehouse.DEFAULT_DATABASE_NAME));
    Mockito.verify(stmtMock, times(3)).execute(anyString());
  }

  @Test
  public void testExecutePromptNo() throws Exception {
    setUpTwoDatabases();
    mockPromptWith("n");

    uut.execute();

    Mockito.verify(stmtMock, times(0)).execute(anyString());
  }

  @Test
  public void testExecuteDryRun() throws Exception {
    setUpTwoDatabases();
    when(uut.schemaTool.isDryRun()).thenReturn(true);

    uut.execute();

    Mockito.verify(stmtMock, times(0)).execute(anyString());
  }

  @Test
  public void testExecuteWithYes() throws Exception {
    setUpTwoDatabases();
    uut.yes = true;

    uut.execute();

    Mockito.verify(stmtMock, times(3)).execute(anyString());
  }
}
