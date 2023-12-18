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
import org.apache.hadoop.hive.metastore.tools.schematool.hms.EmbeddedTaskProvider;
import org.apache.hadoop.hive.metastore.tools.schematool.task.SchemaToolTask;
import org.apache.hadoop.hive.metastore.tools.schematool.task.TaskContext;
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

  private SchemaToolTask uut;

  private Statement stmtMock;

  private final InputStream stdin = System.in;

  @Before
  public void setUp() throws Exception {
    uut = new EmbeddedTaskProvider().getTask("dropAllDatabases");
  }

  @After
  public void tearDown() throws Exception {
    System.setIn(stdin);
  }

  private void mockPromptWith(String answer) {
    InputStream in = new ByteArrayInputStream((answer + "\n").getBytes());
    System.setIn(in);
  }

  private TaskContext setUpTaskContext(SchemaToolCommandLine cl) throws Exception {
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

    TaskContext context = Mockito.mock(TaskContext.class);

    when(context.getCommandLine()).thenReturn(cl);
    when(context.getConnectionToMetastore(anyBoolean())).thenReturn(connMock);
    return context;
  }

  @Test
  public void testExecutePromptYes() throws Exception {
    TaskContext context = setUpTaskContext(new SchemaToolCommandLine(new String[] {"-dropAllDatabases", "-dbType", "hive"}, null));
    mockPromptWith("y");

    uut.executeChain(context);

    Mockito.verify(stmtMock).execute("DROP DATABASE `mydb` CASCADE");
    Mockito.verify(stmtMock).execute(String.format("DROP TABLE `%s`.`table1`", Warehouse.DEFAULT_DATABASE_NAME));
    Mockito.verify(stmtMock).execute(String.format("DROP TABLE `%s`.`table2`", Warehouse.DEFAULT_DATABASE_NAME));
    Mockito.verify(stmtMock, times(3)).execute(anyString());
  }

  @Test
  public void testExecutePromptNo() throws Exception {
    TaskContext context = setUpTaskContext(new SchemaToolCommandLine(new String[] {"-dropAllDatabases", "-dbType", "hive"}, null));
    mockPromptWith("n");

    uut.executeChain(context);

    Mockito.verify(stmtMock, times(0)).execute(anyString());
  }

  @Test
  public void testExecuteDryRun() throws Exception {
    TaskContext context = setUpTaskContext(new SchemaToolCommandLine(
        new String[] {"-dropAllDatabases", "-dbType", "hive", "-yes", "-dryRun"}, null));

    uut.executeChain(context);

    Mockito.verify(stmtMock, times(0)).execute(anyString());
  }

  @Test
  public void testExecuteWithYes() throws Exception {
    TaskContext context = setUpTaskContext(new SchemaToolCommandLine(
        new String[] {"-dropAllDatabases", "-dbType", "hive", "-yes"}, null));

    uut.executeChain(context);

    Mockito.verify(stmtMock, times(3)).execute(anyString());
  }
}
