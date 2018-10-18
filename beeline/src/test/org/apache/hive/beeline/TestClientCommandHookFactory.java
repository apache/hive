/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.beeline;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.sql.Connection;
import java.sql.SQLException;

public class TestClientCommandHookFactory {
  public BeeLine setupMockData(boolean isBeeLine, boolean showDbInPrompt) {
    BeeLine mockBeeLine = mock(BeeLine.class);
    DatabaseConnection mockDatabaseConnection = mock(DatabaseConnection.class);
    Connection mockConnection = mock(Connection.class);
    try {
      when(mockConnection.getSchema()).thenReturn("newDatabase");
      when(mockDatabaseConnection.getConnection()).thenReturn(mockConnection);
    } catch(SQLException sqlException) {
      // We do mnot test this
    }
    when(mockBeeLine.getDatabaseConnection()).thenReturn(mockDatabaseConnection);
    BeeLineOpts mockBeeLineOpts = mock(BeeLineOpts.class);
    when(mockBeeLineOpts.getShowDbInPrompt()).thenReturn(showDbInPrompt);
    when(mockBeeLine.getOpts()).thenReturn(mockBeeLineOpts);
    when(mockBeeLine.isBeeLine()).thenReturn(isBeeLine);

    return mockBeeLine;
  }

  @Test
  public void testGetHookCli() {
    BeeLine beeLine = setupMockData(false, false);
    Assert.assertNull(ClientCommandHookFactory.get().getHook(beeLine, "set a;"));
    Assert.assertTrue(ClientCommandHookFactory.get()
        .getHook(beeLine, "set a=b;") instanceof ClientCommandHookFactory.SetCommandHook);
    Assert.assertTrue(ClientCommandHookFactory.get()
        .getHook(beeLine, "USE a.b") instanceof ClientCommandHookFactory.UseCommandHook);
    Assert.assertNull(ClientCommandHookFactory.get().getHook(beeLine, "coNNect a.b"));
    Assert.assertNull(ClientCommandHookFactory.get().getHook(beeLine, "gO 1"));
    Assert.assertNull(ClientCommandHookFactory.get().getHook(beeLine, "g"));
  }

  @Test
  public void testGetHookBeeLineWithShowDbInPrompt() {
    BeeLine beeLine = setupMockData(true, true);
    Assert.assertNull(ClientCommandHookFactory.get().getHook(beeLine, "set a;"));
    Assert.assertNull(ClientCommandHookFactory.get().getHook(beeLine, "set a=b;"));
    Assert.assertTrue(ClientCommandHookFactory.get()
            .getHook(beeLine, "USE a.b") instanceof ClientCommandHookFactory.UseCommandHook);
    Assert.assertTrue(ClientCommandHookFactory.get()
            .getHook(beeLine, "coNNect a.b") instanceof ClientCommandHookFactory.ConnectCommandHook);
    Assert.assertTrue(ClientCommandHookFactory.get()
            .getHook(beeLine, "gO 1") instanceof ClientCommandHookFactory.GoCommandHook);
    Assert.assertNull(ClientCommandHookFactory.get().getHook(beeLine, "g"));
  }

  @Test
  public void testGetHookBeeLineWithoutShowDbInPrompt() {
    BeeLine beeLine = setupMockData(true, false);
    Assert.assertNull(ClientCommandHookFactory.get().getHook(beeLine, "set a;"));
    Assert.assertNull(ClientCommandHookFactory.get().getHook(beeLine, "set a=b;"));
    Assert.assertNull(ClientCommandHookFactory.get().getHook(beeLine, "USE a.b"));
    Assert.assertNull(ClientCommandHookFactory.get().getHook(beeLine, "coNNect a.b"));
    Assert.assertNull(ClientCommandHookFactory.get().getHook(beeLine, "gO 1"));
    Assert.assertNull(ClientCommandHookFactory.get().getHook(beeLine, "g"));
  }

  @Test
  public void testUseHook() {
    BeeLine beeLine = setupMockData(true, true);
    ClientHook hook = ClientCommandHookFactory.get().getHook(beeLine, "USE newDatabase1");
    Assert.assertTrue(hook instanceof ClientCommandHookFactory.UseCommandHook);
    hook.postHook(beeLine);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(beeLine).setCurrentDatabase(argument.capture());
    Assert.assertEquals("newDatabase1", argument.getValue());
  }

  @Test
  public void testConnectHook() {
    BeeLine beeLine = setupMockData(true, true);
    ClientHook hook = ClientCommandHookFactory.get().getHook(beeLine, "coNNect jdbc:hive2://localhost:10000/newDatabase2 a a");
    Assert.assertTrue(hook instanceof ClientCommandHookFactory.ConnectCommandHook);
    hook.postHook(beeLine);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(beeLine).setCurrentDatabase(argument.capture());
    Assert.assertEquals("newDatabase2", argument.getValue());
  }

  @Test
  public void testGoHook() {
    BeeLine beeLine = setupMockData(true, true);
    ClientHook hook = ClientCommandHookFactory.get().getHook(beeLine, "go 1");
    Assert.assertTrue(hook instanceof ClientCommandHookFactory.GoCommandHook);
    hook.postHook(beeLine);
    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(beeLine).setCurrentDatabase(argument.capture());
    Assert.assertEquals("newDatabase", argument.getValue());
  }
}
