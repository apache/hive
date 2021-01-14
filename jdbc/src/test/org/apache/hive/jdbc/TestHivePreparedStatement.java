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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;

import java.sql.SQLException;

import org.apache.hive.service.rpc.thrift.TCLIService.Iface;
import org.apache.hive.service.rpc.thrift.TExecuteStatementReq;
import org.apache.hive.service.rpc.thrift.TExecuteStatementResp;
import org.apache.hive.service.rpc.thrift.TGetOperationStatusReq;
import org.apache.hive.service.rpc.thrift.TGetOperationStatusResp;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TOperationState;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.apache.hive.service.rpc.thrift.TStatus;
import org.apache.hive.service.rpc.thrift.TStatusCode;
import org.apache.hive.service.rpc.thrift.TCloseOperationResp;
import org.apache.hive.service.rpc.thrift.TCloseOperationReq;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;

public class TestHivePreparedStatement {

  @Mock
  private HiveConnection connection;
  @Mock
  private Iface client;
  @Mock
  private TSessionHandle sessHandle;
  @Mock
  private TExecuteStatementResp tExecStatementResp;
  @Mock
  private TGetOperationStatusResp tGetOperationStatusResp;
  @Mock
  private TCloseOperationResp tCloseOperationResp;
  private TStatus tStatusSuccess = new TStatus(TStatusCode.SUCCESS_STATUS);
  @Mock
  private TOperationHandle tOperationHandle;

  @Before
  public void before() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(tExecStatementResp.getStatus()).thenReturn(tStatusSuccess);
    when(tExecStatementResp.getOperationHandle()).thenReturn(tOperationHandle);

    when(tGetOperationStatusResp.getStatus()).thenReturn(tStatusSuccess);
    when(tGetOperationStatusResp.getOperationState()).thenReturn(TOperationState.FINISHED_STATE);
    when(tGetOperationStatusResp.isSetOperationState()).thenReturn(true);
    when(tGetOperationStatusResp.isSetOperationCompleted()).thenReturn(true);

    when(tCloseOperationResp.getStatus()).thenReturn(tStatusSuccess);

    when(client.GetOperationStatus(any(TGetOperationStatusReq.class)))
        .thenReturn(tGetOperationStatusResp);
    when(client.CloseOperation(any(TCloseOperationReq.class))).thenReturn(tCloseOperationResp);
    when(client.ExecuteStatement(any(TExecuteStatementReq.class))).thenReturn(tExecStatementResp);
  }

  @SuppressWarnings("resource")
  @Test
  public void testNonParameterized() throws Exception {
    String sql = "select 1";
    HivePreparedStatement ps = new HivePreparedStatement(connection, client, sessHandle, sql);
    ps.execute();

    ArgumentCaptor<TExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TExecuteStatementReq.class);
    verify(client).ExecuteStatement(argument.capture());
    assertEquals("select 1", argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void unusedArgument() throws Exception {
    String sql = "select 1";
    HivePreparedStatement ps = new HivePreparedStatement(connection, client, sessHandle, sql);
    ps.setString(1, "asd");
    ps.execute();
  }

  @SuppressWarnings("resource")
  @Test(expected = SQLException.class)
  public void unsetArgument()
      throws Exception {
    String sql = "select 1 from x where a=?";
    HivePreparedStatement ps = new HivePreparedStatement(connection, client, sessHandle, sql);
    ps.execute();
  }

  @SuppressWarnings("resource")
  @Test
  public void oneArgument() throws Exception {
    String sql = "select 1 from x where a=?";
    HivePreparedStatement ps = new HivePreparedStatement(connection, client, sessHandle, sql);
    ps.setString(1, "asd");
    ps.execute();

    ArgumentCaptor<TExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TExecuteStatementReq.class);
    verify(client).ExecuteStatement(argument.capture());
    assertEquals("select 1 from x where a='asd'", argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void escapingOfStringArgument() throws Exception {
    String sql = "select 1 from x where a=?";
    HivePreparedStatement ps = new HivePreparedStatement(connection, client, sessHandle, sql);
    ps.setString(1, "a'\"d");
    ps.execute();

    ArgumentCaptor<TExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TExecuteStatementReq.class);
    verify(client).ExecuteStatement(argument.capture());
    assertEquals("select 1 from x where a='a\\'\"d'", argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void pastingIntoQuery() throws Exception {
    String sql = "select 1 from x where a='e' || ?";
    HivePreparedStatement ps = new HivePreparedStatement(connection, client, sessHandle, sql);
    ps.setString(1, "v");
    ps.execute();

    ArgumentCaptor<TExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TExecuteStatementReq.class);
    verify(client).ExecuteStatement(argument.capture());
    assertEquals("select 1 from x where a='e' || 'v'", argument.getValue().getStatement());
  }

  // HIVE-13625
  @SuppressWarnings("resource")
  @Test
  public void pastingIntoEscapedQuery() throws Exception {
    String sql = "select 1 from x where a='\\044e' || ?";
    HivePreparedStatement ps = new HivePreparedStatement(connection, client, sessHandle, sql);
    ps.setString(1, "v");
    ps.execute();

    ArgumentCaptor<TExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TExecuteStatementReq.class);
    verify(client).ExecuteStatement(argument.capture());
    assertEquals("select 1 from x where a='\\044e' || 'v'", argument.getValue().getStatement());
  }

  @Test
  public void testSingleQuoteSetString() throws Exception {
    String sql = "select * from table where value=?";
    ArgumentCaptor<TExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TExecuteStatementReq.class);
    HivePreparedStatement ps = new HivePreparedStatement(connection, client, sessHandle, sql);

    ps.setString(1, "anyValue\\' or 1=1 --");
    ps.execute();
    verify(client).ExecuteStatement(argument.capture());
    assertEquals("select * from table where value='anyValue\\' or 1=1 --'",
        argument.getValue().getStatement());

    ps.setString(1, "anyValue\\\\' or 1=1 --");
    ps.execute();
    verify(client, times(2)).ExecuteStatement(argument.capture());
    assertEquals("select * from table where value='anyValue\\\\\\' or 1=1 --'",
        argument.getValue().getStatement());
  }

  @Test
  public void testSingleQuoteSetBinaryStream() throws Exception {
    String sql = "select * from table where value=?";
    ArgumentCaptor<TExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TExecuteStatementReq.class);
    HivePreparedStatement ps = new HivePreparedStatement(connection, client, sessHandle, sql);

    ps.setBinaryStream(1, new ByteArrayInputStream("'anyValue' or 1=1".getBytes()));
    ps.execute();
    verify(client).ExecuteStatement(argument.capture());
    assertEquals("select * from table where value='\\'anyValue\\' or 1=1'",
        argument.getValue().getStatement());

    ps.setBinaryStream(1, new ByteArrayInputStream("\\'anyValue\\' or 1=1".getBytes()));
    ps.execute();
    verify(client, times(2)).ExecuteStatement(argument.capture());
    assertEquals("select * from table where value='\\'anyValue\\' or 1=1'",
        argument.getValue().getStatement());
  }
}
