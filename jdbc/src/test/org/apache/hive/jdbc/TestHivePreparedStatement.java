package org.apache.hive.jdbc;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;

import org.apache.hive.service.cli.thrift.TCLIService.Iface;
import org.apache.hive.service.cli.thrift.TExecuteStatementReq;
import org.apache.hive.service.cli.thrift.TExecuteStatementResp;
import org.apache.hive.service.cli.thrift.TGetOperationStatusReq;
import org.apache.hive.service.cli.thrift.TGetOperationStatusResp;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.hive.service.cli.thrift.TOperationState;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.hive.service.cli.thrift.TStatus;
import org.apache.hive.service.cli.thrift.TStatusCode;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TestHivePreparedStatement {

	@Mock
	private HiveConnection connection;
	@Mock
	private Iface client;
	@Mock
	private TSessionHandle sessHandle;
	@Mock
	TExecuteStatementResp tExecStatementResp;
	@Mock
	TGetOperationStatusResp tGetOperationStatusResp;
	private TStatus tStatus_SUCCESS = new TStatus(TStatusCode.SUCCESS_STATUS);
	@Mock
	private TOperationHandle tOperationHandle;

	@Before
	public void before() throws Exception {
		MockitoAnnotations.initMocks(this);
		when(tExecStatementResp.getStatus()).thenReturn(tStatus_SUCCESS);
		when(tExecStatementResp.getOperationHandle()).thenReturn(tOperationHandle);

		when(tGetOperationStatusResp.getStatus()).thenReturn(tStatus_SUCCESS);
		when(tGetOperationStatusResp.getOperationState()).thenReturn(TOperationState.FINISHED_STATE);
		when(tGetOperationStatusResp.isSetOperationState()).thenReturn(true);
		when(tGetOperationStatusResp.isSetOperationCompleted()).thenReturn(true);

		when(client.GetOperationStatus(any(TGetOperationStatusReq.class))).thenReturn(tGetOperationStatusResp);
		when(client.ExecuteStatement(any(TExecuteStatementReq.class))).thenReturn(tExecStatementResp);
	}

	@SuppressWarnings("resource")
	@Test
	public void testNonParameterized() throws Exception {
		String sql = "select 1";
		HivePreparedStatement ps = new HivePreparedStatement(connection, client, sessHandle, sql);
		ps.execute();

		ArgumentCaptor<TExecuteStatementReq> argument = ArgumentCaptor.forClass(TExecuteStatementReq.class);
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
	@Test(expected=SQLException.class)
	public void unsetArgument() throws Exception {
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
		
		ArgumentCaptor<TExecuteStatementReq> argument = ArgumentCaptor.forClass(TExecuteStatementReq.class);
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
		
		ArgumentCaptor<TExecuteStatementReq> argument = ArgumentCaptor.forClass(TExecuteStatementReq.class);
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
		
		ArgumentCaptor<TExecuteStatementReq> argument = ArgumentCaptor.forClass(TExecuteStatementReq.class);
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
		
		ArgumentCaptor<TExecuteStatementReq> argument = ArgumentCaptor.forClass(TExecuteStatementReq.class);
		verify(client).ExecuteStatement(argument.capture());
		assertEquals("select 1 from x where a='\\044e' || 'v'", argument.getValue().getStatement());
	}
}
