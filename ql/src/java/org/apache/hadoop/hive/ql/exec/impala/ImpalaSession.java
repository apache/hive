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
package org.apache.hadoop.hive.ql.exec.impala;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.rpc.thrift.TCloseOperationReq;
import org.apache.hive.service.rpc.thrift.TCloseSessionReq;
import org.apache.hive.service.rpc.thrift.TCloseSessionResp;
import org.apache.hive.service.rpc.thrift.TExecuteStatementReq;
import org.apache.hive.service.rpc.thrift.TExecuteStatementResp;
import org.apache.hive.service.rpc.thrift.TFetchResultsReq;
import org.apache.hive.service.rpc.thrift.TFetchResultsResp;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionResp;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.rpc.thrift.TRowSet;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.apache.hive.service.rpc.thrift.TStatus;
import org.apache.impala.thrift.ImpalaHiveServer2Service;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TExecutePlannedStatementReq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides an interface for a user's Impala session.
 */
public class ImpalaSession {
    private static final Logger LOG = LoggerFactory.getLogger(ImpalaSession.class);

    /* Connection to Impala coordinator */
    private ImpalaConnection connection;
    /* HS2 client to Impala coordinator */
    private ImpalaHiveServer2Service.Client client;
    /* Session handle. Only valid after successful open */
    private TSessionHandle sessionHandle;
    /* Address of server this session was created for */
    private final String address;

    ImpalaSession(String address) {
        this.address = address;
    }

    private void checkThriftStatus(TStatus status) throws HiveException {
        switch (status.getStatusCode()) {
            case SUCCESS_STATUS:
                break;
            case ERROR_STATUS:
                throw new HiveException("Thrift call failed for server " + connection);
            case INVALID_HANDLE_STATUS:
                throw new HiveException("Invalid handle for server " + connection);
            case STILL_EXECUTING_STATUS:
                throw new HiveException("Still executing for server " + connection);
            case SUCCESS_WITH_INFO_STATUS:
                break;
        }
    }

    /* Given a valid TOperationHandle attempts to retrieve rows from Impala. */
    public TRowSet fetch(TOperationHandle opHandle, int fetchSize) throws HiveException {
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(sessionHandle);
        Preconditions.checkArgument(fetchSize > 0);

        TFetchResultsReq req = new TFetchResultsReq();
        req.setOperationHandle(opHandle);
        req.setMaxRows(fetchSize);

        TFetchResultsResp resp;
        try {
            resp = client.FetchResults(req);
        } catch (Exception e) {
            throw new HiveException(e);
        }
        // CDPD-6981: Differentiate non-fatal errors from fatal errors for fetch (no more rows vs connection dropped)
        // CheckThriftStatus(resp.getStatus());
        return resp.getResults();
    }

    public void closeOperation(TOperationHandle opHandle) throws HiveException {
        TCloseOperationReq req = new TCloseOperationReq();
        req.setOperationHandle(opHandle);
        try {
            client.CloseOperation(req);
        } catch (Exception e) {
            throw new HiveException(e);
        }
    }

    /* Executes an Impala plan */
    public TOperationHandle executePlan(String sql, TExecRequest execRequest) throws HiveException {
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(sessionHandle);

        TExecuteStatementReq statementRequest = new TExecuteStatementReq();
        statementRequest.setSessionHandle(sessionHandle);
        statementRequest.setRunAsync(true);
        statementRequest.setStatement(sql);

        TExecutePlannedStatementReq req2 = new TExecutePlannedStatementReq();
        req2.setPlan(execRequest);
        req2.setStatementReq(statementRequest);
        TExecuteStatementResp resp;
        try {
            resp = client.ExecutePlannedStatement(req2);
        } catch (Exception e) {
            throw new HiveException(e);
        }

        checkThriftStatus(resp.getStatus());
        return resp.getOperationHandle();
    }

    /* Executes a query string */
    public TOperationHandle execute(String sql) throws HiveException {
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(sessionHandle);

        TExecuteStatementReq req = new TExecuteStatementReq();
        req.setSessionHandle(sessionHandle);
        req.setRunAsync(true);
        req.setStatement(sql);
        TExecuteStatementResp resp;
        try {
            resp = client.ExecuteStatement(req);
        } catch (Exception e) {
            throw new HiveException(e);
        }

        checkThriftStatus(resp.getStatus());
        return resp.getOperationHandle();
    }

    /* Opens an Impala session */
    public void open() throws HiveException {
        // we've already called open
        if (client != null) {
            return;
        }

        connection = new ImpalaConnection(address);
        client = connection.getClient();

        TOpenSessionReq req = new TOpenSessionReq();
        req.setUsername(SessionState.get().getUserName());
        // CDPD-6958: Investigate columnar vs row oriented result sets from Impala
        // This is to force Impala to send back row oriented data (V6 and above returns columnar
        req.setClient_protocol(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V5);

        TOpenSessionResp resp;
        try {
            resp = client.OpenSession(req);
        } catch (Exception e) {
            throw new HiveException(e);
        }

        checkThriftStatus(resp.getStatus());
        sessionHandle = resp.getSessionHandle();
    }

    /* Closes an Impala session */
    public void close() {
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(sessionHandle);

        TCloseSessionReq req = new TCloseSessionReq();
        req.setSessionHandle(sessionHandle);
        try {
            TCloseSessionResp resp = client.CloseSession(req);
            checkThriftStatus(resp.getStatus());
        } catch (Exception e) {
            // ignore errors on close, but report it in log
            LOG.warn("Failed to close session (" + sessionHandle.getSessionId()
                    + ") to Impala coordinator (" + address + ")", e);
        }

        if (connection != null) {
            connection.close();
        }
    }
}
