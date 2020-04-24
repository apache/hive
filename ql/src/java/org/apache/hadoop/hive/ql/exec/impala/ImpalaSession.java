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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.rpc.thrift.TCloseOperationReq;
import org.apache.hive.service.rpc.thrift.TCloseOperationResp;
import org.apache.hive.service.rpc.thrift.TCloseSessionReq;
import org.apache.hive.service.rpc.thrift.TCloseSessionResp;
import org.apache.hive.service.rpc.thrift.TExecuteStatementReq;
import org.apache.hive.service.rpc.thrift.TExecuteStatementResp;
import org.apache.hive.service.rpc.thrift.TFetchResultsReq;
import org.apache.hive.service.rpc.thrift.TFetchResultsResp;
import org.apache.hive.service.rpc.thrift.THandleIdentifier;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionResp;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.rpc.thrift.TRowSet;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.apache.hive.service.rpc.thrift.TStatus;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TGetBackendConfigReq;
import org.apache.impala.thrift.TGetBackendConfigResp;
import org.apache.impala.thrift.ImpalaHiveServer2Service;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TExecutePlannedStatementReq;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;
import java.util.Map;
import java.util.Random;

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
    /* Query options */
    private Map<String,String> sessionConfig;
    /* Address of server this session was created for */
    private String address;
    /* Generates random numbers for RPC retry sleeps */
    private final Random randGen = new Random();
    /* Initial starting point for sleeps (in milliseconds) */
    private int startSleep;
    /* Maximum sleep time (in milliseconds) */
    private int maxSleep;
    /* Max retries */
    private int maxRetries;
    /* Sleep between row fetch status checks */
    private int rowFetchSleep;
    /* Max retries for row fetching */
    private int rowFetchMaxRetries;
    /* Underlying configured TSocket timeout */
    private int connectionTimeout;
    /* Fetch EOF status */
    private boolean fetchEOF = false;


    public ImpalaSession(HiveConf conf) { init(conf); }
    public void init(HiveConf conf) {
        init(conf.getVar(HiveConf.ConfVars.HIVE_IMPALA_ADDRESS),
                conf.getIntVar(HiveConf.ConfVars.HIVE_IMPALA_RPC_START_RETRY_SLEEP),
                conf.getIntVar(HiveConf.ConfVars.HIVE_IMPALA_RPC_MAX_RETRY_SLEEP),
                conf.getIntVar(HiveConf.ConfVars.HIVE_IMPALA_RPC_RETRY_LIMIT),
                conf.getIntVar(HiveConf.ConfVars.HIVE_IMPALA_ROW_FETCH_RETRY_SLEEP),
                conf.getIntVar(HiveConf.ConfVars.HIVE_IMPALA_ROW_FETCH_MAX_RETRY),
                conf.getIntVar(HiveConf.ConfVars.HIVE_IMPALA_RPC_TIMEOUT));
    }

    void init(String address, int startSleep, int maxSleep, int maxRetries, int rowFetchSleep,
                  int rowFetchMaxRetries, int connectionTimeout) {
        this.address = address;
        this.startSleep = startSleep;
        this.maxSleep = maxSleep;
        this.maxRetries = maxRetries;
        this.rowFetchSleep = rowFetchSleep;
        this.rowFetchMaxRetries = rowFetchMaxRetries;
        this.connectionTimeout = connectionTimeout;
    }

    /* Calculates the next sleep time for RetryRPC. Uses an exponential backoff with jitter approach. */
    private int calculateRPCSleepMilliseconds(int startSleep, int maxSleep, int attempt) {
        LOG.debug("calculateRPCSleepMilliseconds: start: {} max: {} attempt: {}", startSleep, maxSleep, attempt);
        // limit exponent to be 30 (2^30)
        int exponent = Math.min(30, attempt);
        int expPower = (int) Math.pow(2, exponent);
        int calculatedMax = 0;
        try {
            calculatedMax = Math.multiplyExact(startSleep, expPower);
            calculatedMax = Math.min(maxSleep, calculatedMax);
        } catch (ArithmeticException e) {
            // we've overflowed the multiply, so lets use the max sleep
            calculatedMax = maxSleep;
        }
        LOG.debug("calculatedMax: {}", calculatedMax);
        // return between 0 and calculatedMax for jitter
        return randGen.nextInt(calculatedMax);
    }

    /* Sleeps for the specified sleepTimeMs (milliseconds) returns true if interrupted. */
    private static boolean RPCSleep(int sleepTimeMs) {
        Preconditions.checkState(sleepTimeMs >= 0);
        LOG.debug("RPCSleep: Sleeping for {}ms", sleepTimeMs);
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return true;
        }
        return false;
    }

    /* Used to provide an interface for RPC calls consumed by RetryRPC. */
    private interface RPCCall<T> {
        T execute(ImpalaHiveServer2Service.Client c) throws TException;
    }

    /* Retries RPC calls that fail due to TException. */
    private <T> T RetryRPC(String rpcCallName, RPCCall<T> call) throws HiveException {
        int retryCount = 0;
        T resp = null;
        TException lastTException;
        do {
            lastTException = null;
            // retryCount > 0 on 2nd+ iteration of loop
            if (retryCount > 0) {
                int sleepTime = calculateRPCSleepMilliseconds(startSleep, maxSleep, retryCount);
                LOG.info("RetryRPC({}): retry attempt: {} (max {}) sleep time: {} ms", rpcCallName, retryCount,
                        maxRetries, sleepTime);
                // Close previously used connection to free up resources before sleep
                closeImpl();
                if (RPCSleep(sleepTime)) {
                    // Sleep was interrupted, lets give up
                    throw new HiveException(String.format("Impala RPC(%s) was interrupted while sleeping",
                            rpcCallName));
                }
                // Reopen connection and client
                openImpl();
            }
            try {
                resp = call.execute(client);
            } catch (TException e) {
                LOG.info("RetryRPC({}): retry attempt: {} (max {})", rpcCallName, retryCount, maxRetries, e);
                lastTException = e;
            }
            // Loop while we've seen an TException and we haven't exceeded the requested maximum retry count
        } while (lastTException != null && (maxRetries == -1 || retryCount++ < maxRetries));

        // We've exhausted our retry limit, give up and throw
        if (lastTException != null) {
            throw new HiveException(String.format("Impala RPC(%s) failed after %d retries: %s", rpcCallName, maxRetries,
                    lastTException.getMessage()),
                    lastTException);
        }
        return resp;
    }

    /* Checks TStatus status code, returns false if status is success, true if status suggests retrying, or throws
     * an HiveException if an error is encountered.
     */
    private boolean checkThriftStatus(TStatus status) throws HiveException {
        String errmsg;
        switch (status.getStatusCode()) {
            case SUCCESS_STATUS:
            case SUCCESS_WITH_INFO_STATUS:
                return false;
            case ERROR_STATUS:
                errmsg = String.format(
                    "Thrift call failed for server %s error: %s",
                    connection, status.getErrorMessage());
                closeImpl();
                throw new HiveException(errmsg);
            case INVALID_HANDLE_STATUS:
                errmsg = "Invalid handle for server " + connection;
                closeImpl();
                throw new HiveException(errmsg);
            case STILL_EXECUTING_STATUS:
                return true;
        }
        return false;
    }

    /* Given a valid TOperationHandle attempts to retrieve rows from Impala. */
    public TRowSet fetch(TOperationHandle opHandle, long fetchSize) throws HiveException {
        if(fetchEOF) {
            return null;
        }

        Preconditions.checkNotNull(opHandle);
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(sessionHandle);
        Preconditions.checkArgument(fetchSize > 0);

        TFetchResultsReq req = new TFetchResultsReq();
        req.setOperationHandle(opHandle);
        req.setMaxRows(fetchSize);

        TFetchResultsResp resp = null;
        int retryCount = 0;
        do {
            // only log every 30 retries, it is expected for long running queries for this to be retried many times
            if ((retryCount%30) == 0) {
                LOG.info("FetchResults: attempt: {} (max {}) retrySleep: {}ms", retryCount, rowFetchMaxRetries,
                        rowFetchSleep);
            }
            // After 1 loop resp is always non-null
            if (resp != null && RPCSleep(rowFetchSleep)) {
                throw new HiveException("FetchResults: Retry sleep was interrupted");
            }
            resp = RetryRPC("FetchResults", (c) -> c.FetchResults(req));
            // Loop while checkThriftStatus says we should retry and retryCount doesnt exceed requested maxRetry
        } while (checkThriftStatus(resp.getStatus()) &&
                (rowFetchMaxRetries == -1 || retryCount++ < rowFetchMaxRetries));

        if(!resp.isHasMoreRows()) {
              fetchEOF = true;
        }

        return resp.getResults();
    }

    public void closeOperation(TOperationHandle opHandle) throws HiveException {
        Preconditions.checkNotNull(opHandle);
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(sessionHandle);

        TCloseOperationReq req = new TCloseOperationReq();
        req.setOperationHandle(opHandle);
        TCloseOperationResp resp = RetryRPC("CloseOperation", (c) ->  c.CloseOperation(req));
        checkThriftStatus(resp.getStatus());
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
        TExecuteStatementResp resp = RetryRPC("ExecutePlannedStatement",
                (c) -> c.ExecutePlannedStatement(req2));
        checkThriftStatus(resp.getStatus());
        fetchEOF = false;
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
        TExecuteStatementResp resp = RetryRPC("ExecuteStatement", (c) -> c.ExecuteStatement(req));
        checkThriftStatus(resp.getStatus());
        return resp.getOperationHandle();
    }

    private void openImpl() throws HiveException {
        connection = new ImpalaConnection(address, connectionTimeout);
        client = connection.getClient();
    }
    /* Retrieve BackendConfig from impalad */
    public TBackendGflags getBackendConfig() throws HiveException {
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(sessionHandle);

        TGetBackendConfigReq req = new TGetBackendConfigReq();
        req.setSessionHandle(sessionHandle);
        TGetBackendConfigResp resp;
        try {
            resp = client.GetBackendConfig(req);
        } catch (Exception e) {
            throw new HiveException(e);
        }

        checkThriftStatus(resp.getStatus());
        return resp.getBackend_config();
    }

    public boolean isOpen() {
        return client != null;
    }
    /* Opens an Impala session */
    public void open() throws HiveException {
        // we've already called open
        if (client != null) {
            return;
        }

        openImpl();

        TOpenSessionReq req = new TOpenSessionReq();
        req.setUsername(SessionState.get().getUserName());

        // Copy Impala conf variables to Open request
        req.setConfiguration(
             SessionState.get().getConf().subtree("impala").entrySet().stream()
             .filter(e -> !e.getKey().equals("core-site.overridden"))
             .collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue)));

        // CDPD-6958: Investigate columnar vs row oriented result sets from Impala
        // This is to force Impala to send back row oriented data (V6 and above returns columnar
        req.setClient_protocol(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V5);

        TOpenSessionResp resp = RetryRPC("OpenSession", (c) -> c.OpenSession(req));
        checkThriftStatus(resp.getStatus());
        sessionHandle = resp.getSessionHandle();
        sessionConfig = resp.getConfiguration();
    }

    private void closeImpl() {
        client = null;
        connection.close();
        connection = null;
    }

    public THandleIdentifier getSessionId() {
        return sessionHandle.getSessionId();
    }
    public Map<String,String> getSessionConfig() {
        return sessionConfig;
    }

    /* Closes an Impala session */
    public void close() {
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(sessionHandle);

        TCloseSessionReq req = new TCloseSessionReq();
        req.setSessionHandle(sessionHandle);
        try {
            // we retry CloseSession to cleanup resources on the Impala side
            TCloseSessionResp resp = RetryRPC("CloseSession", (c) -> c.CloseSession(req));
            checkThriftStatus(resp.getStatus());
        } catch (Exception e) {
            // ignore TStatus error on close because there is nothing user actionable, but report it in log
            LOG.warn("Failed to close session ({}) to Impala coordinator ({})", getSessionId(), address,
                    e);
        }
        if (connection != null) {
            closeImpl();
        }
    }
}
