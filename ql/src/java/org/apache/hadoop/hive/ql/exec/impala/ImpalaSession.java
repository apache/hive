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
import org.apache.hadoop.hive.ql.plan.impala.ImpalaCompiledPlan;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.rpc.thrift.TCancelOperationReq;
import org.apache.hive.service.rpc.thrift.TCancelOperationResp;
import org.apache.hive.service.rpc.thrift.TCloseOperationReq;
import org.apache.hive.service.rpc.thrift.TCloseOperationResp;
import org.apache.hive.service.rpc.thrift.TCloseSessionReq;
import org.apache.hive.service.rpc.thrift.TCloseSessionResp;
import org.apache.hive.service.rpc.thrift.TExecuteStatementReq;
import org.apache.hive.service.rpc.thrift.TExecuteStatementResp;
import org.apache.hive.service.rpc.thrift.TFetchResultsReq;
import org.apache.hive.service.rpc.thrift.TFetchResultsResp;
import org.apache.hive.service.rpc.thrift.TGetOperationStatusReq;
import org.apache.hive.service.rpc.thrift.TGetOperationStatusResp;
import org.apache.hive.service.rpc.thrift.THandleIdentifier;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionResp;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TOperationState;
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
import org.apache.impala.thrift.TPingImpalaHS2ServiceReq;
import org.apache.impala.thrift.TPingImpalaHS2ServiceResp;
import org.apache.impala.thrift.TGetExecutorMembershipReq;
import org.apache.impala.thrift.TGetExecutorMembershipResp;
import org.apache.impala.thrift.TUpdateExecutorMembershipRequest;
import org.apache.thrift.transport.TTransportException;
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
    /* Shutdown request */
    private Boolean pendingCancel = new Boolean(false);
    /* Buffer size for socket stream */
    private int socketBufferSize;

    public ImpalaSession(HiveConf conf) { init(conf); }
    public void init(HiveConf conf) {
      this.address = conf.getVar(HiveConf.ConfVars.HIVE_IMPALA_ADDRESS);
      this.startSleep = conf.getIntVar(HiveConf.ConfVars.HIVE_IMPALA_RPC_START_RETRY_SLEEP);
      this.maxSleep = conf.getIntVar(HiveConf.ConfVars.HIVE_IMPALA_RPC_MAX_RETRY_SLEEP);
      this.maxRetries = conf.getIntVar(HiveConf.ConfVars.HIVE_IMPALA_RPC_RETRY_LIMIT);
      this.rowFetchSleep = conf.getIntVar(HiveConf.ConfVars.HIVE_IMPALA_ROW_FETCH_RETRY_SLEEP);
      this.rowFetchMaxRetries = conf.getIntVar(HiveConf.ConfVars.HIVE_IMPALA_ROW_FETCH_MAX_RETRY);
      this.connectionTimeout = conf.getIntVar(HiveConf.ConfVars.HIVE_IMPALA_RPC_TIMEOUT);
      this.socketBufferSize = conf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_SOCKET_BUFFER_SIZE);
    }

    /* Calculates the next sleep time for retryRPC. Uses an exponential backoff with jitter approach. */
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

    /* Used to provide an interface for RPC calls consumed by retryRPC. */
    private interface RPCCall<T> {
        T execute(ImpalaHiveServer2Service.Client c) throws TException, HiveException;
    }

    /* Used to provide an interface for RPC calls consumed by retryRPC with number of
     * times the call has been retried provided to callback */
    private interface RPCCallWithRetryCount<T> {
        T execute(ImpalaHiveServer2Service.Client c, int retryCount) throws TException, HiveException;
    }

    private <T> T retryRPC(String rpcCallName, boolean canReOpenSession, RPCCall<T> call) throws HiveException {
      return retryRPC(rpcCallName, canReOpenSession, (c, count) -> call.execute(c));
    }

    /* Retries RPC calls that fail due to TException. */
    private <T> T retryRPC(String rpcCallName, boolean canReOpenSession, RPCCallWithRetryCount<T> call) throws HiveException {
        int retryCount = 0;
        T resp = null;
        Exception lastException = null;
        do {
            boolean retryTransportError = lastException != null && lastException instanceof TTransportException;
            lastException = null;
            // retryCount > 0 on 2nd+ iteration of loop
            if (retryCount > 0) {
                int sleepTime = calculateRPCSleepMilliseconds(startSleep, maxSleep, retryCount);
                LOG.info("retryRPC({}): retry attempt: {} (max {}) sleep time: {} ms", rpcCallName, retryCount,
                        maxRetries, sleepTime);
                // Close prior session if error is not transport related
                if (!retryTransportError) {
                  if (canReOpenSession) {
                    if (connection == null) { // Reconnect if checkThriftStatus closed the connection
                      connectClient();
                    }
                    close(); // Close session, client, and connection
                  } else {
                    // Don't retry RPC that depends on session state
                    throw new HiveException(lastException);
                  }
                }
                if (RPCSleep(sleepTime)) {
                    // Sleep was interrupted, lets give up
                    throw new HiveException(String.format("Impala RPC(%s) was interrupted while sleeping",
                            rpcCallName));
                }
                if(retryTransportError) {
                  connectClient(); // Reopen connection and client
                } else if (canReOpenSession) {
                  open();     // Reopen connection, client, and session
                }
            }
            try {
                resp = call.execute(client, retryCount);
            } catch (Exception e) {
                LOG.info("retryRPC({}): retry attempt: {} (max {})", rpcCallName, retryCount, maxRetries, e);
                lastException = e;
            }
            // Loop while we've seen an TException and we haven't exceeded the requested maximum retry count
        } while (lastException != null && (maxRetries == -1 || retryCount++ < maxRetries));

        // We've exhausted our retry limit, give up and throw
        if (lastException != null) {
            throw new HiveException(String.format("Impala RPC(%s) failed after %d retries: %s", rpcCallName, maxRetries,
                    lastException.getMessage()),
                    lastException);
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
                disconnectClient();
                throw new HiveException(errmsg);
            case INVALID_HANDLE_STATUS:
                errmsg = "Invalid handle for server " + connection;
                disconnectClient();
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
            resp = retryRPC("FetchResults", false, (c) -> c.FetchResults(req));
            // Loop while checkThriftStatus says we should retry and retryCount doesnt exceed requested maxRetry
        } while (checkThriftStatus(resp.getStatus()) &&
                (rowFetchMaxRetries == -1 || retryCount++ < rowFetchMaxRetries));

        if(!resp.isHasMoreRows()) {
              fetchEOF = true;
        }

        return resp.getResults();
    }

    public void notifyShutdown() {
      synchronized (pendingCancel) {
        pendingCancel = true;
      }
    }

    public void closeOperation(TOperationHandle opHandle) throws HiveException {
        Preconditions.checkNotNull(opHandle);
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(sessionHandle);

        TCloseOperationReq req = new TCloseOperationReq();
        req.setOperationHandle(opHandle);
        TCloseOperationResp resp = retryRPC("CloseOperation", false,
                (c) -> {
                  TCloseOperationResp resp2 = c.CloseOperation(req);
                  checkThriftStatus(resp2.getStatus()); // Check errors on every iteration
                  return resp2;
                });
    }

    private long PrepareForExecution() throws HiveException {
          TPingImpalaHS2ServiceReq req = new TPingImpalaHS2ServiceReq(sessionHandle);
          long ping_send_ts = System.nanoTime();
          TPingImpalaHS2ServiceResp resp = retryRPC("PingImpalaHS2Service", true,
                (c, retryCount) -> {
                  req.setSessionHandle(sessionHandle); // Set latest session handle since Ping retry may reopen session
                  TPingImpalaHS2ServiceResp resp2 = client.PingImpalaHS2Service(req);
                  checkThriftStatus(resp2.getStatus()); // Check errors on every iteration
                  return resp2;
                });
          // Move the coordinator timestamp forward by half of the RPC latency
          // to compensate for overlap between the frontend and backend timelines.
          return resp.timestamp + (System.nanoTime() - ping_send_ts) / 2;
    }

    /* Executes an Impala plan */
    public TOperationHandle executePlan(String sql, ImpalaCompiledPlan plan) throws HiveException {
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(sessionHandle);

        TExecuteStatementReq statementRequest = new TExecuteStatementReq();
        statementRequest.setRunAsync(true);
        statementRequest.setStatement(sql);

        TExecutePlannedStatementReq req = new TExecutePlannedStatementReq();
        req.setStatementReq(statementRequest);
        req.setPlan(plan.getExecRequest());

        req.plan.setRemote_submit_time(PrepareForExecution());
        // Don't retry the Execute itself in case the statment was DML or modified state
        TExecuteStatementResp resp;
        statementRequest.setSessionHandle(sessionHandle);
        try {
          plan.getTimeline().markEvent("Submit request");
          resp = client.ExecutePlannedStatement(req);
          checkThriftStatus(resp.getStatus());
        } catch (TException e) {
          throw new HiveException(e);
        }

        fetchEOF = false;
        TOperationHandle opHandle = resp.getOperationHandle();
        TGetOperationStatusReq statusReq = new TGetOperationStatusReq(opHandle);

        while (true) {
          synchronized (pendingCancel) {
            if (pendingCancel) {
              pendingCancel = false;
              TCancelOperationReq cancelReq = new TCancelOperationReq(opHandle);
              TCancelOperationResp cancelResp;
              try {
                  cancelResp = client.CancelOperation(cancelReq);
              } catch (TException e) {
                throw new HiveException(e);
              }
              checkThriftStatus(cancelResp.getStatus());
            }
          }
          TGetOperationStatusResp statusResp = retryRPC("GetOperationStatus", false,
                  (c) -> {
                    TGetOperationStatusResp resp2 = c.GetOperationStatus(statusReq);
                    checkThriftStatus(resp2.getStatus()); // Check errors on every iteration
                    return resp2;
                  });
          if (statusResp.getOperationState() == TOperationState.FINISHED_STATE) {
            break;
          } else if (statusResp.getOperationState() == TOperationState.ERROR_STATE ||
                    statusResp.getOperationState() == TOperationState.CANCELED_STATE) {
            String errMsg = statusResp.getErrorMessage();
            if (errMsg != null && !errMsg.isEmpty()) {
              throw new HiveException("Query was cancelled: " + errMsg);
            } else {
              throw new HiveException("Query was cancelled");
            }
          }
        }

        return opHandle;
    }

    /* Executes a query string */
    public TOperationHandle execute(String sql) throws HiveException {
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(sessionHandle);

        TExecuteStatementReq req = new TExecuteStatementReq();
        req.setRunAsync(true);
        req.setStatement(sql);

        PrepareForExecution();
        // Don't retry the Execute itself in case the statment was DML or modified state
        TExecuteStatementResp resp;
        req.setSessionHandle(sessionHandle);
        try {
          resp = client.ExecuteStatement(req);
          checkThriftStatus(resp.getStatus());
        } catch (TException e) {
          throw new HiveException(e);
        }

        fetchEOF = false;
        return resp.getOperationHandle();
    }

    private void connectClient() throws HiveException {
        connection = new ImpalaConnection(socketBufferSize, address, connectionTimeout);
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

    /**
     * Retrieve the executor membership from impalad
     */
    public TUpdateExecutorMembershipRequest getExecutorMembership() throws HiveException {
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(sessionHandle);

        TGetExecutorMembershipReq req = new TGetExecutorMembershipReq();
        req.setSessionHandle(sessionHandle);
        TGetExecutorMembershipResp resp;
        try {
            resp = client.GetExecutorMembership(req);
        } catch (Exception e) {
            throw new HiveException(e);
        }

        checkThriftStatus(resp.getStatus());
        return resp.getExecutor_membership();
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

        connectClient();

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

        TOpenSessionResp resp = retryRPC("OpenSession", true,
                (c) -> {
                  TOpenSessionResp resp2 = c.OpenSession(req);
                  checkThriftStatus(resp2.getStatus()); // Check errors on every iteration
                  return resp2;
                });
        sessionHandle = resp.getSessionHandle();
        sessionConfig = resp.getConfiguration();
    }

    private void disconnectClient() {
        client = null;
        if (connection != null) {
          connection.close();
          connection = null;
        }
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
            TCloseSessionResp resp = retryRPC("CloseSession", false,
                (c, retryCount) -> {
                  TCloseSessionResp resp2 = client.CloseSession(req);
                  checkThriftStatus(resp2.getStatus());
                  return resp2;
                });
        } catch (Exception e) {
            // ignore TStatus error on close because there is nothing user actionable, but report it in log
            LOG.warn("Failed to close session ({}) to Impala coordinator ({})", getSessionId(), address,
                    e);
        }
        if (connection != null) {
            disconnectClient();
        }
    }
}
