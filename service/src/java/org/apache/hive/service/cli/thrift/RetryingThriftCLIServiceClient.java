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

package org.apache.hive.service.cli.thrift;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.SocketException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.security.sasl.SaslException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.FetchType;
import org.apache.hive.service.cli.GetInfoType;
import org.apache.hive.service.cli.GetInfoValue;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.ICLIService;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RetryingThriftCLIServiceClient. Creates a proxy for a CLIServiceClient
 * implementation and retries calls to it on failure.
 */
public class RetryingThriftCLIServiceClient implements InvocationHandler {
  public static final Logger LOG = LoggerFactory.getLogger(RetryingThriftCLIServiceClient.class);
  private ThriftCLIServiceClient base;
  private final int retryLimit;
  private final int retryDelaySeconds;
  private HiveConf conf;
  private TTransport transport;

  public static class CLIServiceClientWrapper extends CLIServiceClient {
    private final ICLIService cliService;
    private TTransport tTransport;

    public CLIServiceClientWrapper(ICLIService icliService, TTransport tTransport, HiveConf conf) {
      super(conf);
      cliService = icliService;
      this.tTransport = tTransport;
    }

    @Override
    public SessionHandle openSession(String username, String password) throws HiveSQLException {
      return cliService.openSession(username, password, Collections.<String, String>emptyMap());
    }

    @Override
    public String getDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory, String owner,
                                     String renewer) throws HiveSQLException {
      return cliService.getDelegationToken(sessionHandle, authFactory, owner, renewer);
    }

    @Override
    public void cancelDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
                                      String tokenStr) throws HiveSQLException {
      cliService.cancelDelegationToken(sessionHandle, authFactory, tokenStr);
    }

    @Override
    public void renewDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
                                     String tokenStr) throws HiveSQLException {
      cliService.renewDelegationToken(sessionHandle, authFactory, tokenStr);
    }

    @Override
    public SessionHandle openSession(String username, String password, Map<String, String> configuration)
      throws HiveSQLException {
      return cliService.openSession(username, password, configuration);
    }

    @Override
    public SessionHandle openSessionWithImpersonation(String username,
                                                      String password,
                                                      Map<String, String> configuration,
                                                      String delegationToken) throws HiveSQLException {
      return cliService.openSessionWithImpersonation(username, password, configuration, delegationToken);
    }

    @Override
    public void closeSession(SessionHandle sessionHandle) throws HiveSQLException {
      cliService.closeSession(sessionHandle);
    }

    @Override
    public GetInfoValue getInfo(SessionHandle sessionHandle, GetInfoType getInfoType) throws HiveSQLException {
      return cliService.getInfo(sessionHandle, getInfoType);
    }

    @Override
    public OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
        Map<String, String> confOverlay) throws HiveSQLException {
      return cliService.executeStatement(sessionHandle, statement, confOverlay);
    }

    @Override
    public OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
        Map<String, String> confOverlay, long queryTimeout) throws HiveSQLException {
      return cliService.executeStatement(sessionHandle, statement, confOverlay, queryTimeout);
    }

    @Override
    public OperationHandle executeStatementAsync(SessionHandle sessionHandle, String statement,
        Map<String, String> confOverlay) throws HiveSQLException {
      return cliService.executeStatementAsync(sessionHandle, statement, confOverlay);
    }

    @Override
    public OperationHandle executeStatementAsync(SessionHandle sessionHandle, String statement,
        Map<String, String> confOverlay, long queryTimeout) throws HiveSQLException {
      return cliService.executeStatementAsync(sessionHandle, statement, confOverlay, queryTimeout);
    }

    @Override
    public OperationHandle getTypeInfo(SessionHandle sessionHandle) throws HiveSQLException {
      return cliService.getTypeInfo(sessionHandle);
    }

    @Override
    public OperationHandle getCatalogs(SessionHandle sessionHandle) throws HiveSQLException {
      return cliService.getCatalogs(sessionHandle);
    }

    @Override
    public OperationHandle getSchemas(SessionHandle sessionHandle, String catalogName, String schemaName)
      throws HiveSQLException {
      return cliService.getSchemas(sessionHandle, catalogName, schemaName);
    }

    @Override
    public OperationHandle getTables(SessionHandle sessionHandle, String catalogName, String schemaName,
                                     String tableName, List<String> tableTypes) throws HiveSQLException {
      return cliService.getTables(sessionHandle, catalogName, schemaName, tableName, tableTypes);
    }

    @Override
    public OperationHandle getTableTypes(SessionHandle sessionHandle) throws HiveSQLException {
      return null;
    }

    @Override
    public OperationHandle getColumns(SessionHandle sessionHandle, String catalogName, String schemaName,
                                      String tableName, String columnName) throws HiveSQLException {
      return cliService.getColumns(sessionHandle, catalogName, schemaName, tableName, columnName);
    }

    @Override
    public OperationHandle getFunctions(SessionHandle sessionHandle, String catalogName, String schemaName,
                                        String functionName) throws HiveSQLException {
      return cliService.getFunctions(sessionHandle, catalogName, schemaName, functionName);
    }

    @Override
    public OperationStatus getOperationStatus(OperationHandle opHandle, boolean getProgressUpdate) throws HiveSQLException {
      return cliService.getOperationStatus(opHandle, getProgressUpdate);
    }

    @Override
    public String getQueryId(TOperationHandle operationHandle) throws HiveSQLException {
      return cliService.getQueryId(operationHandle);
    }

    @Override
    public void cancelOperation(OperationHandle opHandle) throws HiveSQLException {
      cliService.cancelOperation(opHandle);
    }

    @Override
    public void closeOperation(OperationHandle opHandle) throws HiveSQLException {
      cliService.closeOperation(opHandle);
    }

    @Override
    public TableSchema getResultSetMetadata(OperationHandle opHandle) throws HiveSQLException {
      return cliService.getResultSetMetadata(opHandle);
    }

    @Override
    public RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation, long maxRows,
                               FetchType fetchType) throws HiveSQLException {
      return cliService.fetchResults(opHandle, orientation, maxRows, fetchType);
    }

    public void closeTransport() {
      tTransport.close();
    }

    @Override
    public OperationHandle getPrimaryKeys(SessionHandle sessionHandle,
      String catalog, String schema, String table)
      throws HiveSQLException {
      return cliService.getPrimaryKeys(sessionHandle, catalog, schema, table);
    }

    @Override
    public OperationHandle getCrossReference(SessionHandle sessionHandle,
      String primaryCatalog, String primarySchema, String primaryTable,
      String foreignCatalog, String foreignSchema, String foreignTable)
      throws HiveSQLException {
      return cliService.getCrossReference(sessionHandle, primaryCatalog, primarySchema,
        primaryTable, foreignCatalog, foreignSchema, foreignTable);
    }

    @Override
    public void setApplicationName(SessionHandle sh, String value) throws HiveSQLException {
      cliService.setApplicationName(sh, value);
    }
  }

  protected RetryingThriftCLIServiceClient(HiveConf conf) {
    this.conf = conf;
    retryLimit = conf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_RETRY_LIMIT);
    retryDelaySeconds = (int) conf.getTimeVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_RETRY_DELAY_SECONDS,
      TimeUnit.SECONDS);
  }

  public static CLIServiceClientWrapper newRetryingCLIServiceClient(HiveConf conf) throws HiveSQLException {
    RetryingThriftCLIServiceClient retryClient = new RetryingThriftCLIServiceClient(conf);
    TTransport tTransport = retryClient
      .connectWithRetry(conf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_CONNECTION_RETRY_LIMIT));
    ICLIService cliService =
      (ICLIService) Proxy.newProxyInstance(RetryingThriftCLIServiceClient.class.getClassLoader(),
        CLIServiceClient.class.getInterfaces(), retryClient);
    return new CLIServiceClientWrapper(cliService, tTransport, conf);
  }

  protected TTransport connectWithRetry(int retries) throws HiveSQLException {
    TTransportException exception = null;
    for (int i = 0 ; i < retries; i++) {
      try {
        return connect(conf);
      } catch (TTransportException e) {
        exception = e;
        LOG.warn("Connection attempt " + i, e);
      }
      try {
        Thread.sleep(retryDelaySeconds * 1000);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted", e);
      }
    }
    throw new HiveSQLException("Unable to connect after " + retries + " retries", exception);
  }

  protected synchronized TTransport connect(HiveConf conf) throws HiveSQLException, TTransportException {
    if (transport != null && transport.isOpen()) {
      transport.close();
    }

    String host = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST);
    int port = conf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT);
    LOG.info("Connecting to " + host + ":" + port);

    transport = new TSocket(host, port);
    ((TSocket) transport).setTimeout((int) conf.getTimeVar(HiveConf.ConfVars.SERVER_READ_SOCKET_TIMEOUT,
      TimeUnit.SECONDS) * 1000);
    try {
      ((TSocket) transport).getSocket().setKeepAlive(conf.getBoolVar(HiveConf.ConfVars.SERVER_TCP_KEEP_ALIVE));
    } catch (SocketException e) {
      LOG.error("Error setting keep alive to " + conf.getBoolVar(HiveConf.ConfVars.SERVER_TCP_KEEP_ALIVE), e);
    }

    String userName = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_USER);
    String passwd = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_PASSWORD);

    try {
      transport = PlainSaslHelper.getPlainTransport(userName, passwd, transport);
    } catch (SaslException e) {
      LOG.error("Error creating plain SASL transport", e);
    }

    TProtocol protocol = new TBinaryProtocol(transport);
    transport.open();
    base = new ThriftCLIServiceClient(new TCLIService.Client(protocol), conf);
    LOG.info("Connected!");
    return transport;
  }

  protected class InvocationResult {
    final boolean success;
    final Object result;
    final Throwable exception;

    InvocationResult(boolean success, Object result, Throwable exception) {
      this.success = success;
      this.result = result;
      this.exception = exception;
    }
  }

  protected InvocationResult invokeInternal(Method method, Object[] args) throws Throwable {
    InvocationResult result;
    try {
      Object methodResult = method.invoke(base, args);
      result = new InvocationResult(true, methodResult, null);
    } catch (UndeclaredThrowableException e) {
      throw e.getCause();
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof HiveSQLException) {
        HiveSQLException hiveExc = (HiveSQLException) e.getCause();
        Throwable cause = hiveExc.getCause();
        if ((cause instanceof TApplicationException) ||
          (cause instanceof TProtocolException) ||
          (cause instanceof TTransportException)) {
          result =  new InvocationResult(false, null, hiveExc);
        } else {
          throw hiveExc;
        }
      } else {
        throw e.getCause();
      }
    }
    return result;
  }

  @Override
  public Object invoke(Object o, Method method, Object[] args) throws Throwable {
    int attempts = 0;

    while (true) {
      attempts++;
      InvocationResult invokeResult = invokeInternal(method, args);
      if (invokeResult.success) {
        return invokeResult.result;
      }

      // Error because of thrift client, we have to recreate base object
      connectWithRetry(conf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_CONNECTION_RETRY_LIMIT));

      if (attempts >=  retryLimit) {
        LOG.error(method.getName() + " failed after " + attempts + " retries.",  invokeResult.exception);
        throw invokeResult.exception;
      }

      LOG.warn("Last call ThriftCLIServiceClient." + method.getName() + " failed, attempts = " + attempts,
        invokeResult.exception);
      Thread.sleep(retryDelaySeconds * 1000);
    }
  }

  public int getRetryLimit() {
    return retryLimit;
  }

  public int getRetryDelaySeconds() {
    return retryDelaySeconds;
  }
}
