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

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.hive.service.rpc.thrift.TSetClientInfoReq;
import org.apache.hive.service.rpc.thrift.TSetClientInfoResp;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.security.auth.login.LoginException;
import org.apache.hadoop.hive.common.ServerUtils;
import org.apache.hadoop.hive.common.log.ProgressMonitor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.HadoopShims.KerberosNameShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.service.AbstractService;
import org.apache.hive.service.ServiceException;
import org.apache.hive.service.ServiceUtils;
import org.apache.hive.service.auth.HiveAuthConstants;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.auth.TSetIpAddressProcessor;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.FetchType;
import org.apache.hive.service.cli.GetInfoType;
import org.apache.hive.service.cli.GetInfoValue;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.JobProgressUpdate;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.ProgressMonitorStatusMapper;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.TezProgressMonitorStatusMapper;
import org.apache.hive.service.cli.operation.Operation;
import org.apache.hive.service.cli.session.SessionManager;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TCancelDelegationTokenReq;
import org.apache.hive.service.rpc.thrift.TCancelDelegationTokenResp;
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
import org.apache.hive.service.rpc.thrift.TGetCatalogsReq;
import org.apache.hive.service.rpc.thrift.TGetCatalogsResp;
import org.apache.hive.service.rpc.thrift.TGetColumnsReq;
import org.apache.hive.service.rpc.thrift.TGetColumnsResp;
import org.apache.hive.service.rpc.thrift.TGetCrossReferenceReq;
import org.apache.hive.service.rpc.thrift.TGetCrossReferenceResp;
import org.apache.hive.service.rpc.thrift.TGetDelegationTokenReq;
import org.apache.hive.service.rpc.thrift.TGetDelegationTokenResp;
import org.apache.hive.service.rpc.thrift.TGetFunctionsReq;
import org.apache.hive.service.rpc.thrift.TGetFunctionsResp;
import org.apache.hive.service.rpc.thrift.TGetInfoReq;
import org.apache.hive.service.rpc.thrift.TGetInfoResp;
import org.apache.hive.service.rpc.thrift.TGetOperationStatusReq;
import org.apache.hive.service.rpc.thrift.TGetOperationStatusResp;
import org.apache.hive.service.rpc.thrift.TGetPrimaryKeysReq;
import org.apache.hive.service.rpc.thrift.TGetPrimaryKeysResp;
import org.apache.hive.service.rpc.thrift.TGetQueryIdReq;
import org.apache.hive.service.rpc.thrift.TGetQueryIdResp;
import org.apache.hive.service.rpc.thrift.TGetResultSetMetadataReq;
import org.apache.hive.service.rpc.thrift.TGetResultSetMetadataResp;
import org.apache.hive.service.rpc.thrift.TGetSchemasReq;
import org.apache.hive.service.rpc.thrift.TGetSchemasResp;
import org.apache.hive.service.rpc.thrift.TGetTableTypesReq;
import org.apache.hive.service.rpc.thrift.TGetTableTypesResp;
import org.apache.hive.service.rpc.thrift.TGetTablesReq;
import org.apache.hive.service.rpc.thrift.TGetTablesResp;
import org.apache.hive.service.rpc.thrift.TGetTypeInfoReq;
import org.apache.hive.service.rpc.thrift.TGetTypeInfoResp;
import org.apache.hive.service.rpc.thrift.TJobExecutionStatus;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionResp;
import org.apache.hive.service.rpc.thrift.TProgressUpdateResp;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.rpc.thrift.TRenewDelegationTokenReq;
import org.apache.hive.service.rpc.thrift.TRenewDelegationTokenResp;
import org.apache.hive.service.rpc.thrift.TStatus;
import org.apache.hive.service.rpc.thrift.TStatusCode;
import org.apache.hive.service.server.HiveServer2;
import org.apache.thrift.TException;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ThriftCLIService.
 *
 */
public abstract class ThriftCLIService extends AbstractService implements TCLIService.Iface, Runnable {

  public static final Logger LOG = LoggerFactory.getLogger(ThriftCLIService.class.getName());

  protected CLIService cliService;
  private static final TStatus OK_STATUS = new TStatus(TStatusCode.SUCCESS_STATUS);
  protected static HiveAuthFactory hiveAuthFactory;

  protected int portNum;
  protected InetAddress serverIPAddress;
  protected String hiveHost;
  private boolean isStarted = false;
  protected boolean isEmbedded = false;

  protected HiveConf hiveConf;

  protected int minWorkerThreads;
  protected int maxWorkerThreads;
  protected long workerKeepAliveTime;
  private Thread serverThread;

  protected ThreadLocal<ServerContext> currentServerContext;

  static class ThriftCLIServerContext implements ServerContext {
    private SessionHandle sessionHandle = null;

    public void setSessionHandle(SessionHandle sessionHandle) {
      this.sessionHandle = sessionHandle;
    }

    public SessionHandle getSessionHandle() {
      return sessionHandle;
    }
  }

  public ThriftCLIService(CLIService service, String serviceName) {
    super(serviceName);
    this.cliService = service;
    currentServerContext = new ThreadLocal<ServerContext>();
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    this.hiveConf = hiveConf;

    String hiveHost = System.getenv("HIVE_SERVER2_THRIFT_BIND_HOST");
    if (hiveHost == null) {
      hiveHost = hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST);
    }
    try {
      serverIPAddress = ServerUtils.getHostAddress(hiveHost);
    } catch (UnknownHostException e) {
      throw new ServiceException(e);
    }

    // Initialize common server configs needed in both binary & http modes
    String portString;
    // HTTP mode
    if (HiveServer2.isHTTPTransportMode(hiveConf)) {
      workerKeepAliveTime =
          hiveConf.getTimeVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_WORKER_KEEPALIVE_TIME,
              TimeUnit.SECONDS);
      portString = System.getenv("HIVE_SERVER2_THRIFT_HTTP_PORT");
      if (portString != null) {
        portNum = Integer.parseInt(portString);
      } else {
        portNum = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT);
      }
    }
    // Binary mode
    else {
      workerKeepAliveTime =
          hiveConf.getTimeVar(ConfVars.HIVE_SERVER2_THRIFT_WORKER_KEEPALIVE_TIME, TimeUnit.SECONDS);
      portString = System.getenv("HIVE_SERVER2_THRIFT_PORT");
      if (portString != null) {
        portNum = Integer.parseInt(portString);
      } else {
        portNum = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT);
      }
    }
    minWorkerThreads = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_MIN_WORKER_THREADS);
    maxWorkerThreads = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_MAX_WORKER_THREADS);
    super.init(hiveConf);
  }

  protected abstract void initServer();

  @Override
  public synchronized void start() {
    super.start();
    if (!isStarted && !isEmbedded) {
      initServer();
      serverThread = new Thread(this);
      serverThread.setName("Thrift Server");
      serverThread.start();
      isStarted = true;
    }
  }

  protected abstract void stopServer();

  @Override
  public synchronized void stop() {
    if (isStarted && !isEmbedded) {
      if (serverThread != null) {
        serverThread.interrupt();
        serverThread = null;
      }
      stopServer();
      isStarted = false;
    }
    super.stop();
  }

  public int getPortNumber() {
    return portNum;
  }

  public InetAddress getServerIPAddress() {
    return serverIPAddress;
  }

  @Override
  public TGetDelegationTokenResp GetDelegationToken(TGetDelegationTokenReq req)
      throws TException {
    TGetDelegationTokenResp resp = new TGetDelegationTokenResp();

    if (hiveAuthFactory == null || !hiveAuthFactory.isSASLKerberosUser()) {
      resp.setStatus(unsecureTokenErrorStatus());
    } else {
      try {
        String token = cliService.getDelegationToken(
            new SessionHandle(req.getSessionHandle()),
            hiveAuthFactory, req.getOwner(), req.getRenewer());
        resp.setDelegationToken(token);
        resp.setStatus(OK_STATUS);
      } catch (HiveSQLException e) {
        LOG.error("Error obtaining delegation token", e);
        TStatus tokenErrorStatus = HiveSQLException.toTStatus(e);
        tokenErrorStatus.setSqlState("42000");
        resp.setStatus(tokenErrorStatus);
      }
    }
    return resp;
  }

  @Override
  public TCancelDelegationTokenResp CancelDelegationToken(TCancelDelegationTokenReq req)
      throws TException {
    TCancelDelegationTokenResp resp = new TCancelDelegationTokenResp();

    if (hiveAuthFactory == null || !hiveAuthFactory.isSASLKerberosUser()) {
      resp.setStatus(unsecureTokenErrorStatus());
    } else {
      try {
        cliService.cancelDelegationToken(new SessionHandle(req.getSessionHandle()),
            hiveAuthFactory, req.getDelegationToken());
        resp.setStatus(OK_STATUS);
      } catch (HiveSQLException e) {
        LOG.error("Error canceling delegation token", e);
        resp.setStatus(HiveSQLException.toTStatus(e));
      }
    }
    return resp;
  }

  @Override
  public TRenewDelegationTokenResp RenewDelegationToken(TRenewDelegationTokenReq req)
      throws TException {
    TRenewDelegationTokenResp resp = new TRenewDelegationTokenResp();
    if (hiveAuthFactory == null || !hiveAuthFactory.isSASLKerberosUser()) {
      resp.setStatus(unsecureTokenErrorStatus());
    } else {
      try {
        cliService.renewDelegationToken(new SessionHandle(req.getSessionHandle()),
            hiveAuthFactory, req.getDelegationToken());
        resp.setStatus(OK_STATUS);
      } catch (HiveSQLException e) {
        LOG.error("Error obtaining renewing token", e);
        resp.setStatus(HiveSQLException.toTStatus(e));
      }
    }
    return resp;
  }

  private TStatus unsecureTokenErrorStatus() {
    TStatus errorStatus = new TStatus(TStatusCode.ERROR_STATUS);
    errorStatus.setErrorMessage("Delegation token only supported over remote " +
        "client with kerberos authentication");
    return errorStatus;
  }

  @Override
  public TOpenSessionResp OpenSession(TOpenSessionReq req) throws TException {
    LOG.info("Client protocol version: " + req.getClient_protocol());
    TOpenSessionResp resp = new TOpenSessionResp();
    try {
      SessionHandle sessionHandle = getSessionHandle(req, resp);
      resp.setSessionHandle(sessionHandle.toTSessionHandle());
      Map<String, String> configurationMap = new HashMap<String, String>();
      // Set the updated fetch size from the server into the configuration map for the client
      HiveConf sessionConf = cliService.getSessionConf(sessionHandle);
      configurationMap.put(
        HiveConf.ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_DEFAULT_FETCH_SIZE.varname,
        Integer.toString(sessionConf != null ?
          sessionConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_DEFAULT_FETCH_SIZE) :
          hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_DEFAULT_FETCH_SIZE)));
      resp.setConfiguration(configurationMap);
      resp.setStatus(OK_STATUS);
      ThriftCLIServerContext context =
        (ThriftCLIServerContext)currentServerContext.get();
      if (context != null) {
        context.setSessionHandle(sessionHandle);
      }
    } catch (Exception e) {
      LOG.warn("Error opening session: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TSetClientInfoResp SetClientInfo(TSetClientInfoReq req) throws TException {
    // TODO: We don't do anything for now, just log this for debugging.
    //       We may be able to make use of this later, e.g. for workload management.
    TSetClientInfoResp resp = null;
    if (req.isSetConfiguration()) {
      StringBuilder sb = null;
      SessionHandle sh = null;
      for (Map.Entry<String, String> e : req.getConfiguration().entrySet()) {
        if (sb == null) {
          sh = new SessionHandle(req.getSessionHandle());
          sb = new StringBuilder("Client information for ").append(sh).append(": ");
        } else {
          sb.append(", ");
        }
        sb.append(e.getKey()).append(" = ").append(e.getValue());
        if ("ApplicationName".equals(e.getKey())) {
          try {
            cliService.setApplicationName(sh, e.getValue());
          } catch (Exception ex) {
            LOG.warn("Error setting application name", ex);
            resp = new TSetClientInfoResp(HiveSQLException.toTStatus(ex));
          }
        }
      }
      if (sb != null) {
        LOG.info("{}", sb);
      }
    }
    return resp == null ? new TSetClientInfoResp(OK_STATUS) : resp;
  }

  private String getIpAddress() {
    String clientIpAddress;
    // Http transport mode.
    // We set the thread local ip address, in ThriftHttpServlet.
    if (cliService.getHiveConf().getVar(
        ConfVars.HIVE_SERVER2_TRANSPORT_MODE).equalsIgnoreCase("http")) {
      clientIpAddress = SessionManager.getIpAddress();
    }
    else {
      if (hiveAuthFactory != null && hiveAuthFactory.isSASLWithKerberizedHadoop()) {
        clientIpAddress = hiveAuthFactory.getIpAddress();
      }
      // NOSASL
      else {
        clientIpAddress = TSetIpAddressProcessor.getUserIpAddress();
      }
    }
    LOG.debug("Client's IP Address: " + clientIpAddress);
    return clientIpAddress;
  }

  /**
   * Returns the effective username.
   * 1. If hive.server2.allow.user.substitution = false: the username of the connecting user
   * 2. If hive.server2.allow.user.substitution = true: the username of the end user,
   * that the connecting user is trying to proxy for.
   * This includes a check whether the connecting user is allowed to proxy for the end user.
   * @param req
   * @return
   * @throws HiveSQLException
   */
  private String getUserName(TOpenSessionReq req) throws HiveSQLException, IOException {
    String userName = null;

    if (hiveAuthFactory != null && hiveAuthFactory.isSASLWithKerberizedHadoop()) {
      userName = hiveAuthFactory.getRemoteUser();
    }
    // NOSASL
    if (userName == null) {
      userName = TSetIpAddressProcessor.getUserName();
    }
    // Http transport mode.
    // We set the thread local username, in ThriftHttpServlet.
    if (cliService.getHiveConf().getVar(
        ConfVars.HIVE_SERVER2_TRANSPORT_MODE).equalsIgnoreCase("http")) {
      userName = SessionManager.getUserName();
    }
    if (userName == null) {
      userName = req.getUsername();
    }

    if (cliService.getHiveConf().getBoolVar(ConfVars.HIVE_AUTHORIZATION_KERBEROS_USE_SHORTNAME))
    {
      userName = getShortName(userName);
    }

    String effectiveClientUser = getProxyUser(userName, req.getConfiguration(), getIpAddress());
    LOG.debug("Client's username: " + effectiveClientUser);
    return effectiveClientUser;
  }

  private String getShortName(String userName) throws IOException {
    String ret = null;

    if (userName != null) {
      if (hiveAuthFactory != null && hiveAuthFactory.isSASLKerberosUser()) {
        // KerberosName.getShorName can only be used for kerberos user, but not for the user
        // logged in via other authentications such as LDAP
        KerberosNameShim fullKerberosName = ShimLoader.getHadoopShims().getKerberosNameShim(userName);
        ret = fullKerberosName.getShortName();
      } else {
        int indexOfDomainMatch = ServiceUtils.indexOfDomainMatch(userName);
        ret = (indexOfDomainMatch <= 0) ? userName : userName.substring(0, indexOfDomainMatch);
      }
    }

    return ret;
  }

  /**
   * Create a session handle
   * @param req
   * @param res
   * @return
   * @throws HiveSQLException
   * @throws LoginException
   * @throws IOException
   */
  SessionHandle getSessionHandle(TOpenSessionReq req, TOpenSessionResp res)
      throws HiveSQLException, LoginException, IOException {
    String userName = getUserName(req);
    String ipAddress = getIpAddress();
    TProtocolVersion protocol = getMinVersion(CLIService.SERVER_VERSION,
        req.getClient_protocol());
    SessionHandle sessionHandle;
    if (cliService.getHiveConf().getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS) &&
        (userName != null)) {
      String delegationTokenStr = getDelegationToken(userName);
      sessionHandle = cliService.openSessionWithImpersonation(protocol, userName,
          req.getPassword(), ipAddress, req.getConfiguration(), delegationTokenStr);
    } else {
      sessionHandle = cliService.openSession(protocol, userName, req.getPassword(),
          ipAddress, req.getConfiguration());
    }
    res.setServerProtocolVersion(protocol);
    return sessionHandle;
  }

  private double getProgressedPercentage(OperationHandle opHandle) throws HiveSQLException {
    checkArgument(OperationType.EXECUTE_STATEMENT.equals(opHandle.getOperationType()));
    Operation operation = cliService.getSessionManager().getOperationManager().getOperation(opHandle);
    SessionState state = operation.getParentSession().getSessionState();
    ProgressMonitor monitor = state.getProgressMonitor();
    return monitor == null ? 0.0 : monitor.progressedPercentage();
  }

  private String getDelegationToken(String userName)
      throws HiveSQLException, LoginException, IOException {
    try {
      return cliService.getDelegationTokenFromMetaStore(userName);
    } catch (UnsupportedOperationException e) {
      // The delegation token is not applicable in the given deployment mode
      // such as HMS is not kerberos secured
    }
    return null;
  }

  private TProtocolVersion getMinVersion(TProtocolVersion... versions) {
    TProtocolVersion[] values = TProtocolVersion.values();
    int current = values[values.length - 1].getValue();
    for (TProtocolVersion version : versions) {
      if (current > version.getValue()) {
        current = version.getValue();
      }
    }
    for (TProtocolVersion version : values) {
      if (version.getValue() == current) {
        return version;
      }
    }
    throw new IllegalArgumentException("never");
  }

  @Override
  public TCloseSessionResp CloseSession(TCloseSessionReq req) throws TException {
    TCloseSessionResp resp = new TCloseSessionResp();
    try {
      SessionHandle sessionHandle = new SessionHandle(req.getSessionHandle());
      cliService.closeSession(sessionHandle);
      resp.setStatus(OK_STATUS);
      ThriftCLIServerContext context =
        (ThriftCLIServerContext)currentServerContext.get();
      if (context != null) {
        context.setSessionHandle(null);
      }
    } catch (Exception e) {
      LOG.warn("Error closing session: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetInfoResp GetInfo(TGetInfoReq req) throws TException {
    TGetInfoResp resp = new TGetInfoResp();
    try {
      GetInfoValue getInfoValue =
          cliService.getInfo(new SessionHandle(req.getSessionHandle()),
              GetInfoType.getGetInfoType(req.getInfoType()));
      resp.setInfoValue(getInfoValue.toTGetInfoValue());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting info: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TExecuteStatementResp ExecuteStatement(TExecuteStatementReq req) throws TException {
    TExecuteStatementResp resp = new TExecuteStatementResp();
    try {
      SessionHandle sessionHandle = new SessionHandle(req.getSessionHandle());
      String statement = req.getStatement();
      Map<String, String> confOverlay = req.getConfOverlay();
      Boolean runAsync = req.isRunAsync();
      long queryTimeout = req.getQueryTimeout();
      OperationHandle operationHandle =
          runAsync ? cliService.executeStatementAsync(sessionHandle, statement, confOverlay,
              queryTimeout) : cliService.executeStatement(sessionHandle, statement, confOverlay,
              queryTimeout);
      resp.setOperationHandle(operationHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      // Note: it's rather important that this (and other methods) catch Exception, not Throwable;
      // in combination with HiveSessionProxy.invoke code, perhaps unintentionally, it used
      // to also catch all errors; and now it allows OOMs only to propagate.
      LOG.warn("Error executing statement: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetTypeInfoResp GetTypeInfo(TGetTypeInfoReq req) throws TException {
    TGetTypeInfoResp resp = new TGetTypeInfoResp();
    try {
      OperationHandle operationHandle = cliService.getTypeInfo(new SessionHandle(req.getSessionHandle()));
      resp.setOperationHandle(operationHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting type info: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetCatalogsResp GetCatalogs(TGetCatalogsReq req) throws TException {
    TGetCatalogsResp resp = new TGetCatalogsResp();
    try {
      OperationHandle opHandle = cliService.getCatalogs(new SessionHandle(req.getSessionHandle()));
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting catalogs: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetSchemasResp GetSchemas(TGetSchemasReq req) throws TException {
    TGetSchemasResp resp = new TGetSchemasResp();
    try {
      OperationHandle opHandle = cliService.getSchemas(
          new SessionHandle(req.getSessionHandle()), req.getCatalogName(), req.getSchemaName());
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting schemas: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetTablesResp GetTables(TGetTablesReq req) throws TException {
    TGetTablesResp resp = new TGetTablesResp();
    try {
      OperationHandle opHandle = cliService
          .getTables(new SessionHandle(req.getSessionHandle()), req.getCatalogName(),
              req.getSchemaName(), req.getTableName(), req.getTableTypes());
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting tables: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetTableTypesResp GetTableTypes(TGetTableTypesReq req) throws TException {
    TGetTableTypesResp resp = new TGetTableTypesResp();
    try {
      OperationHandle opHandle = cliService.getTableTypes(new SessionHandle(req.getSessionHandle()));
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting table types: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetColumnsResp GetColumns(TGetColumnsReq req) throws TException {
    TGetColumnsResp resp = new TGetColumnsResp();
    try {
      OperationHandle opHandle = cliService.getColumns(
          new SessionHandle(req.getSessionHandle()),
          req.getCatalogName(),
          req.getSchemaName(),
          req.getTableName(),
          req.getColumnName());
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting columns: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetFunctionsResp GetFunctions(TGetFunctionsReq req) throws TException {
    TGetFunctionsResp resp = new TGetFunctionsResp();
    try {
      OperationHandle opHandle = cliService.getFunctions(
          new SessionHandle(req.getSessionHandle()), req.getCatalogName(),
          req.getSchemaName(), req.getFunctionName());
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting functions: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetOperationStatusResp GetOperationStatus(TGetOperationStatusReq req) throws TException {
    TGetOperationStatusResp resp = new TGetOperationStatusResp();
    OperationHandle operationHandle = new OperationHandle(req.getOperationHandle());
    try {
      OperationStatus operationStatus =
          cliService.getOperationStatus(operationHandle, req.isGetProgressUpdate());
      resp.setOperationState(operationStatus.getState().toTOperationState());
      resp.setErrorMessage(operationStatus.getState().getErrorMessage());
      HiveSQLException opException = operationStatus.getOperationException();
      resp.setTaskStatus(operationStatus.getTaskStatus());
      resp.setOperationStarted(operationStatus.getOperationStarted());
      resp.setOperationCompleted(operationStatus.getOperationCompleted());
      resp.setHasResultSet(operationStatus.getHasResultSet());
      JobProgressUpdate progressUpdate = operationStatus.jobProgressUpdate();
      ProgressMonitorStatusMapper mapper = ProgressMonitorStatusMapper.DEFAULT;
      if ("tez".equals(hiveConf.getVar(ConfVars.HIVE_EXECUTION_ENGINE))) {
        mapper = new TezProgressMonitorStatusMapper();
      }

      TJobExecutionStatus executionStatus =
          mapper.forStatus(progressUpdate.status);
      resp.setProgressUpdateResponse(new TProgressUpdateResp(
          progressUpdate.headers(),
          progressUpdate.rows(),
          progressUpdate.progressedPercentage,
          executionStatus,
          progressUpdate.footerSummary,
          progressUpdate.startTimeMillis
      ));
      if (opException != null) {
        resp.setSqlState(opException.getSQLState());
        resp.setErrorCode(opException.getErrorCode());
        if (opException.getErrorCode() == 29999)
          resp.setErrorMessage(org.apache.hadoop.util.StringUtils.stringifyException(opException));
        else
          resp.setErrorMessage(opException.getMessage());
      } else if (executionStatus == TJobExecutionStatus.NOT_AVAILABLE
          && OperationType.EXECUTE_STATEMENT.equals(operationHandle.getOperationType())) {
        resp.getProgressUpdateResponse().setProgressedPercentage(
            getProgressedPercentage(operationHandle));
      }
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting operation status: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TCancelOperationResp CancelOperation(TCancelOperationReq req) throws TException {
    TCancelOperationResp resp = new TCancelOperationResp();
    try {
      cliService.cancelOperation(new OperationHandle(req.getOperationHandle()));
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error cancelling operation: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TCloseOperationResp CloseOperation(TCloseOperationReq req) throws TException {
    TCloseOperationResp resp = new TCloseOperationResp();
    try {
      cliService.closeOperation(new OperationHandle(req.getOperationHandle()));
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error closing operation: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetResultSetMetadataResp GetResultSetMetadata(TGetResultSetMetadataReq req)
      throws TException {
    TGetResultSetMetadataResp resp = new TGetResultSetMetadataResp();
    try {
      TableSchema schema = cliService.getResultSetMetadata(new OperationHandle(req.getOperationHandle()));
      resp.setSchema(schema.toTTableSchema());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting result set metadata: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TFetchResultsResp FetchResults(TFetchResultsReq req) throws TException {
    TFetchResultsResp resp = new TFetchResultsResp();
    try {
      // Set fetch size
      int maxFetchSize =
        hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_MAX_FETCH_SIZE);
      if (req.getMaxRows() > maxFetchSize) {
        req.setMaxRows(maxFetchSize);
      }
      RowSet rowSet = cliService.fetchResults(
          new OperationHandle(req.getOperationHandle()),
          FetchOrientation.getFetchOrientation(req.getOrientation()),
          req.getMaxRows(),
          FetchType.getFetchType(req.getFetchType()));
      resp.setResults(rowSet.toTRowSet());
      resp.setHasMoreRows(false);
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error fetching results: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetPrimaryKeysResp GetPrimaryKeys(TGetPrimaryKeysReq req)
		throws TException {
    TGetPrimaryKeysResp resp = new TGetPrimaryKeysResp();
    try {
      OperationHandle opHandle = cliService.getPrimaryKeys(
      new SessionHandle(req.getSessionHandle()), req.getCatalogName(),
      req.getSchemaName(), req.getTableName());
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
     LOG.warn("Error getting functions: ", e);
     resp.setStatus(HiveSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetCrossReferenceResp GetCrossReference(TGetCrossReferenceReq req)
		throws TException {
    TGetCrossReferenceResp resp = new TGetCrossReferenceResp();
    try {
      OperationHandle opHandle = cliService.getCrossReference(
        new SessionHandle(req.getSessionHandle()), req.getParentCatalogName(),
	      req.getParentSchemaName(), req.getParentTableName(),
          req.getForeignCatalogName(), req.getForeignSchemaName(), req.getForeignTableName());
          resp.setOperationHandle(opHandle.toTOperationHandle());
          resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting functions: ", e);
	  resp.setStatus(HiveSQLException.toTStatus(e));
	}
    return resp;
  }

  @Override
  public TGetQueryIdResp GetQueryId(TGetQueryIdReq req) throws TException {
    try {
      return new TGetQueryIdResp(cliService.getQueryId(req.getOperationHandle()));
    } catch (HiveSQLException e) {
      throw new TException(e);
    }
  }

  @Override
  public abstract void run();

  /**
   * If the proxy user name is provided then check privileges to substitute the user.
   * @param realUser
   * @param sessionConf
   * @param ipAddress
   * @return
   * @throws HiveSQLException
   */
  private String getProxyUser(String realUser, Map<String, String> sessionConf,
      String ipAddress) throws HiveSQLException {
    String proxyUser = null;
    // Http transport mode.
    // We set the thread local proxy username, in ThriftHttpServlet.
    if (cliService.getHiveConf().getVar(
        ConfVars.HIVE_SERVER2_TRANSPORT_MODE).equalsIgnoreCase("http")) {
      proxyUser = SessionManager.getProxyUserName();
      LOG.debug("Proxy user from query string: " + proxyUser);
    }

    if (proxyUser == null && sessionConf != null && sessionConf.containsKey(HiveAuthConstants.HS2_PROXY_USER)) {
      String proxyUserFromThriftBody = sessionConf.get(HiveAuthConstants.HS2_PROXY_USER);
      LOG.debug("Proxy user from thrift body: " + proxyUserFromThriftBody);
      proxyUser = proxyUserFromThriftBody;
    }

    if (proxyUser == null) {
      return realUser;
    }

    // check whether substitution is allowed
    if (!hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ALLOW_USER_SUBSTITUTION)) {
      throw new HiveSQLException("Proxy user substitution is not allowed");
    }

    // If there's no authentication, then directly substitute the user
    if (HiveAuthConstants.AuthTypes.NONE.toString().
        equalsIgnoreCase(hiveConf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION))) {
      return proxyUser;
    }

    // Verify proxy user privilege of the realUser for the proxyUser
    HiveAuthFactory.verifyProxyAccess(realUser, proxyUser, ipAddress, hiveConf);
    LOG.debug("Verified proxy user: " + proxyUser);
    return proxyUser;
  }
}
