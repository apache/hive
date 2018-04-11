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

package org.apache.hive.service.cli.session;

import java.util.Collections;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.cli.HiveFileProcessor;
import org.apache.hadoop.hive.common.cli.IHiveFileProcessor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.history.HiveHistory;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.processors.SetProcessor;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.thrift.ThriftFormatter;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.common.util.HiveVersionInfo;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.FetchType;
import org.apache.hive.service.cli.GetInfoType;
import org.apache.hive.service.cli.GetInfoValue;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.operation.ExecuteStatementOperation;
import org.apache.hive.service.cli.operation.GetCatalogsOperation;
import org.apache.hive.service.cli.operation.GetColumnsOperation;
import org.apache.hive.service.cli.operation.GetCrossReferenceOperation;
import org.apache.hive.service.cli.operation.GetFunctionsOperation;
import org.apache.hive.service.cli.operation.GetPrimaryKeysOperation;
import org.apache.hive.service.cli.operation.GetSchemasOperation;
import org.apache.hive.service.cli.operation.GetTableTypesOperation;
import org.apache.hive.service.cli.operation.GetTypeInfoOperation;
import org.apache.hive.service.cli.operation.MetadataOperation;
import org.apache.hive.service.cli.operation.Operation;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.server.KillQueryImpl;
import org.apache.hive.service.server.ThreadWithGarbageCleanup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * HiveSession
 *
 */
public class HiveSessionImpl implements HiveSession {

  // Shared between threads (including SessionState!)
  private final SessionHandle sessionHandle;
  private String username;
  private final String password;
  private final HiveConf sessionConf;
  private final long creationTime;
  // TODO: some SessionState internals are not thread safe. The compile-time internals are synced
  //       via session-scope or global compile lock. The run-time internals work by magic!
  //       They probably work because races are relatively unlikely and few tools run parallel
  //       queries from the same session.
  //       1) OperationState should be refactored out of SessionState, and made thread-local.
  //       2) Some parts of session state, like mrStats and vars, need proper synchronization.
  private SessionState sessionState;
  private String ipAddress;
  private List<String> forwardedAddresses;

  private static final String FETCH_WORK_SERDE_CLASS =
      "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
  private static final Logger LOG = LoggerFactory.getLogger(HiveSessionImpl.class);

  private SessionManager sessionManager;
  private OperationManager operationManager;
  // Synchronized by locking on itself.
  private final Set<OperationHandle> opHandleSet = new HashSet<OperationHandle>();
  private boolean isOperationLogEnabled;
  private File sessionLogDir;
  // TODO: the control flow for this needs to be defined. Hive is supposed to be thread-local.
  private Hive sessionHive;

  private volatile long lastAccessTime = System.currentTimeMillis();
  private volatile boolean lockedByUser;
  private final Semaphore operationLock;


  public HiveSessionImpl(SessionHandle sessionHandle, TProtocolVersion protocol,
    String username, String password, HiveConf serverConf, String ipAddress,
    final List<String> forwardedAddresses) {
    this.username = username;
    this.password = password;
    creationTime = System.currentTimeMillis();
    this.sessionHandle = sessionHandle != null ? sessionHandle : new SessionHandle(protocol);
    this.sessionConf = new HiveConf(serverConf);
    this.ipAddress = ipAddress;
    this.forwardedAddresses = forwardedAddresses;
    this.operationLock = serverConf.getBoolVar(
        ConfVars.HIVE_SERVER2_PARALLEL_OPS_IN_SESSION) ? null : new Semaphore(1);
    try {
      // In non-impersonation mode, map scheduler queue to current user
      // if fair scheduler is configured.
      if (! sessionConf.getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS) &&
        sessionConf.getBoolVar(ConfVars.HIVE_SERVER2_MAP_FAIR_SCHEDULER_QUEUE)) {
        ShimLoader.getHadoopShims().refreshDefaultQueue(sessionConf, username);
      }
    } catch (IOException e) {
      LOG.warn("Error setting scheduler queue: " + e, e);
    }
    // Set an explicit session name to control the download directory name
    sessionConf.set(ConfVars.HIVESESSIONID.varname,
        this.sessionHandle.getHandleIdentifier().toString());
    // Use thrift transportable formatter
    sessionConf.set(SerDeUtils.LIST_SINK_OUTPUT_FORMATTER, ThriftFormatter.class.getName());
    sessionConf.setInt(SerDeUtils.LIST_SINK_OUTPUT_PROTOCOL, protocol.getValue());
  }

  @Override
  /**
   * Opens a new HiveServer2 session for the client connection.
   * Creates a new SessionState object that will be associated with this HiveServer2 session.
   * When the server executes multiple queries in the same session,
   * this SessionState object is reused across multiple queries.
   * Note that if doAs is true, this call goes through a proxy object,
   * which wraps the method logic in a UserGroupInformation#doAs.
   * That's why it is important to create SessionState here rather than in the constructor.
   */
  public void open(Map<String, String> sessionConfMap) throws HiveSQLException {
    sessionState = new SessionState(sessionConf, username);
    sessionState.setUserIpAddress(ipAddress);
    sessionState.setIsHiveServerQuery(true);
    sessionState.setForwardedAddresses(SessionManager.getForwardedAddresses());
    sessionState.setIsUsingThriftJDBCBinarySerDe(updateIsUsingThriftJDBCBinarySerDe());
    try {
      if (sessionManager != null) {
        sessionState.setHiveServer2Host(sessionManager.getHiveServer2HostName());
      }
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
    sessionState.setKillQuery(new KillQueryImpl(operationManager));
    SessionState.start(sessionState);
    try {
      sessionState.loadAuxJars();
      sessionState.loadReloadableAuxJars();
    } catch (IOException e) {
      String msg = "Failed to load reloadable jar file path: " + e;
      LOG.error(msg, e);
      throw new HiveSQLException(msg, e);
    }
    try {
      sessionHive = Hive.get(getHiveConf());
    } catch (HiveException e) {
      throw new HiveSQLException("Failed to get metastore connection", e);
    }
    // Process global init file: .hiverc
    processGlobalInitFile();
    // Set fetch size in session conf map
    sessionConfMap = setFetchSize(sessionConfMap);

    if (sessionConfMap != null) {
      configureSession(sessionConfMap);
    }
    lastAccessTime = System.currentTimeMillis();
  }

/**
   * It is used for processing hiverc file from HiveServer2 side.
   */
  private class GlobalHivercFileProcessor extends HiveFileProcessor {
    @Override
    protected BufferedReader loadFile(String fileName) throws IOException {
      FileInputStream initStream = null;
      BufferedReader bufferedReader = null;
      initStream = new FileInputStream(fileName);
      bufferedReader = new BufferedReader(new InputStreamReader(initStream));
      return bufferedReader;
    }

    @Override
    protected int processCmd(String cmd) {
      int rc = 0;
      String cmd_trimed = cmd.trim();
      OperationHandle opHandle = null;
      try {
        //execute in sync mode
        opHandle = executeStatementInternal(cmd_trimed, null, false, 0);
      } catch (HiveSQLException e) {
        LOG.warn("Failed to execute command in global .hiverc file.", e);
        return -1;
      }
      if (opHandle != null) {
        try {
          closeOperation(opHandle);
        } catch (HiveSQLException e) {
          LOG.warn("Failed to close operation for command in .hiverc file.", e);
        }
      }
      return rc;
    }
  }

  private void processGlobalInitFile() {
    IHiveFileProcessor processor = new GlobalHivercFileProcessor();

    try {
      String hiverc = sessionConf.getVar(ConfVars.HIVE_SERVER2_GLOBAL_INIT_FILE_LOCATION);
      if (hiverc != null) {
        File hivercFile = new File(hiverc);
        if (hivercFile.isDirectory()) {
          hivercFile = new File(hivercFile, SessionManager.HIVERCFILE);
        }
        if (hivercFile.isFile()) {
          LOG.info("Running global init file: " + hivercFile);
          int rc = processor.processFile(hivercFile.getAbsolutePath());
          if (rc != 0) {
            LOG.error("Failed on initializing global .hiverc file");
          }
        } else {
          LOG.debug("Global init file " + hivercFile + " does not exist");
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed on initializing global .hiverc file", e);
    }
  }

  private Map<String, String> setFetchSize(Map<String, String> sessionConfMap) {
    int maxFetchSize =
      sessionConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_MAX_FETCH_SIZE);
    String confFetchSize = sessionConfMap != null ?
      sessionConfMap.get(
        "set:hiveconf:" + HiveConf.ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_DEFAULT_FETCH_SIZE.varname) :
        null;
    if (confFetchSize != null && !confFetchSize.isEmpty()) {
        int fetchSize = Integer.parseInt(confFetchSize);
        sessionConfMap.put(
          "set:hiveconf:" + HiveConf.ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_DEFAULT_FETCH_SIZE.varname,
          Integer.toString(fetchSize > maxFetchSize ? maxFetchSize : fetchSize));
    }
    return sessionConfMap;
  }

  private void configureSession(Map<String, String> sessionConfMap) throws HiveSQLException {
    SessionState.setCurrentSessionState(sessionState);
    for (Map.Entry<String, String> entry : sessionConfMap.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith("set:")) {
        try {
          SetProcessor.setVariable(key.substring(4), entry.getValue());
        } catch (Exception e) {
          throw new HiveSQLException(e);
        }
      } else if (key.startsWith("use:")) {
        try {
          if (sessionHive.getDatabase(entry.getValue()) == null) {
            throw new HiveSQLException("Database " + entry.getValue() + " does not exist");
          }
        } catch (HiveException e) {
          throw new HiveSQLException(e);
        }
        SessionState.get().setCurrentDatabase(entry.getValue());
      } else {
        sessionConf.verifyAndSet(key, entry.getValue());
      }
    }
  }

  private boolean updateIsUsingThriftJDBCBinarySerDe() {
	return (8 <= getProtocolVersion().getValue())
      && sessionConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_SERIALIZE_IN_TASKS);
  }

  @Override
  public void setOperationLogSessionDir(File operationLogRootDir) {
    if (!operationLogRootDir.exists()) {
      LOG.warn("The operation log root directory is removed, recreating:" +
          operationLogRootDir.getAbsolutePath());
      if (!operationLogRootDir.mkdirs()) {
        LOG.warn("Unable to create operation log root directory: " +
            operationLogRootDir.getAbsolutePath());
      }
    }
    if (!operationLogRootDir.canWrite()) {
      LOG.warn("The operation log root directory is not writable: " +
          operationLogRootDir.getAbsolutePath());
    }
    sessionLogDir = new File(operationLogRootDir, sessionHandle.getHandleIdentifier().toString());
    isOperationLogEnabled = true;
    if (!sessionLogDir.exists()) {
      if (!sessionLogDir.mkdir()) {
        LOG.warn("Unable to create operation log session directory: " +
            sessionLogDir.getAbsolutePath());
        isOperationLogEnabled = false;
      }
    }
    if (isOperationLogEnabled) {
      LOG.info("Operation log session directory is created: " + sessionLogDir.getAbsolutePath());
    }
  }

  @Override
  public boolean isOperationLogEnabled() {
    return isOperationLogEnabled;
  }

  @Override
  public File getOperationLogSessionDir() {
    return sessionLogDir;
  }

  @Override
  public TProtocolVersion getProtocolVersion() {
    return sessionHandle.getProtocolVersion();
  }

  @Override
  public SessionManager getSessionManager() {
    return sessionManager;
  }

  @Override
  public void setSessionManager(SessionManager sessionManager) {
    this.sessionManager = sessionManager;
  }

  private OperationManager getOperationManager() {
    return operationManager;
  }

  @Override
  public void setOperationManager(OperationManager operationManager) {
    this.operationManager = operationManager;
  }

  protected void acquire(boolean userAccess, boolean isOperation) {
    if (isOperation && operationLock != null) {
      try {
        operationLock.acquire();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
    boolean success = false;
    try {
      acquireAfterOpLock(userAccess);
      success = true;
    } finally {
      if (!success && isOperation && operationLock != null) {
        operationLock.release();
      }
    }
  }

  private synchronized void acquireAfterOpLock(boolean userAccess) {
    // Need to make sure that the this HiveServer2's session's SessionState is
    // stored in the thread local for the handler thread.
    SessionState.setCurrentSessionState(sessionState);
    sessionState.setForwardedAddresses(SessionManager.getForwardedAddresses());
    sessionState.setIsUsingThriftJDBCBinarySerDe(updateIsUsingThriftJDBCBinarySerDe());
    if (userAccess) {
      lastAccessTime = System.currentTimeMillis();
      lockedByUser = true;
    }
    // set the thread name with the logging prefix.
    sessionState.updateThreadName();
    Hive.set(sessionHive);
  }

  /**
   * 1. We'll remove the ThreadLocal SessionState as this thread might now serve
   * other requests.
   * 2. We'll cache the ThreadLocal RawStore object for this background thread for an orderly cleanup
   * when this thread is garbage collected later.
   * @see org.apache.hive.service.server.ThreadWithGarbageCleanup#finalize()
   */
  protected void release(boolean userAccess, boolean isOperation) {
    try {
      releaseBeforeOpLock(userAccess);
    } finally {
      if (isOperation && operationLock != null) {
        operationLock.release();
      }
    }
  }

  private synchronized void releaseBeforeOpLock(boolean userAccess) {
    if (sessionState != null) {
      // can be null in-case of junit tests. skip reset.
      // reset thread name at release time.
      sessionState.resetThreadName();
    }

    SessionState.detachSession();
    if (ThreadWithGarbageCleanup.currentThread() instanceof ThreadWithGarbageCleanup) {
      ThreadWithGarbageCleanup currentThread =
          (ThreadWithGarbageCleanup) ThreadWithGarbageCleanup.currentThread();
      currentThread.cacheThreadLocalRawStore();
    }
    if (userAccess) {
      lastAccessTime = System.currentTimeMillis();
      lockedByUser = false;
    }
  }

  @Override
  public SessionHandle getSessionHandle() {
    return sessionHandle;
  }

  @Override
  public String getPassword() {
    return password;
  }

  @Override
  public HiveConf getHiveConf() {
    sessionConf.setVar(HiveConf.ConfVars.HIVEFETCHOUTPUTSERDE, FETCH_WORK_SERDE_CLASS);
    return sessionConf;
  }

  @Override
  public Hive getSessionHive() {
    return sessionHive;
  }

  @Override
  public IMetaStoreClient getMetaStoreClient() throws HiveSQLException {
    try {
      return getSessionHive().getMSC();
    } catch (MetaException e) {
      throw new HiveSQLException("Failed to get metastore connection: " + e, e);
    }
  }

  @Override
  public GetInfoValue getInfo(GetInfoType getInfoType)
      throws HiveSQLException {
    acquire(true, true);
    try {
      switch (getInfoType) {
      case CLI_SERVER_NAME:
        return new GetInfoValue("Hive");
      case CLI_DBMS_NAME:
        return new GetInfoValue("Apache Hive");
      case CLI_DBMS_VER:
        return new GetInfoValue(HiveVersionInfo.getVersion());
      case CLI_MAX_COLUMN_NAME_LEN:
        return new GetInfoValue(128);
      case CLI_MAX_SCHEMA_NAME_LEN:
        return new GetInfoValue(128);
      case CLI_MAX_TABLE_NAME_LEN:
        return new GetInfoValue(128);
      case CLI_ODBC_KEYWORDS:
        return new GetInfoValue(ParseUtils.getKeywords(ODBC_KEYWORDS));
      case CLI_TXN_CAPABLE:
      default:
        throw new HiveSQLException("Unrecognized GetInfoType value: " + getInfoType.toString());
      }
    } finally {
      release(true, true);
    }
  }

  @Override
  public HiveConf getSessionConf() throws HiveSQLException {
	  return this.sessionConf;
  }

  @Override
  public OperationHandle executeStatement(String statement, Map<String, String> confOverlay) throws HiveSQLException {
    return executeStatementInternal(statement, confOverlay, false, 0);
  }

  @Override
  public OperationHandle executeStatement(String statement, Map<String, String> confOverlay,
      long queryTimeout) throws HiveSQLException {
    return executeStatementInternal(statement, confOverlay, false, queryTimeout);
  }

  @Override
  public OperationHandle executeStatementAsync(String statement, Map<String, String> confOverlay) throws HiveSQLException {
    return executeStatementInternal(statement, confOverlay, true, 0);
  }

  @Override
  public OperationHandle executeStatementAsync(String statement, Map<String, String> confOverlay,
      long queryTimeout) throws HiveSQLException {
    return executeStatementInternal(statement, confOverlay, true, queryTimeout);
  }

  private OperationHandle executeStatementInternal(String statement,
      Map<String, String> confOverlay, boolean runAsync, long queryTimeout) throws HiveSQLException {
    acquire(true, true);

    ExecuteStatementOperation operation = null;
    OperationHandle opHandle = null;
    try {
      operation = getOperationManager().newExecuteStatementOperation(getSession(), statement,
          confOverlay, runAsync, queryTimeout);
      opHandle = operation.getHandle();
      addOpHandle(opHandle);
      operation.run();
      return opHandle;
    } catch (HiveSQLException e) {
      // Refering to SQLOperation.java, there is no chance that a HiveSQLException throws and the
      // async background operation submits to thread pool successfully at the same time. So, Cleanup
      // opHandle directly when got HiveSQLException
      if (opHandle != null) {
        removeOpHandle(opHandle);
        getOperationManager().closeOperation(opHandle);
      }
      throw e;
    } finally {
      if (operation == null || operation.getBackgroundHandle() == null) {
        release(true, true); // Not async, or wasn't submitted for some reason (failure, etc.)
      } else {
        releaseBeforeOpLock(true); // Release, but keep the lock (if present).
      }
    }
  }

  @Override
  public Future<?> submitBackgroundOperation(Runnable work) {
    return getSessionManager().submitBackgroundOperation(
        operationLock == null ? work : new FutureTask<Void>(work, null) {
      protected void done() {
        // We assume this always comes from a user operation that took the lock.
        operationLock.release();
      };
    });
  }

  @Override
  public OperationHandle getTypeInfo()
      throws HiveSQLException {
    acquire(true, true);

    OperationManager operationManager = getOperationManager();
    GetTypeInfoOperation operation = operationManager.newGetTypeInfoOperation(getSession());
    OperationHandle opHandle = operation.getHandle();
    try {
      addOpHandle(opHandle);
      operation.run();
      return opHandle;
    } catch (HiveSQLException e) {
      removeOpHandle(opHandle);
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true, true);
    }
  }

  @Override
  public OperationHandle getCatalogs()
      throws HiveSQLException {
    acquire(true, true);

    OperationManager operationManager = getOperationManager();
    GetCatalogsOperation operation = operationManager.newGetCatalogsOperation(getSession());
    OperationHandle opHandle = operation.getHandle();
    try {
      addOpHandle(opHandle);
      operation.run();
      return opHandle;
    } catch (HiveSQLException e) {
      removeOpHandle(opHandle);
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true, true);
    }
  }

  @Override
  public OperationHandle getSchemas(String catalogName, String schemaName)
      throws HiveSQLException {
    acquire(true, true);

    OperationManager operationManager = getOperationManager();
    GetSchemasOperation operation =
        operationManager.newGetSchemasOperation(getSession(), catalogName, schemaName);
    OperationHandle opHandle = operation.getHandle();
    try {
      addOpHandle(opHandle);
      operation.run();
      return opHandle;
    } catch (HiveSQLException e) {
      removeOpHandle(opHandle);
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true, true);
    }
  }

  @Override
  public OperationHandle getTables(String catalogName, String schemaName, String tableName,
      List<String> tableTypes)
          throws HiveSQLException {
    acquire(true, true);

    OperationManager operationManager = getOperationManager();
    MetadataOperation operation =
        operationManager.newGetTablesOperation(getSession(), catalogName, schemaName, tableName, tableTypes);
    OperationHandle opHandle = operation.getHandle();
    try {
      addOpHandle(opHandle);
      operation.run();
      return opHandle;
    } catch (HiveSQLException e) {
      removeOpHandle(opHandle);
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true, true);
    }
  }

  @Override
  public OperationHandle getTableTypes()
      throws HiveSQLException {
    acquire(true, true);

    OperationManager operationManager = getOperationManager();
    GetTableTypesOperation operation = operationManager.newGetTableTypesOperation(getSession());
    OperationHandle opHandle = operation.getHandle();
    try {
      addOpHandle(opHandle);
      operation.run();
      return opHandle;
    } catch (HiveSQLException e) {
      removeOpHandle(opHandle);
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true, true);
    }
  }

  @Override
  public OperationHandle getColumns(String catalogName, String schemaName,
      String tableName, String columnName)  throws HiveSQLException {
    acquire(true, true);
    String addedJars = Utilities.getResourceFiles(sessionConf, SessionState.ResourceType.JAR);
    if (StringUtils.isNotBlank(addedJars)) {
       IMetaStoreClient metastoreClient = getSession().getMetaStoreClient();
       metastoreClient.setHiveAddedJars(addedJars);
    }
    OperationManager operationManager = getOperationManager();
    GetColumnsOperation operation = operationManager.newGetColumnsOperation(getSession(),
        catalogName, schemaName, tableName, columnName);
    OperationHandle opHandle = operation.getHandle();
    try {
      addOpHandle(opHandle);
      operation.run();
      return opHandle;
    } catch (HiveSQLException e) {
      removeOpHandle(opHandle);
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true, true);
    }
  }

  private void addOpHandle(OperationHandle opHandle) {
    synchronized (opHandleSet) {
      opHandleSet.add(opHandle);
    }
  }

  private void removeOpHandle(OperationHandle opHandle) {
    synchronized (opHandleSet) {
      opHandleSet.remove(opHandle);
    }
  }

  @Override
  public OperationHandle getFunctions(String catalogName, String schemaName, String functionName)
      throws HiveSQLException {
    acquire(true, true);

    OperationManager operationManager = getOperationManager();
    GetFunctionsOperation operation = operationManager
        .newGetFunctionsOperation(getSession(), catalogName, schemaName, functionName);
    OperationHandle opHandle = operation.getHandle();
    try {
      addOpHandle(opHandle);
      operation.run();
      return opHandle;
    } catch (HiveSQLException e) {
      removeOpHandle(opHandle);
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true, true);
    }
  }

  @Override
  public void close() throws HiveSQLException {
    try {
      acquire(true, false);
      // Iterate through the opHandles and close their operations
      List<OperationHandle> ops = null;
      synchronized (opHandleSet) {
        ops = new ArrayList<>(opHandleSet);
        opHandleSet.clear();
      }
      for (OperationHandle opHandle : ops) {
        operationManager.closeOperation(opHandle);
      }
      // Cleanup session log directory.
      cleanupSessionLogDir();
      HiveHistory hiveHist = sessionState.getHiveHistory();
      if (null != hiveHist) {
        hiveHist.closeStream();
      }
      try {
        sessionState.resetThreadName();
        sessionState.close();
      } finally {
        sessionState = null;
      }
    } catch (IOException ioe) {
      throw new HiveSQLException("Failure to close", ioe);
    } finally {
      if (sessionState != null) {
        try {
          sessionState.resetThreadName();
          sessionState.close();
        } catch (Throwable t) {
          LOG.warn("Error closing session", t);
        }
        sessionState = null;
      }
      if (sessionHive != null) {
        try {
          Hive.closeCurrent();
        } catch (Throwable t) {
          LOG.warn("Error closing sessionHive", t);
        }
        sessionHive = null;
      }
      release(true, false);
    }
  }

  private void cleanupSessionLogDir() {
    // In case of test, if we might not want to remove the log directory
    if (isOperationLogEnabled && sessionConf.getBoolVar(ConfVars.HIVE_TESTING_REMOVE_LOGS)) {
      try {
        FileUtils.forceDelete(sessionLogDir);
        LOG.info("Operation log session directory is deleted: "
            + sessionLogDir.getAbsolutePath());
      } catch (Exception e) {
        LOG.error("Failed to cleanup session log dir: " + sessionHandle, e);
      }
    }
  }

  @Override
  public SessionState getSessionState() {
    return sessionState;
  }

  @Override
  public String getUserName() {
    return username;
  }

  @Override
  public void setUserName(String userName) {
    this.username = userName;
  }

  @Override
  public long getLastAccessTime() {
    return lastAccessTime;
  }

  @Override
  public long getCreationTime() {
    return creationTime;
  }

  @Override
  public void closeExpiredOperations() {
    OperationHandle[] handles;
    synchronized (opHandleSet) {
      handles = opHandleSet.toArray(new OperationHandle[opHandleSet.size()]);
    }
    if (handles.length > 0) {
      List<Operation> operations = operationManager.removeExpiredOperations(handles);
      if (!operations.isEmpty()) {
        closeTimedOutOperations(operations);
      }
    }
  }

  @Override
  public long getNoOperationTime() {
    boolean noMoreOpHandle = false;
    synchronized (opHandleSet) {
      noMoreOpHandle = opHandleSet.isEmpty();
    }
    return noMoreOpHandle && !lockedByUser ? System.currentTimeMillis() - lastAccessTime : 0;
  }

  private void closeTimedOutOperations(List<Operation> operations) {
    acquire(false, false);
    try {
      for (Operation operation : operations) {
        removeOpHandle(operation.getHandle());
        try {
          operation.close();
        } catch (Exception e) {
          LOG.warn("Exception is thrown closing timed-out operation, reported open_operations metrics may be incorrect " + operation.getHandle(), e);
        }
      }
    } finally {
      release(false, false);
    }
  }

  @Override
  public void cancelOperation(OperationHandle opHandle) throws HiveSQLException {
    acquire(true, false);
    try {
      sessionManager.getOperationManager().cancelOperation(opHandle);
    } finally {
      release(true, false);
    }
  }

  @Override
  public void closeOperation(OperationHandle opHandle) throws HiveSQLException {
    acquire(true, false);
    try {
      operationManager.closeOperation(opHandle);
      synchronized (opHandleSet) {
        opHandleSet.remove(opHandle);
      }
    } finally {
      release(true, false);
    }
  }

  @Override
  public TableSchema getResultSetMetadata(OperationHandle opHandle) throws HiveSQLException {
    acquire(true, true);
    try {
      return sessionManager.getOperationManager().getOperationResultSetSchema(opHandle);
    } finally {
      release(true, true);
    }
  }

  @Override
  public RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation,
      long maxRows, FetchType fetchType) throws HiveSQLException {
    acquire(true, false);
    try {
      if (fetchType == FetchType.QUERY_OUTPUT) {
        return operationManager.getOperationNextRowSet(opHandle, orientation, maxRows);
      }
      return operationManager.getOperationLogRowSet(opHandle, orientation, maxRows, sessionConf);
    } finally {
      release(true, false);
    }
  }

  protected HiveSession getSession() {
    return this;
  }

  @Override
  public int getOpenOperationCount() {
    return opHandleSet.size();
  }

  @Override
  public String getIpAddress() {
    return ipAddress;
  }

  @Override
  public void setIpAddress(String ipAddress) {
    this.ipAddress = ipAddress;
  }

  @Override
  public List<String> getForwardedAddresses() {
    return forwardedAddresses;
  }

  @Override
  public void setForwardedAddresses(final List<String> forwardedAddresses) {
    this.forwardedAddresses = forwardedAddresses;
  }

  @Override
  public String getDelegationToken(HiveAuthFactory authFactory, String owner, String renewer)
      throws HiveSQLException {
    HiveAuthFactory.verifyProxyAccess(getUserName(), owner, getIpAddress(), getHiveConf());
    return authFactory.getDelegationToken(owner, renewer, getIpAddress());
  }

  @Override
  public void cancelDelegationToken(HiveAuthFactory authFactory, String tokenStr)
      throws HiveSQLException {
    HiveAuthFactory.verifyProxyAccess(getUserName(), getUserFromToken(authFactory, tokenStr),
        getIpAddress(), getHiveConf());
    authFactory.cancelDelegationToken(tokenStr);
  }

  @Override
  public void renewDelegationToken(HiveAuthFactory authFactory, String tokenStr)
      throws HiveSQLException {
    HiveAuthFactory.verifyProxyAccess(getUserName(), getUserFromToken(authFactory, tokenStr),
        getIpAddress(), getHiveConf());
    authFactory.renewDelegationToken(tokenStr);
  }

  // extract the real user from the given token string
  private String getUserFromToken(HiveAuthFactory authFactory, String tokenStr) throws HiveSQLException {
    return authFactory.getUserFromToken(tokenStr);
  }

  @Override
  public OperationHandle getPrimaryKeys(String catalog, String schema,
    String table) throws HiveSQLException {
    acquire(true, true);

    OperationManager operationManager = getOperationManager();
    GetPrimaryKeysOperation operation = operationManager
        .newGetPrimaryKeysOperation(getSession(), catalog, schema, table);
    OperationHandle opHandle = operation.getHandle();
    try {
      addOpHandle(opHandle);
      operation.run();
      return opHandle;
    } catch (HiveSQLException e) {
      removeOpHandle(opHandle);
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true, true);
    }
  }

  @Override
  public OperationHandle getCrossReference(String primaryCatalog,
    String primarySchema, String primaryTable, String foreignCatalog,
    String foreignSchema, String foreignTable) throws HiveSQLException {
    acquire(true, true);

    OperationManager operationManager = getOperationManager();
    GetCrossReferenceOperation operation = operationManager
      .newGetCrossReferenceOperation(getSession(), primaryCatalog,
         primarySchema, primaryTable, foreignCatalog,
         foreignSchema, foreignTable);
    OperationHandle opHandle = operation.getHandle();
    try {
      addOpHandle(opHandle);
      operation.run();
      return opHandle;
    } catch (HiveSQLException e) {
      removeOpHandle(opHandle);
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true, true);
    }
  }

  @Override
  public void setApplicationName(String value) {
    String oldName = sessionState.getHiveVariables().put("wmapp", value);
    if (oldName != null && !oldName.equals(value)) {
      LOG.info("ApplicationName changed from " + oldName + " to " + value);
    }
  }


  // From https://docs.microsoft.com/en-us/sql/t-sql/language-elements/reserved-keywords-transact-sql#odbc-reserved-keywords
  private static final Set<String> ODBC_KEYWORDS = Collections.unmodifiableSet(new HashSet<>(
      Lists.newArrayList("ABSOLUTE", "ACTION", "ADA", "ADD", "ALL", "ALLOCATE", "ALTER", "AND",
      "ANY", "ARE", "AS", "ASC", "ASSERTION", "AT", "AUTHORIZATION", "AVG", "BEGIN", "BETWEEN",
      "BIT_LENGTH", "BIT", "BOTH", "BY", "CASCADE", "CASCADED", "CASE", "CAST", "CATALOG",
      "CHAR_LENGTH", "CHAR", "CHARACTER_LENGTH", "CHARACTER", "CHECK", "CLOSE", "COALESCE",
      "COLLATE", "COLLATION", "COLUMN", "COMMIT", "CONNECT", "CONNECTION", "CONSTRAINT",
      "CONSTRAINTS", "CONTINUE", "CONVERT", "CORRESPONDING", "COUNT", "CREATE", "CROSS",
      "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURRENT", "CURSOR",
      "DATE", "DAY", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DEFERRABLE",
      "DEFERRED", "DELETE", "DESC", "DESCRIBE", "DESCRIPTOR", "DIAGNOSTICS", "DISCONNECT",
      "DISTINCT", "DOMAIN", "DOUBLE", "DROP", "ELSE", "END", "ESCAPE", "EXCEPT", "EXCEPTION",
      "EXEC", "EXECUTE", "EXISTS", "EXTERNAL", "EXTRACT", "FALSE", "FETCH", "FIRST", "FLOAT",
      "FOR", "FOREIGN", "FORTRAN", "FOUND", "FROM", "FULL", "GET", "GLOBAL", "GO", "GOTO", "GRANT",
      "GROUP", "HAVING", "HOUR", "IDENTITY", "IMMEDIATE", "IN", "INCLUDE", "INDEX", "INDICATOR",
      "INITIALLY", "INNER", "INPUT", "INSENSITIVE", "INSERT", "INT", "INTEGER", "INTERSECT",
      "INTERVAL", "INTO", "IS", "ISOLATION", "JOIN", "KEY", "LANGUAGE", "LAST", "LEADING", "LEFT",
      "LEVEL", "LIKE", "LOCAL", "LOWER", "MATCH", "MAX", "MIN", "MINUTE", "MODULE", "MONTH",
      "NAMES", "NATIONAL", "NATURAL", "NCHAR", "NEXT", "NO", "NONE", "NOT", "NULL", "NULLIF",
      "NUMERIC", "OCTET_LENGTH", "OF", "ON", "ONLY", "OPEN", "OPTION", "OR", "ORDER", "OUTER",
      "OUTPUT", "OVERLAPS", "PAD", "PARTIAL", "PASCAL", "POSITION", "PRECISION", "PREPARE",
      "PRESERVE", "PRIMARY", "PRIOR", "PRIVILEGES", "PROCEDURE", "PUBLIC", "READ", "REAL",
      "REFERENCES", "RELATIVE", "RESTRICT", "REVOKE", "RIGHT", "ROLLBACK", "ROWS", "SCHEMA",
      "SCROLL", "SECOND", "SECTION", "SELECT", "SESSION_USER", "SESSION", "SET", "SIZE",
      "SMALLINT", "SOME", "SPACE", "SQL", "SQLCA", "SQLCODE", "SQLERROR", "SQLSTATE", "SQLWARNING",
      "SUBSTRING", "SUM", "SYSTEM_USER", "TABLE", "TEMPORARY", "THEN", "TIME", "TIMESTAMP",
      "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRAILING", "TRANSACTION", "TRANSLATE",
      "TRANSLATION", "TRIM", "TRUE", "UNION", "UNIQUE", "UNKNOWN", "UPDATE", "UPPER", "USAGE",
      "USER", "USING", "VALUE", "VALUES", "VARCHAR", "VARYING", "VIEW", "WHEN", "WHENEVER",
      "WHERE", "WITH", "WORK", "WRITE", "YEAR", "ZONE")));
}
