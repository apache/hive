/**
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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.hooks.HookUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.auth.TSetIpAddressProcessor;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.hive.service.cli.thrift.TProtocolVersion;

/**
 * SessionManager.
 *
 */
public class SessionManager extends CompositeService {

  private static final Log LOG = LogFactory.getLog(CompositeService.class);
  private HiveConf hiveConf;
  private final Map<SessionHandle, HiveSession> handleToSession =
      new ConcurrentHashMap<SessionHandle, HiveSession>();
  private final OperationManager operationManager = new OperationManager();
  private ThreadPoolExecutor backgroundOperationPool;

  public SessionManager() {
    super("SessionManager");
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    try {
      applyAuthorizationConfigPolicy(hiveConf);
    } catch (HiveException e) {
      throw new RuntimeException("Error applying authorization policy on hive configuration", e);
    }

    this.hiveConf = hiveConf;
    int backgroundPoolSize = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_THREADS);
    LOG.info("HiveServer2: Async execution thread pool size: " + backgroundPoolSize);
    int backgroundPoolQueueSize = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_WAIT_QUEUE_SIZE);
    LOG.info("HiveServer2: Async execution wait queue size: " + backgroundPoolQueueSize);
    int keepAliveTime = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_KEEPALIVE_TIME);
    LOG.info("HiveServer2: Async execution thread keepalive time: " + keepAliveTime);
    // Create a thread pool with #backgroundPoolSize threads
    // Threads terminate when they are idle for more than the keepAliveTime
    // An bounded blocking queue is used to queue incoming operations, if #operations > backgroundPoolSize
    backgroundOperationPool = new ThreadPoolExecutor(backgroundPoolSize, backgroundPoolSize,
        keepAliveTime, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(backgroundPoolQueueSize));
    backgroundOperationPool.allowCoreThreadTimeOut(true);
    addService(operationManager);
    super.init(hiveConf);
  }

  private void applyAuthorizationConfigPolicy(HiveConf newHiveConf) throws HiveException {
    // authorization setup using SessionState should be revisited eventually, as
    // authorization and authentication are not session specific settings
    SessionState ss = SessionState.start(newHiveConf);
    ss.applyAuthorizationPolicy();
  }

  @Override
  public synchronized void start() {
    super.start();
  }

  @Override
  public synchronized void stop() {
    super.stop();
    if (backgroundOperationPool != null) {
      backgroundOperationPool.shutdown();
      int timeout = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT);
      try {
        backgroundOperationPool.awaitTermination(timeout, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.warn("HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT = " + timeout +
            " seconds has been exceeded. RUNNING background operations will be shut down", e);
      }
    }
  }

  public SessionHandle openSession(TProtocolVersion protocol, String username, String password,
      Map<String, String> sessionConf) throws HiveSQLException {
    return openSession(protocol, username, password, sessionConf, false, null);
  }

  public SessionHandle openSession(TProtocolVersion protocol, String username, String password,
      Map<String, String> sessionConf, boolean withImpersonation, String delegationToken)
          throws HiveSQLException {
    HiveSession session;
    if (withImpersonation) {
      HiveSessionImplwithUGI hiveSessionUgi = new HiveSessionImplwithUGI(protocol, username, password,
        hiveConf, sessionConf, TSetIpAddressProcessor.getUserIpAddress(), delegationToken);
      session = HiveSessionProxy.getProxy(hiveSessionUgi, hiveSessionUgi.getSessionUgi());
      hiveSessionUgi.setProxySession(session);
    } else {
      session = new HiveSessionImpl(protocol, username, password, hiveConf, sessionConf,
          TSetIpAddressProcessor.getUserIpAddress());
    }
    session.setSessionManager(this);
    session.setOperationManager(operationManager);
    session.open();
    handleToSession.put(session.getSessionHandle(), session);

    try {
      executeSessionHooks(session);
    } catch (Exception e) {
      throw new HiveSQLException("Failed to execute session hooks", e);
    }
    return session.getSessionHandle();
  }

  public void closeSession(SessionHandle sessionHandle) throws HiveSQLException {
    HiveSession session = handleToSession.remove(sessionHandle);
    if (session == null) {
      throw new HiveSQLException("Session does not exist!");
    }
    session.close();
  }

  public HiveSession getSession(SessionHandle sessionHandle) throws HiveSQLException {
    HiveSession session = handleToSession.get(sessionHandle);
    if (session == null) {
      throw new HiveSQLException("Invalid SessionHandle: " + sessionHandle);
    }
    return session;
  }

  public OperationManager getOperationManager() {
    return operationManager;
  }

  private static ThreadLocal<String> threadLocalIpAddress = new ThreadLocal<String>() {
    @Override
    protected synchronized String initialValue() {
      return null;
    }
  };

  public static void setIpAddress(String ipAddress) {
    threadLocalIpAddress.set(ipAddress);
  }

  public static void clearIpAddress() {
    threadLocalIpAddress.remove();
  }

  public static String getIpAddress() {
    return threadLocalIpAddress.get();
  }

  private static ThreadLocal<String> threadLocalUserName = new ThreadLocal<String>(){
    @Override
    protected synchronized String initialValue() {
      return null;
    }
  };

  public static void setUserName(String userName) {
    threadLocalUserName.set(userName);
  }

  public static void clearUserName() {
    threadLocalUserName.remove();
  }

  public static String getUserName() {
    return threadLocalUserName.get();
  }

  private static ThreadLocal<String> threadLocalProxyUserName = new ThreadLocal<String>(){
    @Override
    protected synchronized String initialValue() {
      return null;
    }
  };

  public static void setProxyUserName(String userName) {
    LOG.debug("setting proxy user name based on query param to: " + userName);
    threadLocalProxyUserName.set(userName);
  }

  public static String getProxyUserName() {
    return threadLocalProxyUserName.get();
  }

  public static void clearProxyUserName() {
    threadLocalProxyUserName.remove();
  }

  // execute session hooks
  private void executeSessionHooks(HiveSession session) throws Exception {
    List<HiveSessionHook> sessionHooks = HookUtils.getHooks(hiveConf,
        HiveConf.ConfVars.HIVE_SERVER2_SESSION_HOOK, HiveSessionHook.class);
    for (HiveSessionHook sessionHook : sessionHooks) {
      sessionHook.run(new HiveSessionHookContextImpl(session));
    }
  }

  public Future<?> submitBackgroundOperation(Runnable r) {
    return backgroundOperationPool.submit(r);
  }

}

