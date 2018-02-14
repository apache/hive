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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.common.MetricsVariable;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.hooks.HookUtils;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.operation.Operation;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.server.HiveServer2;
import org.apache.hive.service.server.ThreadFactoryWithGarbageCleanup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SessionManager.
 *
 */
public class SessionManager extends CompositeService {

  public static final String HIVERCFILE = ".hiverc";
  private static final Logger LOG = LoggerFactory.getLogger(CompositeService.class);
  private HiveConf hiveConf;
  private final Map<SessionHandle, HiveSession> handleToSession =
      new ConcurrentHashMap<SessionHandle, HiveSession>();
  private final Map<String, LongAdder> connectionsCount = new ConcurrentHashMap<>();
  private int userLimit;
  private int ipAddressLimit;
  private int userIpAddressLimit;
  private final OperationManager operationManager = new OperationManager();
  private ThreadPoolExecutor backgroundOperationPool;
  private boolean isOperationLogEnabled;
  private File operationLogRootDir;

  private long checkInterval;
  private long sessionTimeout;
  private boolean checkOperation;

  private volatile boolean shutdown;
  // The HiveServer2 instance running this service
  private final HiveServer2 hiveServer2;
  private String sessionImplWithUGIclassName;
  private String sessionImplclassName;

  public SessionManager(HiveServer2 hiveServer2) {
    super(SessionManager.class.getSimpleName());
    this.hiveServer2 = hiveServer2;
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
    //Create operation log root directory, if operation logging is enabled
    if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED)) {
      initOperationLogRootDir();
    }
    createBackgroundOperationPool();
    addService(operationManager);
    initSessionImplClassName();
    Metrics metrics = MetricsFactory.getInstance();
    if(metrics != null){
      registerOpenSesssionMetrics(metrics);
      registerActiveSesssionMetrics(metrics);
    }

    userLimit = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER);
    ipAddressLimit = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_IPADDRESS);
    userIpAddressLimit = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_LIMIT_CONNECTIONS_PER_USER_IPADDRESS);
    LOG.info("Connections limit are user: {} ipaddress: {} user-ipaddress: {}", userLimit, ipAddressLimit,
      userIpAddressLimit);
    super.init(hiveConf);
  }

  private void registerOpenSesssionMetrics(Metrics metrics) {
    MetricsVariable<Integer> openSessionCnt = new MetricsVariable<Integer>() {
      @Override
      public Integer getValue() {
        return getSessions().size();
      }
    };
    MetricsVariable<Integer> openSessionTime = new MetricsVariable<Integer>() {
      @Override
      public Integer getValue() {
        long sum = 0;
        long currentTime = System.currentTimeMillis();
        for (HiveSession s : getSessions()) {
          sum += currentTime - s.getCreationTime();
        }
        // in case of an overflow return -1
        return (int) sum != sum ? -1 : (int) sum;
      }
    };
    metrics.addGauge(MetricsConstant.HS2_OPEN_SESSIONS, openSessionCnt);
    metrics.addRatio(MetricsConstant.HS2_AVG_OPEN_SESSION_TIME, openSessionTime, openSessionCnt);
  }

  private void registerActiveSesssionMetrics(Metrics metrics) {
    MetricsVariable<Integer> activeSessionCnt = new MetricsVariable<Integer>() {
      @Override
      public Integer getValue() {
        Iterable<HiveSession> filtered = Iterables.filter(getSessions(), new Predicate<HiveSession>() {
          @Override
          public boolean apply(HiveSession hiveSession) {
            return hiveSession.getNoOperationTime() == 0L;
          }
        });
        return Iterables.size(filtered);
      }
    };
    MetricsVariable<Integer> activeSessionTime = new MetricsVariable<Integer>() {
      @Override
      public Integer getValue() {
        long sum = 0;
        long currentTime = System.currentTimeMillis();
        for (HiveSession s : getSessions()) {
          if (s.getNoOperationTime() == 0L) {
            sum += currentTime - s.getLastAccessTime();
          }
        }
        // in case of an overflow return -1
        return (int) sum != sum ? -1 : (int) sum;
      }
    };
    metrics.addGauge(MetricsConstant.HS2_ACTIVE_SESSIONS, activeSessionCnt);
    metrics.addRatio(MetricsConstant.HS2_AVG_ACTIVE_SESSION_TIME, activeSessionTime, activeSessionCnt);
  }

  private void initSessionImplClassName() {
    this.sessionImplclassName = hiveConf.getVar(ConfVars.HIVE_SESSION_IMPL_CLASSNAME);
    this.sessionImplWithUGIclassName = hiveConf.getVar(ConfVars.HIVE_SESSION_IMPL_WITH_UGI_CLASSNAME);
  }

  private void createBackgroundOperationPool() {
    int poolSize = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_THREADS);
    LOG.info("HiveServer2: Background operation thread pool size: " + poolSize);
    int poolQueueSize = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_WAIT_QUEUE_SIZE);
    LOG.info("HiveServer2: Background operation thread wait queue size: " + poolQueueSize);
    long keepAliveTime = HiveConf.getTimeVar(
        hiveConf, ConfVars.HIVE_SERVER2_ASYNC_EXEC_KEEPALIVE_TIME, TimeUnit.SECONDS);
    LOG.info(
        "HiveServer2: Background operation thread keepalive time: " + keepAliveTime + " seconds");

    // Create a thread pool with #poolSize threads
    // Threads terminate when they are idle for more than the keepAliveTime
    // A bounded blocking queue is used to queue incoming operations, if #operations > poolSize
    String threadPoolName = "HiveServer2-Background-Pool";
    final BlockingQueue queue = new LinkedBlockingQueue<Runnable>(poolQueueSize);
    backgroundOperationPool = new ThreadPoolExecutor(poolSize, poolSize,
        keepAliveTime, TimeUnit.SECONDS, queue,
        new ThreadFactoryWithGarbageCleanup(threadPoolName));
    backgroundOperationPool.allowCoreThreadTimeOut(true);

    checkInterval = HiveConf.getTimeVar(
        hiveConf, ConfVars.HIVE_SERVER2_SESSION_CHECK_INTERVAL, TimeUnit.MILLISECONDS);
    sessionTimeout = HiveConf.getTimeVar(
        hiveConf, ConfVars.HIVE_SERVER2_IDLE_SESSION_TIMEOUT, TimeUnit.MILLISECONDS);
    checkOperation = HiveConf.getBoolVar(hiveConf,
        ConfVars.HIVE_SERVER2_IDLE_SESSION_CHECK_OPERATION);

    Metrics m = MetricsFactory.getInstance();
    if (m != null) {
      m.addGauge(MetricsConstant.EXEC_ASYNC_QUEUE_SIZE, new MetricsVariable() {
        @Override
        public Object getValue() {
          return queue.size();
        }
      });
      m.addGauge(MetricsConstant.EXEC_ASYNC_POOL_SIZE, new MetricsVariable() {
        @Override
        public Object getValue() {
          return backgroundOperationPool.getPoolSize();
        }
      });
    }
  }

  private void initOperationLogRootDir() {
    operationLogRootDir = new File(
        hiveConf.getVar(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LOG_LOCATION));
    isOperationLogEnabled = true;

    if (operationLogRootDir.exists() && !operationLogRootDir.isDirectory()) {
      LOG.warn("The operation log root directory exists, but it is not a directory: " +
          operationLogRootDir.getAbsolutePath());
      isOperationLogEnabled = false;
    }

    if (!operationLogRootDir.exists()) {
      if (!operationLogRootDir.mkdirs()) {
        LOG.warn("Unable to create operation log root directory: " +
            operationLogRootDir.getAbsolutePath());
        isOperationLogEnabled = false;
      }
    }

    if (isOperationLogEnabled) {
      LOG.info("Operation log root directory is created: " + operationLogRootDir.getAbsolutePath());
      try {
        FileUtils.forceDeleteOnExit(operationLogRootDir);
      } catch (IOException e) {
        LOG.warn("Failed to schedule cleanup HS2 operation logging root dir: " +
            operationLogRootDir.getAbsolutePath(), e);
      }
    }
  }

  @Override
  public synchronized void start() {
    super.start();
    if (checkInterval > 0) {
      startTimeoutChecker();
    }
  }

  private final Object timeoutCheckerLock = new Object();

  private void startTimeoutChecker() {
    final long interval = Math.max(checkInterval, 3000l);  // minimum 3 seconds
    final Runnable timeoutChecker = new Runnable() {
      @Override
      public void run() {
        sleepFor(interval);
        while (!shutdown) {
          long current = System.currentTimeMillis();
          for (HiveSession session : new ArrayList<HiveSession>(handleToSession.values())) {
            if (shutdown) {
              break;
            }
            if (sessionTimeout > 0 && session.getLastAccessTime() + sessionTimeout <= current
                && (!checkOperation || session.getNoOperationTime() > sessionTimeout)) {
              SessionHandle handle = session.getSessionHandle();
              LOG.warn("Session " + handle + " is Timed-out (last access : " +
                  new Date(session.getLastAccessTime()) + ") and will be closed");
              try {
                closeSession(handle);
              } catch (HiveSQLException e) {
                LOG.warn("Exception is thrown closing session " + handle, e);
              } finally {
                Metrics metrics = MetricsFactory.getInstance();
                if (metrics != null) {
                  metrics.incrementCounter(MetricsConstant.HS2_ABANDONED_SESSIONS);
                }
              }
            } else {
              session.closeExpiredOperations();
            }
          }
          sleepFor(interval);
        }
      }

      private void sleepFor(long interval) {
        synchronized (timeoutCheckerLock) {
          try {
            timeoutCheckerLock.wait(interval);
          } catch (InterruptedException e) {
            // Ignore, and break.
          }
        }
      }
    };
    backgroundOperationPool.execute(timeoutChecker);
  }

  private void shutdownTimeoutChecker() {
    shutdown = true;
    synchronized (timeoutCheckerLock) {
      timeoutCheckerLock.notify();
    }
  }


  @Override
  public synchronized void stop() {
    super.stop();
    shutdownTimeoutChecker();
    if (backgroundOperationPool != null) {
      backgroundOperationPool.shutdown();
      long timeout = hiveConf.getTimeVar(
          ConfVars.HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS);
      try {
        backgroundOperationPool.awaitTermination(timeout, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.warn("HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT = " + timeout +
            " seconds has been exceeded. RUNNING background operations will be shut down", e);
      }
      backgroundOperationPool = null;
    }
    cleanupLoggingRootDir();
  }

  private void cleanupLoggingRootDir() {
    if (isOperationLogEnabled) {
      try {
        FileUtils.forceDelete(operationLogRootDir);
      } catch (Exception e) {
        LOG.warn("Failed to cleanup root dir of HS2 logging: " + operationLogRootDir
            .getAbsolutePath(), e);
      }
    }
  }

  public SessionHandle openSession(TProtocolVersion protocol, String username, String password, String ipAddress,
      Map<String, String> sessionConf) throws HiveSQLException {
    return openSession(protocol, username, password, ipAddress, sessionConf, false, null);
  }

  /**
   * Opens a new session and creates a session handle.
   * The username passed to this method is the effective username.
   * If withImpersonation is true (==doAs true) we wrap all the calls in HiveSession
   * within a UGI.doAs, where UGI corresponds to the effective user.
   * @see org.apache.hive.service.cli.thrift.ThriftCLIService#getUserName(TOpenSessionReq)
   *
   * @param protocol
   * @param username
   * @param password
   * @param ipAddress
   * @param sessionConf
   * @param withImpersonation
   * @param delegationToken
   * @return
   * @throws HiveSQLException
   */
  public SessionHandle openSession(TProtocolVersion protocol, String username, String password, String ipAddress,
      Map<String, String> sessionConf, boolean withImpersonation, String delegationToken)
          throws HiveSQLException {
    return createSession(null, protocol, username, password, ipAddress, sessionConf,
      withImpersonation, delegationToken).getSessionHandle();
  }
  public HiveSession createSession(SessionHandle sessionHandle, TProtocolVersion protocol, String username,
    String password, String ipAddress, Map<String, String> sessionConf, boolean withImpersonation,
    String delegationToken)
    throws HiveSQLException {

    // if client proxies connection, use forwarded ip-addresses instead of just the gateway
    final List<String> forwardedAddresses = getForwardedAddresses();
    incrementConnections(username, ipAddress, forwardedAddresses);

    HiveSession session;
    // If doAs is set to true for HiveServer2, we will create a proxy object for the session impl.
    // Within the proxy object, we wrap the method call in a UserGroupInformation#doAs
    if (withImpersonation) {
      HiveSessionImplwithUGI hiveSessionUgi;
      if (sessionImplWithUGIclassName == null) {
        hiveSessionUgi = new HiveSessionImplwithUGI(sessionHandle, protocol, username, password,
            hiveConf, ipAddress, delegationToken, forwardedAddresses);
      } else {
        try {
          Class<?> clazz = Class.forName(sessionImplWithUGIclassName);
          Constructor<?> constructor = clazz.getConstructor(SessionHandle.class, TProtocolVersion.class, String.class,
            String.class, HiveConf.class, String.class, String.class, List.class);
          hiveSessionUgi = (HiveSessionImplwithUGI) constructor.newInstance(sessionHandle,
              protocol, username, password, hiveConf, ipAddress, delegationToken, forwardedAddresses);
        } catch (Exception e) {
          throw new HiveSQLException("Cannot initialize session class:" + sessionImplWithUGIclassName);
        }
      }
      session = HiveSessionProxy.getProxy(hiveSessionUgi, hiveSessionUgi.getSessionUgi());
      hiveSessionUgi.setProxySession(session);
    } else {
      if (sessionImplclassName == null) {
        session = new HiveSessionImpl(sessionHandle, protocol, username, password, hiveConf,
          ipAddress, forwardedAddresses);
      } else {
        try {
        Class<?> clazz = Class.forName(sessionImplclassName);
        Constructor<?> constructor = clazz.getConstructor(SessionHandle.class, TProtocolVersion.class,
          String.class, String.class, HiveConf.class, String.class, List.class);
        session = (HiveSession) constructor.newInstance(sessionHandle, protocol, username, password,
          hiveConf, ipAddress, forwardedAddresses);
        } catch (Exception e) {
          throw new HiveSQLException("Cannot initialize session class:" + sessionImplclassName, e);
        }
      }
    }
    session.setSessionManager(this);
    session.setOperationManager(operationManager);
    try {
      session.open(sessionConf);
    } catch (Exception e) {
      LOG.warn("Failed to open session", e);
      try {
        session.close();
      } catch (Throwable t) {
        LOG.warn("Error closing session", t);
      }
      session = null;
      throw new HiveSQLException("Failed to open new session: " + e.getMessage(), e);
    }
    if (isOperationLogEnabled) {
      session.setOperationLogSessionDir(operationLogRootDir);
    }
    try {
      executeSessionHooks(session);
    } catch (Exception e) {
      LOG.warn("Failed to execute session hooks", e);
      try {
        session.close();
      } catch (Throwable t) {
        LOG.warn("Error closing session", t);
      }
      session = null;
      throw new HiveSQLException("Failed to execute session hooks: " + e.getMessage(), e);
    }
    handleToSession.put(session.getSessionHandle(), session);
    LOG.info("Session opened, " + session.getSessionHandle() + ", current sessions:" + getOpenSessionCount());
    return session;
  }

  private void incrementConnections(final String username, final String ipAddress,
    final List<String> forwardedAddresses) throws HiveSQLException {
    final String clientIpAddress = getOriginClientIpAddress(ipAddress, forwardedAddresses);

    String violation = anyViolations(username, clientIpAddress);
    // increment the counters only when there are no violations
    if (violation == null) {
      if (trackConnectionsPerUser(username)) {
        connectionsCount.computeIfAbsent(username, k -> new LongAdder()).increment();
      }

      if (trackConnectionsPerIpAddress(clientIpAddress)) {
        connectionsCount.computeIfAbsent(clientIpAddress, k -> new LongAdder()).increment();
      }

      if (trackConnectionsPerUserIpAddress(username, clientIpAddress)) {
        connectionsCount.computeIfAbsent(username + ":" + clientIpAddress, k -> new LongAdder()).increment();
      }
    } else {
      LOG.error(violation);
      throw new HiveSQLException(violation);
    }
  }

  private String getOriginClientIpAddress(final String ipAddress, final List<String> forwardedAddresses) {
    if (forwardedAddresses == null || forwardedAddresses.isEmpty()) {
      return ipAddress;
    }
    // order of forwarded ips per X-Forwarded-For http spec (client, proxy1, proxy2)
    return forwardedAddresses.get(0);
  }

  private void decrementConnections(final HiveSession session) {
    final String username = session.getUserName();
    final String clientIpAddress = getOriginClientIpAddress(session.getIpAddress(), session.getForwardedAddresses());
    if (trackConnectionsPerUser(username)) {
      connectionsCount.computeIfPresent(username, (k, v) -> v).decrement();
    }

    if (trackConnectionsPerIpAddress(clientIpAddress)) {
      connectionsCount.computeIfPresent(clientIpAddress, (k, v) -> v).decrement();
    }

    if (trackConnectionsPerUserIpAddress(username, clientIpAddress)) {
      connectionsCount.computeIfPresent(username + ":" + clientIpAddress, (k, v) -> v).decrement();
    }
  }

  private String anyViolations(final String username, final String ipAddress) {
    if (trackConnectionsPerUser(username) && !withinLimits(username, userLimit)) {
      return "Connection limit per user reached (user: " + username + " limit: " + userLimit + ")";
    }

    if (trackConnectionsPerIpAddress(ipAddress) && !withinLimits(ipAddress, ipAddressLimit)) {
      return "Connection limit per ipaddress reached (ipaddress: " + ipAddress + " limit: " + ipAddressLimit + ")";
    }

    if (trackConnectionsPerUserIpAddress(username, ipAddress) &&
      !withinLimits(username + ":" + ipAddress, userIpAddressLimit)) {
      return "Connection limit per user:ipaddress reached (user:ipaddress: " + username + ":" + ipAddress + " limit: " +
        userIpAddressLimit + ")";
    }

    return null;
  }

  private boolean trackConnectionsPerUserIpAddress(final String username, final String ipAddress) {
    return userIpAddressLimit > 0 && username != null && !username.isEmpty() && ipAddress != null &&
      !ipAddress.isEmpty();
  }

  private boolean trackConnectionsPerIpAddress(final String ipAddress) {
    return ipAddressLimit > 0 && ipAddress != null && !ipAddress.isEmpty();
  }

  private boolean trackConnectionsPerUser(final String username) {
    return userLimit > 0 && username != null && !username.isEmpty();
  }

  private boolean withinLimits(final String track, final int limit) {
    if (connectionsCount.containsKey(track)) {
      final int connectionCount = connectionsCount.get(track).intValue();
      if (connectionCount >= limit) {
        return false;
      }
    }
    return true;
  }

  public synchronized void closeSession(SessionHandle sessionHandle) throws HiveSQLException {
    HiveSession session = handleToSession.remove(sessionHandle);
    if (session == null) {
      throw new HiveSQLException("Session does not exist: " + sessionHandle);
    }
    LOG.info("Session closed, " + sessionHandle + ", current sessions:" + getOpenSessionCount());
    try {
      session.close();
    } finally {
      decrementConnections(session);
      // Shutdown HiveServer2 if it has been deregistered from ZooKeeper and has no active sessions
      if (!(hiveServer2 == null) && (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY))
          && (hiveServer2.isDeregisteredWithZooKeeper())) {
        // Asynchronously shutdown this instance of HiveServer2,
        // if there are no active client sessions
        if (getOpenSessionCount() == 0) {
          LOG.info("This instance of HiveServer2 has been removed from the list of server "
              + "instances available for dynamic service discovery. "
              + "The last client session has ended - will shutdown now.");
          Thread shutdownThread = new Thread() {
            @Override
            public void run() {
              hiveServer2.stop();
            }
          };
          shutdownThread.start();
        }
      }
    }
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

  private static ThreadLocal<String> threadLocalIpAddress = new ThreadLocal<String>();

  public static void setIpAddress(String ipAddress) {
    threadLocalIpAddress.set(ipAddress);
  }

  public static void clearIpAddress() {
    threadLocalIpAddress.remove();
  }

  public static String getIpAddress() {
    return threadLocalIpAddress.get();
  }

  private static ThreadLocal<List<String>> threadLocalForwardedAddresses = new ThreadLocal<List<String>>();

  public static void setForwardedAddresses(List<String> ipAddress) {
    threadLocalForwardedAddresses.set(ipAddress);
  }

  public static void clearForwardedAddresses() {
    threadLocalForwardedAddresses.remove();
  }

  public static List<String> getForwardedAddresses() {
    return threadLocalForwardedAddresses.get();
  }

  private static ThreadLocal<String> threadLocalUserName = new ThreadLocal<String>(){
    @Override
    protected String initialValue() {
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
    protected String initialValue() {
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
    List<HiveSessionHook> sessionHooks =
        HookUtils.readHooksFromConf(hiveConf, HiveConf.ConfVars.HIVE_SERVER2_SESSION_HOOK);
    for (HiveSessionHook sessionHook : sessionHooks) {
      sessionHook.run(new HiveSessionHookContextImpl(session));
    }
  }

  public Future<?> submitBackgroundOperation(Runnable r) {
    return backgroundOperationPool.submit(r);
  }

  public Collection<Operation> getOperations() {
    return operationManager.getOperations();
  }

  public Collection<HiveSession> getSessions() {
    return Collections.unmodifiableCollection(handleToSession.values());
  }

  public int getOpenSessionCount() {
    return handleToSession.size();
  }

  public String getHiveServer2HostName() throws Exception {
    if (hiveServer2 == null) {
      return null;
    }
    return hiveServer2.getServerHost();
  }
}

