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
package org.apache.hadoop.hive.ql.exec.tez;

import java.util.concurrent.TimeoutException;

import java.util.concurrent.TimeUnit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;


/** Workload management entry point for HS2. */
public class WorkloadManager
    implements TezSessionPoolSession.Manager, SessionExpirationTracker.RestartImpl {
  private static final Logger LOG = LoggerFactory.getLogger(WorkloadManager.class);
  // TODO: this is a temporary setting that will go away, so it's not in HiveConf.
  public static final String TEST_WM_CONFIG = "hive.test.workload.management";

  private final HiveConf conf;
  private final TezSessionPool<WmTezSession> sessions;
  private final SessionExpirationTracker expirationTracker;
  private final RestrictedConfigChecker restrictedConfig;
  private final QueryAllocationManager allocationManager;
  private final String yarnQueue;
  // TODO: it's not clear that we need to track this - unlike PoolManager we don't have non-pool
  //       sessions, so the pool itself could internally track the sessions it gave out, since
  //       calling close on an unopened session is probably harmless.
  private final IdentityHashMap<TezSessionPoolSession, Boolean> openSessions =
      new IdentityHashMap<>();
  /** Sessions given out (i.e. between get... and return... calls), separated by Hive pool. */
  private final ReentrantReadWriteLock poolsLock = new ReentrantReadWriteLock();
  private final HashMap<String, PoolState> pools = new HashMap<>();
  private final int amRegistryTimeoutMs;

  private static class PoolState {
    // Add stuff here as WM is implemented.
    private final Object lock = new Object();
    private final List<WmTezSession> sessions = new ArrayList<>();
  }

  // TODO: this is temporary before HiveServerEnvironment is merged.
  private static volatile WorkloadManager INSTANCE;
  public static WorkloadManager getInstance() {
    WorkloadManager wm = INSTANCE;
    assert wm != null;
    return wm;
  }

  public static boolean isInUse(Configuration conf) {
    return INSTANCE != null && conf.getBoolean(TEST_WM_CONFIG, false);
  }

  /** Called once, when HS2 initializes. */
  public static WorkloadManager create(String yarnQueue, HiveConf conf) {
    assert INSTANCE == null;
    Token<JobTokenIdentifier> amsToken = createAmsToken();
    // We could derive the expected number of AMs to pass in.
    LlapPluginEndpointClient amComm = new LlapPluginEndpointClientImpl(conf, amsToken, -1);
    QueryAllocationManager qam = new GuaranteedTasksAllocator(conf, amComm);
    // TODO: Hardcode one session for now; initial policies should be passed in.
    return (INSTANCE = new WorkloadManager(yarnQueue, conf, 1, qam, amsToken));
  }

  private static Token<JobTokenIdentifier> createAmsToken() {
    if (!UserGroupInformation.isSecurityEnabled()) return null;
    // This application ID is completely bogus.
    ApplicationId id = ApplicationId.newInstance(
        System.nanoTime(), (int)(System.nanoTime() % 100000));
    JobTokenIdentifier identifier = new JobTokenIdentifier(new Text(id.toString()));
    JobTokenSecretManager jobTokenManager = new JobTokenSecretManager();
    Token<JobTokenIdentifier> sessionToken = new Token<>(identifier, jobTokenManager);
    sessionToken.setService(identifier.getJobId());
    return sessionToken;
  }

  @VisibleForTesting
  WorkloadManager(String yarnQueue, HiveConf conf, int numSessions,
      QueryAllocationManager qam, Token<JobTokenIdentifier> amsToken) {
    this.yarnQueue = yarnQueue;
    this.conf = conf;
    initializeHivePools();

    this.amRegistryTimeoutMs = (int)HiveConf.getTimeVar(
        conf, ConfVars.HIVE_SERVER2_TEZ_WM_AM_REGISTRY_TIMEOUT, TimeUnit.MILLISECONDS);
    sessions = new TezSessionPool<>(conf, numSessions, true);
    restrictedConfig = new RestrictedConfigChecker(conf);
    allocationManager = qam;
    // Only creates the expiration tracker if expiration is configured.
    expirationTracker = SessionExpirationTracker.create(conf, this);
    for (int i = 0; i < numSessions; i++) {
      sessions.addInitialSession(createSession());
    }
  }

  private void initializeHivePools() {
    // TODO: real implementation
    poolsLock.writeLock().lock();
    try {
      pools.put("llap", new PoolState());
    } finally {
      poolsLock.writeLock().unlock();
    }
  }

  public TezSessionState getSession(
      TezSessionState session, String userName, HiveConf conf) throws Exception {
    validateConfig(conf);
    String poolName = mapSessionToPoolName(userName);
    // TODO: do query parallelism enforcement here based on the policies and pools.
    while (true) {
      WmTezSession result = checkSessionForReuse(session);
      // TODO: when proper AM management is implemented, we should call tryGet... here, because the
      //       parallelism will be enforced here, and pool would always have a session for us.
      result = (result == null ? sessions.getSession() : result);
      result.setQueueName(yarnQueue);
      result.setPoolName(poolName);
      if (!ensureAmIsRegistered(result)) continue; // Try another.
      redistributePoolAllocations(poolName, result, null);
      return result;
    }
  }

  @VisibleForTesting
  protected boolean ensureAmIsRegistered(WmTezSession session) throws Exception {
    // Make sure AM is ready to use and registered with AM registry.
    try {
      session.waitForAmPluginInfo(amRegistryTimeoutMs);
    } catch (TimeoutException ex) {
      LOG.error("Timed out waiting for AM registry information for " + session.getSessionId());
      session.destroy();
      return false;
    }
    return true;
  }

  private void redistributePoolAllocations(
      String poolName, WmTezSession sessionToAdd, WmTezSession sessionToRemove) {
    List<WmTezSession> sessionsToUpdate = null;
    double totalAlloc = 0;
    assert sessionToAdd == null || poolName.equals(sessionToAdd.getPoolName());
    assert sessionToRemove == null || poolName.equals(sessionToRemove.getPoolName());
    poolsLock.readLock().lock();
    try {
      PoolState pool = pools.get(poolName);
      synchronized (pool.lock) {
        // This should be a 2nd order fn but it's too much pain in Java for one LOC.
        if (sessionToAdd != null) {
          pool.sessions.add(sessionToAdd);
        }
        if (sessionToRemove != null) {
          if (!pool.sessions.remove(sessionToRemove)) {
            LOG.error("Session " + sessionToRemove + " could not be removed from the pool");
          }
          sessionToRemove.setClusterFraction(0);
        }
        totalAlloc = updatePoolAllocations(pool.sessions);
        sessionsToUpdate = new ArrayList<>(pool.sessions);
      }
    } finally {
      poolsLock.readLock().unlock();
    }
    allocationManager.updateSessionsAsync(totalAlloc, sessionsToUpdate);
  }

  private WmTezSession checkSessionForReuse(TezSessionState session) throws Exception {
    if (session == null) return null;
    WmTezSession result = null;
    if (session instanceof WmTezSession) {
      result = (WmTezSession) session;
      if (result.isOwnedBy(this)) {
        return result;
      }
      // TODO: this should never happen, at least for now. Throw?
      LOG.warn("Attempting to reuse a session not belonging to us: " + result);
      result.returnToSessionManager();
      return null;
    }
    LOG.warn("Attempting to reuse a non-WM session for workload management:" + session);
    if (session instanceof TezSessionPoolSession) {
      session.returnToSessionManager();
    } else {
      session.close(false); // This is a non-pool session, get rid of it.
    }
    return null;
  }

  private double updatePoolAllocations(List<WmTezSession> sessions) {
    // TODO: real implementation involving in-the-pool policy interface, etc.
    double allocation = 1.0 / sessions.size();
    for (WmTezSession session : sessions) {
      session.setClusterFraction(allocation);
    }
    return 1.0;
  }

  private String mapSessionToPoolName(String userName) {
    // TODO: real implementation, probably calling into another class initialized with policies.
    return "llap";
  }

  private void validateConfig(HiveConf conf) throws HiveException {
    String queueName = conf.get(TezConfiguration.TEZ_QUEUE_NAME);
    if ((queueName != null) && !queueName.isEmpty()) {
      LOG.warn("Ignoring " + TezConfiguration.TEZ_QUEUE_NAME + "=" + queueName);
      conf.set(TezConfiguration.TEZ_QUEUE_NAME, yarnQueue);
    }
    if (conf.getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS)) {
      // Should this also just be ignored? Throw for now, doAs unlike queue is often set by admin.
      throw new HiveException(ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname + " is not supported");
    }
    if (restrictedConfig != null) {
      restrictedConfig.validate(conf);
    }
  }

  public void start() throws Exception {
    sessions.startInitialSessions();
    if (expirationTracker != null) {
      expirationTracker.start();
    }
    allocationManager.start();
  }

  public void stop() throws Exception {
    List<TezSessionPoolSession> sessionsToClose = null;
    synchronized (openSessions) {
      sessionsToClose = new ArrayList<TezSessionPoolSession>(openSessions.keySet());
    }
    for (TezSessionState sessionState : sessionsToClose) {
      sessionState.close(false);
    }
    if (expirationTracker != null) {
      expirationTracker.stop();
    }
    allocationManager.stop();
  }

  private WmTezSession createSession() {
    WmTezSession session = createSessionObject(TezSessionState.makeSessionId());
    session.setQueueName(yarnQueue);
    session.setDefault();
    LOG.info("Created new interactive session " + session.getSessionId());
    return session;
  }

  @VisibleForTesting
  protected WmTezSession createSessionObject(String sessionId) {
    return new WmTezSession(sessionId, this, expirationTracker, new HiveConf(conf));
  }

  @Override
  public void returnAfterUse(TezSessionPoolSession session) throws Exception {
    boolean isInterrupted = Thread.interrupted();
    try {
      WmTezSession wmSession = ensureOwnedSession(session);
      redistributePoolAllocations(wmSession.getPoolName(), null, wmSession);
      sessions.returnSession((WmTezSession) session);
    } finally {
      // Reset the interrupt status.
      if (isInterrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }


  /** Closes a running (expired) pool session and reopens it. */
  @Override
  public void closeAndReopenPoolSession(TezSessionPoolSession oldSession) throws Exception {
    sessions.replaceSession(ensureOwnedSession(oldSession), createSession(), false, null, null);
  }

  private WmTezSession ensureOwnedSession(TezSessionState oldSession) {
    if (!(oldSession instanceof WmTezSession) || !((WmTezSession)oldSession).isOwnedBy(this)) {
      throw new AssertionError("Not a WM session " + oldSession);
    }
    WmTezSession session = (WmTezSession) oldSession;
    return session;
  }

  /** Called by TezSessionPoolSession when opened. */
  @Override
  public void registerOpenSession(TezSessionPoolSession session) {
    synchronized (openSessions) {
      openSessions.put(session, true);
    }
  }

  /** Called by TezSessionPoolSession when closed. */
  @Override
  public void unregisterOpenSession(TezSessionPoolSession session) {
    synchronized (openSessions) {
      openSessions.remove(session);
    }
  }

  @VisibleForTesting
  public SessionExpirationTracker getExpirationTracker() {
    return expirationTracker;
  }

  @Override
  public TezSessionState reopen(TezSessionState session, Configuration conf,
      String[] additionalFiles) throws Exception {
    WmTezSession oldSession = ensureOwnedSession(session), newSession = createSession();
    newSession.setPoolName(oldSession.getPoolName());
    HiveConf sessionConf = session.getConf();
    if (sessionConf == null) {
      LOG.warn("Session configuration is null for " + session);
      // default queue name when the initial session was created
      sessionConf = new HiveConf(conf, WorkloadManager.class);
    }
    sessions.replaceSession(oldSession, newSession, true, additionalFiles, sessionConf);
    // We are going to immediately give this session out, so ensure AM registry.
    if (!ensureAmIsRegistered(newSession)) {
      throw new Exception("Session is not usable after reopen");
    }
    redistributePoolAllocations(oldSession.getPoolName(), newSession, oldSession);
    return newSession;
  }

  @Override
  public void destroy(TezSessionState session) throws Exception {
    LOG.warn("Closing a pool session because of retry failure.");
    // We never want to lose pool sessions. Replace it instead; al trigger duck redistribution.
    WmTezSession wmSession = ensureOwnedSession(session);
    closeAndReopenPoolSession(wmSession);
    redistributePoolAllocations(wmSession.getPoolName(), null, wmSession);
  }

  protected final HiveConf getConf() {
    return conf;
  }
}
