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

import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.wm.SessionTriggerProvider;
import org.apache.hadoop.hive.ql.wm.Trigger;
import org.apache.hadoop.hive.ql.wm.TriggerActionHandler;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** Workload management entry point for HS2. */
public class WorkloadManager extends TezSessionPoolSession.AbstractTriggerValidator
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
  // Note: it's not clear that we need to track this - unlike PoolManager we don't have non-pool
  //       sessions, so the pool itself could internally track the sessions it gave out, since
  //       calling close on an unopened session is probably harmless.
  private final IdentityHashMap<TezSessionPoolSession, Boolean> openSessions =
      new IdentityHashMap<>();
  private final int amRegistryTimeoutMs;


  /** Sessions given out (i.e. between get... and return... calls), separated by Hive pool. */
  private final ReentrantReadWriteLock poolsLock = new ReentrantReadWriteLock();
  private final Map<String, PoolState> pools = new HashMap<>();
  // Used to make sure that waiting getSessions don't block update.
  private int internalPoolsVersion;
  private UserPoolMapping userPoolMapping;

  private SessionTriggerProvider sessionTriggerProvider;
  private TriggerActionHandler triggerActionHandler;
  private TriggerValidatorRunnable triggerValidatorRunnable;

  public static class PoolState {
    // Add stuff here as WM is implemented.
    private final Object lock = new Object();
    private final List<WmTezSession> sessions = new ArrayList<>();
    private final Semaphore sessionsClaimed;

    private final String fullName;
    private final double finalFraction;
    private double finalFractionRemaining;
    private final int queryParallelism;
    private final List<Trigger> triggers = new ArrayList<>();

    public PoolState(String fullName, int queryParallelism, double fraction) {
      this.fullName = fullName;
      this.queryParallelism = queryParallelism;
      // A fair semaphore to ensure correct queue order.
      this.sessionsClaimed = new Semaphore(queryParallelism, true);
      this.finalFraction = this.finalFractionRemaining = fraction;
    }

    @Override
    public String toString() {
      return "[" + fullName + ", query parallelism " + queryParallelism
          + ", fraction of the cluster " + finalFraction + ", fraction used by child pools "
          + (finalFraction - finalFractionRemaining) + ", active sessions " + sessions.size()
          + "]";
    }


    public List<Trigger> getTriggers() {
      return triggers;
    }
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
  public static WorkloadManager create(String yarnQueue, HiveConf conf, TmpResourcePlan plan) {
    assert INSTANCE == null;
    Token<JobTokenIdentifier> amsToken = createAmsToken();
    // We could derive the expected number of AMs to pass in.
    LlapPluginEndpointClient amComm = new LlapPluginEndpointClientImpl(conf, amsToken, -1);
    QueryAllocationManager qam = new GuaranteedTasksAllocator(conf, amComm);
    return (INSTANCE = new WorkloadManager(yarnQueue, conf, qam, amsToken, plan));
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
  WorkloadManager(String yarnQueue, HiveConf conf,
      QueryAllocationManager qam, Token<JobTokenIdentifier> amsToken, TmpResourcePlan plan) {
    this.yarnQueue = yarnQueue;
    this.conf = conf;
    int numSessions = initializeHivePools(plan);
    LOG.info("Initializing with " + numSessions + " total query parallelism");

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
    // TODO: add support for per pool action handler and triggers fetcher (+atomic update to active triggers)
    sessionTriggerProvider = new SessionTriggerProvider();
    triggerActionHandler = new TriggerViolationActionHandler();
    triggerValidatorRunnable = new TriggerValidatorRunnable(getSessionTriggerProvider(), getTriggerActionHandler());
    startTriggerValidator(conf);
  }

  private int initializeHivePools(TmpResourcePlan plan) {
    poolsLock.writeLock().lock();
    try {
      // FIXME: Add Triggers from metastore to poolstate
      // Note: we assume here that plan has been validated beforehand, so we don't verify
      //       that fractions or query parallelism add up.
      int totalQueryParallelism = 0;
      // Use recursion to update parents more conveniently; we don't expect a big tree.
      for (TmpHivePool pool : plan.getRootPools()) {
        totalQueryParallelism += addHivePool(pool, null);
      }
      this.userPoolMapping = new UserPoolMapping(plan.getMappings(), pools.keySet());
      internalPoolsVersion = 0; // Initializing for the first time.
      return totalQueryParallelism;
    } finally {
      poolsLock.writeLock().unlock();
    }
  }

  private final static char POOL_SEPARATOR = '/';
  private int addHivePool(TmpHivePool pool, PoolState parent) {
    String fullName = pool.getName();
    int totalQueryParallelism = pool.getQueryParallelism();
    double fraction = pool.getResourceFraction();
    if (parent != null) {
      fullName = parent.fullName + POOL_SEPARATOR + fullName;
      fraction = parent.finalFraction * pool.getResourceFraction();
      parent.finalFractionRemaining -= fraction;
    }
    PoolState state = new PoolState(fullName, totalQueryParallelism, fraction);
    if (pool.getChildren() != null) {
      for (TmpHivePool child : pool.getChildren()) {
        totalQueryParallelism += addHivePool(child, state);
      }
    }
    LOG.info("Adding Hive pool: " + state);
    pools.put(fullName, state);
    return totalQueryParallelism;
  }

  public TezSessionState getSession(
      TezSessionState session, String userName, HiveConf conf) throws Exception {
    validateConfig(conf);
    WmTezSession result = checkSessionForReuse(session);
    boolean hasAcquired = false;
    String poolName = null;
    while (!hasAcquired) { // This loop handles concurrent plan updates while we are waiting.
      poolName = userPoolMapping.mapSessionToPoolName(userName);
      if (poolName == null) {
        throw new HiveException("Cannot find any pool mapping for user " + userName);
      }
      int internalVersion = -1;
      Semaphore sessionsClaimed = null;
      poolsLock.readLock().lock();
      try {
        PoolState pool = pools.get(poolName);
        if (pool == null) throw new AssertionError("Pool " + poolName + " not found.");
        // No need to take the pool lock, semaphore is final.
        sessionsClaimed = pool.sessionsClaimed;
        internalVersion = internalPoolsVersion;
      } finally {
        poolsLock.readLock().unlock();
      }
      // One cannot simply reuse the session if there are other queries waiting; to maintain
      // fairness, we'll try to take the semaphore instantly, and if that fails we'll return
      // this session back to the pool and potentially give the user a new session later.
      if (result != null) {
        // Handle the special case; the pool may be exactly at capacity w/o queue. In that
        // case, we still should be able to reuse.
        boolean isFromTheSamePool = false;
        String oldPoolName = result.getPoolName();
        if (poolName.equals(oldPoolName)) {
          sessionsClaimed.release();
          isFromTheSamePool = true;
        }
        // Note: we call timed acquire because untimed one ignores fairness.
        hasAcquired = sessionsClaimed.tryAcquire(1, TimeUnit.MILLISECONDS);
        if (hasAcquired) {
          poolsLock.readLock().lock();
          boolean doUnlock = true;
          try {
            if (internalVersion == internalPoolsVersion) {
              if (!isFromTheSamePool) {
                // Free up the usage in the old pool. TODO: ideally not under lock; not critical.
                redistributePoolAllocations(oldPoolName, null, result, true);
              }
              doUnlock = false; // Do not unlock; see below.
              break;
            }
          } finally {
            if (doUnlock) {
              poolsLock.readLock().unlock();
            }
          }
          hasAcquired = false;
        }
        // Note: we are short-circuiting session::returnToSessionManager to supply the flag
        returnAfterUse(result, !isFromTheSamePool);
        result = null;
      }
      // We don't expect frequent updates, so check every second.
      while (!(hasAcquired = (hasAcquired || sessionsClaimed.tryAcquire(1, TimeUnit.SECONDS)))) {
        poolsLock.readLock().lock();
        try {
          if (internalVersion != internalPoolsVersion) break;
        } finally {
          poolsLock.readLock().unlock();
        }
      }
      if (!hasAcquired) continue;
      // Keep it simple for now - everything between acquiring the semaphore and adding the session
      // to the pool state is done under read lock, blocking pool updates. It's possible to make
      // it more granular if needed. The only potentially lengthy operation is waiting for an
      // expired session to be restarted in the session pool.
      poolsLock.readLock().lock();
      if (internalVersion == internalPoolsVersion) break;
      poolsLock.readLock().unlock();
      hasAcquired = false;
    }
    // We are holding the lock from the end of the loop.
    try {
      assert hasAcquired;
      while (true) {
        // TODO: ideally, we'd need to implement tryGet and deal with the valid wait from a session
        //       restarting somehow, as opposed to the invalid case of a session missing from the
        //       pool due to some bug. Keep a "restarting" counter in the pool?
        boolean isFromTheSamePool = false;
        if (result == null) {
          result = sessions.getSession();
        } else {
          // If we are just reusing the session from the same pool, do not adjust allocations.
          isFromTheSamePool = poolName.equals(result.getPoolName());
        }
        result.setQueueName(yarnQueue);
        result.setPoolName(poolName);
        if (!ensureAmIsRegistered(result)) continue; // Try another.
        if (!isFromTheSamePool) {
          redistributePoolAllocations(poolName, result, null, false);
        }
        return result;
      }
    } finally {
      poolsLock.readLock().unlock();
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
      String poolName, WmTezSession sessionToAdd, WmTezSession sessionToRemove,
      boolean releaseParallelism) {
    List<WmTezSession> sessionsToUpdate = null;
    double totalAlloc = 0;
    assert sessionToAdd == null || poolName.equals(sessionToAdd.getPoolName());
    assert sessionToRemove == null || poolName.equals(sessionToRemove.getPoolName());
    poolsLock.readLock().lock();
    boolean hasRemoveFailed = false;
    try {
      PoolState pool = pools.get(poolName);
      synchronized (pool.lock) {
        // This should be a 2nd order fn but it's too much pain in Java for one LOC.
        if (sessionToAdd != null) {
          pool.sessions.add(sessionToAdd);
        }
        if (sessionToRemove != null) {
          // TODO: this assumes that the update process will take the write lock, and make
          //       everything right w.r.t. semaphores, pool names and other stuff, since we might
          //       be releasing a different semaphore from the one we acquired if it's across
          //       the update. If the magic in the update is weak, this may become more involved.
          if (!pool.sessions.remove(sessionToRemove)) {
            LOG.error("Session " + sessionToRemove + " could not be removed from the pool");
            if (releaseParallelism) {
              hasRemoveFailed = true;
            }
          } else if (releaseParallelism) {
            pool.sessionsClaimed.release();
          }
          sessionToRemove.setClusterFraction(0);
        }
        totalAlloc = updatePoolAllocations(pool.sessions, pool.finalFractionRemaining);
        sessionsToUpdate = new ArrayList<>(pool.sessions);
      }
    } finally {
      poolsLock.readLock().unlock();
    }
    allocationManager.updateSessionsAsync(totalAlloc, sessionsToUpdate);
    updateSessionsTriggers();
    if (hasRemoveFailed) {
      throw new AssertionError("Cannot remove the session from the pool and release "
          + "the query slot; HS2 may fail to accept queries");
    }
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

  private double updatePoolAllocations(List<WmTezSession> sessions, double totalFraction) {
    // TODO: real implementation involving in-the-pool policy interface, etc.
    double allocation = totalFraction / sessions.size();
    for (WmTezSession session : sessions) {
      session.setClusterFraction(allocation);
    }
    return totalFraction;
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
      sessionsToClose = new ArrayList<>(openSessions.keySet());
    }

    for (TezSessionPoolSession sessionState : sessionsToClose) {
      sessionState.close(false);
    }

    if (expirationTracker != null) {
      expirationTracker.stop();
    }
    allocationManager.stop();

    INSTANCE = null;
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
    returnAfterUse(session, true);
  }

  private void returnAfterUse(
      TezSessionPoolSession session, boolean releaseParallelism) throws Exception {
    boolean isInterrupted = Thread.interrupted();
    try {
      WmTezSession wmSession = ensureOwnedSession(session);
      redistributePoolAllocations(wmSession.getPoolName(), null, wmSession, releaseParallelism);
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
      openSessions.put(session, null);
    }
    updateSessionsTriggers();
  }

  /** Called by TezSessionPoolSession when closed. */
  @Override
  public void unregisterOpenSession(TezSessionPoolSession session) {
    synchronized (openSessions) {
      openSessions.remove(session);
    }
    updateSessionsTriggers();
  }

  private void updateSessionsTriggers() {
    if (sessionTriggerProvider != null) {
      List<TezSessionState> openSessions = new ArrayList<>();
      List<Trigger> activeTriggers = new ArrayList<>();
      for (PoolState poolState : pools.values()) {
        activeTriggers.addAll(poolState.getTriggers());
        openSessions.addAll(poolState.sessions);
      }
      sessionTriggerProvider.setOpenSessions(Collections.unmodifiableList(openSessions));
      sessionTriggerProvider.setActiveTriggers(Collections.unmodifiableList(activeTriggers));
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
    // Do not release the parallelism - we are just replacing the session in the same pool.
    redistributePoolAllocations(oldSession.getPoolName(), newSession, oldSession, false);
    return newSession;
  }

  @Override
  public void destroy(TezSessionState session) throws Exception {
    LOG.warn("Closing a pool session because of retry failure.");
    // We never want to lose pool sessions. Replace it instead; al trigger duck redistribution.
    WmTezSession wmSession = ensureOwnedSession(session);
    closeAndReopenPoolSession(wmSession);
    redistributePoolAllocations(wmSession.getPoolName(), null, wmSession, true);
  }

  @VisibleForTesting
  int getNumSessions() {
    return sessions.getInitialSize();
  }

  @Override
  SessionTriggerProvider getSessionTriggerProvider() {
    return sessionTriggerProvider;
  }

  @Override
  TriggerActionHandler getTriggerActionHandler() {
    return triggerActionHandler;
  }

  @Override
  TriggerValidatorRunnable getTriggerValidatorRunnable() {
    return triggerValidatorRunnable;
  }

  @VisibleForTesting
  public Map<String, PoolState> getPools() {
    return pools;
  }

  protected final HiveConf getConf() {
    return conf;
  }

  public List<String> getTriggerCounterNames() {
    List<Trigger> activeTriggers = sessionTriggerProvider.getActiveTriggers();
    List<String> counterNames = new ArrayList<>();
    for (Trigger trigger : activeTriggers) {
      counterNames.add(trigger.getExpression().getCounterLimit().getName());
    }
    return counterNames;
  }


  // TODO: temporary until real WM schema is created.
  public static class TmpHivePool {
    private final String name;
    private final List<TmpHivePool> children;
    private final int queryParallelism;
    private final double resourceFraction;

    public TmpHivePool(String name,
        List<TmpHivePool> children, int queryParallelism, double resourceFraction) {
      this.name = name;
      this.children = children;
      this.queryParallelism = queryParallelism;
      this.resourceFraction = resourceFraction;
    }

    public String getName() {
      return name;
    }
    public List<TmpHivePool> getChildren() {
      return children;
    }
    public int getQueryParallelism() {
      return queryParallelism;
    }
    public double getResourceFraction() {
      return resourceFraction;
    }
  }

  public static enum TmpUserMappingType {
    USER, DEFAULT
  }

  public static class TmpUserMapping {
    private final TmpUserMappingType type;
    private final String name;
    private final String poolName;
    private final int priority;
    public TmpUserMapping(TmpUserMappingType type, String name, String poolName, int priority) {
      this.type = type;
      this.name = name;
      this.poolName = poolName;
      this.priority = priority;
    }
    public TmpUserMappingType getType() {
      return type;
    }
    public String getName() {
      return name;
    }
    public String getPoolName() {
      return poolName;
    }
    public int getPriority() {
      return priority;
    }
  }

  public static class TmpResourcePlan {
    private final List<TmpHivePool> rootPools;
    private final List<TmpUserMapping> mappings;
    public TmpResourcePlan(List<TmpHivePool> rootPools, List<TmpUserMapping> mappings) {
      this.rootPools = rootPools;
      this.mappings = mappings;
    }
    public List<TmpHivePool> getRootPools() {
      return rootPools;
    }
    public List<TmpUserMapping> getMappings() {
      return mappings;
    }
  }
}
