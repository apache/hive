/**
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
package org.apache.hadoop.hive.ql.exec.tez;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.tezplugins.LlapTaskSchedulerService;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMPoolTrigger;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.ql.exec.tez.UserPoolMapping.MappingInput;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.wm.ExecutionTrigger;
import org.apache.hadoop.hive.ql.wm.SessionTriggerProvider;
import org.apache.hadoop.hive.ql.wm.Trigger;
import org.apache.hadoop.hive.ql.wm.TriggerActionHandler;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;


/** Workload management entry point for HS2. */
public class WorkloadManager extends TezSessionPoolSession.AbstractTriggerValidator
  implements TezSessionPoolSession.Manager, SessionExpirationTracker.RestartImpl {
  private static final Logger LOG = LoggerFactory.getLogger(WorkloadManager.class);
  private static final char POOL_SEPARATOR = '.';
  private static final String POOL_SEPARATOR_STR = "" + POOL_SEPARATOR;

  private final HiveConf conf;
  private final TezSessionPool<WmTezSession> tezAmPool;
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

  // Note: pools can only be modified by the master thread.
  private HashMap<String, PoolState> pools;
  // Used to make sure that waiting getSessions don't block update.
  private UserPoolMapping userPoolMapping;
  private int totalQueryParallelism;
  // We index the get requests to make sure there are no ordering artifacts when we requeue.
  private final AtomicLong getRequestVersion = new AtomicLong(Long.MIN_VALUE);

  private PerPoolTriggerValidatorRunnable triggerValidatorRunnable;

  // Note: we could use RW lock to allow concurrent calls for different sessions, however all
  //       those calls do is add elements to lists and maps; and we'd need to sync those separately
  //       separately, plus have an object to notify because RW lock does not support conditions
  //       in any sensible way. So, for now the lock is going to be epic.
  private final ReentrantLock currentLock = new ReentrantLock();
  private final Condition hasChangesCondition = currentLock.newCondition();
  // The processing thread will switch between these two objects.
  private final EventState one = new EventState(), two = new EventState();
  private boolean hasChanges = false;
  private EventState current = one;
  private Map<String, SessionTriggerProvider> perPoolProviders = new ConcurrentHashMap<>();

  /** The master thread the processes the events from EventState. */
  @VisibleForTesting
  protected final Thread wmThread;
  /** Used by the master thread to offload calls blocking on smth other than fast locks. */
  private final ExecutorService workPool;
  /** Used to schedule timeouts for some async operations. */
  private final ScheduledExecutorService timeoutPool;
  private final WmThreadSyncWork syncWork = new WmThreadSyncWork();
  private ListenableFuture<Boolean> initRpFuture;

  private static final FutureCallback<Object> FATAL_ERROR_CALLBACK = new FutureCallback<Object>() {
    @Override
    public void onSuccess(Object result) {
    }

    @Override
    public void onFailure(Throwable t) {
      // TODO: shut down HS2?
      LOG.error("Workload management fatal error", t);
    }
  };

  // TODO: this is temporary before HiveServerEnvironment is merged.
  private static volatile WorkloadManager INSTANCE;

  public static WorkloadManager getInstance() {
    return INSTANCE;
  }

  /** Called once, when HS2 initializes. */
  public static WorkloadManager create(String yarnQueue, HiveConf conf, WMFullResourcePlan plan) {
    assert INSTANCE == null;
    // We could derive the expected number of AMs to pass in.
    LlapPluginEndpointClient amComm = new LlapPluginEndpointClientImpl(conf, null, -1);
    QueryAllocationManager qam = new GuaranteedTasksAllocator(conf, amComm);
    return (INSTANCE = new WorkloadManager(yarnQueue, conf, qam, plan));
  }

  @VisibleForTesting
  WorkloadManager(String yarnQueue, HiveConf conf,
      QueryAllocationManager qam, WMFullResourcePlan plan) {
    this.yarnQueue = yarnQueue;
    this.conf = conf;
    this.totalQueryParallelism = determineQueryParallelism(plan);
    this.initRpFuture = this.updateResourcePlanAsync(plan);
    LOG.info("Initializing with " + totalQueryParallelism + " total query parallelism");

    this.amRegistryTimeoutMs = (int)HiveConf.getTimeVar(
      conf, ConfVars.HIVE_SERVER2_TEZ_WM_AM_REGISTRY_TIMEOUT, TimeUnit.MILLISECONDS);
    tezAmPool = new TezSessionPool<>(conf, totalQueryParallelism, true,
      oldSession -> createSession(oldSession == null ? null : oldSession.getConf()));
    restrictedConfig = new RestrictedConfigChecker(conf);
    allocationManager = qam;
    // Only creates the expiration tracker if expiration is configured.
    expirationTracker = SessionExpirationTracker.create(conf, this);

    workPool = Executors.newFixedThreadPool(HiveConf.getIntVar(conf, ConfVars.HIVE_SERVER2_TEZ_WM_WORKER_THREADS),
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Workload management worker %d").build());

    timeoutPool = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true)
      .setNameFormat("Workload management timeout thread").build());

    wmThread = new Thread(() -> runWmThread(), "Workload management master");
    wmThread.setDaemon(true);

    final long triggerValidationIntervalMs = HiveConf.getTimeVar(conf,
      HiveConf.ConfVars.HIVE_TRIGGER_VALIDATION_INTERVAL_MS, TimeUnit.MILLISECONDS);
    TriggerActionHandler triggerActionHandler = new KillMoveTriggerActionHandler(this);
    triggerValidatorRunnable = new PerPoolTriggerValidatorRunnable(perPoolProviders, triggerActionHandler,
      triggerValidationIntervalMs);
    startTriggerValidator(triggerValidationIntervalMs);
  }

  private int determineQueryParallelism(WMFullResourcePlan plan) {
    int result = 0;
    for (WMPool pool : plan.getPools()) {
      result += pool.getQueryParallelism();
    }
    return result;
  }

  public void start() throws Exception {
    tezAmPool.start();
    if (expirationTracker != null) {
      expirationTracker.start();
    }
    allocationManager.start();
    wmThread.start();
    initRpFuture.get(); // Wait for the initial resource plan to be applied.
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
    if (wmThread != null) {
      wmThread.interrupt();
    }
    workPool.shutdownNow();
    timeoutPool.shutdownNow();

    INSTANCE = null;
  }

  private void updateSessionTriggerProvidersOnMasterThread() {
    for (Map.Entry<String, PoolState> entry : pools.entrySet()) {
      String poolName = entry.getKey();
      PoolState poolState = entry.getValue();
      final List<Trigger> triggers = Collections.unmodifiableList(poolState.getTriggers());
      final List<TezSessionState> sessionStates = Collections.unmodifiableList(poolState.getSessions());
      SessionTriggerProvider sessionTriggerProvider = perPoolProviders.get(poolName);
      if (sessionTriggerProvider != null) {
        perPoolProviders.get(poolName).setTriggers(triggers);
        perPoolProviders.get(poolName).setSessions(sessionStates);
      } else {
        perPoolProviders.put(poolName, new SessionTriggerProvider(sessionStates, triggers));
      }
    }
  }

  @VisibleForTesting
  Map<String, SessionTriggerProvider> getAllSessionTriggerProviders() {
    return perPoolProviders;
  }

  /** Represent a single iteration of work for the master thread. */
  private final static class EventState {
    private final Set<WmTezSession> toReturn = Sets.newIdentityHashSet(),
      toDestroy = Sets.newIdentityHashSet(), updateErrors = Sets.newIdentityHashSet();
    private final LinkedList<SessionInitContext> initResults = new LinkedList<>();
    private final IdentityHashMap<WmTezSession, SettableFuture<WmTezSession>> toReopen =
      new IdentityHashMap<>();
    private final LinkedList<GetRequest> getRequests = new LinkedList<>();
    private final IdentityHashMap<WmTezSession, GetRequest> toReuse = new IdentityHashMap<>();
    private WMFullResourcePlan resourcePlanToApply = null;
    private boolean hasClusterStateChanged = false;
    private SettableFuture<Boolean> testEvent, applyRpFuture;
    private final List<MoveSession> moveSessions = new LinkedList<>();
  }

  private final static class MoveSession {
    private final WmTezSession srcSession;
    private final String destPool;
    private final SettableFuture<Boolean> future;

    public MoveSession(final WmTezSession srcSession, final String destPool) {
      this.srcSession = srcSession;
      this.destPool = destPool;
      this.future = SettableFuture.create();
    }

    @Override
    public String toString() {
      return srcSession.getSessionId() + " moving from " + srcSession.getPoolName() + " to " + destPool;
    }
  }

  /**
   * The work delegated from the master thread that doesn't have an async implementation
   * (mostly opening and closing the sessions).
   */
  private final static class WmThreadSyncWork {
    private LinkedList<WmTezSession> toRestartInUse = new LinkedList<>(),
      toDestroyNoRestart = new LinkedList<>();
  }

  private void runWmThread() {
    while (true) {
      EventState currentEvents = null;
      currentLock.lock();
      try {
        while (!hasChanges) {
          try {
            hasChangesCondition.await(1, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            LOG.warn("WM thread was interrupted and will now exit");
            return;
          }
        }
        hasChanges = false;
        currentEvents = current;
        current = (currentEvents == one) ? two : one;
      } finally {
        currentLock.unlock();
      }
      try {
        LOG.info("Processing current events");
        processCurrentEvents(currentEvents, syncWork);
        scheduleWork(syncWork);
        updateSessionTriggerProvidersOnMasterThread();
      } catch (InterruptedException ex) {
        LOG.warn("WM thread was interrupted and will now exit");
        return;
      } catch (Exception | AssertionError ex) {
        LOG.error("WM thread encountered an error but will attempt to continue", ex);
        if (currentEvents.testEvent != null) {
          currentEvents.testEvent.setException(ex);
          currentEvents.testEvent = null;
        }
        if (currentEvents.applyRpFuture != null) {
          currentEvents.applyRpFuture.setException(ex);
          currentEvents.applyRpFuture = null;
        }
        // TODO: we either have to kill HS2 or, as the non-actor model would implicitly,
        //       hope for the best and continue on other threads. Do the latter for now.
        continue;
      }
    }
  }

  private void scheduleWork(WmThreadSyncWork context) {
    // Do the work that cannot be done via async calls.

    // 1. Restart pool sessions.
    for (final WmTezSession toRestart : context.toRestartInUse) {
      LOG.info("Replacing " + toRestart + " with a new session");
      workPool.submit(() -> {
        try {
          // Note: sessions in toRestart are always in use, so they cannot expire in parallel.
          tezAmPool.replaceSession(toRestart, false, null);
        } catch (Exception ex) {
          LOG.error("Failed to restart an old session; ignoring " + ex.getMessage());
        }
      });
    }
    context.toRestartInUse.clear();
    // 2. Destroy the sessions that we don't need anymore.
    for (final WmTezSession toDestroy : context.toDestroyNoRestart) {
      LOG.info("Closing " + toDestroy + " without restart");
      workPool.submit(() -> {
        try {
          toDestroy.close(false);
        } catch (Exception ex) {
          LOG.error("Failed to close an old session; ignoring " + ex.getMessage());
        }
      });
    }
    context.toDestroyNoRestart.clear();
  }

  private void processCurrentEvents(EventState e, WmThreadSyncWork syncWork) throws Exception {
    // The order of processing is as follows. We'd reclaim or kill all the sessions that we can
    // reclaim from various user actions and errors, then apply the new plan if any,
    // then give out all we can give out (restart, get and reopen callers) and rebalance the
    // resource allocations in all the affected pools.
    // For every session, we'd check all the concurrent things happening to it.

    // TODO: also account for Tez-internal session restarts;
    //       AM reg info changes; add notifications, ignore errors, and update alloc.
    HashSet<String> poolsToRedistribute = new HashSet<>();

    // 0. Handle initialization results.
    for (SessionInitContext sw : e.initResults) {
      handleInitResultOnMasterThread(sw, syncWork, poolsToRedistribute);
    }
    e.initResults.clear();

    // 1. Handle sessions that are being destroyed by users. Destroy implies return.
    for (WmTezSession sessionToDestroy : e.toDestroy) {
      if (e.toReturn.remove(sessionToDestroy)) {
        LOG.warn("The session was both destroyed and returned by the user; destroying");
      }
      LOG.info("Destroying {}", sessionToDestroy);
      Boolean shouldReturn = handleReturnedInUseSessionOnMasterThread(
        e, sessionToDestroy, poolsToRedistribute);
      if (shouldReturn == null || shouldReturn) {
        // Restart if this session is still relevant, even if there's an internal error.
        syncWork.toRestartInUse.add(sessionToDestroy);
      }
    }
    e.toDestroy.clear();

    // 2. Now handle actual returns. Sessions may be returned to the pool or may trigger expires.
    for (WmTezSession sessionToReturn: e.toReturn) {
      LOG.info("Returning {}", sessionToReturn);
      Boolean shouldReturn = handleReturnedInUseSessionOnMasterThread(
        e, sessionToReturn, poolsToRedistribute);
      if (shouldReturn == null) {
        // Restart if there's an internal error.
        syncWork.toRestartInUse.add(sessionToReturn);
        continue;
      }
      if (!shouldReturn) continue;
      boolean wasReturned = tezAmPool.returnSessionAsync(sessionToReturn);
      if (!wasReturned) {
        syncWork.toDestroyNoRestart.add(sessionToReturn);
      }
    }
    e.toReturn.clear();

    // 3. Reopen is essentially just destroy + get a new session for a session in use.
    for (Map.Entry<WmTezSession, SettableFuture<WmTezSession>> entry : e.toReopen.entrySet()) {
      LOG.info("Reopening {}", entry.getKey());
      handeReopenRequestOnMasterThread(
        e, entry.getKey(), entry.getValue(), poolsToRedistribute, syncWork);
    }
    e.toReopen.clear();

    // 4. All the sessions in use that were not destroyed or returned with a failed update now die.
    for (WmTezSession sessionWithUpdateError : e.updateErrors) {
      LOG.info("Update failed for {}", sessionWithUpdateError);
      handleUpdateErrorOnMasterThread(sessionWithUpdateError, e, syncWork, poolsToRedistribute);
    }
    e.updateErrors.clear();

    // 5. Now apply a resource plan if any. This is expected to be pretty rare.
    boolean hasRequeues = false;
    if (e.resourcePlanToApply != null) {
      LOG.info("Applying new resource plan");
      int getReqCount = e.getRequests.size();
      applyNewResourcePlanOnMasterThread(e, syncWork, poolsToRedistribute);
      hasRequeues = getReqCount != e.getRequests.size();
    }
    e.resourcePlanToApply = null;

    // 6. Handle any move session requests. The way move session works right now is
    // a) sessions get moved to destination pool if there is capacity in destination pool
    // b) if there is no capacity in destination pool, the session gets killed (since we cannot pause a query)
    // TODO: in future this the process of killing can be delayed until the point where a session is actually required.
    // We could consider delaying the move (when destination capacity is full) until there is claim in src pool.
    // May be change command to support ... DELAYED MOVE TO etl ... which will run under src cluster fraction as long
    // as possible
    if (e.moveSessions != null) {
      for (MoveSession moveSession : e.moveSessions) {
        handleMoveSessionOnMasterThread(moveSession, syncWork, poolsToRedistribute);
      }
    }
    e.moveSessions.clear();

    // 7. Handle all the get/reuse requests. We won't actually give out anything here, but merely
    //    map all the requests and place them in an appropriate order in pool queues. The only
    //    exception is the reuse without queue contention; can be granted immediately. If we can't
    //    reuse the session immediately, we will convert the reuse to a normal get, because we
    //    want query level fairness, and don't want the get in queue to hold up a session.
    GetRequest req;
    while ((req = e.getRequests.pollFirst()) != null) {
      LOG.info("Processing a new get request from " + req.mappingInput);
      queueGetRequestOnMasterThread(req, poolsToRedistribute, syncWork);
    }
    e.toReuse.clear();

    // 8. If there was a cluster state change, make sure we redistribute all the pools.
    if (e.hasClusterStateChanged) {
      LOG.info("Processing a cluster state change");
      poolsToRedistribute.addAll(pools.keySet());
      e.hasClusterStateChanged = false;
    }

    // 9. Finally, for all the pools that have changes, promote queued queries and rebalance.
    for (String poolName : poolsToRedistribute) {
      if (LOG.isDebugEnabled()) {
        LOG.info("Processing changes for pool " + poolName + ": " + pools.get(poolName));
      }
      processPoolChangesOnMasterThread(poolName, syncWork, hasRequeues);
    }

    // 10. Notify tests and global async ops.
    if (e.testEvent != null) {
      e.testEvent.set(true);
      e.testEvent = null;
    }
    if (e.applyRpFuture != null) {
      e.applyRpFuture.set(true);
      e.applyRpFuture = null;
    }
  }

  private void handleMoveSessionOnMasterThread(final MoveSession moveSession,
    final WmThreadSyncWork syncWork,
    final HashSet<String> poolsToRedistribute) {
    String destPoolName = moveSession.destPool;
    LOG.info("Handling move session event: {}", moveSession);
    if (validMove(moveSession.srcSession, destPoolName)) {
      // remove from src pool
      Boolean removed = checkAndRemoveSessionFromItsPool(moveSession.srcSession, poolsToRedistribute);
      if (removed != null && removed) {
        // check if there is capacity in dest pool, if so move else kill the session
        if (capacityAvailable(destPoolName)) {
          // add to destination pool
          Boolean added = checkAndAddSessionToAnotherPool(moveSession.srcSession, destPoolName, poolsToRedistribute);
          if (added != null && added) {
            moveSession.future.set(true);
            return;
          } else {
            LOG.error("Failed to move session: {}. Session is not added to destination.", moveSession);
          }
        } else {
          moveSession.srcSession.clearWm();
          moveSession.srcSession.setIsIrrelevantForWm("Destination pool " + destPoolName + " is full. Killing query.");
          syncWork.toRestartInUse.add(moveSession.srcSession);
        }
      } else {
        LOG.error("Failed to move session: {}. Session is not removed from its pool.", moveSession);
      }
    } else {
      LOG.error("Validation failed for move session: {}. Invalid move or session/pool got removed.", moveSession);
    }

    moveSession.future.set(false);
  }

  private Boolean capacityAvailable(final String destPoolName) {
    PoolState destPool = pools.get(destPoolName);
    return destPool.getTotalActiveSessions() < destPool.queryParallelism;
  }

  private boolean validMove(final WmTezSession srcSession, final String destPool) {
    return srcSession != null &&
      destPool != null &&
      !srcSession.isIrrelevantForWm() &&
      srcSession.getPoolName() != null &&
      pools.containsKey(srcSession.getPoolName()) &&
      pools.containsKey(destPool) &&
      !srcSession.getPoolName().equalsIgnoreCase(destPool);
  }

  // ========= Master thread methods

  private void handleInitResultOnMasterThread(
    SessionInitContext sw, WmThreadSyncWork syncWork, HashSet<String> poolsToRedistribute) {
    // For the failures, the users have been notified, we just need to clean up. There's no
    // session here (or it's unused), so no conflicts are possible. We just remove it.
    // For successes, the user has also been notified, so various requests are also possible;
    // however, to start, we'd just put the session into the sessions list and go from there.
    WmTezSession session = null;
    sw.lock.lock();
    try {
      if (sw.state == SessionInitState.CANCELED) {
        // We have processed this on the previous run, after it has already queued the message.
        return;
      }
      assert sw.state == SessionInitState.DONE;
      session = sw.session;
      sw.session = null;
    } finally {
      sw.lock.unlock();
    }
    LOG.info("Processing " + ((session == null) ? "failed" : "successful")
        + " initialization result for pool " + sw.poolName);
    // We could not have removed the pool for this session, or we would have CANCELED the init.
    PoolState pool = pools.get(sw.poolName);
    if (pool == null || !pool.initializingSessions.remove(sw)) {
      // Query parallelism might be fubar.
      LOG.error("Cannot remove initializing session from the pool "
        + sw.poolName + " - internal error");
    }
    poolsToRedistribute.add(sw.poolName);
    if (session != null) {
      if (pool != null) {
        pool.sessions.add(session);
      } else {
        LOG.error("Cannot add new session to the pool "
          + sw.poolName + " because it was removed unexpectedly - internal error " + session);
        syncWork.toRestartInUse.add(session);
      }
    }
  }

  private Boolean handleReturnedInUseSessionOnMasterThread(
    EventState e, WmTezSession session, HashSet<String> poolsToRedistribute) {
    // This handles the common logic for destroy and return - everything except
    // the invalid combination of destroy and return themselves, as well as the actual
    // statement that destroys or returns it.
    if (e.updateErrors.remove(session)) {
      LOG.info("Ignoring an update error for a session being destroyed or returned");
    }
    SettableFuture<WmTezSession> future = e.toReopen.remove(session);
    if (future != null) {
      future.setException(new AssertionError("Invalid reopen attempt"));
    }
    GetRequest reuseRequest = e.toReuse.remove(session);
    if (reuseRequest != null) {
      reuseRequest.future.setException(new AssertionError("Invalid reuse attempt"));
    }
    return checkAndRemoveSessionFromItsPool(session, poolsToRedistribute);
  }

  private void handeReopenRequestOnMasterThread(EventState e, WmTezSession session,
    SettableFuture<WmTezSession> future, HashSet<String> poolsToRedistribute,
    WmThreadSyncWork syncWork) throws Exception {
    if (e.updateErrors.remove(session)) {
      LOG.info("Ignoring an update error for a session being reopened");
    }
    GetRequest reuseRequest = e.toReuse.remove(session);
    if (reuseRequest != null) {
      reuseRequest.future.setException(new AssertionError("Invalid reuse attempt"));
    }
    // In order to expedite things in a general case, we are not actually going to reopen
    // anything. Instead, we will try to give out an existing session from the pool, and restart
    // the problematic one in background.
    String poolName = session.getPoolName();
    Boolean isRemoved = checkAndRemoveSessionFromItsPool(session, poolsToRedistribute);
    // If we fail to remove, it's probably an internal error. We'd try to handle it the same way
    // as above - by restarting the session. We'd fail the caller to avoid exceeding parallelism.
    if (isRemoved == null) {
      future.setException(new RuntimeException("Reopen failed due to an internal error"));
      syncWork.toRestartInUse.add(session);
      return;
    } else if (!isRemoved) {
      future.setException(new RuntimeException("WM killed this session during reopen: "
        + session.getReasonForKill()));
      return; // No longer relevant for WM - bail.
    }
    // If pool didn't exist, removeSessionFromItsPool would have returned null.
    PoolState pool = pools.get(poolName);
    SessionInitContext sw = new SessionInitContext(future, poolName);
    // We have just removed the session from the same pool, so don't check concurrency here.
    pool.initializingSessions.add(sw);
    ListenableFuture<WmTezSession> getFuture = tezAmPool.getSessionAsync();
    Futures.addCallback(getFuture, sw);
    syncWork.toRestartInUse.add(session);
  }

  private void handleUpdateErrorOnMasterThread(WmTezSession sessionWithUpdateError,
    EventState e, WmThreadSyncWork syncWork, HashSet<String> poolsToRedistribute) {
    GetRequest reuseRequest = e.toReuse.remove(sessionWithUpdateError);
    if (reuseRequest != null) {
      // This session is bad, so don't allow reuse; just convert it to normal get.
      reuseRequest.sessionToReuse = null;
    }
    // TODO: we should communicate this to the user more explicitly (use kill query API, or
    //       add an option for bg kill checking to TezTask/monitor?
    // We are assuming the update-error AM is bad and just try to kill it.
    Boolean isRemoved = checkAndRemoveSessionFromItsPool(sessionWithUpdateError, poolsToRedistribute);
    if (isRemoved != null && !isRemoved) {
      // An update error for some session that was actually already killed by us.
      return;
    }
    // Regardless whether it was removed successfully or after failing to remove, restart it.
    // Since we just restart this from under the user, mark it so we handle it properly when
    // the user tries to actually use this session and fails, proceeding to return/destroy it.
    // TODO: propagate this error to TezJobMonitor somehow, after we add the use of KillQuery.
    sessionWithUpdateError.setIsIrrelevantForWm("Failed to update resource allocation");
    syncWork.toRestartInUse.add(sessionWithUpdateError);
  }

  private void applyNewResourcePlanOnMasterThread(
    EventState e, WmThreadSyncWork syncWork, HashSet<String> poolsToRedistribute) {
    int totalQueryParallelism = 0;
    // FIXME: Add Triggers from metastore to poolstate
    // Note: we assume here that plan has been validated beforehand, so we don't verify
    //       that fractions or query parallelism add up, etc.
    this.userPoolMapping = new UserPoolMapping(e.resourcePlanToApply.getMappings(),
        e.resourcePlanToApply.getPlan().getDefaultPoolPath());
    HashMap<String, PoolState> oldPools = pools;
    pools = new HashMap<>();

    // For simplicity, to always have parents while storing pools in a flat structure, we'll
    // first distribute them by levels, then add level by level.
    ArrayList<List<WMPool>> poolsByLevel = new ArrayList<>();
    for (WMPool pool : e.resourcePlanToApply.getPools()) {
      String fullName = pool.getPoolPath();
      int ix = StringUtils.countMatches(fullName, POOL_SEPARATOR_STR);
      while (poolsByLevel.size() <= ix) {
        poolsByLevel.add(new LinkedList<WMPool>()); // We expect all the levels to have items.
      }
      poolsByLevel.get(ix).add(pool);
    }
    for (int level = 0; level < poolsByLevel.size(); ++level) {
      List<WMPool> poolsOnLevel = poolsByLevel.get(level);
      for (WMPool pool : poolsOnLevel) {
        String fullName = pool.getPoolPath();
        int qp = pool.getQueryParallelism();
        double fraction = pool.getAllocFraction();
        if (level > 0) {
          String parentName = fullName.substring(0, fullName.lastIndexOf(POOL_SEPARATOR));
          PoolState parent = pools.get(parentName);
          fraction = parent.finalFraction * fraction;
          parent.finalFractionRemaining -= fraction;
        }
        PoolState state = oldPools == null ? null : oldPools.remove(fullName);
        if (state == null) {
          state = new PoolState(fullName, qp, fraction);
        } else {
          // This will also take care of the queries if query parallelism changed.
          state.update(qp, fraction, syncWork.toRestartInUse, e);
          poolsToRedistribute.add(fullName);
        }
        state.setTriggers(new LinkedList<Trigger>());
        LOG.info("Adding Hive pool: " + state);
        pools.put(fullName, state);
        totalQueryParallelism += qp;
      }
    }
    if (e.resourcePlanToApply.isSetTriggers() && e.resourcePlanToApply.isSetPoolTriggers()) {
      Map<String, Trigger> triggers = new HashMap<>();
      for (WMTrigger trigger : e.resourcePlanToApply.getTriggers()) {
        ExecutionTrigger execTrigger = ExecutionTrigger.fromWMTrigger(trigger);
        triggers.put(trigger.getTriggerName(), execTrigger);
      }
      for (WMPoolTrigger poolTrigger : e.resourcePlanToApply.getPoolTriggers()) {
        PoolState pool = pools.get(poolTrigger.getPool());
        Trigger trigger = triggers.get(poolTrigger.getTrigger());
        pool.triggers.add(trigger);
        poolsToRedistribute.add(pool.fullName);
        LOG.info("Adding pool " + pool.fullName + " trigger " + trigger);
      }
    }

    if (oldPools != null && !oldPools.isEmpty()) {
      // Looks like some pools were removed; kill running queries, re-queue the queued ones.
      for (PoolState oldPool : oldPools.values()) {
        oldPool.destroy(syncWork.toRestartInUse, e.getRequests, e.toReuse);
      }
    }

    LOG.info("Updating with " + totalQueryParallelism + " total query parallelism");
    int deltaSessions = totalQueryParallelism - this.totalQueryParallelism;
    this.totalQueryParallelism = totalQueryParallelism;
    if (deltaSessions == 0) return; // Nothing to do.
    if (deltaSessions < 0) {
      // First, see if we have unused sessions that we were planning to restart; get rid of those.
      int toTransfer = Math.min(-deltaSessions, syncWork.toRestartInUse.size());
      for (int i = 0; i < toTransfer; ++i) {
        syncWork.toDestroyNoRestart.add(syncWork.toRestartInUse.pollFirst());
      }
      deltaSessions += toTransfer;
    }
    if (deltaSessions != 0) {
      failOnFutureFailure(tezAmPool.resizeAsync(
        deltaSessions, syncWork.toDestroyNoRestart));
    }
  }

  @SuppressWarnings("unchecked")
  private void failOnFutureFailure(ListenableFuture<?> future) {
    Futures.addCallback(future, FATAL_ERROR_CALLBACK);
  }

  private void queueGetRequestOnMasterThread(
      GetRequest req, HashSet<String> poolsToRedistribute, WmThreadSyncWork syncWork) {
    String poolName = userPoolMapping.mapSessionToPoolName(req.mappingInput);
    if (poolName == null) {
      req.future.setException(new NoPoolMappingException(
          "Cannot find any pool mapping for " + req.mappingInput));
      returnSessionOnFailedReuse(req, syncWork, poolsToRedistribute);
      return;
    }
    PoolState pool = pools.get(poolName);
    if (pool == null) {
      req.future.setException(new AssertionError(poolName + " not found (internal error)."));
      returnSessionOnFailedReuse(req, syncWork, poolsToRedistribute);
      return;
    }

    PoolState oldPool = null;
    if (req.sessionToReuse != null) {
      // Given that we are trying to reuse, this session MUST be in some pool.sessions.
      // Kills that could have removed it must have cleared sessionToReuse.
      String oldPoolName = req.sessionToReuse.getPoolName();
      oldPool = pools.get(oldPoolName);
      Boolean isRemoved = checkAndRemoveSessionFromItsPool(req.sessionToReuse, poolsToRedistribute);
      if (isRemoved == null || !isRemoved) {
        // This is probably an internal error... abandon the reuse attempt.
        returnSessionOnFailedReuse(req, syncWork, null);
        req.sessionToReuse = null;
      } else if (pool.getTotalActiveSessions() + pool.queue.size() >= pool.queryParallelism) {
        // One cannot simply reuse the session if there are other queries waiting; to maintain
        // fairness, we'll try to take a query slot instantly, and if that fails we'll return
        // this session back to the pool and give the user a new session later.
        returnSessionOnFailedReuse(req, syncWork, null);
        req.sessionToReuse = null;
      }
    }

    if (req.sessionToReuse != null) {
      // If we can immediately reuse a session, there's nothing to wait for - just return.
      req.sessionToReuse.setPoolName(poolName);
      req.sessionToReuse.setQueueName(yarnQueue);
      pool.sessions.add(req.sessionToReuse);
      if (pool != oldPool) {
        poolsToRedistribute.add(poolName);
      }
      req.future.set(req.sessionToReuse);
      return;
    }

    // Otherwise, queue the session and make sure we update this pool.
    pool.queue.addLast(req);
    poolsToRedistribute.add(poolName);
  }


  private void processPoolChangesOnMasterThread(
    String poolName, WmThreadSyncWork context, boolean hasRequeues) throws Exception {
    PoolState pool = pools.get(poolName);
    if (pool == null) return; // Might be from before the new resource plan.

    // 1. First, start the queries from the queue.
    int queriesToStart = Math.min(pool.queue.size(),
      pool.queryParallelism - pool.getTotalActiveSessions());

    if (queriesToStart > 0) {
      LOG.info("Starting {} queries in pool {}", queriesToStart, pool);
    }
    if (hasRequeues) {
      // Sort the queue - we may have put items here out of order.
      Collections.sort(pool.queue, GetRequest.ORDER_COMPARATOR);
    }
    for (int i = 0; i < queriesToStart; ++i) {
      GetRequest queueReq = pool.queue.pollFirst();
      assert queueReq.sessionToReuse == null;
      // Note that in theory, we are guaranteed to have a session waiting for us here, but
      // the expiration, failures, etc. may cause one to be missing pending restart.
      // See SessionInitContext javadoc.
      SessionInitContext sw = new SessionInitContext(queueReq.future, poolName);
      ListenableFuture<WmTezSession> getFuture = tezAmPool.getSessionAsync();
      Futures.addCallback(getFuture, sw);
      // It is possible that all the async methods returned on the same thread because the
      // session with registry data and stuff was available in the pool.
      // If this happens, we'll take the session out here and "cancel" the init so we skip
      // processing the message that the successful init has queued for us.
      boolean isDone = sw.extractSessionAndCancelIfDone(pool.sessions);
      if (!isDone) {
        pool.initializingSessions.add(sw);
      }
      // The user has already been notified of completion by SessionInitContext.
    }

    // 2. Then, update pool allocations.
    double totalAlloc = pool.updateAllocationPercentages();
    // We are calling this here because we expect the method to be completely async. We also don't
    // want this call itself to go on a thread because we want the percent-to-physics conversion
    // logic to be consistent between all the separate calls in one master thread processing round.
    // Note: If allocation manager does not have cluster state, it won't update anything. When the
    //       cluster state changes, it will notify us, and we'd update the queries again.
    allocationManager.updateSessionsAsync(totalAlloc, pool.sessions);
  }

  private void returnSessionOnFailedReuse(
    GetRequest req, WmThreadSyncWork syncWork, HashSet<String> poolsToRedistribute) {
    if (req.sessionToReuse == null) return;
    if (poolsToRedistribute != null) {
      Boolean isRemoved = checkAndRemoveSessionFromItsPool(req.sessionToReuse, poolsToRedistribute);
      // The session cannot have been killed; this happens after all the kills in the current
      // iteration, so we would have cleared sessionToReuse when killing this.
      assert isRemoved == null || isRemoved;
    }
    if (!tezAmPool.returnSessionAsync(req.sessionToReuse)) {
      syncWork.toDestroyNoRestart.add(req.sessionToReuse);
    }
    req.sessionToReuse = null;
  }

  /**
   * Checks if the session is still relevant for WM and if yes, removes it from its thread.
   * @return true if the session was removed; false if the session was already processed by WM
   *         thread (so we are dealing with an outdated request); null if the session should be
   *         in WM but wasn't found in the requisite pool (internal error?).
   */
  private Boolean checkAndRemoveSessionFromItsPool(
    WmTezSession session, HashSet<String> poolsToRedistribute) {
    // It is possible for some request to be queued after a main thread has decided to kill this
    // session; on the next iteration, we'd be processing that request with an irrelevant session.
    if (session.isIrrelevantForWm()) {
      return false;
    }
    // If we did not kill this session we expect everything to be present.
    String poolName = session.getPoolName();
    if (poolName != null) {
      poolsToRedistribute.add(poolName);
      PoolState pool = pools.get(poolName);
      session.clearWm();
      if (pool != null && pool.sessions.remove(session)) {
        return true;
      }
    }
    LOG.error("Session was not in the pool (internal error) " + poolName + ": " + session);
    return null;
  }

  private Boolean checkAndAddSessionToAnotherPool(
    WmTezSession session, String destPoolName, HashSet<String> poolsToRedistribute) {
    if (session.isIrrelevantForWm()) {
      // This is called only during move session handling, removing session already checks this.
      // So this is not expected as remove failing will not even invoke this method
      LOG.error("Unexpected during add session to another pool. If remove failed this should not have been called.");
      return false;
    }

    PoolState destPool = pools.get(destPoolName);
    if (destPool != null && destPool.sessions.add(session)) {
      session.setPoolName(destPoolName);
      poolsToRedistribute.add(destPoolName);
      return true;
    }
    LOG.error("Session {} was not not added to pool {}", session, destPoolName);
    return null;
  }

  // ===== EVENT METHODS

  public ListenableFuture<Boolean> updateResourcePlanAsync(WMFullResourcePlan plan) {
    SettableFuture<Boolean> applyRpFuture = SettableFuture.create();
    currentLock.lock();
    try {
      // TODO: if there's versioning/etc., it will come in here. For now we rely on external
      //       locking or ordering of calls. This should potentially return a Future for that.
      if (current.resourcePlanToApply != null) {
        LOG.warn("Several resource plans are being applied at the same time; using the latest");
        current.applyRpFuture.setException(
          new HiveException("Another plan was applied in parallel"));
      }
      current.resourcePlanToApply = plan;
      current.applyRpFuture = applyRpFuture;
      notifyWmThreadUnderLock();
    } finally {
      currentLock.unlock();
    }
    return applyRpFuture;
  }

  public Future<Boolean> applyMoveSessionAsync(WmTezSession srcSession, String destPoolName) {
    currentLock.lock();
    MoveSession moveSession;
    try {
      moveSession = new MoveSession(srcSession, destPoolName);
      current.moveSessions.add(moveSession);
      LOG.info("Queued move session: {}", moveSession);
      notifyWmThreadUnderLock();
    } finally {
      currentLock.unlock();
    }
    return moveSession.future;
  }

  private final static class GetRequest {
    public static final Comparator<GetRequest> ORDER_COMPARATOR = new Comparator<GetRequest>() {
      @Override
      public int compare(GetRequest o1, GetRequest o2) {
        if (o1.order == o2.order) return 0;
        return o1.order < o2.order ? -1 : 1;
      }
    };
    private final long order;
    private final MappingInput mappingInput;
    private final SettableFuture<WmTezSession> future;
    private WmTezSession sessionToReuse;

    private GetRequest(MappingInput mappingInput, SettableFuture<WmTezSession> future,
        WmTezSession sessionToReuse, long order) {
      assert mappingInput != null;
      this.mappingInput = mappingInput;
      this.future = future;
      this.sessionToReuse = sessionToReuse;
      this.order = order;
    }

    @Override
    public String toString() {
      return "[#" + order + ", " + mappingInput + ", reuse " + sessionToReuse + "]";
    }
  }

  public TezSessionState getSession(
      TezSessionState session, MappingInput input, HiveConf conf) throws Exception {
    // Note: not actually used for pool sessions; verify some things like doAs are not set.
    validateConfig(conf);
    SettableFuture<WmTezSession> future = SettableFuture.create();
    WmTezSession wmSession = checkSessionForReuse(session);
    GetRequest req = new GetRequest(
        input, future, wmSession, getRequestVersion.incrementAndGet());
    currentLock.lock();
    try {
      current.getRequests.add(req);
      if (req.sessionToReuse != null) {
        // Note: we assume reuse is only possible for the same user and config.
        current.toReuse.put(wmSession, req);
      }
      notifyWmThreadUnderLock();
    } finally {
      currentLock.unlock();
    }
    return future.get();
  }

  @Override
  public void destroy(TezSessionState session) throws Exception {
    WmTezSession wmTezSession = ensureOwnedSession(session);
    resetGlobalTezSession(wmTezSession);
    currentLock.lock();
    try {
      current.toDestroy.add(wmTezSession);
      notifyWmThreadUnderLock();
    } finally {
      currentLock.unlock();
    }
  }

  private void resetGlobalTezSession(WmTezSession wmTezSession) {
    // This has to be done synchronously to avoid the caller getting this session again.
    // Ideally we'd get rid of this thread-local nonsense.
    SessionState sessionState = SessionState.get();
    if (sessionState != null && sessionState.getTezSession() == wmTezSession) {
      sessionState.setTezSession(null);
    }
  }

  @Override
  public void returnAfterUse(TezSessionPoolSession session) throws Exception {
    WmTezSession wmTezSession = ensureOwnedSession(session);
    resetGlobalTezSession(wmTezSession);
    currentLock.lock();
    try {
      current.toReturn.add(wmTezSession);
      notifyWmThreadUnderLock();
    } finally {
      currentLock.unlock();
    }
  }

  // TODO: use this
  public void nofityOfClusterStateChange() {
    currentLock.lock();
    try {
      current.hasClusterStateChanged = true;
      notifyWmThreadUnderLock();
    } finally {
      currentLock.unlock();
    }
  }

  public void addUpdateError(WmTezSession wmTezSession) {
    currentLock.lock();
    try {
      current.updateErrors.add(wmTezSession);
      notifyWmThreadUnderLock();
    } finally {
      currentLock.unlock();
    }
  }

  @VisibleForTesting
  /**
   * Adds a test event that's processed at the end of WM iteration.
   * This allows tests to wait for an iteration to finish without messing with the threading
   * logic (that is prone to races if we e.g. remember the state before and wait for it to change,
   * self-deadlocking when triggering things explicitly and calling a blocking API, and hanging
   * forever if we wait for "another iteration"). If addTestEvent is called after all the other
   * calls of interest, it is guaranteed that the events from those calls will be processed
   * fully when the future is triggered.
   */
  Future<Boolean> addTestEvent() {
    SettableFuture<Boolean> testEvent = SettableFuture.create();
    currentLock.lock();
    try {
      current.testEvent = testEvent;
      notifyWmThreadUnderLock();
    } finally {
      currentLock.unlock();
    }
    return testEvent;
  }

  public void notifyInitializationCompleted(SessionInitContext initCtx) {
    currentLock.lock();
    try {
      current.initResults.add(initCtx);
      notifyWmThreadUnderLock();
    } finally {
      currentLock.unlock();
    }
  }


  @Override
  public TezSessionState reopen(TezSessionState session, Configuration conf,
    String[] additionalFiles) throws Exception {
    WmTezSession wmTezSession = ensureOwnedSession(session);
    HiveConf sessionConf = wmTezSession.getConf();
    if (sessionConf == null) {
      LOG.warn("Session configuration is null for " + wmTezSession);
      sessionConf = new HiveConf(conf, WorkloadManager.class);

    }
    // TODO: ideally, we should handle reopen the same way no matter what. However, the cases
    //       with additional files will have to wait until HIVE-17827 is unfucked, because it's
    //       difficult to determine how the additionalFiles are to be propagated/reused between
    //       two sessions. Once the update logic is encapsulated in the session we can remove this.
    if (additionalFiles != null && additionalFiles.length > 0) {
      TezSessionPoolManager.reopenInternal(session, additionalFiles);
      return session;
    }

    SettableFuture<WmTezSession> future = SettableFuture.create();
    currentLock.lock();
    try {
      if (current.toReopen.containsKey(wmTezSession)) {
        throw new AssertionError("The session is being reopened more than once " + session);
      }
      current.toReopen.put(wmTezSession, future);
      notifyWmThreadUnderLock();
    } finally {
      currentLock.unlock();
    }
    return future.get();
  }

  @Override
  public void closeAndReopenExpiredSession(TezSessionPoolSession session) throws Exception {
    // By definition, this session is not in use and can no longer be in use, so it only
    // affects the session pool. We can handle this inline.
    tezAmPool.replaceSession(ensureOwnedSession(session), false, null);
  }

  // ======= VARIOUS UTILITY METHOD

  private void notifyWmThreadUnderLock() {
    if (hasChanges) return;
    hasChanges = true;
    hasChangesCondition.signalAll();
  }

  private WmTezSession checkSessionForReuse(TezSessionState session) throws Exception {
    if (session == null) return null;
    WmTezSession result = null;
    if (session instanceof WmTezSession) {
      result = (WmTezSession) session;
      if (result.isOwnedBy(this)) {
        return result;
      }
      // This should never happen, at least for now. Throw?
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

  private WmTezSession createSession(HiveConf conf) {
    WmTezSession session = createSessionObject(TezSessionState.makeSessionId(), conf);
    session.setQueueName(yarnQueue);
    session.setDefault();
    LOG.info("Created new interactive session object " + session.getSessionId());
    return session;
  }

  @VisibleForTesting
  protected WmTezSession createSessionObject(String sessionId, HiveConf conf) {
    conf = (conf == null) ? new HiveConf(this.conf) : conf;
    conf.set(LlapTaskSchedulerService.LLAP_PLUGIN_ENDPOINT_ENABLED, "true");
    return new WmTezSession(sessionId, this, expirationTracker, conf);
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

  @VisibleForTesting
  int getNumSessions() {
    return tezAmPool.getInitialSize();
  }

  protected final HiveConf getConf() {
    return conf;
  }

  public List<String> getTriggerCounterNames(final TezSessionState session) {
    if (session instanceof WmTezSession) {
      WmTezSession wmTezSession = (WmTezSession) session;
      String poolName = wmTezSession.getPoolName();
      PoolState poolState = pools.get(poolName);
      if (poolState != null) {
        List<String> counterNames = new ArrayList<>();
        List<Trigger> triggers = poolState.getTriggers();
        if (triggers != null) {
          for (Trigger trigger : triggers) {
            counterNames.add(trigger.getExpression().getCounterLimit().getName());
          }
        }
        return counterNames;
      }
    }
    return null;
  }

  @Override
  Runnable getTriggerValidatorRunnable() {
    return triggerValidatorRunnable;
  }

  /**
   * State of a single pool.
   * Unless otherwise specified, the members are only modified by the master thread.
   */
  private static class PoolState {
    // Add stuff here as WM is implemented.
    private final LinkedList<SessionInitContext> initializingSessions = new LinkedList<>();
    // Note: the list is expected to be a few items; if it's longer we may want an IHM.
    private final LinkedList<WmTezSession> sessions = new LinkedList<>();
    private final LinkedList<GetRequest> queue = new LinkedList<>();

    private final String fullName;
    private double finalFraction;
    private double finalFractionRemaining;
    private int queryParallelism = -1;
    private List<Trigger> triggers = new ArrayList<>();

    public PoolState(String fullName, int queryParallelism, double fraction) {
      this.fullName = fullName;
      update(queryParallelism, fraction, null, null);
    }

    public int getTotalActiveSessions() {
      return sessions.size() + initializingSessions.size();
    }

    public void update(int queryParallelism, double fraction,
      List<WmTezSession> toKill, EventState e) {
      this.finalFraction = this.finalFractionRemaining = fraction;
      this.queryParallelism = queryParallelism;
      // TODO: two possible improvements
      //       1) Right now we kill all the queries here; we could just kill -qpDelta.
      //       2) After the queries are killed queued queries would take their place.
      //          If we could somehow restart queries we could instead put them at the front
      //          of the queue (esp. in conjunction with (1)) and rerun them.
      if (queryParallelism < getTotalActiveSessions()) {
        extractAllSessionsToKill("The query pool was resized by administrator", e.toReuse, toKill);
      }
      // We will requeue, and not kill, the queries that are not running yet.
      // Insert them all before the get requests from this iteration.
      GetRequest req;
      while ((req = queue.pollLast()) != null) {
        e.getRequests.addFirst(req);
      }
    }

    public void destroy(List<WmTezSession> toKill, LinkedList<GetRequest> globalQueue,
      IdentityHashMap<WmTezSession, GetRequest> toReuse) {
      extractAllSessionsToKill("The query pool was removed by administrator", toReuse, toKill);
      // All the pending get requests should just be requeued elsewhere.
      // Note that we never queue session reuse so sessionToReuse would be null.
      globalQueue.addAll(0, queue);
      queue.clear();
    }

    public double updateAllocationPercentages() {
      // TODO: real implementation involving in-the-pool policy interface, etc.
      double allocation = finalFractionRemaining / (sessions.size() + initializingSessions.size());
      for (WmTezSession session : sessions) {
        session.setClusterFraction(allocation);
      }
      // Do not give out the capacity of the initializing sessions to the running ones;
      // we expect init to be fast.
      return finalFractionRemaining - allocation * initializingSessions.size();
    }

    public LinkedList<WmTezSession> getSessions() {
      return sessions;
    }

    @Override
    public String toString() {
      return "[" + fullName + ", query parallelism " + queryParallelism
        + ", fraction of the cluster " + finalFraction + ", fraction used by child pools "
        + (finalFraction - finalFractionRemaining) + ", active sessions " + sessions.size()
        + ", initializing sessions " + initializingSessions.size() + "]";
    }

    private void extractAllSessionsToKill(String killReason,
      IdentityHashMap<WmTezSession, GetRequest> toReuse, List<WmTezSession> toKill) {
      for (WmTezSession sessionToKill : sessions) {
        resetRemovedSession(sessionToKill, killReason, toReuse);
        toKill.add(sessionToKill);
      }
      sessions.clear();
      for (SessionInitContext initCtx : initializingSessions) {
        // It is possible that the background init thread has finished in parallel, queued
        // the message for us but also returned the session to the user.
        WmTezSession sessionToKill = initCtx.cancelAndExtractSessionIfDone(killReason);
        if (sessionToKill == null) {
          continue; // Async op in progress; the callback will take care of this.
        }
        resetRemovedSession(sessionToKill, killReason, toReuse);
        toKill.add(sessionToKill);
      }
      initializingSessions.clear();
    }

    private void resetRemovedSession(WmTezSession sessionToKill, String killReason,
      IdentityHashMap<WmTezSession, GetRequest> toReuse) {
      assert killReason != null;
      sessionToKill.setIsIrrelevantForWm(killReason);
      sessionToKill.clearWm();
      GetRequest req = toReuse.remove(sessionToKill);
      if (req != null) {
        req.sessionToReuse = null;
      }
    }

    public void setTriggers(final LinkedList<Trigger> triggers) {
      this.triggers = triggers;
    }

    public List<Trigger> getTriggers() {
      return triggers;
    }
  }


  private enum SessionInitState {
    GETTING, // We are getting a session from TezSessionPool
    WAITING_FOR_REGISTRY, // We have the session but it doesn't have registry info yet.
    DONE, // We have the session with registry info, or we have failed.
    CANCELED // The master thread has CANCELED this and will never look at it again.
  }

  /**
   * The class that serves as a synchronization point, and future callback,
   * for async session initialization, as well as parallel cancellation.
   */
  private final class SessionInitContext implements FutureCallback<WmTezSession> {
    private final String poolName;

    private final ReentrantLock lock = new ReentrantLock();
    private WmTezSession session;
    private SettableFuture<WmTezSession> future;
    private SessionInitState state;
    private String cancelReason;

    public SessionInitContext(SettableFuture<WmTezSession> future, String poolName) {
      this.state = SessionInitState.GETTING;
      this.future = future;
      this.poolName = poolName;
    }

    @Override
    public void onSuccess(WmTezSession session) {
      SessionInitState oldState;
      SettableFuture<WmTezSession> future = null;
      lock.lock();
      try {
        oldState = state;
        switch (oldState) {
        case GETTING: {
          LOG.info("Received a session from AM pool {}", session);
          assert this.state == SessionInitState.GETTING;
          session.setPoolName(poolName);
          session.setQueueName(yarnQueue);
          this.session = session;
          this.state = SessionInitState.WAITING_FOR_REGISTRY;
          break;
        }
        case WAITING_FOR_REGISTRY: {
          assert this.session != null;
          this.state = SessionInitState.DONE;
          assert session == this.session;
          future = this.future;
          this.future = null;
          break;
        }
        case CANCELED: {
          future = this.future;
          this.session = null;
          this.future = null;
          break;
        }
        default: {
          future = this.future;
          this.future = null;
          break;
        }
        }
      } finally {
        lock.unlock();
      }
      switch (oldState) {
      case GETTING: {
        ListenableFuture<WmTezSession> waitFuture = session.waitForAmRegistryAsync(
            amRegistryTimeoutMs, timeoutPool);
        Futures.addCallback(waitFuture, this);
        break;
      }
      case WAITING_FOR_REGISTRY: {
        // Notify the master thread and the user.
        future.set(session);
        notifyInitializationCompleted(this);
        break;
      }
      case CANCELED: {
        // Return session to the pool; we can do it directly here.
        future.setException(new HiveException(
            "The query was killed by workload management: " + cancelReason));
        session.setPoolName(null);
        session.setClusterFraction(0f);
        tezAmPool.returnSession(session);
        break;
      }
      default: {
        AssertionError error = new AssertionError("Unexpected state " + state);
        future.setException(error);
        throw error;
      }
      }
    }

    @Override
    public void onFailure(Throwable t) {
      SettableFuture<WmTezSession> future;
      WmTezSession session;
      boolean wasCanceled = false;
      lock.lock();
      try {
        wasCanceled = (state == SessionInitState.CANCELED);
        session = this.session;
        future = this.future;
        this.future = null;
        this.session = null;
        if (!wasCanceled) {
          this.state = SessionInitState.DONE;
        }
      } finally {
        lock.unlock();
      }
      future.setException(t);
      if (!wasCanceled) {
        if (LOG.isDebugEnabled()) {
          LOG.info("Queueing the initialization failure with " + session);
        }
        notifyInitializationCompleted(this); // Report failure to the main thread.
      }
      if (session != null) {
        session.clearWm();
        // We can just restart the session if we have received one.
        try {
          tezAmPool.replaceSession(session, false, null);
        } catch (Exception e) {
          LOG.error("Failed to restart a failed session", e);
        }
      }
    }

    /** Cancel the async operation (even if it's done), and return the session if done. */
    public WmTezSession cancelAndExtractSessionIfDone(String cancelReason) {
      lock.lock();
      try {
        SessionInitState state = this.state;
        this.state = SessionInitState.CANCELED;
        this.cancelReason = cancelReason;
        if (state == SessionInitState.DONE) {
          WmTezSession result = this.session;
          this.session = null;
          return result;
        } else {
          // In the states where a background operation is in progress, wait for the callback.
          // Also, ignore any duplicate calls; also don't kill failed ones - handled elsewhere.
          if (state == SessionInitState.CANCELED) {
            LOG.warn("Duplicate call to extract " + session);
          }
          return null;
        }
      } finally {
        lock.unlock();
      }
    }

    /** Extracts the session and cancel the operation, both only if done. */
    public boolean extractSessionAndCancelIfDone(List<WmTezSession> results) {
      lock.lock();
      try {
        if (state != SessionInitState.DONE) return false;
        this.state = SessionInitState.CANCELED;
        if (this.session != null) {
          results.add(this.session);
        } // Otherwise we have failed; the callback has taken care of the failure.
        this.session = null;
        return true;
      } finally {
        lock.unlock();
      }
    }
  }

  boolean isManaged(MappingInput input) {
    // This is always replaced atomically, so we don't care about concurrency here.
    return userPoolMapping.mapSessionToPoolName(input) != null;
  }

  @VisibleForTesting
  TezSessionPool<WmTezSession> getTezAmPool() {
    return tezAmPool;
  }

  public final static class NoPoolMappingException extends Exception {
    public NoPoolMappingException(String message) {
      super(message);
    }

    private static final long serialVersionUID = 346375346724L;
  }
}
