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

import org.apache.hadoop.hive.metastore.api.WMPoolSchedulingPolicy;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.math.DoubleMath;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.tezplugins.LlapTaskSchedulerService;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMPoolTrigger;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.ql.exec.tez.AmPluginNode.AmPluginInfo;
import org.apache.hadoop.hive.ql.exec.tez.TezSessionState.HiveResources;
import org.apache.hadoop.hive.ql.exec.tez.UserPoolMapping.MappingInput;
import org.apache.hadoop.hive.ql.exec.tez.WmEvent.EventType;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.KillQuery;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.wm.ExecutionTrigger;
import org.apache.hadoop.hive.ql.wm.SessionTriggerProvider;
import org.apache.hadoop.hive.ql.wm.Trigger;
import org.apache.hadoop.hive.ql.wm.TriggerActionHandler;
import org.apache.hadoop.hive.ql.wm.WmContext;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.Ref;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** Workload management entry point for HS2.
 * Note on how this class operates.
 * There are tons of things that could be happening in parallel that are a real pain to sync.
 * Therefore, it uses an actor-ish model where the master thread, in processCurrentEvents method,
 * processes a bunch of events that have accumulated since the previous iteration, repeatedly
 * and quickly, doing physical work via async calls or via worker threads.
 * That way the bulk of the state (pools, etc.) does not require any sync, and we mostly have
 * a consistent view of the conflicting events when we process things. However, that also means
 * none of that state can be accessed directly - most changes that touch pool state, or interact
 * with background operations like init, need to go thru eventstate; see e.g. returnAfterUse.
 */
public class WorkloadManager extends TezSessionPoolSession.AbstractTriggerValidator
  implements TezSessionPoolSession.Manager, SessionExpirationTracker.RestartImpl,
             WorkloadManagerMxBean {
  private static final Logger LOG = LoggerFactory.getLogger(WorkloadManager.class);
  private static final char POOL_SEPARATOR = '.';
  private static final String POOL_SEPARATOR_STR = "" + POOL_SEPARATOR;

  private final ObjectMapper objectMapper;
  // Various final services, configs, etc.
  private final HiveConf conf;
  private final TezSessionPool<WmTezSession> tezAmPool;
  private final SessionExpirationTracker expirationTracker;
  private final RestrictedConfigChecker restrictedConfig;
  private final QueryAllocationManager allocationManager;
  private final String yarnQueue;
  private final int amRegistryTimeoutMs;
  private final boolean allowAnyPool;
  private final MetricsSystem metricsSystem;
  // Note: it's not clear that we need to track this - unlike PoolManager we don't have non-pool
  //       sessions, so the pool itself could internally track the ses  sions it gave out, since
  //       calling close on an unopened session is probably harmless.
  private final IdentityHashMap<TezSessionPoolSession, Boolean> openSessions =
      new IdentityHashMap<>();
  // We index the get requests to make sure there are no ordering artifacts when we requeue.
  private final AtomicLong getRequestVersion = new AtomicLong(Long.MIN_VALUE);

  // The below group of fields (pools, etc.) can only be modified by the master thread.
  private Map<String, PoolState> pools;
  private String rpName, defaultPool; // For information only.
  private int totalQueryParallelism;

  /**
   * The queries being killed. This is used to sync between the background kill finishing and the
   * query finishing and user returning the sessions, which can happen in separate iterations
   * of the master thread processing, yet need to be aware of each other.
   */
  private Map<WmTezSession, KillQueryContext> killQueryInProgress = new IdentityHashMap<>();
  // Used to make sure that waiting getSessions don't block update.
  private UserPoolMapping userPoolMapping;
  // End of master thread state

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
  private final WmThreadSyncWork syncWork = new WmThreadSyncWork();
  // End sync stuff.

  private PerPoolTriggerValidatorRunnable triggerValidatorRunnable;
  private Map<String, SessionTriggerProvider> perPoolProviders = new ConcurrentHashMap<>();

  // The master thread and various workers.
  /** The master thread the processes the events from EventState. */
  @VisibleForTesting
  protected final Thread wmThread;
  /** Used by the master thread to offload calls blocking on smth other than fast locks. */
  private final ExecutorService workPool;
  /** Used to schedule timeouts for some async operations. */
  private final ScheduledExecutorService timeoutPool;
  /** To retry delayed moves and check for expires delayed moves at specified intervals **/
  private final Thread delayedMoveThread;
  private final int delayedMoveTimeOutSec;
  private final int delayedMoveValidationIntervalSec;

  private LlapPluginEndpointClientImpl amComm;

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

  private static volatile WorkloadManager INSTANCE;

  public static WorkloadManager getInstance() {
    return INSTANCE;
  }

  /** Called once, when HS2 initializes. */
  public static WorkloadManager create(String yarnQueue, HiveConf conf, WMFullResourcePlan plan)
    throws ExecutionException, InterruptedException {
    assert INSTANCE == null;
    // We could derive the expected number of AMs to pass in.
    // Note: we pass a null token here; the tokens to talk to plugin endpoints will only be
    //       known once the AMs register, and they are different for every AM (unlike LLAP token).
    LlapPluginEndpointClientImpl amComm = new LlapPluginEndpointClientImpl(conf, null, -1);
    QueryAllocationManager qam = new GuaranteedTasksAllocator(conf, amComm);
    return (INSTANCE = new WorkloadManager(amComm, yarnQueue, conf, qam, plan));
  }

  @VisibleForTesting
  WorkloadManager(LlapPluginEndpointClientImpl amComm, String yarnQueue, HiveConf conf,
      QueryAllocationManager qam, WMFullResourcePlan plan) throws ExecutionException, InterruptedException {
    this.yarnQueue = yarnQueue;
    this.conf = conf;
    this.totalQueryParallelism = determineQueryParallelism(plan);
    this.allocationManager = qam;
    this.allocationManager.setClusterChangedCallback(() -> notifyOfClusterStateChange());

    this.amComm = amComm;
    if (this.amComm != null) {
      this.amComm.init(conf);
    }
    LOG.info("Initializing with " + totalQueryParallelism + " total query parallelism");

    this.amRegistryTimeoutMs = (int)HiveConf.getTimeVar(
      conf, ConfVars.HIVE_SERVER2_TEZ_WM_AM_REGISTRY_TIMEOUT, TimeUnit.MILLISECONDS);
    tezAmPool = new TezSessionPool<>(conf, totalQueryParallelism, true,
      oldSession -> createSession(oldSession == null ? null : oldSession.getConf()));
    restrictedConfig = new RestrictedConfigChecker(conf);
    // Only creates the expiration tracker if expiration is configured.
    expirationTracker = SessionExpirationTracker.create(conf, this);

    workPool = Executors.newFixedThreadPool(HiveConf.getIntVar(
        conf, ConfVars.HIVE_SERVER2_WM_WORKER_THREADS), new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("Workload management worker %d").build());

    timeoutPool = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
      .setDaemon(true).setNameFormat("Workload management timeout thread").build());

    allowAnyPool = HiveConf.getBoolVar(conf, ConfVars.HIVE_SERVER2_WM_ALLOW_ANY_POOL_VIA_JDBC);
    if (HiveConf.getBoolVar(conf, ConfVars.HIVE_SERVER2_WM_POOL_METRICS)) {
      metricsSystem = DefaultMetricsSystem.instance();
    } else {
      metricsSystem = null;
    }

    wmThread = new Thread(() -> runWmThread(), "Workload management master");
    wmThread.setDaemon(true);
    wmThread.start();

    delayedMoveTimeOutSec = (int) HiveConf.getTimeVar(conf,
        ConfVars.HIVE_SERVER2_WM_DELAYED_MOVE_TIMEOUT, TimeUnit.SECONDS);
    delayedMoveValidationIntervalSec = (int) HiveConf.getTimeVar(conf,
        ConfVars.HIVE_SERVER2_WM_DELAYED_MOVE_VALIDATOR_INTERVAL, TimeUnit.SECONDS);

    if ((HiveConf.getBoolVar(conf, ConfVars.HIVE_SERVER2_WM_DELAYED_MOVE)) &&
        (delayedMoveTimeOutSec > 0)) {
      delayedMoveThread = new Thread(() -> runDelayedMoveThread(), "Workload management delayed move");
      delayedMoveThread.setDaemon(true);
      LOG.info("Starting delayed move timeout validator with interval: {} s; " +
              "delayed move timeout is set to : {} s",
               delayedMoveValidationIntervalSec, delayedMoveTimeOutSec);
      delayedMoveThread.start();
    } else {
      delayedMoveThread = null;
    }

    updateResourcePlanAsync(plan).get(); // Wait for the initial resource plan to be applied.

    objectMapper = new ObjectMapper();
    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    // serialize json based on field annotations only
    objectMapper.setVisibility(objectMapper.getSerializationConfig().getDefaultVisibilityChecker()
      .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
      .withSetterVisibility(JsonAutoDetect.Visibility.NONE));
  }

  private static int determineQueryParallelism(WMFullResourcePlan plan) {
    if (plan == null) return 0;
    int result = 0;
    for (WMPool pool : plan.getPools()) {
      result += pool.getQueryParallelism();
    }
    return result;
  }

  public void start() throws Exception {
    initTriggers();
    tezAmPool.start();
    if (expirationTracker != null) {
      expirationTracker.start();
    }
    if (amComm != null) {
      amComm.start();
    }
    allocationManager.start();
  }

  private void initTriggers() {
    if (triggerValidatorRunnable == null) {
      final long triggerValidationIntervalMs = HiveConf.getTimeVar(conf,
        HiveConf.ConfVars.HIVE_TRIGGER_VALIDATION_INTERVAL, TimeUnit.MILLISECONDS);
      TriggerActionHandler<?> triggerActionHandler = new KillMoveTriggerActionHandler(this);
      triggerValidatorRunnable = new PerPoolTriggerValidatorRunnable(perPoolProviders, triggerActionHandler,
        triggerValidationIntervalMs);
      startTriggerValidator(triggerValidationIntervalMs);
    }
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

    if (delayedMoveThread != null) {
      delayedMoveThread.interrupt();
    }

    if (amComm != null) {
      amComm.stop();
    }
    workPool.shutdownNow();
    timeoutPool.shutdownNow();

    if (triggerValidatorRunnable != null) {
      stopTriggerValidator();
    }
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
        toDestroy = Sets.newIdentityHashSet();
    private final Map<WmTezSession, Boolean> killQueryResults = new IdentityHashMap<>();
    private final LinkedList<SessionInitContext> initResults = new LinkedList<>();
    private final IdentityHashMap<WmTezSession, SettableFuture<WmTezSession>> toReopen =
        new IdentityHashMap<>();
    private final IdentityHashMap<WmTezSession, Integer> updateErrors = new IdentityHashMap<>();
    private final LinkedList<GetRequest> getRequests = new LinkedList<>();
    private final IdentityHashMap<WmTezSession, GetRequest> toReuse = new IdentityHashMap<>();
    private WMFullResourcePlan resourcePlanToApply = null;
    private boolean doClearResourcePlan = false;
    private boolean hasClusterStateChanged = false;
    private List<SettableFuture<Boolean>> testEvents = new LinkedList<>();
    private SettableFuture<Boolean> applyRpFuture;
    private SettableFuture<List<String>> dumpStateFuture;
    private final List<MoveSession> moveSessions = new LinkedList<>();
  }

  private final static class MoveSession {
    private final WmTezSession srcSession;
    private final String destPool;
    private final SettableFuture<Boolean> future;
    private long startTime;

    public MoveSession(final WmTezSession srcSession, final String destPool) {
      this.srcSession = srcSession;
      this.destPool = destPool;
      this.future = SettableFuture.create();
      this.startTime = System.currentTimeMillis();
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
    private List<WmTezSession> toRestartInUse = new LinkedList<>(),
        toDestroyNoRestart = new LinkedList<>();
    private Map<WmTezSession, KillQueryContext> toKillQuery = new IdentityHashMap<>();
    private List<Path> pathsToDelete = Lists.newArrayList();
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
        for (SettableFuture<Boolean> testEvent : currentEvents.testEvents) {
          LOG.info("Failing test event " + System.identityHashCode(testEvent));
          testEvent.setException(ex);
        }
        currentEvents.testEvents.clear();
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

  private void runDelayedMoveThread() {
    while (true) {
      try {
        Thread.sleep(delayedMoveValidationIntervalSec * 1000);
        currentLock.lock();
        LOG.info("Retry delayed moves and check for expired delayed moves.");
        notifyWmThreadUnderLock();
      } catch (InterruptedException ex) {
        LOG.warn("WM Delayed Move thread was interrupted and will now exit");
        return;
      } finally {
        currentLock.unlock();
      }
    }
  }

  private void scheduleWork(WmThreadSyncWork context) {
    // Do the work that cannot be done via async calls.

    // 1. Kill queries.
    for (KillQueryContext killCtx : context.toKillQuery.values()) {
      final WmTezSession toKill = killCtx.session;
      final String reason = killCtx.reason;
      LOG.info("Killing query for {}", toKill);
      workPool.submit(() -> {
        // Note: we get query ID here, rather than in the caller, where it would be more correct
        //       because we know which exact query we intend to kill. This is valid because we
        //       are not expecting query ID to change - we never reuse the session for which a
        //       query is being killed until both the kill, and the user, return it.
        String queryId = toKill.getQueryId();
        KillQuery kq = toKill.getKillQuery();
        try {
          if (kq != null && queryId != null) {
            WmEvent wmEvent = new WmEvent(WmEvent.EventType.KILL);
            LOG.info("Invoking KillQuery for " + queryId + ": " + reason);
            try {
              UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
              SessionState ss = new SessionState(new HiveConf(), ugi.getShortUserName());
              ss.setIsHiveServerQuery(true);
              SessionState.start(ss);
              kq.killQuery(queryId, reason, toKill.getConf());
              addKillQueryResult(toKill, true);
              killCtx.killSessionFuture.set(true);
              wmEvent.endEvent(toKill);
              LOG.debug("Killed " + queryId);
              return;
            } catch (HiveException|IOException ex) {
              LOG.error("Failed to kill " + queryId + "; will try to restart AM instead" , ex);
            }
          } else {
            LOG.info("Will queue restart for {}; queryId {}, killQuery {}", toKill, queryId, kq);
          }
        } finally {
          toKill.setQueryId(null);
        }
        // We cannot restart in place because the user might receive a failure and return the
        // session to the master thread without the "irrelevant" flag set. In fact, the query might
        // have succeeded in the gap and the session might already be returned. Queue restart thru
        // the master thread.
        addKillQueryResult(toKill, false);
      });
    }
    context.toKillQuery.clear();

    // 2. Restart pool sessions.
    for (final WmTezSession toRestart : context.toRestartInUse) {
      LOG.info("Replacing {} with a new session", toRestart);
      toRestart.setQueryId(null);
      workPool.submit(() -> {
        try {
          WmEvent wmEvent = new WmEvent(WmEvent.EventType.RESTART);
          // Note: sessions in toRestart are always in use, so they cannot expire in parallel.
          tezAmPool.replaceSession(toRestart);
          wmEvent.endEvent(toRestart);
        } catch (Exception ex) {
          LOG.error("Failed to restart an old session; ignoring", ex);
        }
      });
    }
    context.toRestartInUse.clear();

    // 3. Destroy the sessions that we don't need anymore.
    for (final WmTezSession toDestroy : context.toDestroyNoRestart) {
      LOG.info("Closing {} without restart", toDestroy);
      workPool.submit(() -> {
        try {
          WmEvent wmEvent = new WmEvent(WmEvent.EventType.DESTROY);
          toDestroy.close(false);
          wmEvent.endEvent(toDestroy);
        } catch (Exception ex) {
          LOG.error("Failed to close an old session; ignoring " + ex.getMessage());
        }
      });
    }
    context.toDestroyNoRestart.clear();

    // 4. Delete unneeded directories that were replaced by other ones via reopen.
    for (final Path path : context.pathsToDelete) {
      LOG.info("Deleting {}", path);
      workPool.submit(() -> {
        try {
          path.getFileSystem(conf).delete(path, true);
        } catch (Exception ex) {
          LOG.error("Failed to delete an old path; ignoring " + ex.getMessage());
        }
      });
    }
    context.pathsToDelete.clear();
  }

  /**
   * This is the main method of the master thread the processes one set of events.
   * Be mindful of the fact that events can be queued while we are processing events, so
   * in addition to making sure we keep the current set consistent (e.g. no need to handle
   * update errors for a session that should already be destroyed), this needs to guard itself
   * against the future iterations - e.g. what happens if we kill a query due to plan change,
   * but the DAG finished before the kill happens and the user queues a "return" event? Etc.
   * DO NOT block for a long time in this method.
   * @param e Input events.
   * @param syncWork Output tasks that cannot be called via async methods.
   */
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

    // 1. Handle kill query results - part 1, just put them in place. We will resolve what
    //    to do with the sessions after we go thru all the concurrent user actions.
    for (Map.Entry<WmTezSession, Boolean> entry : e.killQueryResults.entrySet()) {
      WmTezSession killQuerySession = entry.getKey();
      boolean killResult = entry.getValue();
      LOG.debug("Processing KillQuery {} for {}",
          killResult ? "success" : "failure", killQuerySession);
      // Note: do not cancel any user actions here; user actions actually interact with kills.
      KillQueryContext killCtx = killQueryInProgress.get(killQuerySession);
      if (killCtx == null) {
        LOG.error("Internal error - cannot find the context for killing {}", killQuerySession);
        continue;
      }
      killCtx.handleKillQueryCallback(!killResult);
    }
    e.killQueryResults.clear();

    // 2. Handle sessions that are being destroyed by users. Destroy implies return.
    for (WmTezSession sessionToDestroy : e.toDestroy) {
      if (e.toReturn.remove(sessionToDestroy)) {
        LOG.warn("The session was both destroyed and returned by the user; destroying");
      }
      LOG.info("Destroying {}", sessionToDestroy);
      RemoveSessionResult rr = handleReturnedInUseSessionOnMasterThread(
          e, sessionToDestroy, poolsToRedistribute, false);
      if (rr == RemoveSessionResult.OK || rr == RemoveSessionResult.NOT_FOUND) {
        // Restart even if there's an internal error.
        syncWork.toRestartInUse.add(sessionToDestroy);
      }
    }
    e.toDestroy.clear();

    // 3. Now handle actual returns. Sessions may be returned to the pool or may trigger expires.
    for (WmTezSession sessionToReturn: e.toReturn) {
      LOG.info("Returning {}", sessionToReturn);
      RemoveSessionResult rr = handleReturnedInUseSessionOnMasterThread(
          e, sessionToReturn, poolsToRedistribute, true);
      switch (rr) {
      case OK:
        WmEvent wmEvent = new WmEvent(WmEvent.EventType.RETURN);
        boolean wasReturned = tezAmPool.returnSessionAsync(sessionToReturn);
        if (!wasReturned) {
          syncWork.toDestroyNoRestart.add(sessionToReturn);
        } else {
          if (sessionToReturn.getWmContext() != null && sessionToReturn.getWmContext().isQueryCompleted()) {
            sessionToReturn.resolveReturnFuture();
          }
          wmEvent.endEvent(sessionToReturn);
        }
        break;
      case NOT_FOUND:
        syncWork.toRestartInUse.add(sessionToReturn); // Restart if there's an internal error.
        break;
      case IGNORE:
        break;
      default: throw new AssertionError("Unknown state " + rr);
      }
    }
    e.toReturn.clear();

    // 4. Reopen is essentially just destroy + get a new session for a session in use.
    for (Map.Entry<WmTezSession, SettableFuture<WmTezSession>> entry : e.toReopen.entrySet()) {
      LOG.info("Reopening {}", entry.getKey());
      handeReopenRequestOnMasterThread(
        e, entry.getKey(), entry.getValue(), poolsToRedistribute, syncWork);
    }
    e.toReopen.clear();

    // 5. All the sessions in use that were not destroyed or returned with a failed update now die.
    for (Map.Entry<WmTezSession, Integer> entry : e.updateErrors.entrySet()) {
      WmTezSession sessionWithUpdateError = entry.getKey();
      int failedEndpointVersion = entry.getValue();
      LOG.info("Update failed for {}", sessionWithUpdateError);
      handleUpdateErrorOnMasterThread(
          sessionWithUpdateError, failedEndpointVersion, e.toReuse, syncWork, poolsToRedistribute);
    }
    e.updateErrors.clear();

    // 6. Now apply a resource plan if any. This is expected to be pretty rare.
    boolean hasRequeues = false;
    if (e.resourcePlanToApply != null || e.doClearResourcePlan) {
      LOG.info("Applying new resource plan");
      int getReqCount = e.getRequests.size();
      applyNewResourcePlanOnMasterThread(e, syncWork, poolsToRedistribute);
      hasRequeues = getReqCount != e.getRequests.size();
    }
    e.resourcePlanToApply = null;
    e.doClearResourcePlan = false;

    // 7. Handle any move session requests. The way move session works right now is
    // a) sessions get moved to destination pool if there is capacity in destination pool
    // b) if there is no capacity in destination pool, the session gets killed (since we cannot pause a query)
    // TODO: in future this the process of killing can be delayed until the point where a session is actually required.
    // We could consider delaying the move (when destination capacity is full) until there is claim in src pool.
    // May be change command to support ... DELAYED MOVE TO etl ... which will run under src cluster fraction as long
    // as possible
    Map<WmTezSession, WmEvent> recordMoveEvents = new HashMap<>();
    boolean convertToDelayedMove = HiveConf.getBoolVar(conf, ConfVars.HIVE_SERVER2_WM_DELAYED_MOVE);
    for (MoveSession moveSession : e.moveSessions) {
      handleMoveSessionOnMasterThread(moveSession, syncWork, poolsToRedistribute, e.toReuse,
          recordMoveEvents, convertToDelayedMove);
    }
    e.moveSessions.clear();

    // 8. Handle all the get/reuse requests. We won't actually give out anything here, but merely
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

    // 9. If delayed move is set to true, process the "delayed moves" now.
    if (HiveConf.getBoolVar(conf, ConfVars.HIVE_SERVER2_WM_DELAYED_MOVE)) {
      for (String poolName : pools.keySet()) {
        processDelayedMovesForPool(poolName, poolsToRedistribute, recordMoveEvents, syncWork, e.toReuse);
      }
    }

    // 10. Resolve all the kill query requests in flight. Nothing below can affect them.
    Iterator<KillQueryContext> iter = killQueryInProgress.values().iterator();
    while (iter.hasNext()) {
      KillQueryContext ctx = iter.next();
      KillQueryResult kr = ctx.process();
      switch (kr) {
      case IN_PROGRESS: continue; // Either the user or the kill is not done yet.
      case OK: {
        iter.remove();
        LOG.debug("Kill query succeeded; returning to the pool: {}", ctx.session);
        ctx.killSessionFuture.set(true);
        WmEvent wmEvent = new WmEvent(WmEvent.EventType.RETURN);
        if (!tezAmPool.returnSessionAsync(ctx.session)) {
          syncWork.toDestroyNoRestart.add(ctx.session);
        } else {
          if (ctx.session.getWmContext() != null && ctx.session.getWmContext().isQueryCompleted()) {
            ctx.session.resolveReturnFuture();
          }
          wmEvent.endEvent(ctx.session);
        }

        // Running query metrics needs to be updated for the pool
        updatePoolMetricsAfterKillTrigger(poolsToRedistribute, ctx);
        break;
      }
      case RESTART_REQUIRED: {
        iter.remove();
        ctx.killSessionFuture.set(true);
        LOG.debug("Kill query failed; restarting: {}", ctx.session);
        // Note: we assume here the session, before we resolve killQuery result here, is still
        //       "in use". That is because all the user ops above like return, reopen, etc.
        //       don't actually return/reopen/... when kill query is in progress.
        syncWork.toRestartInUse.add(ctx.session);

        // Running query metrics needs to be updated for the pool
        updatePoolMetricsAfterKillTrigger(poolsToRedistribute, ctx);
        break;
      }
      default: throw new AssertionError("Unknown state " + kr);
      }
    }

    // 11. If there was a cluster state change, make sure we redistribute all the pools.
    if (e.hasClusterStateChanged) {
      LOG.info("Processing a cluster state change");
      poolsToRedistribute.addAll(pools.keySet());
      e.hasClusterStateChanged = false;
    }

    // 12. Finally, for all the pools that have changes, promote queued queries and rebalance.
    for (String poolName : poolsToRedistribute) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Processing changes for pool " + poolName + ": " + pools.get(poolName));
      }
      processPoolChangesOnMasterThread(poolName, hasRequeues, syncWork);
    }

    // 13. Save state for future iterations.
    for (KillQueryContext killCtx : syncWork.toKillQuery.values()) {
      if (killQueryInProgress.put(killCtx.session, killCtx) != null) {
        LOG.error("One query killed several times - internal error {}", killCtx.session);
      }
    }

    // 14. To record move events, we need to cluster fraction updates that happens at step 12.
    for (Map.Entry<WmTezSession, WmEvent> entry : recordMoveEvents.entrySet()) {
      entry.getValue().endEvent(entry.getKey());
    }

    // 15. Give our final state to UI/API requests if any.
    if (e.dumpStateFuture != null) {
      List<String> result = new ArrayList<>();
      result.add("RESOURCE PLAN " + rpName + "; default pool " + defaultPool);
      for (PoolState ps : pools.values()) {
        dumpPoolState(ps, result);
      }
      e.dumpStateFuture.set(result);
      e.dumpStateFuture = null;
    }

    // 16. Notify tests and global async ops.
    for (SettableFuture<Boolean> testEvent : e.testEvents) {
      LOG.info("Triggering test event " + System.identityHashCode(testEvent));
      testEvent.set(null);
    }
    e.testEvents.clear();

    if (e.applyRpFuture != null) {
      e.applyRpFuture.set(true);
      e.applyRpFuture = null;
    }
  }

  private void updatePoolMetricsAfterKillTrigger(HashSet<String> poolsToRedistribute, KillQueryContext ctx) {
    String poolName = ctx.getPoolName();
    if (StringUtils.isNotBlank(poolName)) {
      poolsToRedistribute.add(poolName);
      PoolState pool = pools.get(poolName);
      if ((pool != null) && (pool.metrics != null)) {
          LOG.debug(String.format("Removing 1 query from pool %s, Current numRunningQueries: %s", pool.fullName,
              pool.metrics.numRunningQueries.value()));
          pool.metrics.removeRunningQueries(1);
      }
    }
  }

  private void dumpPoolState(PoolState ps, List<String> set) {
    StringBuilder sb = new StringBuilder();
    sb.append("POOL ").append(ps.fullName).append(": qp ").append(ps.queryParallelism)
      .append(", %% ").append(ps.finalFraction).append(", sessions: ").append(ps.sessions.size())
      .append(", initializing: ").append(ps.initializingSessions.size()).append(", queued: ")
      .append(ps.queue.size());
    set.add(sb.toString());
    sb.setLength(0);
    for (WmTezSession session : ps.sessions) {
      double cf = session.hasClusterFraction() ? session.getClusterFraction() : 0;
      sb.append("RUNNING: ").append(cf).append(" (") .append(session.getAllocationState())
        .append(") => ").append(session.getSessionId());
      set.add(sb.toString());
      sb.setLength(0);
    }
    for (SessionInitContext session : ps.initializingSessions) {
      sb.append("INITIALIZING: state ").append(session.state);
      set.add(sb.toString());
      sb.setLength(0);
    }
    for (GetRequest session : ps.queue) {
      sb.append("QUEUED: from ").append(session.mappingInput);
      set.add(sb.toString());
      sb.setLength(0);
    }
  }

  private static enum MoveSessionResult {
    OK, // Normal case - the session was moved.
    KILLED, // Killed because destination pool was full and delayed move is false.
    CONVERTED_TO_DELAYED_MOVE, // the move session was added to the pool's delayed moves as the dest. pool was full
    // and delayed move is true.
    ERROR
  }

  private MoveSessionResult handleMoveSessionOnMasterThread(final MoveSession moveSession,
      final WmThreadSyncWork syncWork,
      final HashSet<String> poolsToRedistribute,
      final Map<WmTezSession, GetRequest> toReuse,
      final Map<WmTezSession, WmEvent> recordMoveEvents,
      final boolean convertToDelayedMove) {
    String destPoolName = moveSession.destPool;
    LOG.info("Handling move session event: {}, Convert to Delayed Move: {}", moveSession, convertToDelayedMove);
    if (validMove(moveSession.srcSession, destPoolName)) {
      String srcPoolName = moveSession.srcSession.getPoolName();
      PoolState srcPool = pools.get(srcPoolName);
      boolean capacityAvailableInDest = capacityAvailable(destPoolName);
      // If delayed move is set to true and if destination pool doesn't have enough capacity, don't kill the query.
      // Let the query run in source pool. Add the session to the source pool's delayed move sessions.
      if (convertToDelayedMove && !capacityAvailableInDest) {
        srcPool.delayedMoveSessions.add(moveSession);
        moveSession.srcSession.setDelayedMove(true);
        LOG.info("Converting Move: {} to a delayed move. Since destination pool {} is full, running in source pool {}"
            + " as long as possible.", moveSession, destPoolName, srcPoolName);
        moveSession.future.set(false);
        return MoveSessionResult.CONVERTED_TO_DELAYED_MOVE;
      }
      WmEvent moveEvent = new WmEvent(WmEvent.EventType.MOVE);
      // remove from src pool
      if (moveSession.srcSession.isDelayedMove()) {
        moveSession.srcSession.setDelayedMove(false);
      }
      RemoveSessionResult rr = checkAndRemoveSessionFromItsPool(
          moveSession.srcSession, poolsToRedistribute, true, true);
      if (rr == RemoveSessionResult.OK) {
        // check if there is capacity in dest pool, if so move else kill the session
        if (capacityAvailableInDest) {
          // add to destination pool
          Boolean added = checkAndAddSessionToAnotherPool(
              moveSession.srcSession, destPoolName, poolsToRedistribute);
          if (added != null && added) {
            moveSession.future.set(true);
            recordMoveEvents.put(moveSession.srcSession, moveEvent);
            return MoveSessionResult.OK;
          } else {
            LOG.error("Failed to move session: {}. Session is not added to destination.", moveSession);
          }
        } else {
          WmTezSession session = moveSession.srcSession;
          KillQueryContext killQueryContext = new KillQueryContext(session, "Destination pool " + destPoolName +
             " is full. Killing query.");
          resetAndQueueKill(syncWork.toKillQuery, killQueryContext, toReuse);
          moveSession.future.set(false);
          return MoveSessionResult.KILLED;
        }
      } else {
        LOG.error("Failed to move session: {}. Session is not removed from its pool.", moveSession);
      }
    } else {
      LOG.error("Validation failed for move session: {}. Invalid move or session/pool got removed.", moveSession);
    }
    moveSession.future.set(false);
    return MoveSessionResult.ERROR;
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

  private boolean validDelayedMove(final MoveSession moveSession, final PoolState pool, final String poolName) {
    return  validMove(moveSession.srcSession, moveSession.destPool) &&
        moveSession.srcSession.isDelayedMove() &&
        moveSession.srcSession.getPoolName().equalsIgnoreCase(poolName) &&
        pool.getSessions().contains(moveSession.srcSession);
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

  private RemoveSessionResult handleReturnedInUseSessionOnMasterThread(
      EventState e, WmTezSession session, HashSet<String> poolsToRedistribute, boolean isReturn) {
    // This handles the common logic for destroy and return - everything except
    // the invalid combination of destroy and return themselves, as well as the actual
    // statement that destroys or returns it.
    if (e.updateErrors.remove(session) != null) {
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
    session.setQueryId(null);
    return checkAndRemoveSessionFromItsPool(session, poolsToRedistribute, isReturn, true);
  }

  private void handeReopenRequestOnMasterThread(EventState e, WmTezSession session,
      SettableFuture<WmTezSession> future, HashSet<String> poolsToRedistribute,
      WmThreadSyncWork syncWork) throws Exception {
    if (e.updateErrors.remove(session) != null) {
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
    // Do not update metrics, we'd immediately add the session back if we are able to remove.
    RemoveSessionResult rr = checkAndRemoveSessionFromItsPool(
        session, poolsToRedistribute, false, false);
    switch (rr) {
    case OK:
      // If pool didn't exist, checkAndRemoveSessionFromItsPool wouldn't have returned OK.
      PoolState pool = pools.get(poolName);
      SessionInitContext sw = new SessionInitContext(future, poolName, session.getQueryId(),
          session.getWmContext(), session.extractHiveResources());
      // We have just removed the session from the same pool, so don't check concurrency here.
      pool.initializingSessions.add(sw);
      // Do not update metrics - see above.
      sw.start();
      syncWork.toRestartInUse.add(session);
      return;
    case IGNORE:
      // Reopen implies the use of the reopened session for the same query that we gave it out
      // for; so, as we would have failed an active query, fail the user before it's started.
      future.setException(new RuntimeException("WM killed this session during reopen: "
          + session.getReasonForKill()));
      return; // No longer relevant for WM.
    case NOT_FOUND:
      // If we fail to remove, it's probably an internal error. We'd try to handle it the same way
      // as above - by restarting the session. We'd fail the caller to avoid exceeding parallelism.
      future.setException(new RuntimeException("Reopen failed due to an internal error"));
      syncWork.toRestartInUse.add(session);
      return;
    default: throw new AssertionError("Unknown state " + rr);
    }
  }

  private void handleUpdateErrorOnMasterThread(WmTezSession session,
      int failedEndpointVersion, IdentityHashMap<WmTezSession, GetRequest> toReuse,
      WmThreadSyncWork syncWork, HashSet<String> poolsToRedistribute) {
    // First, check if the registry has been updated since the error, and skip the error if
    // we have received new, valid registry info (TODO: externally, add a grace period for this?).
    Ref<Integer> endpointVersion = new Ref<>(-1);
    AmPluginInfo info = session.getAmPluginInfo(endpointVersion);
    if (info != null && endpointVersion.value > failedEndpointVersion) {
      LOG.info("Ignoring an update error; endpoint information has been updated to {}", info);
      return;
    }
    GetRequest reuseRequest = toReuse.remove(session);
    if (reuseRequest != null) {
      // This session is bad, so don't allow reuse; just convert it to normal get.
      reuseRequest.sessionToReuse = null;
    }

    // We are assuming the update-error AM is bad and just try to kill it.
    RemoveSessionResult rr = checkAndRemoveSessionFromItsPool(
        session, poolsToRedistribute, null, true);
    switch (rr) {
    case OK:
    case NOT_FOUND:
      // Regardless whether it was removed successfully or after failing to remove, restart it.
      // Since we just restart this from under the user, mark it so we handle it properly when
      // the user tries to actually use this session and fails, proceeding to return/destroy it.
      session.setIsIrrelevantForWm("Failed to update resource allocation");
      // We assume AM might be bad so we will not try to kill the query here; just scrap the AM.
      // TODO: propagate this error to TezJobMonitor somehow? Without using killQuery
      syncWork.toRestartInUse.add(session);
      break;
    case IGNORE:
      return; // An update error for some session that was actually already killed by us.
    default:
      throw new AssertionError("Unknown state " + rr);
    }
  }

  private void applyNewResourcePlanOnMasterThread(
    EventState e, WmThreadSyncWork syncWork, HashSet<String> poolsToRedistribute) {
    int totalQueryParallelism = 0;
    WMFullResourcePlan plan = e.resourcePlanToApply;
    if (plan == null) {
      // NULL plan means WM is disabled via a command; it could still be reenabled.
      LOG.info("Disabling workload management because the resource plan has been removed");
      this.rpName = null;
      this.defaultPool = null;
      this.userPoolMapping = new UserPoolMapping(null, null);
    } else {
      this.rpName = plan.getPlan().getName();
      this.defaultPool = plan.getPlan().getDefaultPoolPath();
      this.userPoolMapping = new UserPoolMapping(plan.getMappings(), defaultPool);
    }
    // Note: we assume here that plan has been validated beforehand, so we don't verify
    //       that fractions or query parallelism add up, etc.
    Map<String, PoolState> oldPools = pools;
    pools = new HashMap<>();

    ArrayList<List<WMPool>> poolsByLevel = new ArrayList<>();
    if (plan != null) {
      // For simplicity, to always have parents while storing pools in a flat structure, we'll
      // first distribute them by levels, then add level by level.
      for (WMPool pool : plan.getPools()) {
        String fullName = pool.getPoolPath();
        int ix = StringUtils.countMatches(fullName, POOL_SEPARATOR_STR);
        while (poolsByLevel.size() <= ix) {
          poolsByLevel.add(new LinkedList<WMPool>()); // We expect all the levels to have items.
        }
        poolsByLevel.get(ix).add(pool);
      }
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
          state = new PoolState(fullName, qp, fraction, pool.getSchedulingPolicy(), metricsSystem);
        } else {
          // This will also take care of the queries if query parallelism changed.
          state.update(qp, fraction, syncWork, e, pool.getSchedulingPolicy());
          poolsToRedistribute.add(fullName);
        }
        state.setTriggers(new LinkedList<Trigger>());
        LOG.info("Adding Hive pool: " + state);
        pools.put(fullName, state);
        totalQueryParallelism += qp;
      }
    }
    for (PoolState pool : pools.values()) {
      if (pool.metrics != null) {
        pool.metrics.setMaxExecutors(
            allocationManager.translateAllocationToCpus(pool.finalFractionRemaining));
      }
    }
    // TODO: in the current impl, triggers are added to RP. For tez, no pool triggers (mapping between trigger name and
    // pool name) will exist which means all triggers applies to tez. For LLAP, pool triggers has to exist for attaching
    // triggers to specific pools.
    // For usability,
    // Provide a way for triggers sharing/inheritance possibly with following modes
    // ONLY - only to pool
    // INHERIT - child pools inherit from parent
    // GLOBAL - all pools inherit
    if (plan != null && plan.isSetTriggers() && plan.isSetPoolTriggers()) {
      Map<String, Trigger> triggers = new HashMap<>();
      for (WMTrigger trigger : plan.getTriggers()) {
        ExecutionTrigger execTrigger = ExecutionTrigger.fromWMTrigger(trigger);
        triggers.put(trigger.getTriggerName(), execTrigger);
      }
      for (WMPoolTrigger poolTrigger : plan.getPoolTriggers()) {
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
        oldPool.destroy(syncWork, e.getRequests, e.toReuse);
      }
    }

    LOG.info("Updating with " + totalQueryParallelism + " total query parallelism");
    int deltaSessions = totalQueryParallelism - this.totalQueryParallelism;
    this.totalQueryParallelism = totalQueryParallelism;
    if (deltaSessions == 0) return; // Nothing to do.
    if (deltaSessions < 0) {
      // First, see if we have sessions that we were planning to restart/kill; get rid of those.
      deltaSessions = transferSessionsToDestroy(
          syncWork.toKillQuery.keySet(), syncWork.toDestroyNoRestart, deltaSessions);
      deltaSessions = transferSessionsToDestroy(
          syncWork.toRestartInUse, syncWork.toDestroyNoRestart, deltaSessions);
    }
    if (deltaSessions != 0) {
      failOnFutureFailure(tezAmPool.resizeAsync(deltaSessions, syncWork.toDestroyNoRestart));
    }
  }

  private static int transferSessionsToDestroy(Collection<WmTezSession> source,
      List<WmTezSession> toDestroy, int deltaSessions) {
    // We were going to kill some queries and reuse the sessions, or maybe restart and put the new
    // ones back into the AM pool. However, the AM pool has shrunk, so we will close them instead.
    if (deltaSessions >= 0) return deltaSessions;
    int toTransfer = Math.min(-deltaSessions, source.size());
    Iterator<WmTezSession> iter = source.iterator();
    for (int i = 0; i < toTransfer; ++i) {
      WmTezSession session = iter.next();
      LOG.debug("Will destroy {} instead of restarting", session);
      if (!session.isIrrelevantForWm()) {
        session.setIsIrrelevantForWm("Killed due to workload management plan change");
      }
      toDestroy.add(session);
      iter.remove();
    }
    return deltaSessions + toTransfer;
  }

  private void failOnFutureFailure(ListenableFuture<?> future) {
    Futures.addCallback(future, FATAL_ERROR_CALLBACK, MoreExecutors.directExecutor());
  }

  private void queueGetRequestOnMasterThread(
      GetRequest req, HashSet<String> poolsToRedistribute, WmThreadSyncWork syncWork) {
    String poolName = userPoolMapping.mapSessionToPoolName(
        req.mappingInput, allowAnyPool, allowAnyPool ? pools.keySet() : null);
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
      RemoveSessionResult rr = checkAndRemoveSessionFromItsPool(
          req.sessionToReuse, poolsToRedistribute, true, false);
      if (rr != RemoveSessionResult.OK) {
        if (oldPool.metrics != null) {
          oldPool.metrics.removeRunningQueries(1);
        }
        // Abandon the reuse attempt.
        returnSessionOnFailedReuse(req, syncWork, null);
        req.sessionToReuse = null;
      } else if (pool.getTotalActiveSessions() + pool.queue.size() >= pool.queryParallelism) {
        if (oldPool.metrics != null) {
          oldPool.metrics.removeRunningQueries(1);
        }
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
      req.sessionToReuse.setQueryId(req.queryId);
      // Do not update metrics - we didn't update on removal.
      pool.sessions.add(req.sessionToReuse);
      if (pool != oldPool) {
        poolsToRedistribute.add(poolName);
      }
      req.future.set(req.sessionToReuse);
      return;
    }

    // Otherwise, queue the session and make sure we update this pool.
    pool.queue.addLast(req);
    if (pool.metrics != null) {
      pool.metrics.addQueuedQuery();
    }
    poolsToRedistribute.add(poolName);
  }


  private void processPoolChangesOnMasterThread(
      String poolName, boolean hasRequeues, WmThreadSyncWork syncWork) throws Exception {
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
      if (pool.metrics != null) {
        pool.metrics.moveQueuedToRunning();
      }
      assert queueReq.sessionToReuse == null;
      // Note that in theory, we are guaranteed to have a session waiting for us here, but
      // the expiration, failures, etc. may cause one to be missing pending restart.
      // See SessionInitContext javadoc.
      SessionInitContext sw = new SessionInitContext(
          queueReq.future, poolName, queueReq.queryId, queueReq.wmContext, null);
      sw.start();
      // It is possible that all the async methods returned on the same thread because the
      // session with registry data and stuff was available in the pool.
      // If this happens, we'll take the session out here and "cancel" the init so we skip
      // processing the message that the successful init has queued for us.
      boolean isDone = sw.extractSessionAndCancelIfDone(pool.sessions, syncWork.pathsToDelete);
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
    int cpusAllocated = allocationManager.updateSessionsAsync(totalAlloc, pool.sessions);
    if (pool.metrics != null) {
      pool.metrics.setExecutors(cpusAllocated);
      if (cpusAllocated > 0) {
        // Update max executors now that cluster info is definitely available.
        pool.metrics.setMaxExecutors(allocationManager.translateAllocationToCpus(totalAlloc));
      }
    }
  }

  private void processDelayedMovesForPool(final String poolName, final HashSet<String> poolsToRedistribute, final Map<WmTezSession, WmEvent> recordMoveEvents,
      WmThreadSyncWork syncWork, IdentityHashMap<WmTezSession, GetRequest> toReuse) {
    long currentTime = System.currentTimeMillis();
    PoolState pool = pools.get(poolName);
    int movedCount = 0;
    int queueSize = pool.queue.size();
    int remainingCapacity = pool.queryParallelism - pool.getTotalActiveSessions();
    int delayedMovesToProcess = (queueSize > remainingCapacity) ? (queueSize - remainingCapacity) : 0;
    Iterator<MoveSession> iter = pool.delayedMoveSessions.iterator();
    while (iter.hasNext()) {
      MoveSession moveSession = iter.next();
      MoveSessionResult result;

      //Discard the delayed move if invalid
      if (!validDelayedMove(moveSession, pool, poolName)) {
        iter.remove();
        continue;
      }

      // Process the delayed move if
      // 1. The delayed move has timed out or
      // 2. The destination pool has freed up or
      // 3. If the source pool has incoming requests and we need to free up capacity in the source pool
      // to accommodate these requests.
      if ((movedCount < delayedMovesToProcess) || (capacityAvailable(moveSession.destPool))
          || ((delayedMoveTimeOutSec > 0) && ((currentTime - moveSession.startTime) >= delayedMoveTimeOutSec * 1000))){
        LOG.info("Processing delayed move {} for pool {}", moveSession, poolName);
        result = handleMoveSessionOnMasterThread(moveSession, syncWork, poolsToRedistribute, toReuse, recordMoveEvents,
            false);
        LOG.info("Result of processing delayed move {} for pool {}: {}", moveSession, poolName, result);
        iter.remove();
        if ((result == MoveSessionResult.OK) || (result == MoveSessionResult.KILLED)) {
          movedCount++;
        }
      }
    }
  }

  private void returnSessionOnFailedReuse(
      GetRequest req, WmThreadSyncWork syncWork, HashSet<String> poolsToRedistribute) {
    WmTezSession session = req.sessionToReuse;
    if (session == null) return;
    req.sessionToReuse = null;
    session.setQueryId(null);
    if (poolsToRedistribute != null) {
      RemoveSessionResult rr = checkAndRemoveSessionFromItsPool(
          session, poolsToRedistribute, true, true);
      // The session cannot have been killed just now; this happens after all the kills in
      // the current iteration, so we would have cleared sessionToReuse when killing this.
      boolean isOk = (rr == RemoveSessionResult.OK);
      assert isOk || rr == RemoveSessionResult.IGNORE;
      if (!isOk) return;
    }
    WmEvent wmEvent = new WmEvent(WmEvent.EventType.RETURN);
    if (!tezAmPool.returnSessionAsync(session)) {
      syncWork.toDestroyNoRestart.add(session);
    } else {
      if (session.getWmContext() != null && session.getWmContext().isQueryCompleted()) {
        session.resolveReturnFuture();
      }
      wmEvent.endEvent(session);
    }
  }

  /** The result of trying to remove a presumably-active session from a pool on a user request. */
  private static enum RemoveSessionResult {
    OK, // Normal case - an active session was removed from the pool.
    IGNORE, // Session was restarted out of bounds, any user-side handling should be ignored.
            // Or, session is being killed, need to coordinate between that and the user.
            // These two cases don't need to be distinguished for now.
    NOT_FOUND // The session is active but not found in the pool - internal error.
  }

  /**
   * Checks if the session is still relevant for WM and if yes, removes it from its thread.
   * @param isSessionOk Whether the user thinks the session being returned in some way is ok;
   *                    true means it is (return, reuse); false mean it isn't (reopen, destroy);
   *                    null means this is not a user call.
   * @return true if the session was removed; false if the session was already processed by WM
   *         thread (so we are dealing with an outdated request); null if the session should be
   *         in WM but wasn't found in the requisite pool (internal error?).
   */
  private RemoveSessionResult checkAndRemoveSessionFromItsPool(WmTezSession session,
      Set<String> poolsToRedistribute, Boolean isSessionOk, boolean updateMetrics) {
    // It is possible for some request to be queued after a main thread has decided to kill this
    // session; on the next iteration, we'd be processing that request with an irrelevant session.
    if (session.isIrrelevantForWm()) {
      return RemoveSessionResult.IGNORE;
    }
    if (killQueryInProgress.containsKey(session)) {
      if (isSessionOk != null) {
        killQueryInProgress.get(session).handleUserCallback(!isSessionOk);
      }
      return RemoveSessionResult.IGNORE;
    }
    // If we did not kill this session we expect everything to be present.
    String poolName = session.getPoolName();
    if (poolName != null) {
      poolsToRedistribute.add(poolName);
      PoolState pool = pools.get(poolName);
      session.clearWm();
      if (pool != null && pool.sessions.remove(session)) {
        if (updateMetrics && pool.metrics != null) {
          pool.metrics.removeRunningQueries(1);
        }
        return RemoveSessionResult.OK;
      }
    }
    LOG.error("Session was not in the pool (internal error) " + poolName + ": " + session);
    return RemoveSessionResult.NOT_FOUND;
  }

  private Boolean checkAndAddSessionToAnotherPool(
    WmTezSession session, String destPoolName, Set<String> poolsToRedistribute) {
    if (session.isIrrelevantForWm()) {
      // This is called only during move session handling, removing session already checks this.
      // So this is not expected as remove failing will not even invoke this method
      LOG.error("Unexpected during add session to another pool. If remove failed this should not have been called.");
      return false;
    }

    PoolState destPool = pools.get(destPoolName);
    if (destPool != null && destPool.sessions.add(session)) {
      if (destPool.metrics != null) {
        destPool.metrics.addRunningQuery();
      }
      session.setPoolName(destPoolName);
      updateTriggers(session);
      poolsToRedistribute.add(destPoolName);
      return true;
    }
    LOG.error("Session {} was not added to pool {}", session, destPoolName);
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
      current.applyRpFuture = applyRpFuture;
      if (plan == null) {
        current.resourcePlanToApply = null;
        current.doClearResourcePlan = true;
      } else {
        current.resourcePlanToApply = plan;
        current.doClearResourcePlan = false;
      }
      notifyWmThreadUnderLock();
    } finally {
      currentLock.unlock();
    }
    return applyRpFuture;
  }

  Future<Boolean> applyMoveSessionAsync(WmTezSession srcSession, String destPoolName) {
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

  Future<Boolean> applyKillSessionAsync(WmTezSession wmTezSession, String killReason) {
    KillQueryContext killQueryContext;
    currentLock.lock();
    try {
      killQueryContext = new KillQueryContext(wmTezSession, killReason);
      resetAndQueueKill(syncWork.toKillQuery, killQueryContext, current.toReuse);
      LOG.info("Queued session for kill: {}", killQueryContext.session);
      notifyWmThreadUnderLock();
    } finally {
      currentLock.unlock();
    }
    return killQueryContext.killSessionFuture;
  }

  private final static class GetRequest {
    public static final Comparator<GetRequest> ORDER_COMPARATOR = (o1, o2) -> {
      if (o1.order == o2.order) return 0;
      return o1.order < o2.order ? -1 : 1;
    };
    private final long order;
    private final MappingInput mappingInput;
    private final SettableFuture<WmTezSession> future;
    private WmTezSession sessionToReuse;
    private final String queryId;
    private final WmContext wmContext;

    private GetRequest(MappingInput mappingInput, String queryId,
      SettableFuture<WmTezSession> future, WmTezSession sessionToReuse, long order,
      final WmContext wmContext) {
      assert mappingInput != null;
      this.mappingInput = mappingInput;
      this.queryId = queryId;
      this.future = future;
      this.sessionToReuse = sessionToReuse;
      this.order = order;
      this.wmContext = wmContext;
    }

    @Override
    public String toString() {
      return "[#" + order + ", " + mappingInput + ", reuse " + sessionToReuse + "]";
    }
  }

  @VisibleForTesting
  public WmTezSession getSession(
      TezSessionState session, MappingInput input, HiveConf conf) throws Exception {
    return getSession(session, input, conf, null);
  }

  public WmTezSession getSession(TezSessionState session, MappingInput input, HiveConf conf,
      final WmContext wmContext) throws Exception {
    WmEvent wmEvent = new WmEvent(WmEvent.EventType.GET);
    // Note: not actually used for pool sessions; verify some things like doAs are not set.
    validateConfig(conf);
    String queryId = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_QUERY_ID);
    SettableFuture<WmTezSession> future = SettableFuture.create();
    WmTezSession wmSession = checkSessionForReuse(session);
    GetRequest req = new GetRequest(
        input, queryId, future, wmSession, getRequestVersion.incrementAndGet(), wmContext);
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
    try {
      WmTezSession sessionState = future.get();
      wmEvent.endEvent(sessionState);
      return sessionState;
    } catch (ExecutionException ex) {
      Throwable realEx = ex.getCause();
      throw realEx instanceof Exception ? (Exception)realEx : ex;
    }
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
      wmTezSession.createAndSetReturnFuture();
      current.toReturn.add(wmTezSession);
      notifyWmThreadUnderLock();
    } finally {
      currentLock.unlock();
    }
  }

  public void notifyOfInconsistentAllocation(WmTezSession session) {
    // We just act as a pass-thru between the session and allocation manager. We don't change the
    // allocation target (only WM thread can do that); therefore we can do this directly and
    // actualState-based sync will take care of multiple potential message senders.
    allocationManager.updateSessionAsync(session);
  }

  public void notifyOfClusterStateChange() {
    currentLock.lock();
    try {
      current.hasClusterStateChanged = true;
      notifyWmThreadUnderLock();
    } finally {
      currentLock.unlock();
    }
  }

  void addUpdateError(WmTezSession wmTezSession, int endpointVersion) {
    currentLock.lock();
    try {
      Integer existing = current.updateErrors.get(wmTezSession);
      // Only store the latest error, if there are multiple.
      if (existing != null && existing >= endpointVersion) return;
      current.updateErrors.put(wmTezSession, endpointVersion);
      notifyWmThreadUnderLock();
    } finally {
      currentLock.unlock();
    }
  }


  @Override
  public List<String> getWmStateDescription() {
    Future<List<String>> future = null;
    currentLock.lock();
    try {
      if (current.dumpStateFuture != null) {
        future = current.dumpStateFuture;
      } else {
        future = current.dumpStateFuture = SettableFuture.create();
        notifyWmThreadUnderLock();
      }
    } finally {
      currentLock.unlock();
    }
    try {
      return future.get();
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Error getting description", e);
      return Lists.newArrayList("Error: " + e.toString());
    }
  }

  private void addKillQueryResult(WmTezSession toKill, boolean success) {
    currentLock.lock();
    try {
      current.killQueryResults.put(toKill, success);
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
      LOG.info("Adding test event " + System.identityHashCode(testEvent));
      current.testEvents.add(testEvent);
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
  public TezSessionState reopen(TezSessionState session) throws Exception {
    WmTezSession wmTezSession = ensureOwnedSession(session);
    HiveConf sessionConf = wmTezSession.getConf();
    if (sessionConf == null) {
      // TODO: can this ever happen?
      LOG.warn("Session configuration is null for " + wmTezSession);
      sessionConf = new HiveConf(conf, WorkloadManager.class);
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
    tezAmPool.replaceSession(ensureOwnedSession(session));
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
    tezAmPool.notifyClosed(session);
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

  void updateTriggers(final WmTezSession session) {
    WmContext wmContext = session.getWmContext();
    String poolName = session.getPoolName();
    PoolState poolState = pools.get(poolName);
    if (wmContext != null && poolState != null) {
      wmContext.addTriggers(poolState.getTriggers());
      LOG.info("Subscribed to counters: {}", wmContext.getSubscribedCounters());
    }
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
    private final LinkedList<MoveSession> delayedMoveSessions = new LinkedList<MoveSession>();
    private final WmPoolMetrics metrics;

    private final String fullName;
    private double finalFraction;
    private double finalFractionRemaining;
    private int queryParallelism = -1;
    private List<Trigger> triggers = new ArrayList<>();
    private WMPoolSchedulingPolicy schedulingPolicy;

    public PoolState(String fullName, int queryParallelism, double fraction,
        String schedulingPolicy, MetricsSystem ms) {
      this.fullName = fullName;
      // TODO: this actually calls the metrics system and getMetrics - that may be expensive.
      //       For now it looks like it should be ok to do on WM thread.
      this.metrics = ms == null ? null : WmPoolMetrics.create(fullName, ms);
      update(queryParallelism, fraction, null, null, schedulingPolicy);
    }

    public int getTotalActiveSessions() {
      return sessions.size() + initializingSessions.size();
    }

    public void update(int queryParallelism, double fraction,
        WmThreadSyncWork syncWork, EventState e, String schedulingPolicy) {
      this.finalFraction = this.finalFractionRemaining = fraction;
      this.queryParallelism = queryParallelism;
      if (metrics != null) {
        metrics.setParallelQueries(queryParallelism);
      }
      try {
        this.schedulingPolicy = MetaStoreUtils.parseSchedulingPolicy(schedulingPolicy);
      } catch (IllegalArgumentException ex) {
        // This should be validated at change time; let's fall back to a default here.
        LOG.error("Unknown scheduling policy " + schedulingPolicy + "; using FAIR");
        this.schedulingPolicy = WMPoolSchedulingPolicy.FAIR;
      }
      // TODO: two possible improvements
      //       1) Right now we kill all the queries here; we could just kill -qpDelta.
      //       2) After the queries are killed queued queries would take their place.
      //          If we could somehow restart queries we could instead put them at the front
      //          of the queue (esp. in conjunction with (1)) and rerun them.
      if (queryParallelism < getTotalActiveSessions()) {
        extractAllSessionsToKill("The query pool was resized by administrator",
            e.toReuse, syncWork);
      }
      // We will requeue, and not kill, the queries that are not running yet.
      // Insert them all before the get requests from this iteration.
      GetRequest req;
      if (metrics != null) {
        metrics.removeQueuedQueries(queue.size());
      }

      while ((req = queue.pollLast()) != null) {
        e.getRequests.addFirst(req);
      }
    }

    public void destroy(WmThreadSyncWork syncWork,
        LinkedList<GetRequest> globalQueue, IdentityHashMap<WmTezSession, GetRequest> toReuse) {
      extractAllSessionsToKill("The query pool was removed by administrator", toReuse, syncWork);
      // All the pending get requests should just be requeued elsewhere.
      // Note that we never queue session reuse so sessionToReuse would be null.
      globalQueue.addAll(0, queue);
      if (metrics != null) {
        metrics.removeQueuedQueries(queue.size());
        metrics.destroy();
      }
      queue.clear();
    }

    public double updateAllocationPercentages() {
      switch (schedulingPolicy) {
      case FAIR:
        int totalSessions = sessions.size() + initializingSessions.size();
        if (totalSessions == 0) return 0;
        double allocation = finalFractionRemaining / totalSessions;
        for (WmTezSession session : sessions) {
          updateSessionAllocationWithEvent(session, allocation);
        }
        // Do not give out the capacity of the initializing sessions to the running ones;
        // we expect init to be fast.
        return finalFractionRemaining - allocation * initializingSessions.size();
      case FIFO:
        if (sessions.isEmpty()) return 0;
        boolean isFirst = true;
        for (WmTezSession session : sessions) {
          updateSessionAllocationWithEvent(session, isFirst ? finalFractionRemaining : 0);
          isFirst = false;
        }
        return finalFractionRemaining;
      default:
        throw new AssertionError("Unexpected enum value " + schedulingPolicy);
      }
    }

    private void updateSessionAllocationWithEvent(WmTezSession session, double allocation) {
      WmEvent event = null;
      WmContext ctx = session.getWmContext();
      if (ctx != null && session.hasClusterFraction()
          && !DoubleMath.fuzzyEquals(session.getClusterFraction(), allocation, 0.0001f)) {
        event = new WmEvent(EventType.UPDATE);
      }
      session.setClusterFraction(allocation);
      if (event != null) {
        event.endEvent(session);
      }
    }

    public LinkedList<WmTezSession> getSessions() {
      return sessions;
    }

    public LinkedList<SessionInitContext> getInitializingSessions() {
      return initializingSessions;
    }

    @Override
    public String toString() {
      return "[" + fullName + ", query parallelism " + queryParallelism
        + ", fraction of the cluster " + finalFraction + ", fraction used by child pools "
        + (finalFraction - finalFractionRemaining) + ", active sessions " + sessions.size()
        + ", initializing sessions " + initializingSessions.size() + "]";
    }

    private void extractAllSessionsToKill(String killReason,
        IdentityHashMap<WmTezSession, GetRequest> toReuse,
        WmThreadSyncWork syncWork) {
      int totalCount = sessions.size() + initializingSessions.size();
      delayedMoveSessions.clear();
      for (WmTezSession sessionToKill : sessions) {
        resetRemovedSessionToKill(syncWork.toKillQuery,
          new KillQueryContext(sessionToKill, killReason), toReuse);
      }
      sessions.clear();
      for (SessionInitContext initCtx : initializingSessions) {
        // It is possible that the background init thread has finished in parallel, queued
        // the message for us but also returned the session to the user.
        WmTezSession sessionToKill = initCtx.cancelAndExtractSessionIfDone(
            killReason, syncWork.pathsToDelete);
        if (sessionToKill == null) {
          continue; // Async op in progress; the callback will take care of this.
        }
        resetRemovedSessionToKill(syncWork.toKillQuery,
          new KillQueryContext(sessionToKill, killReason), toReuse);
      }
      initializingSessions.clear();
      if (metrics != null) {
        metrics.removeRunningQueries(totalCount);
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
    private final static int MAX_ATTEMPT_NUMBER = 1; // Retry once.

    private final String poolName, queryId;

    private final ReentrantLock lock = new ReentrantLock();
    private WmTezSession session;
    private SettableFuture<WmTezSession> future;
    private SessionInitState state;
    private String cancelReason;
    private HiveResources prelocalizedResources;
    private Path pathToDelete;
    private WmContext wmContext;
    private int attemptNumber = 0;

    public SessionInitContext(SettableFuture<WmTezSession> future,
        String poolName, String queryId, WmContext wmContext,
        HiveResources prelocalizedResources) {
      this.state = SessionInitState.GETTING;
      this.future = future;
      this.poolName = poolName;
      this.queryId = queryId;
      this.prelocalizedResources = prelocalizedResources;
      this.wmContext = wmContext;
    }

    public void start() throws Exception {
      ListenableFuture<WmTezSession> getFuture = tezAmPool.getSessionAsync();
      Futures.addCallback(getFuture, this, MoreExecutors.directExecutor());
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
          session.setQueryId(queryId);
          if (prelocalizedResources != null) {
            pathToDelete = session.replaceHiveResources(prelocalizedResources, true);
          }
          if (wmContext != null) {
            session.setWmContext(wmContext);
          }
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
        Futures.addCallback(waitFuture, this, MoreExecutors.directExecutor());
        break;
      }
      case WAITING_FOR_REGISTRY: {
        // Notify the master thread and the user.
        notifyInitializationCompleted(this);
        future.set(session);
        break;
      }
      case CANCELED: {
        // Return session to the pool; we can do it directly here.
        future.setException(new HiveException(
            "The query was killed by workload management: " + cancelReason));
        session.clearWm();
        session.setQueryId(null);
        session.setWmContext(null);
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
      boolean wasCanceled = false, doRetry = false;
      lock.lock();
      try {
        wasCanceled = (state == SessionInitState.CANCELED);
        session = this.session;
        this.session = null;
        doRetry = !wasCanceled && (attemptNumber < MAX_ATTEMPT_NUMBER);
        if (doRetry) {
          ++attemptNumber;
          this.state = SessionInitState.GETTING;
          future = null;
        } else {
          future = this.future;
          this.future = null;
          if (!wasCanceled) {
            this.state = SessionInitState.DONE;
          }
        }
      } finally {
        lock.unlock();
      }
      if (doRetry) {
        try {
          start();
          return;
        } catch (Exception e) {
          LOG.error("Failed to retry; propagating original error. The new error is ", e);
        } finally {
          discardSessionOnFailure(session);
        }
      }
      if (!wasCanceled) {
        LOG.debug("Queueing the initialization failure with {}", session);
        notifyInitializationCompleted(this); // Report failure to the main thread.
      }
      future.setException(t);
      discardSessionOnFailure(session);
    }

    public void discardSessionOnFailure(WmTezSession session) {
      if (session == null) return;
      session.clearWm();
      session.setQueryId(null);
      // We can just restart the session if we have received one.
      try {
        tezAmPool.replaceSession(session);
      } catch (Exception e) {
        LOG.error("Failed to restart a failed session", e);
      }
    }

    /** Cancel the async operation (even if it's done), and return the session if done. */
    public WmTezSession cancelAndExtractSessionIfDone(String cancelReason, List<Path> toDelete) {
      lock.lock();
      try {
        SessionInitState state = this.state;
        this.state = SessionInitState.CANCELED;
        this.cancelReason = cancelReason;
        if (state == SessionInitState.DONE) {
          WmTezSession result = this.session;
          this.session = null;
          if (pathToDelete != null) {
            toDelete.add(pathToDelete);
          }
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
    public boolean extractSessionAndCancelIfDone(
        List<WmTezSession> results, List<Path> toDelete) {
      lock.lock();
      try {
        if (state != SessionInitState.DONE) return false;
        this.state = SessionInitState.CANCELED;
        if (pathToDelete != null) {
          toDelete.add(pathToDelete);
        }
        if (this.session != null) {
          results.add(this.session);
        } // Otherwise we have failed; the callback has taken care of the failure.
        this.session = null;
        return true;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public String toString() {
      return "[state=" + state + ", session=" + session + "]";
    }
  }

  boolean isManaged(MappingInput input) {
    // This is always replaced atomically, so we don't care about concurrency here.
    UserPoolMapping mapping = userPoolMapping;
    if (mapping != null) {
      // Don't pass in the pool set - not thread safe; if the user is trying to force us to
      // use a non-existent pool, we want to fail anyway. We will fail later during get.
      String mappedPool = mapping.mapSessionToPoolName(input, allowAnyPool, null);
      LOG.info("Mapping input: {} mapped to pool: {}", input, mappedPool);
      return true;
    }
    return false;
  }

  private enum KillQueryResult {
    OK,
    RESTART_REQUIRED,
    IN_PROGRESS
  }

  /**
   * When we kill a query without killing a session, we need two things to come back before reuse.
   * First of all, kill query itself should come back, and second the user should handle it
   * and let go of the session (or, the query could finish and it could give the session back
   * even before we try to kill the query). We also need to handle cases where the user doesn't
   * like the session even before we kill it, or the kill fails and the user is happily computing
   * away. This class is to collect and make sense of the state around all this.
   */
  static final class KillQueryContext {
    private SettableFuture<Boolean> killSessionFuture;
    private String poolName;
    private final String reason;
    private final WmTezSession session;
    // Note: all the fields are only modified by master thread.
    private boolean isUserDone = false, isKillDone = false,
        hasKillFailed = false, hasUserFailed = false;

    KillQueryContext(WmTezSession session, String reason) {
      this.session = session;
      this.reason = reason;
      this.killSessionFuture = SettableFuture.create();
    }

    private void handleKillQueryCallback(boolean hasFailed) {
      isKillDone = true;
      hasKillFailed = hasFailed;
    }

    private void handleUserCallback(boolean hasFailed) {
      if (isUserDone) {
        LOG.warn("Duplicate user call for a session being killed; ignoring");
        return;
      }
      isUserDone = true;
      hasUserFailed = hasFailed;
    }

    private KillQueryResult process() {
      if (!isUserDone && hasKillFailed) {
        // The user has not returned and the kill has failed.
        // We are going to brute force kill the AM; whatever user does is now irrelevant.
        session.setIsIrrelevantForWm(reason);
        return KillQueryResult.RESTART_REQUIRED;
      }
      if (!isUserDone || !isKillDone) return KillQueryResult.IN_PROGRESS; // Someone is not done.
      // Both user and the kill have returned.
      if (hasUserFailed && hasKillFailed) {
        // If the kill failed and the user also thinks the session is invalid, restart it.
        session.setIsIrrelevantForWm(reason);
        return KillQueryResult.RESTART_REQUIRED;
      }
      // Otherwise, we can reuse the session. Either the kill has failed but the user managed to
      // return early (in fact, can it fail because the query has completed earlier?), or the user
      // has failed because the query was killed from under it.
      return KillQueryResult.OK;
    }

    String getPoolName() {
      return poolName;
    }

    void setPoolName(String poolName) {
      this.poolName = poolName;
    }

    @Override
    public String toString() {
      return "KillQueryContext [isUserDone=" + isUserDone + ", isKillDone=" + isKillDone
          + ", hasKillFailed=" + hasKillFailed + ", hasUserFailed=" + hasUserFailed
          + ", session=" + session + ", reason=" + reason + "]";
    }
  }

  private static void resetRemovedSessionToKill(Map<WmTezSession, KillQueryContext> toKillQuery,
    KillQueryContext killQueryContext, Map<WmTezSession, GetRequest> toReuse) {
    toKillQuery.put(killQueryContext.session, killQueryContext);
    killQueryContext.session.clearWm();
    GetRequest req = toReuse.remove(killQueryContext.session);
    if (req != null) {
      req.sessionToReuse = null;
    }
  }

  private void resetAndQueueKill(Map<WmTezSession, KillQueryContext> toKillQuery,
    KillQueryContext killQueryContext, Map<WmTezSession, GetRequest> toReuse) {

    WmTezSession toKill = killQueryContext.session;
    String poolName = toKill.getPoolName();

    boolean validPoolName = StringUtils.isNotBlank(poolName);

    if (validPoolName) {
      killQueryContext.setPoolName(poolName);
    }
    toKillQuery.put(toKill, killQueryContext);

    // The way this works is, a session in WM pool will move back to tez AM pool on a kill and will get
    // reassigned back to WM pool on GetRequest based on user pool mapping. Only if we remove the session from active
    // sessions list of its WM pool will the queue'd GetRequest be processed
    if (validPoolName) {
      PoolState poolState = pools.get(poolName);
      if (poolState != null) {
        poolState.getSessions().remove(toKill);
        Iterator<SessionInitContext> iter = poolState.getInitializingSessions().iterator();
        while (iter.hasNext()) {
          if (iter.next().session == toKill) {
            iter.remove();
            break;
          }
        }
      }
    }

    toKill.clearWm();
    GetRequest req = toReuse.remove(toKill);
    if (req != null) {
      req.sessionToReuse = null;
    }
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
