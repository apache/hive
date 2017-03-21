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
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.net.URISyntaxException;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;

import javax.security.auth.login.LoginException;

import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tez.dag.api.TezException;

/**
 * This class is for managing multiple tez sessions particularly when
 * HiveServer2 is being used to submit queries.
 *
 * In case the user specifies a queue explicitly, a new session is created
 * on that queue and assigned to the session state.
 */
public class TezSessionPoolManager {

  private enum CustomQueueAllowed {
    TRUE,
    FALSE,
    IGNORE
  }

  private static final Logger LOG = LoggerFactory.getLogger(TezSessionPoolManager.class);
  private static final Random rdm = new Random();

  private BlockingQueue<TezSessionPoolSession> defaultQueuePool;

  /** Priority queue sorted by expiration time of live sessions that could be expired. */
  private PriorityBlockingQueue<TezSessionPoolSession> expirationQueue;
  /** The background restart queue that is populated when expiration is triggered by a foreground
   * thread (i.e. getting or returning a session), to avoid delaying it. */
  private BlockingQueue<TezSessionPoolSession> restartQueue;
  private Thread expirationThread;
  private Thread restartThread;

  private Semaphore llapQueue;
  private HiveConf initConf = null;
  // Config settings.
  private int numConcurrentLlapQueries = -1;
  private long sessionLifetimeMs = 0;
  private long sessionLifetimeJitterMs = 0;
  private CustomQueueAllowed customQueueAllowed = CustomQueueAllowed.TRUE;
  private List<ConfVars> restrictedHiveConf = new ArrayList<>();
  private List<String> restrictedNonHiveConf = new ArrayList<>();

  /** A queue for initial sessions that have not been started yet. */
  private Queue<TezSessionPoolSession> initialSessions =
      new ConcurrentLinkedQueue<TezSessionPoolSession>();
  /**
   * Indicates whether we should try to use defaultSessionPool.
   * We assume that setupPool is either called before any activity, or not called at all.
   */
  private volatile boolean hasInitialSessions = false;

  private static TezSessionPoolManager sessionPool = null;

  private static final List<TezSessionPoolSession> openSessions
    = new LinkedList<TezSessionPoolSession>();

  public static TezSessionPoolManager getInstance()
      throws Exception {
    if (sessionPool == null) {
      sessionPool = new TezSessionPoolManager();
    }

    return sessionPool;
  }

  protected TezSessionPoolManager() {
  }

  private void startInitialSession(TezSessionPoolSession sessionState) throws Exception {
    HiveConf newConf = new HiveConf(initConf); // TODO Why is this configuration management not happening inside TezSessionPool.
    // Makes no senses for it to be mixed up like this.
    boolean isUsable = sessionState.tryUse();
    if (!isUsable) throw new IOException(sessionState + " is not usable at pool startup");
    newConf.set(TezConfiguration.TEZ_QUEUE_NAME, sessionState.getQueueName());
    sessionState.open(newConf);
    if (sessionState.returnAfterUse()) {
      defaultQueuePool.put(sessionState);
    }
  }

  public void startPool() throws Exception {
    if (initialSessions.isEmpty()) return;
    int threadCount = Math.min(initialSessions.size(),
        HiveConf.getIntVar(initConf, ConfVars.HIVE_SERVER2_TEZ_SESSION_MAX_INIT_THREADS));
    Preconditions.checkArgument(threadCount > 0);
    if (threadCount == 1) {
      while (true) {
        TezSessionPoolSession session = initialSessions.poll();
        if (session == null) break;
        startInitialSession(session);
      }
    } else {
      // TODO What is this doing now ?
      final SessionState parentSessionState = SessionState.get();
      // The runnable has no mutable state, so each thread can run the same thing.
      final AtomicReference<Exception> firstError = new AtomicReference<>(null);
      Runnable runnable = new Runnable() {
        public void run() {
          if (parentSessionState != null) {
            SessionState.setCurrentSessionState(parentSessionState);
          }
          while (true) {
            TezSessionPoolSession session = initialSessions.poll();
            if (session == null) break;
            try {
              startInitialSession(session);
            } catch (Exception e) {
              if (!firstError.compareAndSet(null, e)) {
                LOG.error("Failed to start session; ignoring due to previous error", e);
                // TODO Why even continue after this. We're already in a state where things are messed up ?
              }
            }
          }
        }
      };
      Thread[] threads = new Thread[threadCount - 1];
      for (int i = 0; i < threads.length; ++i) {
        threads[i] = new Thread(runnable, "Tez session init " + i);
        threads[i].start();
      }
      runnable.run();
      for (int i = 0; i < threads.length; ++i) {
        threads[i].join();
      }
      Exception ex = firstError.get();
      if (ex != null) {
        throw ex;
      }
    }
    if (expirationThread != null) {
      expirationThread.start();
      restartThread.start();
    }
  }

  public void setupPool(HiveConf conf) throws InterruptedException {
    String[] defaultQueueList = HiveConf.getTrimmedStringsVar(
        conf, HiveConf.ConfVars.HIVE_SERVER2_TEZ_DEFAULT_QUEUES);
    int emptyNames = 0; // We don't create sessions for empty entries.
    for (String queueName : defaultQueueList) {
      if (queueName.isEmpty()) {
        ++emptyNames;
      }
    }
    int numSessions = conf.getIntVar(ConfVars.HIVE_SERVER2_TEZ_SESSIONS_PER_DEFAULT_QUEUE);
    int numSessionsTotal = numSessions * (defaultQueueList.length - emptyNames);
    if (numSessionsTotal > 0) {
      defaultQueuePool = new ArrayBlockingQueue<TezSessionPoolSession>(numSessionsTotal);
    }

    numConcurrentLlapQueries = conf.getIntVar(ConfVars.HIVE_SERVER2_LLAP_CONCURRENT_QUERIES);
    llapQueue = new Semaphore(numConcurrentLlapQueries, true);

    this.initConf = conf;
    String queueAllowedStr = HiveConf.getVar(initConf,
        ConfVars.HIVE_SERVER2_TEZ_SESSION_CUSTOM_QUEUE_ALLOWED);
    try {
      this.customQueueAllowed = CustomQueueAllowed.valueOf(queueAllowedStr.toUpperCase());
    } catch (Exception ex) {
      throw new RuntimeException("Invalid value '" + queueAllowedStr + "' for " +
          ConfVars.HIVE_SERVER2_TEZ_SESSION_CUSTOM_QUEUE_ALLOWED.varname);
    }
    String[] restrictedConfigs = HiveConf.getTrimmedStringsVar(initConf,
        ConfVars.HIVE_SERVER2_TEZ_SESSION_RESTRICTED_CONFIGS);
    if (restrictedConfigs != null && restrictedConfigs.length > 0) {
      HashMap<String, ConfVars> confVars = HiveConf.getOrCreateReverseMap();
      for (String confName : restrictedConfigs) {
        if (confName == null || confName.isEmpty()) continue;
        confName = confName.toLowerCase();
        ConfVars cv = confVars.get(confName);
        if (cv != null) {
          restrictedHiveConf.add(cv);
        } else {
          LOG.warn("A restricted config " + confName + " is not recognized as a Hive setting.");
          restrictedNonHiveConf.add(confName);
        }
      }
    }
    

    sessionLifetimeMs = conf.getTimeVar(
        ConfVars.HIVE_SERVER2_TEZ_SESSION_LIFETIME, TimeUnit.MILLISECONDS);
    if (sessionLifetimeMs != 0) {
      sessionLifetimeJitterMs = conf.getTimeVar(
          ConfVars.HIVE_SERVER2_TEZ_SESSION_LIFETIME_JITTER, TimeUnit.MILLISECONDS);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Session expiration is enabled; session lifetime is "
            + sessionLifetimeMs + " + [0, " + sessionLifetimeJitterMs + ") ms");
      }
      expirationQueue = new PriorityBlockingQueue<>(11, new Comparator<TezSessionPoolSession>() {
        @Override
        public int compare(TezSessionPoolSession o1, TezSessionPoolSession o2) {
          assert o1.expirationNs != null && o2.expirationNs != null;
          return o1.expirationNs.compareTo(o2.expirationNs);
        }
      });
      restartQueue = new LinkedBlockingQueue<>();
    }
    this.hasInitialSessions = numSessionsTotal > 0;
    // From this point on, session creation will wait for the default pool (if # of sessions > 0).

    if (sessionLifetimeMs != 0) {
      expirationThread = new Thread(new Runnable() {
        @Override
        public void run() {
          runExpirationThread();
        }
      }, "TezSessionPool-expiration");
      restartThread = new Thread(new Runnable() {
        @Override
        public void run() {
          runRestartThread();
        }
      }, "TezSessionPool-cleanup");
    }

    /*
     * In a single-threaded init case, with this the ordering of sessions in the queue will be
     * (with 2 sessions 3 queues) s1q1, s1q2, s1q3, s2q1, s2q2, s2q3 there by ensuring uniform
     * distribution of the sessions across queues at least to begin with. Then as sessions get
     * freed up, the list may change this ordering.
     * In a multi threaded init case it's a free for all.
     */
    for (int i = 0; i < numSessions; i++) {
      for (String queueName : defaultQueueList) {
        if (queueName.isEmpty()) {
          continue;
        }
        initialSessions.add(createAndInitSession(queueName, true));
      }
    }
  }

  // TODO Create and init session sets up queue, isDefault - but does not initialize the configuration
  private TezSessionPoolSession createAndInitSession(String queue, boolean isDefault) {
    TezSessionPoolSession sessionState = createSession(TezSessionState.makeSessionId());
    // TODO When will the queue ever be null.
    // Pass queue and default in as constructor parameters, and make them final.
    if (queue != null) {
      sessionState.setQueueName(queue);
    }
    if (isDefault) {
      sessionState.setDefault();
    }
    LOG.info("Created new tez session for queue: " + queue +
        " with session id: " + sessionState.getSessionId());
    return sessionState;
  }

  private TezSessionState getSession(HiveConf conf, boolean doOpen)
      throws Exception {
    String queueName = conf.get(TezConfiguration.TEZ_QUEUE_NAME);
    boolean hasQueue = (queueName != null) && !queueName.isEmpty();
    if (hasQueue) {
      switch (customQueueAllowed) {
      case FALSE: throw new HiveException("Specifying " + TezConfiguration.TEZ_QUEUE_NAME + " is not allowed");
      case IGNORE: {
        LOG.warn("User has specified " + queueName + " queue; ignoring the setting");
        queueName = null;
        hasQueue = false;
        conf.unset(TezConfiguration.TEZ_QUEUE_NAME);
      }
      default: // All good.
      }
    }
    for (ConfVars var : restrictedHiveConf) {
      String userValue = HiveConf.getVarWithoutType(conf, var),
          serverValue = HiveConf.getVarWithoutType(initConf, var);
      // Note: with some trickery, we could add logic for each type in ConfVars; for now the
      // potential spurious mismatches (e.g. 0 and 0.0 for float) should be easy to work around.
      validateRestrictedConfigValues(var.varname, userValue, serverValue);
    }
    for (String var : restrictedNonHiveConf) {
      String userValue = conf.get(var), serverValue = initConf.get(var);
      validateRestrictedConfigValues(var, userValue, serverValue);
    }

    // TODO Session re-use completely disabled for doAs=true. Always launches a new session.
    boolean nonDefaultUser = conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS);

    /*
     * if the user has specified a queue name themselves, we create a new session.
     * also a new session is created if the user tries to submit to a queue using
     * their own credentials. We expect that with the new security model, things will
     * run as user hive in most cases.
     */
    if (nonDefaultUser || !hasInitialSessions || hasQueue) {
      LOG.info("QueueName: {} nonDefaultUser: {} defaultQueuePool: {} hasInitialSessions: {}",
              queueName, nonDefaultUser, defaultQueuePool, hasInitialSessions);
      return getNewSessionState(conf, queueName, doOpen);
    }

    LOG.info("Choosing a session from the defaultQueuePool");
    while (true) {
      TezSessionPoolSession result = defaultQueuePool.take();
      if (result.tryUse()) return result;
      LOG.info("Couldn't use a session [" + result + "]; attempting another one");
    }
  }

  private void validateRestrictedConfigValues(
      String var, String userValue, String serverValue) throws HiveException {
    if ((userValue == null) != (serverValue == null)
        || (userValue != null && !userValue.equals(serverValue))) {
      String logValue = initConf.isHiddenConfig(var) ? "(hidden)" : serverValue;
      throw new HiveException(var + " is restricted from being set; server is configured"
          + " to use " + logValue + ", but the query configuration specifies " + userValue);
    }
  }

  /**
   * @param conf HiveConf that is used to initialize the session
   * @param queueName could be null. Set in the tez session.
   * @param doOpen
   * @return
   * @throws Exception
   */
  private TezSessionState getNewSessionState(HiveConf conf,
      String queueName, boolean doOpen) throws Exception {
    TezSessionPoolSession retTezSessionState = createAndInitSession(queueName, false);
    if (queueName != null) {
      conf.set(TezConfiguration.TEZ_QUEUE_NAME, queueName);
    }
    if (doOpen) {
      retTezSessionState.open(conf);
      LOG.info("Started a new session for queue: " + queueName +
          " session id: " + retTezSessionState.getSessionId());
    }
    return retTezSessionState;
  }

  public void returnSession(TezSessionState tezSessionState, boolean llap)
      throws Exception {
    // Ignore the interrupt status while returning the session, but set it back
    // on the thread in case anything else needs to deal with it.
    boolean isInterrupted = Thread.interrupted();

    try {
      if (isInterrupted) {
        LOG.info("returnSession invoked with interrupt status set");
      }
      if (llap && (this.numConcurrentLlapQueries > 0)) {
        llapQueue.release();
      }
      if (tezSessionState.isDefault() &&
          tezSessionState instanceof TezSessionPoolSession) {
        LOG.info("The session " + tezSessionState.getSessionId()
            + " belongs to the pool. Put it back in");
        SessionState sessionState = SessionState.get();
        if (sessionState != null) {
          sessionState.setTezSession(null);
        }
        TezSessionPoolSession poolSession =
            (TezSessionPoolSession) tezSessionState;
        if (poolSession.returnAfterUse()) {
          defaultQueuePool.put(poolSession);
        }
      }
      // non default session nothing changes. The user can continue to use the existing
      // session in the SessionState
    } finally {
      // Reset the interrupt status.
      if (isInterrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public static void closeIfNotDefault(
      TezSessionState tezSessionState, boolean keepTmpDir) throws Exception {
    LOG.info("Closing tez session if not default: " + tezSessionState);
    if (!tezSessionState.isDefault()) {
      tezSessionState.close(keepTmpDir);
    }
  }

  public void stop() throws Exception {
    if ((sessionPool == null) || !this.hasInitialSessions) {
      return;
    }

    List<TezSessionPoolSession> sessionsToClose = null;
    synchronized (openSessions) {
      sessionsToClose = new ArrayList<TezSessionPoolSession>(openSessions);
    }
    // we can just stop all the sessions
    for (TezSessionState sessionState : sessionsToClose) {
      if (sessionState.isDefault()) {
        sessionState.close(false);
      }
    }

    if (expirationThread != null) {
      expirationThread.interrupt();
    }
    if (restartThread != null) {
      restartThread.interrupt();
    }
  }

  /**
   * This is called only in extreme cases where even our retry of submit fails. This method would
   * close even default sessions and remove it from the queue.
   *
   * @param tezSessionState
   *          the session to be closed
   * @throws Exception
   */
  public void destroySession(TezSessionState tezSessionState) throws Exception {
    LOG.warn("We are closing a " + (tezSessionState.isDefault() ? "default" : "non-default")
        + " session because of retry failure.");
    tezSessionState.close(false);
  }

  protected TezSessionPoolSession createSession(String sessionId) {
    return new TezSessionPoolSession(sessionId, this);
  }

  /*
   * This method helps to re-use a session in case there has been no change in
   * the configuration of a session. This will happen only in the case of non-hive-server2
   * sessions for e.g. when a CLI session is started. The CLI session could re-use the
   * same tez session eliminating the latencies of new AM and containers.
   */
  private static boolean canWorkWithSameSession(TezSessionState session, HiveConf conf)
       throws HiveException {
    if (session == null || conf == null || !session.isOpen()) {
      return false;
    }

    try {
      UserGroupInformation ugi = Utils.getUGI();
      String userName = ugi.getShortUserName();
      // TODO Will these checks work if some other user logs in. Isn't a doAs check required somewhere here as well.
      // Should a doAs check happen here instead of after the user test.
      // With HiveServer2 - who is the incoming user in terms of UGI (the hive user itself, or the user who actually submitted the query)

      // Working in the assumption that the user here will be the hive user if doAs = false, we'll make it past this false check.
      LOG.info("The current user: " + userName + ", session user: " + session.getUser());
      if (userName.equals(session.getUser()) == false) {
        LOG.info("Different users incoming: " + userName + " existing: " + session.getUser());
        return false;
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }

    boolean doAsEnabled = conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS);
    // either variables will never be null because a default value is returned in case of absence
    if (doAsEnabled != session.getConf().getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS)) {
      return false;
    }

    if (!session.isDefault()) {
      String queueName = session.getQueueName();
      String confQueueName = conf.get(TezConfiguration.TEZ_QUEUE_NAME);
      LOG.info("Current queue name is " + queueName + " incoming queue name is " + confQueueName);
      return (queueName == null) ? confQueueName == null : queueName.equals(confQueueName);
    } else {
      // this session should never be a default session unless something has messed up.
      throw new HiveException("The pool session " + session + " should have been returned to the pool"); 
    }
  }

  public TezSessionState getSession(
      TezSessionState session, HiveConf conf, boolean doOpen, boolean llap) throws Exception {
    if (llap && (this.numConcurrentLlapQueries > 0)) {
      llapQueue.acquire(); // blocks if no more llap queries can be submitted.
    }

    if (canWorkWithSameSession(session, conf)) {
      return session;
    }

    if (session != null) {
      closeIfNotDefault(session, false);
    }

    return getSession(conf, doOpen);
  }

  /** Reopens the session that was found to not be running. */
  public void reopenSession(TezSessionState sessionState, HiveConf conf,
      String[] additionalFiles, boolean keepTmpDir) throws Exception {
    HiveConf sessionConf = sessionState.getConf();
    if (sessionConf != null && sessionConf.get(TezConfiguration.TEZ_QUEUE_NAME) != null) {
      // user has explicitly specified queue name
      conf.set(TezConfiguration.TEZ_QUEUE_NAME, sessionConf.get(TezConfiguration.TEZ_QUEUE_NAME));
    } else {
      // default queue name when the initial session was created
      if (sessionState.getQueueName() != null) {
        conf.set(TezConfiguration.TEZ_QUEUE_NAME, sessionState.getQueueName());
      }
    }
    // TODO: close basically resets the object to a bunch of nulls.
    //       We should ideally not reuse the object because it's pointless and error-prone.
    sessionState.close(keepTmpDir); // Clean up stuff.
    sessionState.open(conf, additionalFiles);
  }

  public void closeNonDefaultSessions(boolean keepTmpDir) throws Exception {
    List<TezSessionPoolSession> sessionsToClose = null;
    synchronized (openSessions) {
      sessionsToClose = new ArrayList<TezSessionPoolSession>(openSessions);
    }
    for (TezSessionPoolSession sessionState : sessionsToClose) {
      System.err.println("Shutting down tez session.");
      closeIfNotDefault(sessionState, keepTmpDir);
    }
  }

  /** Closes a running (expired) pool session and reopens it. */
  private void closeAndReopenPoolSession(TezSessionPoolSession oldSession) throws Exception {
    String queueName = oldSession.getQueueName();
    if (queueName == null) {
      LOG.warn("Pool session has a null queue: " + oldSession);
    }
    HiveConf conf = oldSession.getConf();
    Path scratchDir = oldSession.getTezScratchDir();
    boolean isDefault = oldSession.isDefault();
    Set<String> additionalFiles = oldSession.getAdditionalFilesNotFromConf();
    try {
      oldSession.close(false);
      defaultQueuePool.remove(oldSession);  // Make sure it's removed.
    } finally {
      TezSessionPoolSession newSession = createAndInitSession(queueName, isDefault);
      // There's some bogus code that can modify the queue name. Force-set it for pool sessions.
      conf.set(TezConfiguration.TEZ_QUEUE_NAME, queueName);
      newSession.open(conf, additionalFiles, scratchDir);
      defaultQueuePool.put(newSession);
    }
  }

  /** Logic for the thread that restarts the sessions expired during foreground operations. */
  private void runRestartThread() {
    try {
      while (true) {
        TezSessionPoolSession next = restartQueue.take();
        LOG.info("Restarting the expired session [" + next + "]");
        try {
          closeAndReopenPoolSession(next);
        } catch (InterruptedException ie) {
          throw ie;
        } catch (Exception e) {
          LOG.error("Failed to close or restart a session, ignoring", e);
        }
      }
    } catch (InterruptedException e) {
      LOG.info("Restart thread is exiting due to an interruption");
    }
  }

  /** Logic for the thread that tracks session expiration and restarts sessions in background. */
  private void runExpirationThread() {
    try {
      while (true) {
        TezSessionPoolSession nextToExpire = null;
        while (true) {
          // Restart the sessions until one of them refuses to restart.
          nextToExpire = expirationQueue.take();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Seeing if we can expire [" + nextToExpire + "]");
          }
          try {
            if (!nextToExpire.tryExpire(false)) break;
          } catch (Exception e) {
            // Reopen happens even when close fails, so there's not much to do here.
            LOG.error("Failed to expire session " + nextToExpire + "; ignoring", e);
            nextToExpire = null;
            break; // Not strictly necessary; do the whole queue check again.
          }
          LOG.info("Tez session [" + nextToExpire + "] has expired");
        }
        if (nextToExpire != null && LOG.isDebugEnabled()) {
          LOG.debug("[" + nextToExpire + "] is not ready to expire; adding it back");
        }

        // See addToExpirationQueue for why we re-check the queue.
        synchronized (expirationQueue) {
          // Add back the non-expired session. No need to notify, we are the only ones waiting.
          if (nextToExpire != null) {
            expirationQueue.add(nextToExpire);
          }
          nextToExpire = expirationQueue.peek();
          if (nextToExpire != null) {
            // Add some margin to the wait to avoid rechecking close to the boundary.
            long timeToWaitMs = 10 + (nextToExpire.expirationNs - System.nanoTime()) / 1000000L;
            timeToWaitMs = Math.max(1, timeToWaitMs);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Waiting for ~" + timeToWaitMs + "ms to expire [" + nextToExpire + "]");
            }
            expirationQueue.wait(timeToWaitMs);
          } else if (LOG.isDebugEnabled()) {
            // Don't wait if empty - go to take() above, that will wait for us.
            LOG.debug("Expiration queue is empty");
          }
        }
      }
    } catch (InterruptedException e) {
      LOG.info("Expiration thread is exiting due to an interruption");
    }
  }

  /**
   * TezSession that keeps track of expiration and use.
   * It has 3 states - not in use, in use, and expired. When in the pool, it is not in use;
   * use and expiration may compete to take the session out of the pool and change it to the
   * corresponding states. When someone tries to get a session, they check for expiration time;
   * if it's time, the expiration is triggered; in that case, or if it was already triggered, the
   * caller gets a different session. When the session is in use when it expires, the expiration
   * thread ignores it and lets the return to the pool take care of the expiration.
   */
  @VisibleForTesting
  static class TezSessionPoolSession extends TezSessionState {
    private static final int STATE_NONE = 0, STATE_IN_USE = 1, STATE_EXPIRED = 2;

    private final AtomicInteger sessionState = new AtomicInteger(STATE_NONE);
    private Long expirationNs;
    private final TezSessionPoolManager parent; // Static class allows us to be used in tests.

    public TezSessionPoolSession(String sessionId, TezSessionPoolManager parent) {
      super(sessionId);
      this.parent = parent;
    }

    @Override
    public void close(boolean keepTmpDir) throws Exception {
      try {
        super.close(keepTmpDir);
      } finally {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Closed a pool session [" + this + "]");
        }
        synchronized (openSessions) {
          openSessions.remove(this);
        }
        if (parent.expirationQueue != null) {
          parent.expirationQueue.remove(this);
        }
      }
    }

    @Override
    protected void openInternal(HiveConf conf, Collection<String> additionalFiles,
        boolean isAsync, LogHelper console, Path scratchDir)
            throws IOException, LoginException, URISyntaxException, TezException {
      super.openInternal(conf, additionalFiles, isAsync, console, scratchDir);
      synchronized (openSessions) {
        openSessions.add(this);
      }
      if (parent.expirationQueue != null) {
        long jitterModMs = (long)(parent.sessionLifetimeJitterMs * rdm.nextFloat());
        expirationNs = System.nanoTime() + (parent.sessionLifetimeMs + jitterModMs) * 1000000L;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding a pool session [" + this + "] to expiration queue");
        }
        parent.addToExpirationQueue(this);
      }
    }

    @Override
    public String toString() {
      if (expirationNs == null) return super.toString();
      long expiresInMs = (expirationNs - System.nanoTime()) / 1000000L;
      return super.toString() + ", expires in " + expiresInMs + "ms";
    }

    /**
     * Tries to use this session. When the session is in use, it will not expire.
     * @return true if the session can be used; false if it has already expired.
     */
    public boolean tryUse() throws Exception {
      while (true) {
        int oldValue = sessionState.get();
        if (oldValue == STATE_IN_USE) throw new AssertionError(this + " is already in use");
        if (oldValue == STATE_EXPIRED) return false;
        int finalState = shouldExpire() ? STATE_EXPIRED : STATE_IN_USE;
        if (sessionState.compareAndSet(STATE_NONE, finalState)) {
          if (finalState == STATE_IN_USE) return true;
          closeAndRestartExpiredSession(true); // Restart asynchronously, don't block the caller.
          return false;
        }
      }
    }

    /**
     * Notifies the session that it's no longer in use. If the session has expired while in use,
     * this method will take care of the expiration.
     * @return True if the session was returned, false if it was restarted.
     */
    public boolean returnAfterUse() throws Exception {
      int finalState = shouldExpire() ? STATE_EXPIRED : STATE_NONE;
      if (!sessionState.compareAndSet(STATE_IN_USE, finalState)) {
        throw new AssertionError("Unexpected state change; currently " + sessionState.get());
      }
      if (finalState == STATE_NONE) return true;
      closeAndRestartExpiredSession(true);
      return false;
    }

    /**
     * Tries to expire and restart the session.
     * @param isAsync Whether the restart should happen asynchronously.
     * @return True if the session was, or will be restarted.
     */
    public boolean tryExpire(boolean isAsync) throws Exception {
      if (expirationNs == null) return true;
      if (!shouldExpire()) return false;
      // Try to expire the session if it's not in use; if in use, bail.
      while (true) {
        if (sessionState.get() != STATE_NONE) return true; // returnAfterUse will take care of this
        if (sessionState.compareAndSet(STATE_NONE, STATE_EXPIRED)) {
          closeAndRestartExpiredSession(isAsync);
          return true;
        }
      }
    }

    private void closeAndRestartExpiredSession(boolean async) throws Exception {
      if (async) {
        parent.restartQueue.add(this);
      } else {
        parent.closeAndReopenPoolSession(this);
      }
    }

    private boolean shouldExpire() {
      return expirationNs != null && (System.nanoTime() - expirationNs) >= 0;
    }
  }

  private void addToExpirationQueue(TezSessionPoolSession session) {
    // Expiration queue is synchronized and notified upon when adding elements. Without jitter, we
    // wouldn't need this, and could simple look at the first element and sleep for the wait time.
    // However, when many things are added at once, it may happen that we will see the one that
    // expires later first, and will sleep past the earlier expiration times. When we wake up we
    // may kill many sessions at once. To avoid this, we will add to queue under lock and recheck
    // time before we wait. We don't have to worry about removals; at worst we'd wake up in vain.
    // Example: expirations of 1:03:00, 1:00:00, 1:02:00 are added (in this order due to jitter).
    // If the expiration threads sees that 1:03 first, it will sleep for 1:03, then wake up and
    // kill all 3 sessions at once because they all have expired, removing any effect from jitter.
    // Instead, expiration thread rechecks the first queue item and waits on the queue. If nothing
    // is added to the queue, the item examined is still the earliest to be expired. If someone
    // adds to the queue while it is waiting, it will notify the thread and it would wake up and
    // recheck the queue.
    synchronized (expirationQueue) {
      expirationQueue.add(session);
      expirationQueue.notifyAll();
    }
  }
}
