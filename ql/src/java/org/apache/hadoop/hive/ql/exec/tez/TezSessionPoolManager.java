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

import java.io.IOException;
import java.net.URISyntaxException;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import javax.security.auth.login.LoginException;

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
  private int blockingQueueLength = -1;
  private HiveConf initConf = null;
  int numConcurrentLlapQueries = -1;
  private long sessionLifetimeMs = 0;
  private long sessionLifetimeJitterMs = 0;

  private boolean inited = false;



  private static TezSessionPoolManager sessionPool = null;

  private static List<TezSessionState> openSessions = Collections
      .synchronizedList(new LinkedList<TezSessionState>());

  public static TezSessionPoolManager getInstance()
      throws Exception {
    if (sessionPool == null) {
      sessionPool = new TezSessionPoolManager();
    }

    return sessionPool;
  }

  protected TezSessionPoolManager() {
  }

  public void startPool() throws Exception {
    this.inited = true;
    for (int i = 0; i < blockingQueueLength; i++) {
      HiveConf newConf = new HiveConf(initConf);
      TezSessionPoolSession sessionState = defaultQueuePool.take();
      boolean isUsable = sessionState.tryUse();
      if (!isUsable) throw new IOException(sessionState + " is not usable at pool startup");
      newConf.set("tez.queue.name", sessionState.getQueueName());
      sessionState.open(newConf);
      if (sessionState.returnAfterUse()) {
        defaultQueuePool.put(sessionState);
      }
    }
    if (expirationThread != null) {
      expirationThread.start();
      restartThread.start();
    }
  }

  public void setupPool(HiveConf conf) throws InterruptedException {
    String defaultQueues = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_TEZ_DEFAULT_QUEUES);
    int numSessions = conf.getIntVar(ConfVars.HIVE_SERVER2_TEZ_SESSIONS_PER_DEFAULT_QUEUE);
    numConcurrentLlapQueries = conf.getIntVar(ConfVars.HIVE_SERVER2_LLAP_CONCURRENT_QUERIES);
    sessionLifetimeMs = conf.getTimeVar(
        ConfVars.HIVE_SERVER2_TEZ_SESSION_LIFETIME, TimeUnit.MILLISECONDS);
    if (sessionLifetimeMs != 0) {
      sessionLifetimeJitterMs = conf.getTimeVar(
          ConfVars.HIVE_SERVER2_TEZ_SESSION_LIFETIME_JITTER, TimeUnit.MILLISECONDS);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Starting session expiration threads; session lifetime is "
            + sessionLifetimeMs + " + [0, " + sessionLifetimeJitterMs + ") ms");
      }
      expirationQueue = new PriorityBlockingQueue<>(11, new Comparator<TezSessionPoolSession>() {
        public int compare(TezSessionPoolSession o1, TezSessionPoolSession o2) {
          assert o1.expirationNs != null && o2.expirationNs != null;
          return o1.expirationNs.compareTo(o2.expirationNs);
        }
      });
      restartQueue = new LinkedBlockingQueue<>();
      expirationThread = new Thread(new Runnable() {
        public void run() {
          runExpirationThread();
        }
      }, "TezSessionPool-expiration");
      restartThread = new Thread(new Runnable() {
        public void run() {
          runRestartThread();
        }
      }, "TezSessionPool-cleanup");
    }

    // the list of queues is a comma separated list.
    String defaultQueueList[] = defaultQueues.split(",");
    defaultQueuePool = new ArrayBlockingQueue<TezSessionPoolSession>(
        numSessions * defaultQueueList.length);
    llapQueue = new Semaphore(numConcurrentLlapQueries, true);

    this.initConf = conf;
    /*
     *  with this the ordering of sessions in the queue will be (with 2 sessions 3 queues)
     *  s1q1, s1q2, s1q3, s2q1, s2q2, s2q3 there by ensuring uniform distribution of
     *  the sessions across queues at least to begin with. Then as sessions get freed up, the list
     *  may change this ordering.
     */
    blockingQueueLength = 0;
    for (int i = 0; i < numSessions; i++) {
      for (String queue : defaultQueueList) {
        if (queue.length() == 0) {
          continue;
        }
        defaultQueuePool.put(createAndInitSession(queue, true));
        blockingQueueLength++;
      }
    }
  }

  private TezSessionPoolSession createAndInitSession(String queue, boolean isDefault) {
    TezSessionPoolSession sessionState = createSession(TezSessionState.makeSessionId());
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

  private TezSessionState getSession(HiveConf conf, boolean doOpen,
      boolean forceCreate)
      throws Exception {

    String queueName = conf.get("tez.queue.name");

    boolean nonDefaultUser = conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS);

    /*
     * if the user has specified a queue name themselves, we create a new session.
     * also a new session is created if the user tries to submit to a queue using
     * their own credentials. We expect that with the new security model, things will
     * run as user hive in most cases.
     */
    if (forceCreate || !(this.inited)
        || ((queueName != null) && (!queueName.isEmpty()))
        || (nonDefaultUser) || (defaultQueuePool == null) || (blockingQueueLength <= 0)) {
      LOG.info("QueueName: " + queueName + " nonDefaultUser: " + nonDefaultUser +
          " defaultQueuePool: " + defaultQueuePool +
          " blockingQueueLength: " + blockingQueueLength);
      return getNewSessionState(conf, queueName, doOpen);
    }

    LOG.info("Choosing a session from the defaultQueuePool");
    while (true) {
      TezSessionPoolSession result = defaultQueuePool.take();
      if (result.tryUse()) return result;
      LOG.info("Couldn't use a session [" + result + "]; attempting another one");
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
      conf.set("tez.queue.name", queueName);
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
    if (llap && (this.numConcurrentLlapQueries > 0)) {
      llapQueue.release();
    }
    if (tezSessionState.isDefault() && tezSessionState instanceof TezSessionPoolSession) {
      LOG.info("The session " + tezSessionState.getSessionId()
          + " belongs to the pool. Put it back in");
      SessionState sessionState = SessionState.get();
      if (sessionState != null) {
        sessionState.setTezSession(null);
      }
      TezSessionPoolSession poolSession = (TezSessionPoolSession)tezSessionState;
      if (poolSession.returnAfterUse()) {
        defaultQueuePool.put(poolSession);
      }
    }
    // non default session nothing changes. The user can continue to use the existing
    // session in the SessionState
  }

  public void closeIfNotDefault(
      TezSessionState tezSessionState, boolean keepTmpDir) throws Exception {
    LOG.info("Closing tez session default? " + tezSessionState.isDefault());
    if (!tezSessionState.isDefault()) {
      tezSessionState.close(keepTmpDir);
    }
  }

  public void stop() throws Exception {
    if ((sessionPool == null) || (this.inited == false)) {
      return;
    }

    // we can just stop all the sessions
    Iterator<TezSessionState> iter = openSessions.iterator();
    while (iter.hasNext()) {
      TezSessionState sessionState = iter.next();
      if (sessionState.isDefault()) {
        sessionState.close(false);
        iter.remove();
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

  public TezSessionState getSession(TezSessionState session, HiveConf conf, boolean doOpen,
      boolean llap) throws Exception {
    return getSession(session, conf, doOpen, false, llap);
  }

  /*
   * This method helps to re-use a session in case there has been no change in
   * the configuration of a session. This will happen only in the case of non-hive-server2
   * sessions for e.g. when a CLI session is started. The CLI session could re-use the
   * same tez session eliminating the latencies of new AM and containers.
   */
  private boolean canWorkWithSameSession(TezSessionState session, HiveConf conf)
       throws HiveException {
    if (session == null || conf == null) {
      return false;
    }

    try {
      UserGroupInformation ugi = Utils.getUGI();
      String userName = ugi.getShortUserName();
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
    if (doAsEnabled != conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS)) {
      return false;
    }

    if (!session.isDefault()) {
      String queueName = session.getQueueName();
      LOG.info("Current queue name is " + queueName + " incoming queue name is "
          + conf.get("tez.queue.name"));
      if (queueName == null) {
        if (conf.get("tez.queue.name") != null) {
          // queue names are different
          return false;
        } else {
          return true;
        }
      }

      if (!queueName.equals(conf.get("tez.queue.name"))) {
        // the String.equals method handles the case of conf not having the queue name as well.
        return false;
      }
    } else {
      // this session should never be a default session unless something has messed up.
      throw new HiveException("Default queue should always be returned." +
      "Hence we should not be here.");
    }

    return true;
  }

  public TezSessionState getSession(TezSessionState session, HiveConf conf, boolean doOpen,
      boolean forceCreate, boolean llap) throws Exception {
    if (llap && (this.numConcurrentLlapQueries > 0)) {
      llapQueue.acquire(); // blocks if no more llap queries can be submitted.
    }
    if (canWorkWithSameSession(session, conf)) {
      return session;
    }

    if (session != null) {
      closeIfNotDefault(session, false);
    }

    return getSession(conf, doOpen, forceCreate);
  }

  public void closeAndOpen(TezSessionState sessionState, HiveConf conf,
      String[] additionalFiles, boolean keepTmpDir) throws Exception {
    HiveConf sessionConf = sessionState.getConf();
    if (sessionConf != null && sessionConf.get("tez.queue.name") != null) {
      conf.set("tez.queue.name", sessionConf.get("tez.queue.name"));
    }
    closeIfNotDefault(sessionState, keepTmpDir);
    sessionState.open(conf, additionalFiles);
  }

  public List<TezSessionState> getOpenSessions() {
    return openSessions;
  }

  private void closeAndReopen(TezSessionPoolSession oldSession) throws Exception {
    String queueName = oldSession.getQueueName();
    HiveConf conf = oldSession.getConf();
    Path scratchDir = oldSession.getTezScratchDir();
    boolean isDefault = oldSession.isDefault();
    Set<String> additionalFiles = oldSession.getAdditionalFilesNotFromConf();
    try {
      oldSession.close(false);
      defaultQueuePool.remove(oldSession);  // Make sure it's removed.
    } finally {
      TezSessionPoolSession newSession = createAndInitSession(queueName, isDefault);
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
          closeAndReopen(next);
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
   * */
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
        openSessions.remove(this);
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
      openSessions.add(this);
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
        parent.closeAndReopen(this);
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
