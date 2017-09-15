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

import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.Semaphore;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.tez.TezSessionPoolSession.OpenSessionTracker;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * This class is for managing multiple tez sessions particularly when
 * HiveServer2 is being used to submit queries.
 *
 * In case the user specifies a queue explicitly, a new session is created
 * on that queue and assigned to the session state.
 */
public class TezSessionPoolManager
  implements SessionExpirationTracker.RestartImpl, OpenSessionTracker {

  private enum CustomQueueAllowed {
    TRUE,
    FALSE,
    IGNORE
  }

  private static final Logger LOG = LoggerFactory.getLogger(TezSessionPoolManager.class);
  static final Random rdm = new Random();

  private Semaphore llapQueue;
  private HiveConf initConf = null;
  // Config settings.
  private int numConcurrentLlapQueries = -1;
  private CustomQueueAllowed customQueueAllowed = CustomQueueAllowed.TRUE;

  private TezSessionPool defaultSessionPool;
  private SessionExpirationTracker expirationTracker;
  private RestrictedConfigChecker restrictedConfig;

  /**
   * Indicates whether we should try to use defaultSessionPool.
   * We assume that setupPool is either called before any activity, or not called at all.
   */
  private volatile boolean hasInitialSessions = false;

  private static TezSessionPoolManager instance = null;

  /** This is used to close non-default sessions, and also all sessions when stopping. */
  private final List<TezSessionPoolSession> openSessions = new LinkedList<>();

  /** Note: this is not thread-safe. */
  public static TezSessionPoolManager getInstance() throws Exception {
    TezSessionPoolManager local = instance;
    if (local == null) {
      instance = local = new TezSessionPoolManager();
    }

    return local;
  }

  protected TezSessionPoolManager() {
  }

  public void startPool() throws Exception {
    if (defaultSessionPool != null) {
      defaultSessionPool.startInitialSessions();
    }
    if (expirationTracker != null) {
      expirationTracker.start();
    }
  }

  public void setupPool(HiveConf conf) throws InterruptedException {
    String[] defaultQueueList = HiveConf.getTrimmedStringsVar(
        conf, HiveConf.ConfVars.HIVE_SERVER2_TEZ_DEFAULT_QUEUES);
    this.initConf = conf;
    int emptyNames = 0; // We don't create sessions for empty entries.
    for (String queueName : defaultQueueList) {
      if (queueName.isEmpty()) {
        ++emptyNames;
      }
    }
    int numSessions = conf.getIntVar(ConfVars.HIVE_SERVER2_TEZ_SESSIONS_PER_DEFAULT_QUEUE);
    int numSessionsTotal = numSessions * (defaultQueueList.length - emptyNames);
    if (numSessionsTotal > 0) {
      // TODO: this can be enabled to test. Will only be used in WM case for now.
      boolean enableAmRegistry = false;
      defaultSessionPool = new TezSessionPool(initConf, numSessionsTotal, enableAmRegistry);
    }

    numConcurrentLlapQueries = conf.getIntVar(ConfVars.HIVE_SERVER2_LLAP_CONCURRENT_QUERIES);
    llapQueue = new Semaphore(numConcurrentLlapQueries, true);

    String queueAllowedStr = HiveConf.getVar(initConf,
        ConfVars.HIVE_SERVER2_TEZ_SESSION_CUSTOM_QUEUE_ALLOWED);
    try {
      this.customQueueAllowed = CustomQueueAllowed.valueOf(queueAllowedStr.toUpperCase());
    } catch (Exception ex) {
      throw new RuntimeException("Invalid value '" + queueAllowedStr + "' for " +
          ConfVars.HIVE_SERVER2_TEZ_SESSION_CUSTOM_QUEUE_ALLOWED.varname);
    }

    restrictedConfig = new RestrictedConfigChecker(conf);
    // Only creates the expiration tracker if expiration is configured.
    expirationTracker = SessionExpirationTracker.create(conf, this);

    // From this point on, session creation will wait for the default pool (if # of sessions > 0).
    this.hasInitialSessions = numSessionsTotal > 0;
    if (!hasInitialSessions) {
      return;
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
        HiveConf sessionConf = new HiveConf(initConf);
        defaultSessionPool.addInitialSession(createAndInitSession(queueName, true, sessionConf));
      }
    }
  }

  // TODO Create and init session sets up queue, isDefault - but does not initialize the configuration
  private TezSessionPoolSession createAndInitSession(
      String queue, boolean isDefault, HiveConf conf) {
    TezSessionPoolSession sessionState = createSession(TezSessionState.makeSessionId(), conf);
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

  private TezSessionState getSession(HiveConf conf, boolean doOpen) throws Exception {
    // NOTE: this can be called outside of HS2, without calling setupPool. Basically it should be
    //       able to handle not being initialized. Perhaps we should get rid of the instance and
    //       move the setupPool code to ctor. For now, at least hasInitialSessions will be false.
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

    // Check the restricted configs that the users cannot set.
    if (restrictedConfig != null) {
      restrictedConfig.validate(conf);
    }

    // Propagate this value from HS2; don't allow users to set it.
    // In HS2, initConf will be set; it won't be set otherwise as noone calls setupPool.
    // TODO: add a section like the restricted configs for overrides when there's more than one.
    if (initConf != null) {
      conf.set(ConfVars.LLAP_CACHE_DEFAULT_FS_FILE_ID.varname,
          HiveConf.getVarWithoutType(initConf, ConfVars.LLAP_CACHE_DEFAULT_FS_FILE_ID));
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
              queueName, nonDefaultUser, defaultSessionPool, hasInitialSessions);
      return getNewSessionState(conf, queueName, doOpen);
    }

    LOG.info("Choosing a session from the defaultQueuePool");
    return defaultSessionPool.getSession();
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
    TezSessionPoolSession retTezSessionState = createAndInitSession(queueName, false, conf);
    if (queueName != null) {
      conf.set(TezConfiguration.TEZ_QUEUE_NAME, queueName);
    }
    if (doOpen) {
      retTezSessionState.open();
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
        defaultSessionPool.returnSession((TezSessionPoolSession)tezSessionState);
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
    if ((instance == null) || !this.hasInitialSessions) {
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
    if (expirationTracker != null) {
      expirationTracker.stop();
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

  protected TezSessionPoolSession createSession(String sessionId, HiveConf conf) {
    return new TezSessionPoolSession(sessionId, this, expirationTracker, conf);
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
  public void reopenSession(TezSessionState sessionState, Configuration conf) throws Exception {
    HiveConf sessionConf = sessionState.getConf();
    if (sessionState.getQueueName() != null
        && sessionConf.get(TezConfiguration.TEZ_QUEUE_NAME) == null) {
      sessionConf.set(TezConfiguration.TEZ_QUEUE_NAME, sessionState.getQueueName());
    }
    Set<String> oldAdditionalFiles = sessionState.getAdditionalFilesNotFromConf();
    // TODO: close basically resets the object to a bunch of nulls.
    //       We should ideally not reuse the object because it's pointless and error-prone.
    // Close the old one, but keep the tmp files around.
    sessionState.close(true);
    // TODO: should we reuse scratchDir too?
    sessionState.open(oldAdditionalFiles, null);
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
  @Override
  public void closeAndReopenPoolSession(TezSessionPoolSession oldSession) throws Exception {
    String queueName = oldSession.getQueueName();
    if (queueName == null) {
      LOG.warn("Pool session has a null queue: " + oldSession);
    }
    TezSessionPoolSession newSession = createAndInitSession(
        queueName, oldSession.isDefault(), oldSession.getConf());
    defaultSessionPool.replaceSession(oldSession, newSession);
  }

  /** Called by TezSessionPoolSession when opened. */
  @Override
  public void registerOpenSession(TezSessionPoolSession session) {
    synchronized (openSessions) {
      openSessions.add(session);
    }
  }

  /** Called by TezSessionPoolSession when closed. */
  @Override
  public void unregisterOpenSession(TezSessionPoolSession session) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Closed a pool session [" + this + "]");
    }
    synchronized (openSessions) {
      openSessions.remove(session);
    }
  }

  @VisibleForTesting
  public SessionExpirationTracker getExpirationTracker() {
    return expirationTracker;
  }
}
