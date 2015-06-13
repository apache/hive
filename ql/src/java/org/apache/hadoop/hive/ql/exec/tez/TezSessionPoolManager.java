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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * This class is for managing multiple tez sessions particularly when
 * HiveServer2 is being used to submit queries.
 *
 * In case the user specifies a queue explicitly, a new session is created
 * on that queue and assigned to the session state.
 */
public class TezSessionPoolManager {

  private static final Log LOG = LogFactory.getLog(TezSessionPoolManager.class);

  private BlockingQueue<TezSessionState> defaultQueuePool;
  private int blockingQueueLength = -1;
  private HiveConf initConf = null;

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
      TezSessionState sessionState = defaultQueuePool.take();
      newConf.set("tez.queue.name", sessionState.getQueueName());
      sessionState.open(newConf);
      openSessions.add(sessionState);
      defaultQueuePool.put(sessionState);
    }
  }

  public void setupPool(HiveConf conf) throws InterruptedException {

    String defaultQueues = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_TEZ_DEFAULT_QUEUES);
    int numSessions = conf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_SESSIONS_PER_DEFAULT_QUEUE);

    // the list of queues is a comma separated list.
    String defaultQueueList[] = defaultQueues.split(",");
    defaultQueuePool =
        new ArrayBlockingQueue<TezSessionState>(numSessions * defaultQueueList.length);
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
        TezSessionState sessionState = createSession(TezSessionState.makeSessionId());
        sessionState.setQueueName(queue);
        sessionState.setDefault();
        LOG.info("Created new tez session for queue: " + queue +
            " with session id: " + sessionState.getSessionId());
        defaultQueuePool.put(sessionState);
        blockingQueueLength++;
      }
    }
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
    return defaultQueuePool.take();
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
    TezSessionState retTezSessionState = createSession(TezSessionState.makeSessionId());
    if (queueName != null) {
      conf.set("tez.queue.name", queueName);
    }
    String what = "Created";
    if (doOpen) {
      retTezSessionState.open(conf);
      openSessions.add(retTezSessionState);
      what = "Started";
    }

    LOG.info(what + " a new session for queue: " + queueName +
        " session id: " + retTezSessionState.getSessionId());
    return retTezSessionState;
  }

  public void returnSession(TezSessionState tezSessionState)
      throws Exception {
    if (tezSessionState.isDefault()) {
      LOG.info("The session " + tezSessionState.getSessionId()
          + " belongs to the pool. Put it back in");
      SessionState sessionState = SessionState.get();
      if (sessionState != null) {
        sessionState.setTezSession(null);
      }
      defaultQueuePool.put(tezSessionState);
    }
    // non default session nothing changes. The user can continue to use the existing
    // session in the SessionState
  }

  public void close(TezSessionState tezSessionState, boolean keepTmpDir) throws Exception {
    LOG.info("Closing tez session default? " + tezSessionState.isDefault());
    if (!tezSessionState.isDefault()) {
      tezSessionState.close(keepTmpDir);
      openSessions.remove(tezSessionState);
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
  }

  protected TezSessionState createSession(String sessionId) {
    return new TezSessionState(sessionId);
  }

  public TezSessionState getSession(
      TezSessionState session, HiveConf conf, boolean doOpen) throws Exception {
    return getSession(session, conf, doOpen, false);
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

  public TezSessionState getSession(TezSessionState session, HiveConf conf,
      boolean doOpen, boolean forceCreate) throws Exception {
    if (canWorkWithSameSession(session, conf)) {
      return session;
    }

    if (session != null) {
      close(session, false);
    }

    return getSession(conf, doOpen, forceCreate);
  }

  public void closeAndOpen(TezSessionState sessionState, HiveConf conf, boolean keepTmpDir)
      throws Exception {
    closeAndOpen(sessionState, conf, null, keepTmpDir);
  }

  public void closeAndOpen(TezSessionState sessionState, HiveConf conf,
      String[] additionalFiles, boolean keepTmpDir) throws Exception {
    HiveConf sessionConf = sessionState.getConf();
    if (sessionConf != null && sessionConf.get("tez.queue.name") != null) {
      conf.set("tez.queue.name", sessionConf.get("tez.queue.name"));
    }
    close(sessionState, keepTmpDir);
    sessionState.open(conf, additionalFiles);
    openSessions.add(sessionState);
  }

  public List<TezSessionState> getOpenSessions() {
    return openSessions;
  }
}
