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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.KillQuery;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.wm.WmContext;
import org.apache.hadoop.hive.registry.impl.TezAmInstance;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * TezSession that is aware of the session pool, and also keeps track of expiration and use.
 * It has 3 states - not in use, in use, and expired. When in the pool, it is not in use;
 * use and expiration may compete to take the session out of the pool and change it to the
 * corresponding states. When someone tries to get a session, they check for expiration time;
 * if it's time, the expiration is triggered; in that case, or if it was already triggered, the
 * caller gets a different session. When the session is in use when it expires, the expiration
 * thread ignores it and lets the return to the pool take care of the expiration.
 *
 * Because of the lack of multiple inheritance in Java, this uses composition.
 */
@VisibleForTesting
class TezSessionPoolSession implements TezSession {
  protected static final Logger LOG = LoggerFactory.getLogger(TezSessionPoolSession.class);
  private static final int STATE_NONE = 0, STATE_IN_USE = 1, STATE_EXPIRED = 2;

  public interface Manager {
    void registerOpenSession(TezSessionPoolSession session);
    void unregisterOpenSession(TezSessionPoolSession session);
    void returnAfterUse(TezSessionPoolSession session) throws Exception;
    TezSession reopen(TezSession session) throws Exception;
    void destroy(TezSession session) throws Exception;
  }

  private final AtomicInteger sessionState = new AtomicInteger(STATE_NONE);
  private Long expirationNs;
  private final Manager manager;
  private final SessionExpirationTracker expirationTracker;
  private final TezSession baseSession;


  public TezSessionPoolSession(Manager manager,
      SessionExpirationTracker tracker, TezSession superr) {
    this.baseSession = superr;
    this.manager = manager;
    this.expirationTracker = tracker;
  }

  void setExpirationNs(long expirationNs) {
    this.expirationNs = expirationNs;
  }

  Long getExpirationNs() {
    return expirationNs;
  }

  @Override
  public void close(boolean keepTmpDir) throws Exception {
    try {
      baseSession.close(keepTmpDir);
    } finally {
      manager.unregisterOpenSession(this);
      if (expirationTracker != null) {
        expirationTracker.removeFromExpirationQueue(this);
      }
    }
  }

  @Override
  public void open(String[] additionalFilesNotFromConf)
      throws LoginException, IOException, URISyntaxException, TezException {
    baseSession.open(additionalFilesNotFromConf);
    afterOpen();
  }

  @Override
  public void open() throws IOException, LoginException, URISyntaxException, TezException {
    baseSession.open();
    afterOpen();
  }

  @Override
  public void open(boolean isPoolInit) throws IOException, LoginException, URISyntaxException, TezException {
    baseSession.open(isPoolInit);
    afterOpen();
  }

  private void afterOpen() {
    manager.registerOpenSession(this);
    if (expirationTracker != null) {
      boolean isNotExpired = expirationTracker.addToExpirationQueue(this, 0L);
      assert isNotExpired;
    }
  }

  @Override
  public boolean reconnect(String applicationId, long amAgeMs) throws IOException,
      LoginException, URISyntaxException, TezException {
    if (expirationTracker != null && !expirationTracker.isOldAmUsable(amAgeMs)) {
      closeExpiredOnReconnect(applicationId);
      return false;
    }
    if (!baseSession.reconnect(applicationId, amAgeMs)) {
      return false;
    }
    manager.registerOpenSession(this);
    if (expirationTracker != null && !expirationTracker.addToExpirationQueue(this, amAgeMs)) {
      closeExpiredOnReconnect(applicationId);
      return false;
    }
    return true;
  }

  private void closeExpiredOnReconnect(String applicationId) {
    LOG.warn("Not using an old AM due to expiration timeout: " + applicationId);
    try {
      this.close(false);
    } catch (Exception e) {
      LOG.info("Failed to close the old AM", e);
    }
  }

  @Override
  public void open(HiveResources resources)
      throws LoginException, IOException, URISyntaxException, TezException {
    baseSession.open(resources);
    afterOpen();
  }

  // TODO: this is only supported in CLI, might be good to try to remove it.
  @Override
  public void beginOpen(String[] additionalFiles, LogHelper console)
      throws IOException, LoginException, URISyntaxException, TezException {
    baseSession.beginOpen(additionalFiles, console);
    afterOpen();
  }

  @Override
  public void endOpen() throws InterruptedException, CancellationException {
    baseSession.endOpen();
  }

  @Override
  public void ensureLocalResources(Configuration conf,
      String[] newFilesNotFromConf) throws IOException, LoginException,
      URISyntaxException, TezException {
    baseSession.ensureLocalResources(conf, newFilesNotFromConf);
  }

  @Override
  public HiveResources extractHiveResources() {
    return baseSession.extractHiveResources();
  }

  @Override
  public Path replaceHiveResources(HiveResources resources, boolean isAsync) {
    return baseSession.replaceHiveResources(resources, isAsync);
  }

  @Override
  public boolean killQuery(String reason) throws HiveException {
    return baseSession.killQuery(reason);
  }

  // *********** Methods specific to a pool session.

  /**
   * Tries to use this session. When the session is in use, it will not expire.
   * @return true if the session can be used; false if it has already expired.
   */
  public boolean tryUse(boolean ignoreExpiration) {
    while (true) {
      int oldValue = sessionState.get();
      if (oldValue == STATE_IN_USE) throw new AssertionError(this + " is already in use");
      if (oldValue == STATE_EXPIRED) return false;
      int finalState = (!ignoreExpiration && shouldExpire()) ? STATE_EXPIRED : STATE_IN_USE;
      if (sessionState.compareAndSet(STATE_NONE, finalState)) {
        if (finalState == STATE_IN_USE) return true;
        // Restart asynchronously, don't block the caller.
        expirationTracker.closeAndRestartExpiredSessionAsync(this);
        return false;
      }
    }
  }

  boolean stopUsing() {
    int finalState = shouldExpire() ? STATE_EXPIRED : STATE_NONE;
    if (!sessionState.compareAndSet(STATE_IN_USE, finalState)) {
      throw new AssertionError("Unexpected state change; currently " + sessionState.get());
    }
    if (finalState == STATE_NONE) return true;
    expirationTracker.closeAndRestartExpiredSessionAsync(this);
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
        if (isAsync) {
          expirationTracker.closeAndRestartExpiredSessionAsync(this);
        } else {
          expirationTracker.closeAndRestartExpiredSession(this);
        }
        return true;
      }
    }
  }

  private final boolean shouldExpire() {
    return expirationNs != null && (System.nanoTime() - expirationNs) >= 0;
  }

  @Override
  public void returnToSessionManager() throws Exception {
    manager.returnAfterUse(this);
  }

  @Override
  public TezSession reopen() throws Exception {
    return manager.reopen(this);
  }

  @Override
  public void destroy() throws Exception {
    manager.destroy(this);
  }

  public boolean isOwnedBy(Manager parent) {
    return this.manager == parent;
  }

  public void updateFromRegistry(TezAmInstance si, int ephSeqVersion) {
    // Nothing to do.
  }

  @Override
  public String toString() {
    return baseSession.toString() + getExpirationString();
  }

  private String getExpirationString() {
    if (expirationNs == null) return "";
    long expiresInMs = (expirationNs - System.nanoTime()) / 1000000L;
    return ", expires in " + expiresInMs + "ms";
  }

  //  ********** The methods that we redirect to base.
  // We could instead have a separate "data" interface that would "return superr" here, and
  // "return this" in the actual session implementation; however that would require everyone to
  // call session.getData().method() for some arbitrary set of methods. Let's keep all the
  // ugliness in one place.

  @Override
  public HiveConf getConf() {
    return baseSession.getConf();
  }

  @Override
  public String getSessionId() {
    return baseSession.getSessionId();
  }

  @Override
  public String getUser() {
    return baseSession.getUser();
  }

  @Override
  public boolean isOpen() {
    return baseSession.isOpen();
  }

  @Override
  public void setQueueName(String queueName) {
    baseSession.setQueueName(queueName);
  }

  @Override
  public String getQueueName() {
    return baseSession.getQueueName();
  }

  @Override
  public void setDefault() {
    baseSession.setDefault();

  }

  @Override
  public boolean isDefault() {
    return baseSession.isDefault();
  }

  @Override
  public boolean getDoAsEnabled() {
    return baseSession.getDoAsEnabled();
  }

  @Override
  public boolean getLegacyLlapMode() {
    return baseSession.getLegacyLlapMode();
  }

  @Override
  public void setLegacyLlapMode(boolean b) {
    baseSession.setLegacyLlapMode(b);
  }

  @Override
  public WmContext getWmContext() {
    return baseSession.getWmContext();
  }

  @Override
  public void setWmContext(WmContext ctx) {
    baseSession.setWmContext(ctx);
  }

  @Override
  public LocalResource getAppJarLr() {
    return baseSession.getAppJarLr();
  }

  @Override
  public List<LocalResource> getLocalizedResources() {
    return baseSession.getLocalizedResources();
  }

  @Override
  public TezClient getTezClient() {
    return baseSession.getTezClient();
  }

    @Override
  public boolean isOpening() {
    return baseSession.isOpening();
  }

  @Override
  public void setOwnerThread() {
    baseSession.setOwnerThread();
  }

  @Override
  public void unsetOwnerThread() {
    baseSession.unsetOwnerThread();
  }

  @Override
  public void setKillQuery(KillQuery kq) {
    baseSession.setKillQuery(kq);
  }

  // ********** End of the methods that we redirect to base.
}
