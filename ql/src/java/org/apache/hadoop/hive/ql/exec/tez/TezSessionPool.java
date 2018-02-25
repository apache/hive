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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.registry.ServiceInstanceStateChangeListener;
import org.apache.hadoop.hive.registry.impl.TezAmInstance;
import org.apache.hadoop.hive.registry.impl.TezAmRegistryImpl;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distinct from TezSessionPool manager in that it implements a session pool, and nothing else.
 */
class TezSessionPool<SessionType extends TezSessionPoolSession> {
  private static final Logger LOG = LoggerFactory.getLogger(TezSessionPool.class);

  public interface SessionObjectFactory<SessionType> {
    SessionType create(SessionType oldSession);
  }

  private final HiveConf initConf;
  private int initialSize = 0; // For testing only.
  private final SessionObjectFactory<SessionType> sessionObjFactory;

  private final ReentrantLock poolLock = new ReentrantLock(true);
  private final Condition notEmpty = poolLock.newCondition();
  private final LinkedList<SessionType> pool = new LinkedList<>();
  private final LinkedList<SettableFuture<SessionType>> asyncRequests = new LinkedList<>();
  /**
   * The number of sessions that needs to be started or killed because of the resize calls on
   * the pool. When increasing the size, we set this to a positive number and start new sessions
   * on background threads, gradually bringing it back to 0.
   * When decreasing the size, we try to kill as many existing sessions as we can; if that is
   * not enough because the sessions are in use or being restarted, we kill them as they are
   * re-added to the pool.
   * Repeated calls to resize adjust the delta to ensure correctness between different resizes.
   */
  private final AtomicInteger deltaRemaining = new AtomicInteger();

  private final String amRegistryName;
  private final TezAmRegistryImpl amRegistry;

  private final ConcurrentHashMap<String, SessionType> bySessionId =
      new ConcurrentHashMap<>();
  // Preserved at initialization time to have a session to use during resize.
  // TODO: rather, Tez sessions should not depend on SessionState.
  private SessionState parentSessionState;

  TezSessionPool(HiveConf initConf, int numSessionsTotal, boolean useAmRegistryIfPresent,
      SessionObjectFactory<SessionType> sessionFactory) {
    this.initConf = initConf;
    this.initialSize = numSessionsTotal;
    this.amRegistry = useAmRegistryIfPresent ? TezAmRegistryImpl.create(initConf, true) : null;
    this.amRegistryName = amRegistry == null ? null : amRegistry.getRegistryName();
    this.sessionObjFactory = sessionFactory;
  }

  void start() throws Exception {
    if (amRegistry != null) {
      amRegistry.start();
      amRegistry.initializeWithoutRegistering();
      // Note: we may later have special logic to pick up old AMs, if any.
      amRegistry.registerStateChangeListener(new ChangeListener());
      amRegistry.populateCache(true);
    }

    this.parentSessionState = SessionState.get();
    if (initialSize == 0) return; // May be resized later.

    int threadCount = Math.min(initialSize,
        HiveConf.getIntVar(initConf, ConfVars.HIVE_SERVER2_TEZ_SESSION_MAX_INIT_THREADS));
    Preconditions.checkArgument(threadCount > 0);
    if (threadCount == 1) {
      for (int i = 0; i < initialSize; ++i) {
        SessionType session = sessionObjFactory.create(null);
        if (session == null) break;
        startInitialSession(session);
      }
    } else {
      final AtomicInteger remaining = new AtomicInteger(initialSize);
      @SuppressWarnings("unchecked")
      FutureTask<Boolean>[] threadTasks = new FutureTask[threadCount];
      for (int i = threadTasks.length - 1; i >= 0; --i) {
        threadTasks[i] = new FutureTask<Boolean>(new CreateSessionsRunnable(remaining));
        if (i == 0) {
          // Start is blocking, so run one of the tasks on the main thread.
          threadTasks[i].run();
        } else {
          new Thread(threadTasks[i], "Tez session init " + i).start();
        }
      }
      for (int i = 0; i < threadTasks.length; ++i) {
        threadTasks[i].get();
      }
    }
  }

  SessionType getSession() throws Exception {
    while (true) {
      SessionType result = null;
      poolLock.lock();
      try {
        while ((result = pool.poll()) == null) {
          notEmpty.await(100, TimeUnit.MILLISECONDS);
        }
      } finally {
        poolLock.unlock();
      }
      if (result.tryUse(false)) return result;
      LOG.info("Couldn't use a session [" + result + "]; attempting another one");
    }
  }

  ListenableFuture<SessionType> getSessionAsync() throws Exception {
    SettableFuture<SessionType> future = SettableFuture.create();
    poolLock.lock();
    try {
      // Try to get the session quickly.
      while (true) {
        SessionType result = pool.poll();
        if (result == null) break;
        if (!result.tryUse(false)) continue;
        future.set(result);
        return future;
      }
      // The pool is empty; queue the request.
      asyncRequests.add(future);
      return future;
    } finally {
      poolLock.unlock();
    }
  }

  void returnSession(SessionType session) {
    returnSessionInternal(session, false);
  }

  boolean returnSessionAsync(SessionType session) {
    return returnSessionInternal(session, true);
  }

  private boolean returnSessionInternal(SessionType session, boolean isAsync) {
    // Make sure that if the session is returned to the pool, it doesn't live in the global.
    SessionState sessionState = SessionState.get();
    if (sessionState != null) {
      sessionState.setTezSession(null);
    }
    if (!session.stopUsing()) return true; // The session will be restarted and return to us.
    boolean canPutBack = putSessionBack(session, true);
    if (canPutBack) return true;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Closing an unneeded returned session " + session);
    }

    if (isAsync) return false; // The caller is responsible for destroying the session.
    try {
      session.close(false);
    } catch (Exception ex) {
      LOG.error("Failed to close " + session, ex);
    }
    return true;
  }

  /**
   * Puts session back into the pool.
   * @return true if the session has been put back; false if it's not needed and should be killed.
   */
  private boolean putSessionBack(SessionType session, boolean isFirst) {
    SettableFuture<SessionType> future = null;
    poolLock.lock();
    try {
      // See if we need to kill some sessions because the pool was resized down while
      // a bunch of sessions were outstanding. See also deltaRemaining javadoc.
      while (true) {
        int remainingToKill = -deltaRemaining.get();
        if (remainingToKill <= 0) break; // No need to kill anything.
        if (deltaRemaining.compareAndSet(-remainingToKill, -remainingToKill + 1)) {
          return false;
        }
      }
      // If there are async requests, satisfy them first.
      if (!asyncRequests.isEmpty()) {
        if (!session.tryUse(false)) {
          return true; // Session has expired and will be returned to us later.
        }
        future = asyncRequests.poll();
      }
      if (future == null) {
        // Put session into the pool.
        if (isFirst) {
          pool.addFirst(session);
        } else {
          pool.addLast(session);
        }
        notEmpty.signalAll();
      }
    } finally {
      poolLock.unlock();
    }
    if (future != null) {
      future.set(session);
    }
    return true;
  }

  void replaceSession(SessionType oldSession) throws Exception {
    // Re-setting the queue config is an old hack that we may remove in future.
    SessionType newSession = sessionObjFactory.create(oldSession);
    String queueName = oldSession.getQueueName();
    try {
      oldSession.close(false);
    } finally {
      poolLock.lock();
      try {
        // The expiring session may or may not be in the pool.
        pool.remove(oldSession);
      } finally {
        poolLock.unlock();
      }

      notifyClosed(oldSession);
      // There's some bogus code that can modify the queue name. Force-set it for pool sessions.
      // TODO: this might only be applicable to TezSessionPoolManager; try moving it there?
      newSession.getConf().set(TezConfiguration.TEZ_QUEUE_NAME, queueName);
      // The caller probably created the new session with the old config, but update the
      // registry again just in case. TODO: maybe we should enforce that.
      configureAmRegistry(newSession);
      if (SessionState.get() == null && parentSessionState != null) {
        // Tez session relies on a threadlocal for open... If we are on some non-session thread,
        // just use the same SessionState we used for the initial sessions.
        // Technically, given that all pool sessions are initially based on this state, shoudln't
        // we also set this at all times and not rely on an external session stuff? We should
        // probably just get rid of the thread local usage in TezSessionState.
        SessionState.setCurrentSessionState(parentSessionState);
      }
      newSession.open();
      if (!putSessionBack(newSession, false)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Closing an unneeded session " + newSession
              + "; trying to replace " + oldSession);
        }
        try {
          newSession.close(false);
        } catch (Exception ex) {
          LOG.error("Failed to close an unneeded session", ex);
        }
      }
    }
  }


  private void startInitialSession(SessionType session) throws Exception {
    boolean isUsable = session.tryUse(true);
    if (!isUsable) throw new IOException(session + " is not usable at pool startup");
    session.getConf().set(TezConfiguration.TEZ_QUEUE_NAME, session.getQueueName());
    configureAmRegistry(session);
    session.open();
    if (session.stopUsing()) {
      if (!putSessionBack(session, false)) {
        LOG.warn("Couldn't add a session during initialization");
        try {
          session.close(false);
        } catch (Exception ex) {
          LOG.error("Failed to close an unneeded session", ex);
        }
      }
    }
  }

  private void configureAmRegistry(SessionType session) {
    if (amRegistryName != null) {
      bySessionId.put(session.getSessionId(), session);
      HiveConf conf = session.getConf();
      conf.set(ConfVars.LLAP_TASK_SCHEDULER_AM_REGISTRY_NAME.varname, amRegistryName);
      conf.set(ConfVars.HIVESESSIONID.varname, session.getSessionId());
    }
  }


  private final class ChangeListener
    implements ServiceInstanceStateChangeListener<TezAmInstance> {

    @Override
    public void onCreate(TezAmInstance si, int ephSeqVersion) {
      String sessionId = si.getSessionId();
      SessionType session = bySessionId.get(sessionId);
      if (session != null) {
        LOG.info("AM for " + sessionId + ", v." + ephSeqVersion + " has registered; updating ["
            + session + "] with an endpoint at " + si.getPluginPort());
        session.updateFromRegistry(si, ephSeqVersion);
      } else {
        LOG.warn("AM for an unknown " + sessionId + " has registered; ignoring");
      }
    }

    @Override
    public void onUpdate(TezAmInstance serviceInstance, int ephSeqVersion) {
      // We currently never update the znode once registered.
      // AM recovery will create a new node when it calls register.
      LOG.warn("Received an unexpected update for instance={}. Ignoring", serviceInstance);
    }

    @Override
    public void onRemove(TezAmInstance serviceInstance, int ephSeqVersion) {
      String sessionId = serviceInstance.getSessionId();
      // For now, we don't take any pool action. In future, we might restore the session based
      // on this and get rid of the logic outside of the pool that replaces/reopens/etc.
      SessionType session = bySessionId.get(sessionId);
      if (session != null) {
        LOG.info("AM for " + sessionId + ", v." + ephSeqVersion
            + " has unregistered; updating [" + session + "]");
        session.updateFromRegistry(null, ephSeqVersion);
      } else {
        LOG.warn("AM for an unknown " + sessionId + " has unregistered; ignoring");
      }
    }
  }

  @VisibleForTesting
  int getInitialSize() {
    return initialSize;
  }

  /**
   * Resizes the pool asynchronously.
   * @param delta A number of threads to add or remove.
   * @param toClose An output list to which newly-unneeded sessions, to be closed by the caller.
   */
  public ListenableFuture<?> resizeAsync(int delta, List<SessionType> toClose) {
    if (delta == 0) return createDummyFuture();
    poolLock.lock();
    try {
      if (delta < 0) {
        return resizeDownInternal(-delta, toClose);
      } else {
        return resizeUpInternal(delta);
      }
    } finally {
      poolLock.unlock();
    }
  }

  private ListenableFuture<?> resizeUpInternal(int delta) {
    // 1) Cancel the kills if any, to avoid killing the returned sessions.
    //    Also sets the count for the async initialization.
    int oldVal;
    do {
      oldVal = deltaRemaining.get();
    } while (!deltaRemaining.compareAndSet(oldVal, oldVal + delta));
    int toStart = oldVal + delta;
    if (toStart <= 0) return createDummyFuture();
    LOG.info("Resizing the pool; adding " + toStart + " sessions");

    // 2) If we need to create some extra sessions, we'd do it just like startup does.
    int threadCount = Math.max(1, Math.min(toStart,
        HiveConf.getIntVar(initConf, ConfVars.HIVE_SERVER2_TEZ_SESSION_MAX_INIT_THREADS)));
    List<ListenableFutureTask<Boolean>> threadTasks = new ArrayList<>(threadCount);
    // This is an async method, so always launch threads, even for a single task.
    for (int i = 0; i < threadCount; ++i) {
      ListenableFutureTask<Boolean> task = ListenableFutureTask.create(
          new CreateSessionsRunnable(deltaRemaining));
      new Thread(task, "Tez pool resize " + i).start();
      threadTasks.add(task);
    }
    return Futures.allAsList(threadTasks);
  }

  private ListenableFuture<Boolean> resizeDownInternal(int delta, List<SessionType> toClose) {
    // 1) Cancel the previous expansion, if any.
    while (true) {
      int expansionCount = deltaRemaining.get();
      if (expansionCount <= 0) break;
      int expansionCancelled = Math.min(expansionCount, delta);
      if (deltaRemaining.compareAndSet(expansionCount, expansionCount - expansionCancelled)) {
        delta -= expansionCancelled;
        break;
      }
    }
    // 2) Drain unused sessions; the close() is sync so delegate to the caller.
    while (delta > 0) {
      SessionType session = pool.poll();
      if (session == null) break;
      if (!session.tryUse(true)) continue;
      toClose.add(session);
      --delta;
    }
    // 3) If too many sessions are outstanding (e.g. due to expiration restarts - should
    //    not happen with in-use sessions because WM already kills the extras), we will kill
    //    them as they come back from restarts.
    if (delta > 0) {
      int oldVal;
      do {
        oldVal = deltaRemaining.get();
      } while (!deltaRemaining.compareAndSet(oldVal, oldVal - delta));
    }
    return createDummyFuture();
  }

  private ListenableFuture<Boolean> createDummyFuture() {
    SettableFuture<Boolean> f = SettableFuture.create();
    f.set(true);
    return f;
  }

  private final class CreateSessionsRunnable implements Callable<Boolean> {
    private final AtomicInteger remaining;

    private CreateSessionsRunnable(AtomicInteger remaining) {
      this.remaining = remaining;
    }

    public Boolean call() throws Exception {
      if (parentSessionState != null) {
        SessionState.setCurrentSessionState(parentSessionState);
      }
      while (true) {
        int oldVal = remaining.get();
        if (oldVal <= 0) return true;
        if (!remaining.compareAndSet(oldVal, oldVal - 1)) continue;
        startInitialSession(sessionObjFactory.create(null));
      }
    }
  }

  @VisibleForTesting
  int getCurrentSize() {
    poolLock.lock();
    try {
      return pool.size();
    } finally {
      poolLock.unlock();
    }
  }

  /**
   * Should be called when the session is no longer needed, to remove it from bySessionId.
   */
  public void notifyClosed(TezSessionState session) {
    bySessionId.remove(session.getSessionId());
  }
}
