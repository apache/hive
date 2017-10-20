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


import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;
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

  /** A queue for initial sessions that have not been started yet. */
  private final Queue<SessionType> initialSessions =
      new ConcurrentLinkedQueue<SessionType>();

  private final HiveConf initConf;
  private int initialSize;

  // TODO: eventually, this will need to support resize. That would probably require replacement
  //       with a RW lock, a semaphore and linked list.
  private BlockingDeque<SessionType> defaultQueuePool;

  private final String amRegistryName;
  private final TezAmRegistryImpl amRegistry;

  private final ConcurrentHashMap<String, SessionType> bySessionId =
      new ConcurrentHashMap<>();


  TezSessionPool(HiveConf initConf, int numSessionsTotal, boolean useAmRegistryIfPresent) {
    this.initConf = initConf;
    assert numSessionsTotal > 0;
    defaultQueuePool = new LinkedBlockingDeque<>(numSessionsTotal);
    this.amRegistry = useAmRegistryIfPresent ? TezAmRegistryImpl.create(initConf, true) : null;
    this.amRegistryName = amRegistry == null ? null : amRegistry.getRegistryName();
  }

  void startInitialSessions() throws Exception {
    initialSize = initialSessions.size();
    if (initialSessions.isEmpty()) return;
    if (amRegistry != null) {
      amRegistry.start();
      amRegistry.initializeWithoutRegistering();
      // Note: we may later have special logic to pick up old AMs, if any.
      amRegistry.registerStateChangeListener(new ChangeListener());
      amRegistry.populateCache(true);
    }

    int threadCount = Math.min(initialSessions.size(),
        HiveConf.getIntVar(initConf, ConfVars.HIVE_SERVER2_TEZ_SESSION_MAX_INIT_THREADS));
    Preconditions.checkArgument(threadCount > 0);
    if (threadCount == 1) {
      while (true) {
        SessionType session = initialSessions.poll();
        if (session == null) break;
        startInitialSession(session);
      }
    } else {
      final SessionState parentSessionState = SessionState.get();
      // The runnable has no mutable state, so each thread can run the same thing.
      final AtomicReference<Exception> firstError = new AtomicReference<>(null);
      Runnable runnable = new Runnable() {
        public void run() {
          if (parentSessionState != null) {
            SessionState.setCurrentSessionState(parentSessionState);
          }
          while (true) {
            SessionType session = initialSessions.poll();
            if (session == null) break;
            if (firstError.get() != null) break; // Best-effort.
            try {
              startInitialSession(session);
            } catch (Exception e) {
              if (!firstError.compareAndSet(null, e)) {
                LOG.error("Failed to start session; ignoring due to previous error", e);
              }
              break;
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
  }

  void addInitialSession(SessionType session) {
    initialSessions.add(session);
  }

  SessionType getSession() throws Exception {
    while (true) {
      SessionType result = defaultQueuePool.take();
      if (result.tryUse()) return result;
      LOG.info("Couldn't use a session [" + result + "]; attempting another one");
    }
  }

  void returnSession(SessionType session) throws Exception {
    // Make sure that if the session is returned to the pool, it doesn't live in the global.
    SessionState sessionState = SessionState.get();
    if (sessionState != null) {
      sessionState.setTezSession(null);
    }
    if (session.stopUsing()) {
      defaultQueuePool.putFirst(session);
    }
  }

  void replaceSession(SessionType oldSession, SessionType newSession,
      boolean keepTmpDir, String[] additionalFilesArray, HiveConf conf) throws Exception {
    // Retain the stuff from the old session.
    // Re-setting the queue config is an old hack that we may remove in future.
    Path scratchDir = oldSession.getTezScratchDir();
    String queueName = oldSession.getQueueName();
    Set<String> additionalFiles = null;
    if (additionalFilesArray != null) {
      additionalFiles = new HashSet<>();
      for (String file : additionalFilesArray) {
        additionalFiles.add(file);
      }
    } else {
      additionalFiles = oldSession.getAdditionalFilesNotFromConf();
    }
    try {
      oldSession.close(keepTmpDir);
      boolean wasRemoved = defaultQueuePool.remove(oldSession);
      if (!wasRemoved) {
        LOG.error("Old session was closed but it was not in the pool", oldSession);
      }
      bySessionId.remove(oldSession.getSessionId());
    } finally {
      // There's some bogus code that can modify the queue name. Force-set it for pool sessions.
      // TODO: this might only be applicable to TezSessionPoolManager; try moving it there?
      newSession.getConf().set(TezConfiguration.TEZ_QUEUE_NAME, queueName);
      // The caller probably created the new session with the old config, but update the
      // registry again just in case. TODO: maybe we should enforce that.
      configureAmRegistry(newSession);
      newSession.open(additionalFiles, scratchDir);
      defaultQueuePool.put(newSession);
    }
  }

  private void startInitialSession(SessionType session) throws Exception {
    boolean isUsable = session.tryUse();
    if (!isUsable) throw new IOException(session + " is not usable at pool startup");
    session.getConf().set(TezConfiguration.TEZ_QUEUE_NAME, session.getQueueName());
    configureAmRegistry(session);
    session.open();
    if (session.stopUsing()) {
      defaultQueuePool.put(session);
    }
  }

  private void configureAmRegistry(SessionType session) {
    if (amRegistryName != null) {
      bySessionId.put(session.getSessionId(), session);
      HiveConf conf = session.getConf();
      conf.set(ConfVars.LLAP_TASK_SCHEDULER_AM_REGISTRY_NAME.varname, amRegistryName);
      conf.set(ConfVars.HIVESESSIONID.varname, session.getSessionId());
      // TODO: can be enable temporarily for testing
      // conf.set(LlapTaskSchedulerService.LLAP_PLUGIN_ENDPOINT_ENABLED, "true");
    }
  }


  private final class ChangeListener
    implements ServiceInstanceStateChangeListener<TezAmInstance> {

    @Override
    public void onCreate(TezAmInstance si) {
      String sessionId = si.getSessionId();
      SessionType session = bySessionId.get(sessionId);
      if (session != null) {
        LOG.info("AM for " + sessionId + " has registered; updating [" + session
            + "] with an endpoint at " + si.getPluginPort());
        session.updateFromRegistry(si);
      } else {
        LOG.warn("AM for an unknown " + sessionId + " has registered; ignoring");
      }
    }

    @Override
    public void onUpdate(TezAmInstance serviceInstance) {
      // Presumably we'd get those later if AM updates its stuff.
      LOG.warn("Received an unexpected update for instance={}. Ignoring", serviceInstance);
    }

    @Override
    public void onRemove(TezAmInstance serviceInstance) {
      String sessionId = serviceInstance.getSessionId();
      // For now, we don't take any action. In future, we might restore the session based
      // on this and get rid of the logic outside of the pool that replaces/reopens/etc.
      LOG.warn("AM for " + sessionId + " has disappeared from the registry");
      bySessionId.remove(sessionId);
    }
  }

  int getInitialSize() {
    return initialSize;
  }
}
