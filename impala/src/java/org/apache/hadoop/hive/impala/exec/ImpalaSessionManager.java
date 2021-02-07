/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.impala.exec;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hive.common.util.ShutdownHookManager;
import org.apache.impala.thrift.TUpdateExecutorMembershipRequest;
import org.apache.impala.util.ExecutorMembershipSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.engine.EngineSession;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TBackendGflags;

/**
 * Simple implementation of <i>ImpalaSessionManager</i>
 *   - returns ImpalaSession when requested through <i>getSession</i> and keeps track of
 *       created sessions. Currently no limit on the number sessions.
 */
public class ImpalaSessionManager {
  private static final Logger LOG = LoggerFactory.getLogger(ImpalaSessionManager.class);

  private Set<EngineSession> createdSessions = Collections.synchronizedSet(new HashSet<EngineSession>());
  private volatile boolean inited = false;

  // A cluster membership cache.
  private LoadingCache<MembershipCacheKey, ExecutorMembershipSnapshot> membershipCache;

  // The cache expiration is chosen based on a couple of factors:
  //   1. Single node removal or addition is unlikely to change plans significantly, so we
  //      don't want the (potentially small) overhead of re-loading the cache in such cases.
  //   2. On a kubernetes platform it may take couple of minutes for a 10 node cluster to be
  //      brought up either at startup or during auto-scaling..hence choosing an expiration
  //      time based on that seems reasonable
  // TODO: CDPD-19122: Make the cluster membership refresh interval configurable
  private static final int EXPIRATION_SECS = 60;

  private static ImpalaSessionManager instance;

  static {
    ShutdownHookManager.addShutdownHook(new Runnable() {
      public void run() {
        try {
          if (instance != null) {
            instance.shutdown();
          }
        } catch (Exception e) {
          // ignore
        }
      }
    });
  }

  /**
   * A MembershipCacheKey is a placeholder for an impala session object. The session
   * object can keep changing via the setter but a static instance of MembershipCacheKey
   * allows cache lookups based on this instance rather than individual sessions.
   */
  private static class MembershipCacheKey {
    public static MembershipCacheKey INSTANCE = new MembershipCacheKey();
    private EngineSession session;
    private MembershipCacheKey() {
    }
    public void setSession(EngineSession session) {
      this.session = session;
    }
    public EngineSession getSession() {
      return session;
    }
  }

  /**
   * Ensure that the supplied session has the current executor membership snapshot.
   */
  public synchronized void ensureCurrentMembership(EngineSession session) {
    MembershipCacheKey.INSTANCE.setSession(session);
    // The retrieval from the loading cache will automatically trigger an update of the
    // ExecutorMembershipSnapshot which is a singleton object exposed to all sessions.
    membershipCache.getUnchecked(MembershipCacheKey.INSTANCE);
    MembershipCacheKey.INSTANCE.setSession(null);
  }

  /**
   * Create a cache for executor membership snapshot that is updated based
   * on the expiration settings. The cache has exactly 1 entry in it because
   * there is a single ExecutorMembershipSnapshot in the whole HS2 process.
   * We leverage the loading cache because it allows us to delegate the expiration
   * and Futures based loading to the cache instead of managing it ourselves.
   */
  private void createMembershipCache(EngineSession session) {
    CacheLoader<MembershipCacheKey, ExecutorMembershipSnapshot> loader =
        new CacheLoader<MembershipCacheKey, ExecutorMembershipSnapshot>() {
          //  load() will only be called in response to a get() from the cache
          //  and the only way a get() would be called is from a valid Impala
          //  session. If the session terminated early due to an exception,
          //  the get and load would not be called for that particular session.
          @Override
          public ExecutorMembershipSnapshot load(MembershipCacheKey key) throws HiveException {
            ImpalaSessionImpl session = (ImpalaSessionImpl) key.getSession();
            TUpdateExecutorMembershipRequest req = session.getExecutorMembership();
            ExecutorMembershipSnapshot.update(req);
            return ExecutorMembershipSnapshot.getCluster();
          }
        };
    membershipCache = CacheBuilder.newBuilder()
        .expireAfterWrite(EXPIRATION_SECS, TimeUnit.SECONDS)
        .maximumSize(1)
        .build(loader);

    // init the cache for the first time
    ensureCurrentMembership(session);
  }

  public static synchronized ImpalaSessionManager getInstance()
      throws HiveException {
    if (instance == null) {
      instance = new ImpalaSessionManager();
    }

    return instance;
  }

  private ImpalaSessionManager() {
  }

  private void initBackendConfigAndExecMembership(HiveConf hiveConf) throws HiveException {
    try {
      if (BackendConfig.INSTANCE == null) {
        ImpalaSessionImpl session = getSession(hiveConf);

        final TBackendGflags cfg = session.getBackendConfig();

        BackendConfig.create(cfg,
            false /* don't initialize SqlScanner or AuthToLocal */);

        createMembershipCache(session);
      }
      FeSupport.loadLibrary(true);
    } catch(Exception e) {
      throw new HiveException(e);
    }
  }

  public void setup(HiveConf hiveConf) throws HiveException {
    if (!inited) {
      synchronized (this) {
        if (!inited) {
          LOG.info("Setting up the session manager.");
          inited = true;

          // Do this after inited is set since it will recurse into getSession
          try {
            initBackendConfigAndExecMembership(hiveConf);
          } catch (Exception e) {
            inited = false;
            throw e;
          }
        }
      }
    }
  }

  /**
   * Returns an ImpalaSession associated with the Hive SessionState.
   * This method attempts to reuse an existing session if available and
   * when the Impala configuration has not been updated.
   * Otherwise
   *   - close it and remove it from the list (if applicable).
   *   - create a new session and add it to the list.
   *
   * This method does not attempt to validate the disposition of
   * the transport/socket associated with the returned ImpalaSession.
   * It is expected that ImpalaSession RPC methods try to re-establish
   * a valid session when applicable.
   */
  public ImpalaSessionImpl getSession(HiveConf conf)
      throws HiveException {
    setup(conf);

    ImpalaSessionImpl existingSession = (ImpalaSessionImpl) SessionState.get().getEngineSession();
    HiveConf sessionConf = SessionState.get().getConf();

    // Impala configurations are updated close the existing session
    // In case of async queries or confOverlay is not empty,
    // sessionConf and conf are different objects
    if (sessionConf.getImpalaConfigUpdated() || conf.getImpalaConfigUpdated()) {
      closeSession(existingSession);
      existingSession = null;
      conf.setImpalaConfigUpdated(false);
      sessionConf.setImpalaConfigUpdated(false);
    }

    if (existingSession != null) {
      // Open the session if it is closed.
      if (!existingSession.isOpen()) {
        existingSession.open();
      }
      return existingSession;
    }

    // We use 'conf' to initialize those fields in an ImpalaSession that can be set up
    // via Hive-related configurations, e.g., 'HiveConf.ConfVars.HIVE_IMPALA_ADDRESS',
    // whereas we use SessionState.get().getConf() in impalaSession.open() to set up
    // Impala's query options that will be stored in a TOpenSessionReq, which in turn
    // will be sent to the Impala backend.
    ImpalaSessionImpl impalaSession = new ImpalaSessionImpl(conf);
    impalaSession.open();

    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("New session (%s) is created.", impalaSession.getSessionId()));
    }
    createdSessions.add(impalaSession);
    SessionState.get().setEngineSession(impalaSession);
    return impalaSession;
  }

  public void closeSession(EngineSession impalaSession) throws HiveException {
    if (impalaSession == null || !impalaSession.isOpen()) {
      return;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Closing session (%s).", impalaSession.getSessionId()));
    }
    impalaSession.close();
    createdSessions.remove(impalaSession);
  }

  public void shutdown() {
    LOG.info("Closing the session manager.");
    synchronized (createdSessions) {
      for (EngineSession session : createdSessions ) {
        session.close();
      }
      createdSessions.clear();
    }
    inited = false;
  }
}
