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
package org.apache.hadoop.hive.ql.exec.impala;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hive.common.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
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

  private Set<ImpalaSession> createdSessions = Collections.synchronizedSet(new HashSet<ImpalaSession>());
  private volatile boolean inited = false;

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

  public static synchronized ImpalaSessionManager getInstance()
      throws HiveException {
    if (instance == null) {
      instance = new ImpalaSessionManager();
    }

    return instance;
  }

  private ImpalaSessionManager() {
  }

  private void initBackendConfig(HiveConf hiveConf) throws HiveException {
    try {
      if (BackendConfig.INSTANCE == null) {
        ImpalaSession session = getSession(hiveConf);

        final TBackendGflags cfg = session.getBackendConfig();
        BackendConfig.create(cfg,
            false /* don't initialize SqlScanner or AuthToLocal */);
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
            initBackendConfig(hiveConf);
          } catch (Exception e) {
            inited = false;
            throw e;
          }
        }
      }
    }
  }

  /**
   * If the <i>existingSession</i> can be reused return it.
   * Otherwise
   *   - close it and remove it from the list.
   *   - create a new session and add it to the list.
   */
  public ImpalaSession getSession(HiveConf conf)
      throws HiveException {
    setup(conf);

    ImpalaSession existingSession = SessionState.get().getImpalaSession();
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

    ImpalaSession impalaSession = new ImpalaSession(conf);
    impalaSession.open();

    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("New session (%s) is created.", impalaSession.getSessionId()));
    }
    createdSessions.add(impalaSession);
    SessionState.get().setImpalaSession(impalaSession);
    return impalaSession;
  }

  public void closeSession(ImpalaSession impalaSession) throws HiveException {
    if (impalaSession == null) {
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
      for (ImpalaSession session : createdSessions ) {
        session.close();
      }
      createdSessions.clear();
    }
    inited = false;
  }
}
