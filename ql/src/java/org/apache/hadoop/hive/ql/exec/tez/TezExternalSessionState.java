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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.tez.monitoring.TezJobMonitor;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.serviceplugins.api.ServicePluginsDescriptor;

/**
 * A {@code TezSession} implementation that represents externally managed Tez sessions.
 * <p>
 * Unlike {@code TezSessionState}, these sessions are not created or owned by HiveServer2.
 * Instead, HiveServer2 connects to an already existing Tez session.
 * </p>
 *
 * <h3>Lifecycle</h3>
 * <ol>
 *   <li>An instance of {@code TezExternalSessionState} is created.</li>
 *   <li>An artificial application ID is acquired from a registry. This does not
 *       correspond to a real YARN application, as the session is unmanaged.</li>
 *   <li>A {@code TezClient} is instantiated but not started (unlike in
 *       {@code TezSessionState}), allowing the rest of the Hive codebase to
 *       interact with it transparently.</li>
 * </ol>
 *
 * <p>
 * This abstraction enables Hive components to interact with external Tez
 * sessions using the same interfaces as internally managed sessions.
 * </p>
 */
public class TezExternalSessionState extends TezSessionState {
  private static final Object DEFAULT_CONF_CREATE_LOCK = new Object();
  private static TezConfiguration defaultTezConfiguration;

  private String externalAppId;
  private boolean isDestroying = false;
  private final ExternalSessionsRegistry registry;

  public TezExternalSessionState(String sessionId, HiveConf conf) {
    super(sessionId, conf);
    this.registry = ExternalSessionsRegistry.getClient(conf);
    synchronized (DEFAULT_CONF_CREATE_LOCK) {
      if (defaultTezConfiguration == null) {
        defaultTezConfiguration = createDefaultTezConfig();
      }
    }
  }

  /**
   * No-op implementation.
   * <p>
   * External Tez sessions are not backed by a YARN application and therefore
   * do not manage or localize resources. As a result, there are no local
   * resources to ensure for this session type.
   * </p>
   */
  @Override
  public void ensureLocalResources(Configuration conf, String[] newFilesNotFromConf) {
  }

  @Override
  protected void openInternal(String[] additionalFilesNotFromConf,
                              boolean isAsync, LogHelper console, HiveResources resources)
      throws IOException, TezException {
    initQueueAndUser();

    boolean llapMode = isLlapMode();

    Map<String, String> amEnv = new HashMap<>();
    MRHelpers.updateEnvBasedOnMRAMEnv(conf, amEnv);

    TezConfiguration tezConfig = new TezConfiguration(defaultTezConfiguration);
    setupSessionAcls(tezConfig, conf);
    ServicePluginsDescriptor spd = createServicePluginDescriptor(llapMode, tezConfig);
    Credentials llapCredentials = createLlapCredentials(llapMode, tezConfig);

    final TezClient session = TezClient.newBuilder("HIVE-" + getSessionId(), tezConfig)
        .setIsSession(true)
        .setCredentials(llapCredentials).setServicePluginDescriptor(spd)
        .build();

    LOG.info("Opening new External Tez Session (id: {})", getSessionId());
    TezJobMonitor.initShutdownHook();

    // External sessions doesn't support async mode (getClient should be much cheaper than open,
    // and the async mode is anyway only used for CLI).
    if (isAsync) {
      LOG.info("Ignoring the async argument for an external session {}", getSessionId());
    }
    try {
      externalAppId = registry.getSession();
    } catch (TezException | IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }

    session.getClient(ApplicationId.fromString(externalAppId));
    LOG.info("Started an external session; client name {}, app ID {}", session.getClientName(), externalAppId);
    setTezClient(session);
  }

  @Override
  public void close(boolean keepDagFilesDir) throws Exception {
    // We never close external sessions that don't have errors.
    if (externalAppId != null) {
      registry.returnSession(externalAppId);
    }
    externalAppId = null;
    if (isDestroying) {
      super.close(keepDagFilesDir);
    }
  }

  @Override
  public TezSession reopen() throws Exception {
    isDestroying = true;
    // Reopen will actually close this session, and get a new external app.
    // It could instead somehow communicate to the external manager that the session is bad.
    return super.reopen();
  }

  @Override
  public void destroy() throws Exception {
    isDestroying = true;
    // This will actually close the session. We assume the external manager will restart it.
    // It could instead somehow communicate to the external manager that the session is bad.
    super.destroy();
  }

  @Override
  public boolean killQuery(String reason) throws HiveException {
    if (killQuery == null || wmContext == null) {
      return false;
    }
    String queryId = wmContext.getQueryId();
    if (queryId == null) {
      return false;
    }
    LOG.info("Killing the query {}: {}", queryId, reason);
    killQuery.killQuery(queryId, reason, conf, false);
    return true;
  }
}
