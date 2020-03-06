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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.tez.monitoring.TezJobMonitor;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.KillQuery;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezAppMasterStatus;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.serviceplugins.api.ServicePluginsDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TezExternalSessionState extends TezSessionState {
  private static final Logger LOG = LoggerFactory.getLogger(TezExternalSessionState.class.getName());
  private String externalAppId;
  private boolean isDestroying = false;
  private final ExternalSessionsRegistry registry;

  public TezExternalSessionState(
      DagUtils utils, HiveConf conf, ExternalSessionsRegistry registry) {
    super(utils, conf);
    this.registry = registry;
  }

  public TezExternalSessionState(String sessionId, HiveConf conf,
    ExternalSessionsRegistry registry) {
    super(sessionId, conf);
    this.registry = registry;
  }

  @Override
  public void ensureLocalResources(Configuration conf,
      String[] newFilesNotFromConf) throws IOException, LoginException,
      URISyntaxException, TezException {
    return; // A no-op for an external session.
  }

  @Override
  protected void openInternal(String[] additionalFilesNotFromConf,
      boolean isAsync, LogHelper console, HiveResources resources, boolean isPoolInit)
          throws IOException, LoginException, URISyntaxException, TezException {
    initQueueAndUser();

    boolean llapMode = isLlapMode();

    Map<String, String> amEnv = new HashMap<String, String>();
    MRHelpers.updateEnvBasedOnMRAMEnv(conf, amEnv);

    TezConfiguration tezConfig = createTezConfig();
    ServicePluginsDescriptor spd = createServicePluginDescriptor(llapMode, tezConfig);
    Credentials llapCredentials = createLlapCredentials(llapMode, tezConfig);

    final TezClient session = TezClient.newBuilder("HIVE-" + getSessionId(), tezConfig)
        .setIsSession(true)
        .setCredentials(llapCredentials).setServicePluginDescriptor(spd)
        .build();

    LOG.info("Opening new External Tez Session (id: " + getSessionId() + ")");
    TezJobMonitor.initShutdownHook();

    // External sessions doesn't support async mode (getClient should be much cheaper than open,
    // and the async mode is anyway only used for CLI).
    if (isAsync) {
      LOG.info("Ignoring the async argument for an external session {}", getSessionId());
    }
    final boolean perQuerySession = conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_USE_PER_QUERY_TEZ_EXTERNAL_SESSION);
    final String hiveQueryId = conf.getVar(HiveConf.ConfVars.HIVEQUERYID);
    try {
      if (perQuerySession) {
        LOG.info("Requesting tez session corresponding to queryId: {}", hiveQueryId);
        externalAppId = registry.getSession(hiveQueryId);
      } else {
        externalAppId = registry.getSession();
      }
    } catch (TezException | LoginException | IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }

    session.getClient(ApplicationId.fromString(externalAppId));
    LOG.info("Started an external session; client name {}, app ID {}, perQuerySession: {} hiveQueryId: {}",
        session.getClientName(), externalAppId, perQuerySession, hiveQueryId);
    setTezClient(session);

    // If we are picking up this external session for the first time, check whether the AM is
    // currently running anything. This could be the case if the last HiveServer2 instance died
    // unexpectedly without being able to kill its running queries, leaving the external AMs
    // still in running state.
    // In this situation, we cannot currently do anything with the already running DAG that the
    // external AM may be running - there is nothing tracking this DAG.
    // So just kill the DAG execution if possible.
    tryKillRunningDAGs(session);
  }

  @Override
  public void close(boolean keepDagFilesDir) throws Exception {
    // We never close external sessions that don't have errors.
    if (externalAppId != null) {
      LOG.info("Returning external session with appID: {}", externalAppId);
      // Make sure that if the session is returned to the pool, it doesn't live in the global.
      SessionState sessionState = SessionState.get();
      if (sessionState != null) {
        sessionState.setTezSession(null);
      }
      registry.returnSession(externalAppId);
    }
    externalAppId = null;
    if (isDestroying) {
      super.close(keepDagFilesDir);
    }
  }

  public TezSession reopen() throws Exception {
    isDestroying = true;
    LOG.info("Reopening external session with appId: {}", externalAppId);
    // Reopen will actually close this session, and get a new external app.
    // It could instead somehow communicate to the external manager that the session is bad.
    return super.reopen();
  }

  public void destroy() throws Exception {
    LOG.info("Destroying external session with appId: {}", externalAppId);
    isDestroying = true;
    // This will actually close the session. We assume the external manager will restart it.
    // It could instead somehow communicate to the external manager that the session is bad.
    super.destroy();
  }

  @Override
  public boolean killQuery(String reason) throws HiveException {
    if (killQuery == null || wmContext == null) return false;
    String queryId = wmContext.getQueryId();
    if (queryId == null) return false;
    LOG.info("Killing the query {}: {}", queryId, reason);
    killQuery.killQuery(queryId, reason, conf, false);
    return true;
  }

  @Override
  public String toString() {
    return super.toString() + ", externalAppId=" + externalAppId;
  }

  private void tryKillRunningDAGs(TezClient session) throws IOException, TezException {
    TezAppMasterStatus amStatus = session.getAppMasterStatus();
    if (amStatus == TezAppMasterStatus.RUNNING) {
      LOG.info("External session has an AM which appears to be already running a DAG: client name {}, app ID {}",
              session.getClientName(), externalAppId);
      List<DAGClient> dagClients;
      try {
        dagClients = session.getCurrentDAGClients();
      } catch (Exception err) {
        throw new IOException("Error getting DAGClients for app ID " + externalAppId, err);
      }

      for (DAGClient dagClient : dagClients) {
        LOG.info("External session: attempting to kill dagId {} on app ID {}",
                dagClient.getDagIdentifierString(), externalAppId);
        try {
          dagClient.tryKillDAG();
        } catch (Exception err) {
          throw new TezException("Error while trying to kill existing DAG " + dagClient.getDagIdentifierString() + " running on app ID " + externalAppId, err);
        } finally {
          dagClient.close();
        }
      }
    }
  }
}
