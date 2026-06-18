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
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.tez.monitoring.TezJobMonitor;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolBlockingPB;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC;
import org.apache.tez.dag.api.records.DAGProtos;
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
  private static volatile TezConfiguration defaultTezConfiguration;

  private String externalAppId;
  private volatile boolean isOpen = false;
  private volatile boolean isDestroying = false;
  private final ExternalSessionsRegistry registry;

  public TezExternalSessionState(String sessionId, HiveConf conf) {
    super(sessionId, conf);
    this.registry = ExternalSessionsRegistryFactory.getClient(conf);
    synchronized (DEFAULT_CONF_CREATE_LOCK) {
      if (defaultTezConfiguration == null) {
        defaultTezConfiguration = createDefaultTezConfig();
      }
    }
  }

  @Override
  public void ensureLocalResources(Configuration conf, String[] newFilesNotFromConf) {
    /*
     * No-op implementation.
     * External Tez sessions are not backed by a YARN application and therefore
     * do not manage or localize resources. As a result, there are no local
     * resources to ensure for this session type.
     */
  }

  @Override
  protected void openInternal(String[] additionalFilesNotFromConf,
                              boolean isAsync, LogHelper console, HiveResources resources)
      throws IOException, TezException {
    if (isOpen) {
      LOG.info("External Tez session {} is already open, skipping duplicate openInternal call", getSessionId());
      return;
    }

    initQueueAndUser();

    boolean llapMode = isLlapMode();

    TezConfiguration tezConfig = new TezConfiguration(defaultTezConfiguration);
    setupSessionAcls(tezConfig, conf);
    ServicePluginsDescriptor spd = createServicePluginDescriptor(llapMode, tezConfig);
    Credentials llapCredentials = createLlapCredentials(llapMode, tezConfig);

    final TezClient sessionTezClient = TezClient.newBuilder("HIVE-" + getSessionId(), tezConfig)
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

    sessionTezClient.getClient(ApplicationId.fromString(externalAppId));
    LOG.info("Started an external session; client name {}, app ID {}", sessionTezClient.getClientName(), externalAppId);
    setTezClient(sessionTezClient);
    isOpen = true;
  }

  @Override
  public void close(boolean keepDagFilesDir) throws Exception {
    // We never close external sessions that don't have errors.
    try {
      if (externalAppId != null) {
        LOG.debug("Returning external session with appID: {}", externalAppId);
        SessionState sessionState = SessionState.get();
        if (sessionState != null) {
          sessionState.setTezSession(null);
        }
        registry.returnSession(externalAppId);
      }
    } catch (Exception e) {
      LOG.warn("Caught exception while trying to return external session {}, moving on with session state closure",
          externalAppId, e);
    }

    externalAppId = null;
    isOpen = false;
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
  public boolean isOpen(){
    return isOpen;
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

  @Override
  public DAGClient submitDAG(DAG dag) throws TezException, IOException {
    if (!registry.isClaimed(externalAppId)) {
      throw new TezException("Cannot submit DAG as the Tez Session no-longer owns the AM: " + externalAppId);
    }
    try {
      return getTezClient().submitDAG(dag);
    } catch (TezException e) {
      if (e.getMessage() == null || !e.getMessage().contains("App master already running a DAG")) {
        throw e;
      }
      tryKillRunningDAGs(getTezClient());
      return getTezClient().submitDAG(dag);
    }
  }

  private void tryKillRunningDAGs(TezClient session) throws TezException {
    if (!registry.isClaimed(externalAppId)) {
      throw new TezException("Cannot kill running DAG as the Tez Session no-longer owns the AM: " + externalAppId);
    }
    LOG.info("External session has an AM which is already running a DAG on app ID {}", externalAppId);
    DAGClientAMProtocolBlockingPB proxy = session.sendAMHeartbeat(null);
    if (proxy == null) {
      throw new TezException("Error while trying to connect to AM for app ID " + externalAppId);
    }
    long killTimeoutMs = TimeUnit.SECONDS.toMillis(
        HiveConf.getIntVar(conf, ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_WAIT_MAX_ATTEMPTS));
    try {
      DAGClientAMProtocolRPC.GetAllDAGsResponseProto allDAGSResponse =
          proxy.getAllDAGs(null, DAGClientAMProtocolRPC.GetAllDAGsRequestProto.newBuilder().build());
      for (String dagId : allDAGSResponse.getDagIdList()) {
        LOG.info("External session: attempting to kill dagId {} on app ID {}", dagId, externalAppId);
        proxy.tryKillDAG(null, DAGClientAMProtocolRPC.TryKillDAGRequestProto.newBuilder().setDagId(dagId).build());
        waitForDagTerminal(proxy, dagId, killTimeoutMs);
      }
    } catch (Exception e) {
      throw new TezException("Error while trying to kill existing DAG running on app ID " + externalAppId, e);
    }
  }

  private void waitForDagTerminal(DAGClientAMProtocolBlockingPB proxy, String dagId, long timeoutMs)
      throws TezException, ServiceException {
    long startTimeMs = System.currentTimeMillis();
    long pollIntervalMs = conf.getTimeVar(ConfVars.TEZ_DAG_STATUS_CHECK_INTERVAL, TimeUnit.MILLISECONDS);
    while (System.currentTimeMillis() - startTimeMs < timeoutMs) {
      long remainingMs = timeoutMs - (System.currentTimeMillis() - startTimeMs);
      DAGClientAMProtocolRPC.GetDAGStatusResponseProto response = proxy.getDAGStatus(null,
          DAGClientAMProtocolRPC.GetDAGStatusRequestProto.newBuilder()
              .setDagId(dagId)
              .setTimeout(Math.min(pollIntervalMs, remainingMs))
              .build());
      if (response.hasDagStatus() && response.getDagStatus().hasState()
          && isTerminalDagState(response.getDagStatus().getState())) {
        LOG.info("External session: dagId {} on app ID {} reached terminal state {}", dagId, externalAppId,
            response.getDagStatus().getState());
        return;
      }
    }
    throw new TezException("Timed out after " + timeoutMs + " ms waiting for orphan DAG " + dagId
        + " on app ID " + externalAppId + " to reach terminal state after kill");
  }

  private static boolean isTerminalDagState(DAGProtos.DAGStatusStateProto state) {
    return switch (state) {
    case DAG_SUCCEEDED, DAG_KILLED, DAG_FAILED, DAG_ERROR -> true;
    default -> false;
    };
  }
}
