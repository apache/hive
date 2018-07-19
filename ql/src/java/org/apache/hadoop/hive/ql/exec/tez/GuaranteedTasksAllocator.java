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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.plugin.rpc.LlapPluginProtocolProtos.UpdateQueryRequestProto;
import org.apache.hadoop.hive.llap.plugin.rpc.LlapPluginProtocolProtos.UpdateQueryResponseProto;
import org.apache.hadoop.hive.ql.exec.tez.AmPluginNode.AmPluginInfo;
import org.apache.hadoop.hive.ql.exec.tez.LlapPluginEndpointClient.UpdateRequestContext;
import org.apache.hadoop.hive.ql.optimizer.physical.LlapClusterStateForCompile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implements query resource allocation using guaranteed tasks. */
public class GuaranteedTasksAllocator implements QueryAllocationManager {
  private static final Logger LOG = LoggerFactory.getLogger(
      GuaranteedTasksAllocator.class);
  private final static long CLUSTER_INFO_UPDATE_INTERVAL_MS = 120 * 1000L;

  private final Configuration conf;
  private final LlapClusterStateForCompile clusterState;
  private final Thread clusterStateUpdateThread;
  private final LlapPluginEndpointClient amCommunicator;
  private Runnable clusterChangedCallback;

  public GuaranteedTasksAllocator(
      Configuration conf, LlapPluginEndpointClient amCommunicator) {
    this.conf = conf;
    this.clusterState = new LlapClusterStateForCompile(conf, CLUSTER_INFO_UPDATE_INTERVAL_MS);
    this.amCommunicator = amCommunicator;
    this.clusterStateUpdateThread = new Thread(new Runnable() {
      private int lastExecutorCount = -1;
      @Override
      public void run() {
        while (true) {
          int executorCount = getExecutorCount(true); // Trigger an update if needed.
          
          if (executorCount != lastExecutorCount && lastExecutorCount >= 0) {
            clusterChangedCallback.run();
          }
          lastExecutorCount = executorCount;
          try {
            Thread.sleep(CLUSTER_INFO_UPDATE_INTERVAL_MS / 2);
          } catch (InterruptedException e) {
            LOG.info("Cluster state update thread was interrupted");
            return;
          }
        }
      }
    }, "Cluster State Updater");
    clusterStateUpdateThread.setDaemon(true);
  }

  @Override
  public void start() {
    // Try to get cluster information once, to avoid immediate cluster-update event in WM.
    clusterState.initClusterInfo();
    clusterStateUpdateThread.start();
  }

  @Override
  public void stop() {
    clusterStateUpdateThread.interrupt(); // Don't wait for the thread.
  }

  @VisibleForTesting
  protected int getExecutorCount(boolean allowUpdate) {
    if (allowUpdate && !clusterState.initClusterInfo()) {
      LOG.warn("Failed to get LLAP cluster information for "
          + HiveConf.getTrimmedVar(this.conf, ConfVars.LLAP_DAEMON_SERVICE_HOSTS)
          + "; we may rely on outdated cluster status");
    }
    if (!clusterState.hasClusterInfo()) {
      LOG.error("No cluster information available to allocate; no guaranteed tasks will be used");
      return 0;
    }
    int unknownNodes = clusterState.getNodeCountWithUnknownExecutors();
    if (unknownNodes > 0) {
      LOG.error("There are " + unknownNodes + " nodes with unknown executor count; only " +
          clusterState.getKnownExecutorCount() + " guaranteed tasks will be allocated");
    }
    return clusterState.getKnownExecutorCount();
  }

  @Override
  public int translateAllocationToCpus(double allocation) {
    // Do not make a remote call under any circumstances - this is supposed to be async.
    return (int)Math.round(getExecutorCount(false) * allocation);
  }

  @Override
  public int updateSessionsAsync(Double totalMaxAlloc, List<WmTezSession> sessionsToUpdate) {
    // Do not make a remote call under any circumstances - this is supposed to be async.
    int totalCount = getExecutorCount(false);
    int totalToDistribute = -1;
    if (totalMaxAlloc != null) {
      totalToDistribute = (int)Math.round(totalCount * totalMaxAlloc);
    }
    int totalDistributed = 0;
    double lastDelta = 0;
    for (int i = 0; i < sessionsToUpdate.size(); ++i) {
      WmTezSession session = sessionsToUpdate.get(i);
      int intAlloc = -1;
      if (i + 1 == sessionsToUpdate.size() && totalToDistribute >= 0) {
        intAlloc = totalToDistribute;
        // We rely on the caller to supply a reasonable total; we could log a warning
        // if this doesn't match the allocation of the last session beyond some threshold.
      } else {
        // This ensures we don't create skew, e.g. with 8 ducks and 5 queries with simple rounding
        // we'd produce 2-2-2-2-0 as we round 1.6; whereas adding the last delta to the next query
        // we'd round 1.6-1.2-1.8-1.4-2.0 and thus give out 2-1-2-1-2, as intended.
        // Note that fractions don't have to all be the same like in this example.
        assert session.hasClusterFraction();
        double fraction = session.getClusterFraction();
        double allocation = fraction * totalCount + lastDelta;
        double roundedAlloc = Math.round(allocation);
        lastDelta = allocation - roundedAlloc;
        if (roundedAlloc < 0) {
          roundedAlloc = 0; // Can this happen? Delta cannot exceed 0.5.
        }
        intAlloc = (int)roundedAlloc;
      }
      // Make sure we don't give out more than allowed due to double/rounding artifacts.
      if (totalToDistribute >= 0) {
        if (intAlloc > totalToDistribute) {
          intAlloc = totalToDistribute;
        }
        totalToDistribute -= intAlloc;
      }
      // This will only send update if it's necessary.
      totalDistributed += intAlloc;
      updateSessionAsync(session, intAlloc);
    }
    return totalDistributed;
  }

  @Override
  public void updateSessionAsync(WmTezSession session) {
    updateSessionAsync(session, null); // Resend existing value if necessary.
  }

  private void updateSessionAsync(final WmTezSession session, final Integer intAlloc) {
    Integer valueToSend = session.setSendingGuaranteed(intAlloc);
    if (valueToSend == null) return;
    // Note: this assumes that the pattern where the same session object is reset with a different
    //       Tez client is not used. It was used a lot in the past but appears to be gone from most
    //       HS2 session pool paths, and this patch removes the last one (reopen).
    UpdateQueryRequestProto request = UpdateQueryRequestProto
        .newBuilder().setGuaranteedTaskCount(valueToSend.intValue()).build();
    LOG.info("Updating {} with {} guaranteed tasks", session.getSessionId(), intAlloc);
    amCommunicator.sendUpdateQuery(request, (AmPluginNode)session, new UpdateCallback(session));
  }

  private final class UpdateCallback implements UpdateRequestContext {
    private final WmTezSession session;
    private int endpointVersion = -1;

    private UpdateCallback(WmTezSession session) {
      this.session = session;
    }

    @Override
    public void setResponse(UpdateQueryResponseProto response) {
      int nextUpdate = session.setSentGuaranteed();
      if (nextUpdate >= 0) {
        LOG.info("Sending a new update " + nextUpdate + " to " + session + " in the response");
        updateSessionAsync(session, nextUpdate);
      }
    }

    @Override
    public void indicateError(Throwable t) {
      LOG.error("Failed to update guaranteed tasks count for the session " + session, t);
      boolean isOkToFail = session.setFailedToSendGuaranteed();
      if (isOkToFail) return;
      // RPC already handles retries, so we will just try to kill the session here.
      // This will cause the current query to fail. We could instead keep retrying.
      try {
        session.handleUpdateError(endpointVersion);
      } catch (Exception e) {
        LOG.error("Failed to kill the session " + session);
      }
    }

    @Override
    public void setNodeInfo(AmPluginInfo info, int version) {
      endpointVersion = version;
    }
  }

  @Override
  public void setClusterChangedCallback(Runnable clusterChangedCallback) {
    this.clusterChangedCallback = clusterChangedCallback;
  }
}
