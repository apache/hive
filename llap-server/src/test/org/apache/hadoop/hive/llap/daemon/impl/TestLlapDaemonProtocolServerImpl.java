/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.daemon.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.configuration.LlapDaemonConfiguration;
import org.apache.hadoop.hive.llap.daemon.ContainerRunner;
import org.apache.hadoop.hive.llap.impl.LlapManagementProtocolClientImpl;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorMetrics;
import org.apache.hadoop.hive.llap.protocol.LlapManagementProtocolPB;
import org.apache.hadoop.hive.llap.protocol.LlapProtocolBlockingPB;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmissionStateProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.GetDaemonMetricsRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.GetDaemonMetricsResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SetCapacityRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SetCapacityResponseProto;
import org.apache.hadoop.hive.llap.impl.LlapProtocolClientImpl;
import org.junit.Test;

public class TestLlapDaemonProtocolServerImpl {


  @Test(timeout = 10000)
  public void testSimpleCall() throws ServiceException, IOException {
    LlapDaemonConfiguration daemonConf = new LlapDaemonConfiguration();
    int numHandlers = HiveConf.getIntVar(daemonConf, ConfVars.LLAP_DAEMON_RPC_NUM_HANDLERS);
    ContainerRunner containerRunnerMock = mock(ContainerRunner.class);
    LlapProtocolServerImpl server =
        new LlapProtocolServerImpl(null, numHandlers, containerRunnerMock,
           new AtomicReference<InetSocketAddress>(), new AtomicReference<InetSocketAddress>(),
           0, 0, 0, null, null);
    when(containerRunnerMock.submitWork(any(SubmitWorkRequestProto.class))).thenReturn(
        SubmitWorkResponseProto
            .newBuilder()
            .setSubmissionState(SubmissionStateProto.ACCEPTED)
            .build());
    try {
      server.init(new Configuration());
      server.start();
      InetSocketAddress serverAddr = server.getBindAddress();

      LlapProtocolBlockingPB client =
          new LlapProtocolClientImpl(new Configuration(), serverAddr.getHostName(),
              serverAddr.getPort(), null, null, null);
      SubmitWorkResponseProto responseProto = client.submitWork(null,
          SubmitWorkRequestProto.newBuilder()
              .setAmHost("amhost")
              .setAmPort(2000).build());
      assertEquals(responseProto.getSubmissionState().name(),
          SubmissionStateProto.ACCEPTED.name());

    } finally {
      server.stop();
    }
  }

  @Test(timeout = 10000)
  public void testGetDaemonMetrics() throws ServiceException, IOException {
    LlapDaemonConfiguration daemonConf = new LlapDaemonConfiguration();
    int numHandlers = HiveConf.getIntVar(daemonConf, ConfVars.LLAP_DAEMON_RPC_NUM_HANDLERS);
    LlapDaemonExecutorMetrics executorMetrics =
        LlapDaemonExecutorMetrics.create("LLAP", "SessionId", numHandlers, 1, new int[] {30, 60, 300}, 0, 0L, 0);
    LlapProtocolServerImpl server =
        new LlapProtocolServerImpl(null, numHandlers, null,
            new AtomicReference<InetSocketAddress>(), new AtomicReference<InetSocketAddress>(),
            0, 0, 0, null, executorMetrics);
    executorMetrics.addMetricsFallOffFailedTimeLost(10);
    executorMetrics.addMetricsFallOffKilledTimeLost(11);
    executorMetrics.addMetricsFallOffSuccessTimeLost(12);
    executorMetrics.addMetricsPreemptionTimeLost(13);
    executorMetrics.addMetricsPreemptionTimeToKill(14);
    executorMetrics.incrExecutorTotalExecutionFailed();
    executorMetrics.incrExecutorTotalKilled();
    executorMetrics.incrExecutorTotalRequestsHandled();
    executorMetrics.incrExecutorTotalSuccess();
    executorMetrics.incrTotalEvictedFromWaitQueue();
    executorMetrics.incrTotalRejectedRequests();
    executorMetrics.setCacheMemoryPerInstance(15);
    executorMetrics.setExecutorNumPreemptableRequests(16);
    executorMetrics.setExecutorNumQueuedRequests(17);
    executorMetrics.setJvmMaxMemory(18);
    executorMetrics.setMemoryPerInstance(19);
    executorMetrics.setNumExecutorsAvailable(20);
    executorMetrics.setWaitQueueSize(21);

    try {
      server.init(new Configuration());
      server.start();
      InetSocketAddress serverAddr = server.getManagementBindAddress();

      LlapManagementProtocolPB client =
          new LlapManagementProtocolClientImpl(new Configuration(), serverAddr.getHostName(),
              serverAddr.getPort(), null, null);
      GetDaemonMetricsResponseProto responseProto = client.getDaemonMetrics(null,
          GetDaemonMetricsRequestProto.newBuilder().build());
      Map<String, Long> result = new HashMap<>(responseProto.getMetricsCount());
      responseProto.getMetricsList().forEach(me -> result.put(me.getKey(), me.getValue()));

      assertTrue("Checking ExecutorFallOffFailedTimeLost", result.get("ExecutorFallOffFailedTimeLost") == 10);
      assertTrue("Checking ExecutorFallOffKilledTimeLost", result.get("ExecutorFallOffKilledTimeLost") == 11);
      assertTrue("Checking ExecutorFallOffSuccessTimeLost", result.get("ExecutorFallOffSuccessTimeLost") == 12);
      assertTrue("Checking ExecutorTotalPreemptionTimeLost", result.get("ExecutorTotalPreemptionTimeLost") == 13);
      assertTrue("Checking ExecutorTotalPreemptionTimeToKill", result.get("ExecutorTotalPreemptionTimeToKill") == 14);
      assertTrue("Checking ExecutorTotalFailed", result.get("ExecutorTotalFailed") == 1);
      assertTrue("Checking ExecutorTotalKilled", result.get("ExecutorTotalKilled") == 1);
      assertTrue("Checking ExecutorTotalRequestsHandled", result.get("ExecutorTotalRequestsHandled") == 1);
      assertTrue("Checking ExecutorTotalSuccess", result.get("ExecutorTotalSuccess") == 1);
      assertTrue("Checking ExecutorTotalEvictedFromWaitQueue", result.get("ExecutorTotalEvictedFromWaitQueue") == 1);
      assertTrue("Checking ExecutorTotalRejectedRequests", result.get("ExecutorTotalRejectedRequests") == 1);
      assertTrue("Checking ExecutorCacheMemoryPerInstance", result.get("ExecutorCacheMemoryPerInstance") == 15);
      assertTrue("Checking ExecutorNumPreemptableRequests", result.get("ExecutorNumPreemptableRequests") == 16);
      assertTrue("Checking ExecutorNumQueuedRequests", result.get("ExecutorNumQueuedRequests") == 17);
      assertTrue("Checking ExecutorJvmMaxMemory", result.get("ExecutorJvmMaxMemory") == 18);
      assertTrue("Checking ExecutorMemoryPerInstance", result.get("ExecutorMemoryPerInstance") == 19);
      assertTrue("Checking ExecutorNumExecutorsAvailable", result.get("ExecutorNumExecutorsAvailable") == 20);
      assertTrue("Checking ExecutorWaitQueueSize", result.get("ExecutorWaitQueueSize") == 21);

    } finally {
      server.stop();
    }
  }

  @Test(timeout = 10000)
  public void testSetCapacity() throws ServiceException, IOException {
    LlapDaemonConfiguration daemonConf = new LlapDaemonConfiguration();
    int numHandlers = HiveConf.getIntVar(daemonConf, ConfVars.LLAP_DAEMON_RPC_NUM_HANDLERS);
    ContainerRunner containerRunnerMock = mock(ContainerRunner.class);
    when(containerRunnerMock.setCapacity(any(SetCapacityRequestProto.class))).thenReturn(
        SetCapacityResponseProto
            .newBuilder()
            .build());
    LlapDaemonExecutorMetrics executorMetrics =
        LlapDaemonExecutorMetrics.create("LLAP", "SessionId", numHandlers, 1, new int[] {30, 60, 300}, 0, 0L, 0);
    LlapProtocolServerImpl server =
        new LlapProtocolServerImpl(null, numHandlers, containerRunnerMock,
            new AtomicReference<InetSocketAddress>(), new AtomicReference<InetSocketAddress>(),
            0, 0, 0, null, executorMetrics);

    try {
      server.init(new Configuration());
      server.start();
      InetSocketAddress serverAddr = server.getManagementBindAddress();

      LlapManagementProtocolPB client =
          new LlapManagementProtocolClientImpl(new Configuration(), serverAddr.getHostName(),
              serverAddr.getPort(), null, null);
      client.setCapacity(null,
          SetCapacityRequestProto.newBuilder().setExecutorNum(1).setQueueSize(1).build());
    } finally {
      server.stop();
    }
  }
}
