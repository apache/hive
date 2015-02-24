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

package org.apache.tez.dag.app.launcher;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.hive.llap.daemon.LlapDaemonConfiguration;
import org.apache.hadoop.hive.llap.daemon.LlapDaemonProtocolBlockingPB;
import org.apache.hadoop.hive.llap.daemon.impl.LlapDaemonProtocolClientImpl;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.RunContainerRequestProto;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.rm.NMCommunicatorEvent;
import org.apache.tez.dag.app.rm.NMCommunicatorLaunchRequestEvent;
import org.apache.tez.dag.app.rm.container.AMContainerEvent;
import org.apache.tez.dag.app.rm.container.AMContainerEventLaunchFailed;
import org.apache.tez.dag.app.rm.container.AMContainerEventLaunched;
import org.apache.tez.dag.app.rm.container.AMContainerEventType;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.events.ContainerLaunchedEvent;

public class DaemonContainerLauncher extends AbstractService implements ContainerLauncher {

  // TODO Support interruptability of tasks which haven't yet been launched.

  // TODO May need multiple connections per target machine, depending upon how synchronization is handled in the RPC layer

  static final Log LOG = LogFactory.getLog(DaemonContainerLauncher.class);

  private final AppContext context;
  private final ListeningExecutorService executor;
  private final String tokenIdentifier;
  private final TaskAttemptListener tal;
  private final Map<String, LlapDaemonProtocolBlockingPB> proxyMap;
  private final int servicePort;
  private final Clock clock;


  // Configuration passed in here to set up final parameters
  public DaemonContainerLauncher(AppContext appContext, Configuration conf,
                                 TaskAttemptListener tal) {
    super(DaemonContainerLauncher.class.getName());
    this.clock = appContext.getClock();
    // TODO Scale this based on numDaemons / threads per daemon
    int numThreads = conf.getInt(LlapDaemonConfiguration.LLAP_DAEMON_AM_COMMUNICATOR_NUM_THREADS,
        LlapDaemonConfiguration.LLAP_DAEMON_AM_COMMUNICATOR_NUM_THREADS_DEFAULT);
    this.servicePort = conf.getInt(LlapDaemonConfiguration.LLAP_DAEMON_RPC_PORT,
        LlapDaemonConfiguration.LLAP_DAEMON_RPC_PORT_DEFAULT);
    ExecutorService localExecutor = Executors.newFixedThreadPool(numThreads,
        new ThreadFactoryBuilder().setNameFormat("DaemonCommunicator #%2d").build());
    executor = MoreExecutors.listeningDecorator(localExecutor);
    this.context = appContext;
    this.tokenIdentifier = context.getApplicationID().toString();
    this.tal = tal;
    this.proxyMap = new HashMap<String, LlapDaemonProtocolBlockingPB>();
  }

  public void serviceStop() {
    executor.shutdownNow();
  }

  private synchronized LlapDaemonProtocolBlockingPB getProxy(String hostname) {
    LlapDaemonProtocolBlockingPB proxy = proxyMap.get(hostname);
    if (proxy == null) {
      proxy = new LlapDaemonProtocolClientImpl(getConfig(), hostname, servicePort);
      proxyMap.put(hostname, proxy);
    }
    return proxy;
  }

  @Override
  public void handle(NMCommunicatorEvent event) {
    switch (event.getType()) {
      case CONTAINER_LAUNCH_REQUEST:
        NMCommunicatorLaunchRequestEvent launchEvent = (NMCommunicatorLaunchRequestEvent) event;
        InetSocketAddress address = tal.getTaskCommunicator(launchEvent.getTaskCommId()).getAddress();
        ListenableFuture<Void> future = executor.submit(
            new SubmitCallable(getProxy(launchEvent.getNodeId().getHost()), launchEvent,
                tokenIdentifier, address.getHostName(), address.getPort()));
        Futures.addCallback(future, new SubmitCallback(launchEvent.getContainerId(),
            launchEvent.getContainer().getNodeId().getHost()));
        break;
      case CONTAINER_STOP_REQUEST:
        LOG.info("DEBUG: Ignoring STOP_REQUEST for event: " + event);
        // TODO should this be sending out a Container terminated message ? Noone tells AMContainer
        // that the container is actually done (normally received from RM)
        // TODO Sending this out for an unlaunched container is invalid
        context.getEventHandler().handle(new AMContainerEvent(event.getContainerId(),
            AMContainerEventType.C_NM_STOP_SENT));
        break;
    }
  }


  private static class SubmitCallable implements Callable<Void> {

    private final NMCommunicatorLaunchRequestEvent event;
    private final String tokenIdentifier;
    private final String amHost;
    private final int amPort;
    private final LlapDaemonProtocolBlockingPB daemonProxy;

    private SubmitCallable(LlapDaemonProtocolBlockingPB daemonProxy,
                           NMCommunicatorLaunchRequestEvent event, String tokenIdentifier,
                           String amHost, int amPort) {
      this.event = event;
      this.daemonProxy = daemonProxy;
      this.tokenIdentifier = tokenIdentifier;
      this.amHost = amHost;
      this.amPort = amPort;
    }


    @Override
    public Void call() throws Exception {
      RunContainerRequestProto.Builder requestBuilder = RunContainerRequestProto.newBuilder();
      // Need the taskAttemptListenerAddress
      requestBuilder.setAmHost(amHost).setAmPort(amPort);
      requestBuilder.setAppAttemptNumber(event.getContainer().getId().getApplicationAttemptId().getAttemptId());
      requestBuilder.setApplicationIdString(
          event.getContainer().getId().getApplicationAttemptId().getApplicationId().toString());
      requestBuilder.setTokenIdentifier(tokenIdentifier);
      requestBuilder.setContainerIdString(event.getContainer().getId().toString());
      requestBuilder.setCredentialsBinary(
          ByteString.copyFrom(event.getContainerLaunchContext().getTokens()));
      requestBuilder.setUser(System.getenv(ApplicationConstants.Environment.USER.name()));

      RunContainerRequestProto request = requestBuilder.build();
      daemonProxy.runContainer(null, request);
      return null;
    }
  }

  private class SubmitCallback implements FutureCallback<Void> {

    private final ContainerId containerId;
    private final String host;

    private SubmitCallback(ContainerId containerId, String host) {
      this.containerId = containerId;
      this.host = host;
    }

    @Override
    public void onSuccess(Void result) {
      LOG.info("Container: " + containerId + " launch succeeded on host: " + host);
      context.getEventHandler().handle(new AMContainerEventLaunched(containerId));
      ContainerLaunchedEvent lEvt = new ContainerLaunchedEvent(
          containerId, clock.getTime(), context.getApplicationAttemptId());
      context.getHistoryHandler().handle(new DAGHistoryEvent(
          null, lEvt));
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.error("Failed to launch container: " + containerId + " on host: " + host, t);
      sendContainerLaunchFailedMsg(containerId, t);

    }
  }

  @SuppressWarnings("unchecked")
  void sendContainerLaunchFailedMsg(ContainerId containerId, Throwable t) {
    context.getEventHandler().handle(new AMContainerEventLaunchFailed(containerId, t == null ? "" : t.getMessage()));
  }


}
