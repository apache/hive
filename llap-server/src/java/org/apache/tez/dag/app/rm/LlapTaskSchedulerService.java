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

package org.apache.tez.dag.app.rm;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.types.AddressTypes;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.hive.llap.daemon.LlapDaemonConfiguration;
import org.apache.hadoop.hive.llap.daemon.registry.impl.LlapRegistryService;
import org.apache.tez.dag.api.TaskAttemptEndReason;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.AppContext;


public class LlapTaskSchedulerService extends TaskSchedulerService {

  private static final Log LOG = LogFactory.getLog(LlapTaskSchedulerService.class);

  private final ExecutorService appCallbackExecutor;
  private final TaskSchedulerAppCallback appClientDelegate;
  private final AppContext appContext;
  private final List<String> serviceHosts;
  private final Set<String> serviceHostSet;
  private final ContainerFactory containerFactory;
  private final Random random = new Random();
  private final int containerPort;

  private final String clientHostname;
  private final int clientPort;
  private final String trackingUrl;
  private final AtomicBoolean isStopped = new AtomicBoolean(false);
  private final ConcurrentMap<Object, ContainerId> runningTasks =
      new ConcurrentHashMap<Object, ContainerId>();

  // Per daemon
  private final int memoryPerInstance;
  private final int coresPerInstance;
  private final int executorsPerInstance;

  // Per Executor Thread
  private final Resource resourcePerExecutor;

  private final LlapRegistryService registry = new LlapRegistryService();

  public LlapTaskSchedulerService(TaskSchedulerAppCallback appClient, AppContext appContext,
                                    String clientHostname, int clientPort, String trackingUrl,
                                    long customAppIdIdentifier,
                                    Configuration conf) {
    // Accepting configuration here to allow setting up fields as final
    super(LlapTaskSchedulerService.class.getName());
    this.appCallbackExecutor = createAppCallbackExecutorService();
    this.appClientDelegate = createAppCallbackDelegate(appClient);
    this.appContext = appContext;
    this.serviceHosts = new LinkedList<String>();
    this.serviceHostSet = new HashSet<>();
    this.containerFactory = new ContainerFactory(appContext, customAppIdIdentifier);
    this.memoryPerInstance = conf
        .getInt(LlapDaemonConfiguration.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB,
            LlapDaemonConfiguration.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB_DEFAULT);
    this.coresPerInstance = conf
        .getInt(LlapDaemonConfiguration.LLAP_DAEMON_VCPUS_PER_INSTANCE,
            LlapDaemonConfiguration.LLAP_DAEMON_VCPUS_PER_INSTANCE_DEFAULT);
    this.executorsPerInstance = conf.getInt(LlapDaemonConfiguration.LLAP_DAEMON_NUM_EXECUTORS,
        LlapDaemonConfiguration.LLAP_DAEMON_NUM_EXECUTORS_DEFAULT);
    this.clientHostname = clientHostname;
    this.clientPort = clientPort;
    this.trackingUrl = trackingUrl;

    int memoryPerExecutor = (int) (memoryPerInstance / (float) executorsPerInstance);
    int coresPerExecutor = (int) (coresPerInstance / (float) executorsPerInstance);
    this.resourcePerExecutor = Resource.newInstance(memoryPerExecutor, coresPerExecutor);

    String instanceId = conf.getTrimmed(LlapDaemonConfiguration.LLAP_DAEMON_SERVICE_HOSTS);

    if (instanceId == null || false == instanceId.startsWith("@")) {
      String[] hosts = conf.getTrimmedStrings(LlapDaemonConfiguration.LLAP_DAEMON_SERVICE_HOSTS);
      if (hosts == null || hosts.length == 0) {
        hosts = new String[] { "localhost" };
        serviceHosts.add("localhost");
        serviceHostSet.add("localhost");
      }
    }

    this.containerPort = conf.getInt(LlapDaemonConfiguration.LLAP_DAEMON_RPC_PORT,
        LlapDaemonConfiguration.LLAP_DAEMON_RPC_PORT_DEFAULT);

    if (serviceHosts.size() > 0) {
      LOG.info("Running with configuration: " +
          "memoryPerInstance=" + memoryPerInstance +
          ", vcoresPerInstance=" + coresPerInstance +
          ", executorsPerInstance=" + executorsPerInstance +
          ", resourcePerInstanceInferred=" + resourcePerExecutor +
          ", hosts=" + serviceHosts.toString() +
          ", rpcPort=" + containerPort);
    } else {
      LOG.info("Running with configuration: " +
          "memoryPerInstance=" + memoryPerInstance +
          ", vcoresPerInstance=" + coresPerInstance +
          ", executorsPerInstance=" + executorsPerInstance +
          ", resourcePerInstanceInferred=" + resourcePerExecutor +
          ", hosts=<pending>" +
          ", rpcPort=<pending>");
    }

  }

  @Override
  public void serviceInit(Configuration conf) {
    registry.init(conf);
  }


  @Override
  public void serviceStart() {
    registry.start();
    if (serviceHosts.size() > 0) {
      return;
    }
    LOG.info("Reading YARN registry for service records");
    try {
      Map<String, ServiceRecord> workers = registry.getWorkers();
      for (ServiceRecord srv : workers.values()) {
        Endpoint rpc = srv.getInternalEndpoint("llap");
        if (rpc != null) {
          LOG.info("Examining endpoint: " + rpc);
          final String host =
              RegistryTypeUtils.getAddressField(rpc.addresses.get(0),
                  AddressTypes.ADDRESS_HOSTNAME_FIELD);
          serviceHosts.add(host);
          serviceHostSet.add(host);
        } else {
          LOG.info("The SRV record was " + srv);
        }
      }
      LOG.info("Re-inited with configuration: " +
          "memoryPerInstance=" + memoryPerInstance +
          ", vcoresPerInstance=" + coresPerInstance +
          ", executorsPerInstance=" + executorsPerInstance +
          ", resourcePerInstanceInferred=" + resourcePerExecutor +
          ", hosts="+ serviceHosts.toString());
    } catch (IOException ioe) {
      throw new TezUncheckedException(ioe);
    }
  }

  @Override
  public void serviceStop() {
    if (!this.isStopped.getAndSet(true)) {
      appCallbackExecutor.shutdownNow();
    }
  }

  @Override
  public Resource getAvailableResources() {
    // TODO This needs information about all running executors, and the amount of memory etc available across the cluster.
    return Resource
        .newInstance(Ints.checkedCast(serviceHosts.size() * memoryPerInstance),
            serviceHosts.size() * coresPerInstance);
  }

  @Override
  public int getClusterNodeCount() {
    return serviceHosts.size();
  }

  @Override
  public void resetMatchLocalityForAllHeldContainers() {
  }

  @Override
  public Resource getTotalResources() {
    return Resource
        .newInstance(Ints.checkedCast(serviceHosts.size() * memoryPerInstance),
            serviceHosts.size() * coresPerInstance);
  }

  @Override
  public void blacklistNode(NodeId nodeId) {
    LOG.info("DEBUG: BlacklistNode not supported");
  }

  @Override
  public void unblacklistNode(NodeId nodeId) {
    LOG.info("DEBUG: unBlacklistNode not supported");
  }

  @Override
  public void allocateTask(Object task, Resource capability, String[] hosts, String[] racks,
                           Priority priority, Object containerSignature, Object clientCookie) {
    String host = selectHost(hosts);
    Container container = containerFactory.createContainer(resourcePerExecutor, priority, host, containerPort);
    runningTasks.put(task, container.getId());
    appClientDelegate.taskAllocated(task, clientCookie, container);
  }


  @Override
  public void allocateTask(Object task, Resource capability, ContainerId containerId,
                           Priority priority, Object containerSignature, Object clientCookie) {
    String host = selectHost(null);
    Container container = containerFactory.createContainer(resourcePerExecutor, priority, host, containerPort);
    runningTasks.put(task, container.getId());
    appClientDelegate.taskAllocated(task, clientCookie, container);
  }

  @Override
  public boolean deallocateTask(Object task, boolean taskSucceeded, TaskAttemptEndReason endReason) {
    ContainerId containerId = runningTasks.remove(task);
    if (containerId == null) {
      LOG.error("Could not determine ContainerId for task: " + task +
          " . Could have hit a race condition. Ignoring." +
          " The query may hang since this \"unknown\" container is now taking up a slot permanently");
      return false;
    }
    appClientDelegate.containerBeingReleased(containerId);
    return true;
  }

  @Override
  public Object deallocateContainer(ContainerId containerId) {
    LOG.info("DEBUG: Ignoring deallocateContainer for containerId: " + containerId);
    return null;
  }

  @Override
  public void setShouldUnregister() {

  }

  @Override
  public boolean hasUnregistered() {
    // Nothing to do. No registration involved.
    return true;
  }

  private ExecutorService createAppCallbackExecutorService() {
    return Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
        .setNameFormat("TaskSchedulerAppCaller #%d").setDaemon(true).build());
  }

  private TaskSchedulerAppCallback createAppCallbackDelegate(
      TaskSchedulerAppCallback realAppClient) {
    return new TaskSchedulerAppCallbackWrapper(realAppClient,
        appCallbackExecutor);
  }

  private String selectHost(String[] requestedHosts) {
    String host = null;
    if (requestedHosts != null && requestedHosts.length > 0) {
      Arrays.sort(requestedHosts);
      host = requestedHosts[0];
      if (serviceHostSet.contains(host)) {
        LOG.info("Selected host: " + host + " from requested hosts: " + Arrays.toString(requestedHosts));
      } else {
        LOG.info("Preferred host: " + host + " not present. Attempting to select another one");
        host = null;
        for (String h : requestedHosts) {
          if (serviceHostSet.contains(h)) {
            host = h;
            break;
          }
        }
        if (host == null) {
          LOG.info("Requested hosts: " + Arrays.toString(requestedHosts) + " not present. Randomizing the host");
        }
      }
    }
    if (host == null) {
      host = serviceHosts.get(random.nextInt(serviceHosts.size()));
      LOG.info("Selected random host: " + host + " since the request contained no host information");
    }
    return host;
  }

  static class ContainerFactory {
    final ApplicationAttemptId customAppAttemptId;
    AtomicInteger nextId;

    public ContainerFactory(AppContext appContext, long appIdLong) {
      this.nextId = new AtomicInteger(1);
      ApplicationId appId = ApplicationId
          .newInstance(appIdLong, appContext.getApplicationAttemptId().getApplicationId().getId());
      this.customAppAttemptId = ApplicationAttemptId
          .newInstance(appId, appContext.getApplicationAttemptId().getAttemptId());
    }

    public Container createContainer(Resource capability, Priority priority, String hostname, int port) {
      ContainerId containerId = ContainerId.newInstance(customAppAttemptId, nextId.getAndIncrement());
      NodeId nodeId = NodeId.newInstance(hostname, port);
      String nodeHttpAddress = "hostname:0";

      Container container = Container.newInstance(containerId,
          nodeId,
          nodeHttpAddress,
          capability,
          priority,
          null);

      return container;
    }
  }
}
