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
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
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
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.hive.llap.daemon.LlapDaemonConfiguration;
import org.apache.hadoop.hive.llap.daemon.registry.impl.LlapRegistryService;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.dag.api.TaskAttemptEndReason;
import org.apache.tez.dag.app.AppContext;


public class LlapTaskSchedulerService extends TaskSchedulerService {

  private static final Log LOG = LogFactory.getLog(LlapTaskSchedulerService.class);

  private static final float BACKOFF_FACTOR = 1.2f;

  private final ExecutorService appCallbackExecutor;
  private final TaskSchedulerAppCallback appClientDelegate;

  // Set of active hosts
  @VisibleForTesting
  final LinkedHashMap<String, NodeInfo> activeHosts = new LinkedHashMap<>();
  // Populated each time activeHosts is modified
  @VisibleForTesting
  String []activeHostList;

  // Set of all hosts in the system.
  @VisibleForTesting
  final ConcurrentMap<String, NodeInfo> allHosts = new ConcurrentHashMap<>();

  // Tracks currently allocated containers.
  private final Map<ContainerId, String> containerToHostMap = new HashMap<>();

  // Tracks tasks which could not be allocated immediately.
  @VisibleForTesting
  final TreeMap<Priority, List<TaskInfo>> pendingTasks =
      new TreeMap<>(new Comparator<Priority>() {
        @Override
        public int compare(Priority o1, Priority o2) {
          return o1.getPriority() - o2.getPriority();
        }
      });

  // Tracks running and queued tasks. Cleared after a task completes.
  private final ConcurrentMap<Object, TaskInfo> knownTasks =
      new ConcurrentHashMap<>();

  @VisibleForTesting
  final DelayQueue<NodeInfo> disabledNodes = new DelayQueue<>();

  private final ContainerFactory containerFactory;
  private final Random random = new Random();
  private final int containerPort;
  private final Clock clock;
  private final ListeningExecutorService executor;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
  private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();


  // TODO Track resources used by this application on specific hosts, and make scheduling decisions accordingly.
  // Ideally implement in a way where updates from ZK, if they do come, can just be plugged in.
  // A heap based on available capacity - which is updated each time stats are updated,
  // or anytime assignment numbers are changed. Especially for random allocations (no host request).
  // For non-random allocations - Walk through all pending tasks to get local assignments, then start assigning them to non local hosts.
  // Also setup a max over-subscribe limit as part of this.

  private final AtomicBoolean isStopped = new AtomicBoolean(false);

  private final long nodeReEnableTimeout;


  // Per daemon
  private final int memoryPerInstance;
  private final int coresPerInstance;
  private final int executorsPerInstance;

  // Per Executor Thread
  private final Resource resourcePerExecutor;

  private final boolean initFromRegistry;
  private final LlapRegistryService registry = new LlapRegistryService();
  private final PendingTaskSchedulerCallable pendingTaskSchedulerCallable = new PendingTaskSchedulerCallable();
  private ListenableFuture<Void> pendingTaskSchedulerFuture;

  @VisibleForTesting
  private final AtomicInteger dagCounter = new AtomicInteger(1);
  // Statistics to track allocations
  // All of stats variables are visible for testing.
  @VisibleForTesting
  StatsPerDag dagStats = new StatsPerDag();





  public LlapTaskSchedulerService(TaskSchedulerAppCallback appClient, AppContext appContext,
                                    String clientHostname, int clientPort, String trackingUrl,
                                    long customAppIdIdentifier,
                                    Configuration conf) {
    // Accepting configuration here to allow setting up fields as final

    super(LlapTaskSchedulerService.class.getName());
    this.appCallbackExecutor = createAppCallbackExecutorService();
    this.appClientDelegate = createAppCallbackDelegate(appClient);
    this.clock = appContext.getClock();
    this.containerFactory = new ContainerFactory(appContext, customAppIdIdentifier);
    this.memoryPerInstance = conf
        .getInt(LlapDaemonConfiguration.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB,
            LlapDaemonConfiguration.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB_DEFAULT);
    this.coresPerInstance = conf
        .getInt(LlapDaemonConfiguration.LLAP_DAEMON_VCPUS_PER_INSTANCE,
            LlapDaemonConfiguration.LLAP_DAEMON_VCPUS_PER_INSTANCE_DEFAULT);
    this.executorsPerInstance = conf.getInt(LlapDaemonConfiguration.LLAP_DAEMON_NUM_EXECUTORS,
        LlapDaemonConfiguration.LLAP_DAEMON_NUM_EXECUTORS_DEFAULT);
    this.nodeReEnableTimeout = conf.getLong(
        LlapDaemonConfiguration.LLAP_DAEMON_TASK_SCHEDULER_NODE_REENABLE_TIMEOUT_MILLIS,
        LlapDaemonConfiguration.LLAP_DAEMON_TASK_SCHEDULER_NODE_REENABLE_TIMEOUT_MILLIS_DEFAULT);

    int memoryPerExecutor = (int) (memoryPerInstance / (float) executorsPerInstance);
    int coresPerExecutor = (int) (coresPerInstance / (float) executorsPerInstance);
    this.resourcePerExecutor = Resource.newInstance(memoryPerExecutor, coresPerExecutor);

    String instanceId = conf.getTrimmed(LlapDaemonConfiguration.LLAP_DAEMON_SERVICE_HOSTS);

    Preconditions.checkNotNull(instanceId,
        LlapDaemonConfiguration.LLAP_DAEMON_SERVICE_HOSTS + " must be defined");

    if (!instanceId.startsWith("@")) { // Manual setup. Not via the service registry
      initFromRegistry = false;
      String[] hosts = conf.getTrimmedStrings(LlapDaemonConfiguration.LLAP_DAEMON_SERVICE_HOSTS);
      Preconditions.checkState(hosts != null && hosts.length != 0,
          LlapDaemonConfiguration.LLAP_DAEMON_SERVICE_HOSTS + "must be defined");
      for (String host : hosts) {
        NodeInfo nodeInfo = new NodeInfo(host, BACKOFF_FACTOR, clock);
        activeHosts.put(host, nodeInfo);
        allHosts.put(host, nodeInfo);
      }
      activeHostList = activeHosts.keySet().toArray(new String[activeHosts.size()]);
    } else {
      initFromRegistry = true;
    }

    this.containerPort = conf.getInt(LlapDaemonConfiguration.LLAP_DAEMON_RPC_PORT,
        LlapDaemonConfiguration.LLAP_DAEMON_RPC_PORT_DEFAULT);
    ExecutorService executorService = Executors.newFixedThreadPool(1,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("LlapScheduler").build());
    executor = MoreExecutors.listeningDecorator(executorService);

    if (activeHosts.size() > 0) {
      LOG.info("Running with configuration: " +
          "memoryPerInstance=" + memoryPerInstance +
          ", vCoresPerInstance=" + coresPerInstance +
          ", executorsPerInstance=" + executorsPerInstance +
          ", resourcePerInstanceInferred=" + resourcePerExecutor +
          ", hosts=" + allHosts.keySet() +
          ", rpcPort=" + containerPort +
          ", nodeReEnableTimeout=" + nodeReEnableTimeout +
          ", nodeReEnableBackOffFactor=" + BACKOFF_FACTOR);
    } else {
      LOG.info("Running with configuration: " +
          "memoryPerInstance=" + memoryPerInstance +
          ", vCoresPerInstance=" + coresPerInstance +
          ", executorsPerInstance=" + executorsPerInstance +
          ", resourcePerInstanceInferred=" + resourcePerExecutor +
          ", hosts=<pending>" +
          ", rpcPort=<pending>" +
          ", nodeReEnableTimeout=" + nodeReEnableTimeout +
          ", nodeReEnableBackOffFactor=" + BACKOFF_FACTOR);
    }

  }

  @Override
  public void serviceInit(Configuration conf) {
    if (initFromRegistry) {
      registry.init(conf);
    }
  }


  @Override
  public void serviceStart() throws IOException {

    writeLock.lock();
    try {
      pendingTaskSchedulerFuture = executor.submit(pendingTaskSchedulerCallable);
      if (initFromRegistry) {
        registry.start();
        if (activeHosts.size() > 0) {
          return;
        }
        LOG.info("Reading YARN registry for service records");

        Map<String, ServiceRecord> workers = registry.getWorkers();
        for (ServiceRecord srv : workers.values()) {
          Endpoint rpc = srv.getInternalEndpoint("llap");
          if (rpc != null) {
            LOG.info("Examining endpoint: " + rpc);
            final String host =
                RegistryTypeUtils.getAddressField(rpc.addresses.get(0),
                    AddressTypes.ADDRESS_HOSTNAME_FIELD);
            NodeInfo nodeInfo = new NodeInfo(host, BACKOFF_FACTOR, clock);
            activeHosts.put(host, nodeInfo);
            allHosts.put(host, nodeInfo);
          } else {

            LOG.info("The SRV record was " + srv);
          }
        }
        activeHostList = activeHosts.keySet().toArray(new String[activeHosts.size()]);



        LOG.info("Re-inited with configuration: " +
            "memoryPerInstance=" + memoryPerInstance +
            ", vCoresPerInstance=" + coresPerInstance +
            ", executorsPerInstance=" + executorsPerInstance +
            ", resourcePerInstanceInferred=" + resourcePerExecutor +
            ", hosts=" + allHosts.keySet());

      }
    } finally {
      writeLock.unlock();
    }

  }

  @Override
  public void serviceStop() {
    writeLock.lock();
    try {
      if (!this.isStopped.getAndSet(true)) {
        pendingTaskSchedulerCallable.shutdown();
        if (pendingTaskSchedulerFuture != null) {
          pendingTaskSchedulerFuture.cancel(true);
        }
        executor.shutdownNow();
        if (initFromRegistry) {
          registry.stop();
        }
        appCallbackExecutor.shutdownNow();
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public Resource getAvailableResources() {
    // TODO This needs information about all running executors, and the amount of memory etc available across the cluster.
    // No lock required until this moves to using something other than allHosts
    return Resource
        .newInstance(Ints.checkedCast(allHosts.size() * memoryPerInstance),
            allHosts.size() * coresPerInstance);
  }

  @Override
  public int getClusterNodeCount() {
    // No lock required until this moves to using something other than allHosts
    return allHosts.size();
  }

  @Override
  public void resetMatchLocalityForAllHeldContainers() {
    // This is effectively DAG completed, and can be used to reset statistics being tracked.
    LOG.info("DAG: " + dagCounter.get() + " completed. Scheduling stats: " + dagStats);
    dagCounter.incrementAndGet();
    dagStats = new StatsPerDag();
  }

  @Override
  public Resource getTotalResources() {
    // No lock required until this moves to using something other than allHosts
    return Resource
        .newInstance(Ints.checkedCast(allHosts.size() * memoryPerInstance),
            allHosts.size() * coresPerInstance);
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
    TaskInfo taskInfo = new TaskInfo(task, clientCookie, priority, capability, hosts, racks, clock.getTime());
    writeLock.lock();
    try {
      dagStats.registerTaskRequest(hosts, racks);
    } finally {
      writeLock.unlock();
    }
    boolean scheduled = scheduleTask(taskInfo);
    if (!scheduled) {
      addPendingTask(taskInfo);
    }
  }


  @Override
  public void allocateTask(Object task, Resource capability, ContainerId containerId,
                           Priority priority, Object containerSignature, Object clientCookie) {
    // Container affinity can be implemented as Host affinity for LLAP. Not required until
    // 1:1 edges are used in Hive.
    TaskInfo taskInfo = new TaskInfo(task, clientCookie, priority, capability, null, null, clock.getTime());
    writeLock.lock();
    try {
      dagStats.registerTaskRequest(null, null);
    } finally {
      writeLock.unlock();
    }
    boolean scheduled = scheduleTask(taskInfo);
    if (!scheduled) {
      addPendingTask(taskInfo);
    }
  }

  // This may be invoked before a container is ever assigned to a task. allocateTask... app decides
  // the task is no longer required, and asks for a de-allocation.
  @Override
  public boolean deallocateTask(Object task, boolean taskSucceeded, TaskAttemptEndReason endReason) {
    writeLock.lock(); // Updating several local structures
    TaskInfo taskInfo;
    try {
      taskInfo = knownTasks.remove(task);
      if (taskInfo == null) {
        LOG.error("Could not determine ContainerId for task: " + task +
            " . Could have hit a race condition. Ignoring." +
            " The query may hang since this \"unknown\" container is now taking up a slot permanently");
        return false;
      }
      if (taskInfo.containerId == null) {
        if (taskInfo.assigned) {
          LOG.error(
              "Task: " + task + " assigned, but could not find the corresponding containerId." +
                  " The query may hang since this \"unknown\" container is now taking up a slot permanently");
        } else {
          LOG.info("Ignoring deallocate request for task " + task +
              " which hasn't been assigned to a container");
          removePendingTask(taskInfo);
        }
        return false;
      }
      String hostForContainer = containerToHostMap.remove(taskInfo.containerId);
      assert hostForContainer != null;
      String assignedHost = taskInfo.assignedHost;
      assert assignedHost != null;

      if (taskSucceeded) {
        // The node may have been blacklisted at this point - which means it may not be in the activeNodeList.
        NodeInfo nodeInfo = allHosts.get(assignedHost);
        assert nodeInfo != null;
        nodeInfo.registerTaskSuccess();
        // TODO Consider un-blacklisting the node since at least 1 slot should have become available on the node.
      } else if (!taskSucceeded && endReason != null && EnumSet
          .of(TaskAttemptEndReason.SERVICE_BUSY, TaskAttemptEndReason.COMMUNICATION_ERROR)
          .contains(endReason)) {
        if (endReason == TaskAttemptEndReason.COMMUNICATION_ERROR) {
          dagStats.registerCommFailure(taskInfo.assignedHost);
        } else if (endReason == TaskAttemptEndReason.SERVICE_BUSY) {
          dagStats.registerTaskRejected(taskInfo.assignedHost);
        }
        disableNode(assignedHost);
      }
    } finally {
      writeLock.unlock();
    }
    appClientDelegate.containerBeingReleased(taskInfo.containerId);
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

  @VisibleForTesting
  TaskSchedulerAppCallback createAppCallbackDelegate(
      TaskSchedulerAppCallback realAppClient) {
    return new TaskSchedulerAppCallbackWrapper(realAppClient,
        appCallbackExecutor);
  }

  /**
   * @param requestedHosts the list of preferred hosts. null implies any host
   * @return
   */
  private String selectHost(String[] requestedHosts) {
    // TODO Change this to work off of what we think is remaining capacity for a host

    readLock.lock(); // Read-lock. Not updating any stats at the moment.
    try {
      // Check if any hosts are active. If there's any active host, an allocation will happen.
      if (activeHosts.size() == 0) {
        return null;
      }

      String host = null;
      if (requestedHosts != null && requestedHosts.length > 0) {
        // Pick the first host always. Weak attempt at cache affinity.
        Arrays.sort(requestedHosts);
        host = requestedHosts[0];
        if (activeHosts.get(host) != null) {
          LOG.info("Selected host: " + host + " from requested hosts: " +
              Arrays.toString(requestedHosts));
        } else {
          LOG.info("Preferred host: " + host + " not present. Attempting to select another one");
          host = null;
          for (String h : requestedHosts) {
            if (activeHosts.get(h) != null) {
              host = h;
              break;
            }
          }
          if (host == null) {
            host = activeHostList[random.nextInt(activeHostList.length)];
            LOG.info("Requested hosts: " + Arrays.toString(requestedHosts) +
                " not present. Randomizing the host");
          }
        }
      } else {
        host = activeHostList[random.nextInt(activeHostList.length)];
        LOG.info("Selected random host: " + host + " since the request contained no host information");
      }
      return host;
    } finally {
      readLock.unlock();
    }
  }


  private void reenableDisabledNode(NodeInfo nodeInfo) {
    writeLock.lock();
    try {
      nodeInfo.enableNode();
      activeHosts.put(nodeInfo.hostname, nodeInfo);
      activeHostList = activeHosts.keySet().toArray(new String[activeHosts.size()]);
    } finally {
      writeLock.unlock();
    }
  }

  private void disableNode(String hostname) {
    writeLock.lock();
    try {
      NodeInfo nodeInfo = activeHosts.remove(hostname);
      if (nodeInfo == null) {
        LOG.debug("Node: " + hostname + " already disabled, or invalid. Not doing anything.");
      } else {
        nodeInfo.disableNode(nodeReEnableTimeout);
        disabledNodes.add(nodeInfo);
      }
      activeHostList = activeHosts.keySet().toArray(new String[activeHosts.size()]);
    } finally {
      writeLock.unlock();
    }
  }


  private void addPendingTask(TaskInfo taskInfo) {
    writeLock.lock();
    try {
      dagStats.registerDelayedAllocation();
      List<TaskInfo> tasksAtPriority = pendingTasks.get(taskInfo.priority);
      if (tasksAtPriority == null) {
        tasksAtPriority = new LinkedList<>();
        pendingTasks.put(taskInfo.priority, tasksAtPriority);
      }
      tasksAtPriority.add(taskInfo);
      knownTasks.putIfAbsent(taskInfo.task, taskInfo);
    } finally {
      writeLock.unlock();
    }
  }

  private void removePendingTask(TaskInfo taskInfo) {
    writeLock.lock();
    try {
      Priority priority = taskInfo.priority;
      List<TaskInfo> taskInfoList = pendingTasks.get(priority);
      if (taskInfoList == null || taskInfoList.isEmpty() || !taskInfoList.remove(taskInfo)) {
        LOG.warn(
            "Could not find task: " + taskInfo.task + " in pending list, at priority: " + priority);
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void schedulePendingTasks() {
    writeLock.lock();
    try {
      Iterator<Entry<Priority, List<TaskInfo>>> pendingIterator =  pendingTasks.entrySet().iterator();
      while (pendingIterator.hasNext()) {
        Entry<Priority, List<TaskInfo>> entry = pendingIterator.next();
        List<TaskInfo> taskListAtPriority = entry.getValue();
        Iterator<TaskInfo> taskIter = taskListAtPriority.iterator();
        boolean scheduledAllAtPriority = true;
        while (taskIter.hasNext()) {
          TaskInfo taskInfo = taskIter.next();
          boolean scheduled = scheduleTask(taskInfo);
          if (scheduled) {
            taskIter.remove();
          } else {
            scheduledAllAtPriority = false;
            // Don't try assigning tasks at the next priority.
            break;
          }
        }
        if (taskListAtPriority.isEmpty()) {
          // Remove the entry, if there's nothing left at the specific priority level
          pendingIterator.remove();
        }
        if (!scheduledAllAtPriority) {
          // Don't attempt scheduling for additional priorities
          break;
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  private boolean scheduleTask(TaskInfo taskInfo) {
    String host = selectHost(taskInfo.requestedHosts);
    if (host == null) {
      return false;
    } else {
      Container container =
          containerFactory.createContainer(resourcePerExecutor, taskInfo.priority, host, containerPort);
      writeLock.lock(); // While updating local structures
      try {
        dagStats.registerTaskAllocated(taskInfo.requestedHosts, taskInfo.requestedRacks, host);
        taskInfo.setAssignmentInfo(host, container.getId());
        knownTasks.putIfAbsent(taskInfo.task, taskInfo);
        containerToHostMap.put(container.getId(), host);
      } finally {
        writeLock.unlock();
      }

      appClientDelegate.taskAllocated(taskInfo.task, taskInfo.clientCookie, container);
      return true;
    }
  }

  private class PendingTaskSchedulerCallable implements Callable<Void> {

    private AtomicBoolean isShutdown = new AtomicBoolean(false);

    @Override
    public Void call() {

      while (!isShutdown.get() && !Thread.currentThread().isInterrupted()) {
        try {
          while (true) {
            NodeInfo nodeInfo = disabledNodes.take();
            // A node became available. Enable the node and try scheduling.
            reenableDisabledNode(nodeInfo);
            schedulePendingTasks();
          }
        } catch (InterruptedException e) {
          if (isShutdown.get()) {
            LOG.info("Disabled node wait interrupted after shutdown. Stopping the disabled node poll");
            break;
          } else {
            LOG.warn("Interrupted while waiting for disabled nodes.");
            throw new RuntimeException("Interrupted while waiting for disabled nodes", e);
          }
        }
      }
      return null;
    }

    // Call this first, then send in an interrupt to the thread.
    public void shutdown() {
      isShutdown.set(true);
    }
  }

  @VisibleForTesting
  static class NodeInfo implements Delayed {
    private final float constBackOffFactor;
    final String hostname;
    private final Clock clock;

    long expireTimeMillis = -1;
    private long numSuccessfulTasks = 0;
    private long numSuccessfulTasksAtLastBlacklist = -1;
    float cumulativeBackoffFactor = 1.0f;

    NodeInfo(String hostname, float backoffFactor, Clock clock) {
      this.hostname = hostname;
      constBackOffFactor = backoffFactor;
      this.clock = clock;
    }

    void enableNode() {
      expireTimeMillis = -1;
    }

    void disableNode(long duration) {
      long currentTime = clock.getTime();
      if (numSuccessfulTasksAtLastBlacklist == numSuccessfulTasks) {
        // Blacklisted again, without any progress. Will never kick in for the first run.
        cumulativeBackoffFactor = cumulativeBackoffFactor * constBackOffFactor;
      } else {
        // Was able to execute something before the last blacklist. Reset the exponent.
        cumulativeBackoffFactor = 1.0f;
      }
      expireTimeMillis = currentTime + (long) (duration * cumulativeBackoffFactor);
      numSuccessfulTasksAtLastBlacklist = numSuccessfulTasks;

    }

    void registerTaskSuccess() {
      numSuccessfulTasks++;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return expireTimeMillis - clock.getTime();
    }

    @Override
    public int compareTo(Delayed o) {
      NodeInfo other = (NodeInfo) o;
      if (other.expireTimeMillis > this.expireTimeMillis) {
        return 1;
      } else if (other.expireTimeMillis < this.expireTimeMillis) {
        return -1;
      } else {
        return 0;
      }
    }

    @Override
    public String toString() {
      return "NodeInfo{" +
          "constBackOffFactor=" + constBackOffFactor +
          ", hostname='" + hostname + '\'' +
          ", expireTimeMillis=" + expireTimeMillis +
          ", numSuccessfulTasks=" + numSuccessfulTasks +
          ", numSuccessfulTasksAtLastBlacklist=" + numSuccessfulTasksAtLastBlacklist +
          ", cumulativeBackoffFactor=" + cumulativeBackoffFactor +
          '}';
    }
  }


  @VisibleForTesting
  static class StatsPerDag {
    int numRequestedAllocations = 0;
    int numRequestsWithLocation = 0;
    int numRequestsWithoutLocation = 0;
    int numLocalAllocations = 0;
    int numNonLocalAllocations = 0;
    int numAllocationsNoLocalityRequest = 0;
    int numRejectedTasks = 0;
    int numCommFailures = 0;
    int numDelayedAllocations = 0;
    Map<String, AtomicInteger> localityBasedNumAllocationsPerHost = new HashMap<>();
    Map<String, AtomicInteger> numAllocationsPerHost = new HashMap<>();

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("NumRequestedAllocations=").append(numRequestedAllocations).append(", ");
      sb.append("NumRequestsWithlocation=").append(numRequestsWithLocation).append(", ");
      sb.append("NumLocalAllocations=").append(numLocalAllocations).append(",");
      sb.append("NumNonLocalAllocations=").append(numNonLocalAllocations).append(",");
      sb.append("NumRequestsWithoutLocation=").append(numRequestsWithoutLocation).append(", ");
      sb.append("NumRejectedTasks=").append(numRejectedTasks).append(", ");
      sb.append("NumCommFailures=").append(numCommFailures).append(", ");
      sb.append("NumDelayedAllocations=").append(numDelayedAllocations).append(", ");
      sb.append("LocalityBasedAllocationsPerHost=").append(localityBasedNumAllocationsPerHost).append(", ");
      sb.append("NumAllocationsPerHost=").append(numAllocationsPerHost);
      return sb.toString();
    }

    void registerTaskRequest(String []requestedHosts, String[] requestedRacks) {
      numRequestedAllocations++;
      // TODO Change after HIVE-9987. For now, there's no rack matching.
      if (requestedHosts != null && requestedHosts.length != 0) {
        numRequestsWithLocation++;
      } else {
        numRequestsWithoutLocation++;
      }
    }

    void registerTaskAllocated(String[] requestedHosts, String [] requestedRacks, String allocatedHost) {
      // TODO Change after HIVE-9987. For now, there's no rack matching.
      if (requestedHosts != null && requestedHosts.length != 0) {
        Set<String> requestedHostSet = new HashSet<>(Arrays.asList(requestedHosts));
        if (requestedHostSet.contains(allocatedHost)) {
          numLocalAllocations++;
          _registerAllocationInHostMap(allocatedHost, localityBasedNumAllocationsPerHost);
        } else {
          // KKK TODO Log all non-local allocations
          numNonLocalAllocations++;
        }
      } else {
        numAllocationsNoLocalityRequest++;
      }
      _registerAllocationInHostMap(allocatedHost, numAllocationsPerHost);
    }

    void registerCommFailure(String host) {
      numCommFailures++;
    }

    void registerTaskRejected(String host) {
      numRejectedTasks++;
    }

    void registerDelayedAllocation() {
      numDelayedAllocations++;
    }

    private void _registerAllocationInHostMap(String host, Map<String, AtomicInteger> hostMap) {
      AtomicInteger val = hostMap.get(host);
      if (val == null) {
        val = new AtomicInteger(0);
        hostMap.put(host, val);
      }
      val.incrementAndGet();
    }
  }

  private static class TaskInfo {
    final Object task;
    final Object clientCookie;
    final Priority priority;
    final Resource capability;
    final String[] requestedHosts;
    final String[] requestedRacks;
    final long requestTime;
    ContainerId containerId;
    String assignedHost;
    private boolean assigned = false;

    public TaskInfo(Object task, Object clientCookie,
                    Priority priority, Resource capability, String[] hosts, String[] racks,
                    long requestTime) {
      this.task = task;
      this.clientCookie = clientCookie;
      this.priority = priority;
      this.capability = capability;
      this.requestedHosts = hosts;
      this.requestedRacks = racks;
      this.requestTime = requestTime;
    }

    void setAssignmentInfo(String host, ContainerId containerId) {
      this.assignedHost = host;
      this.containerId = containerId;
      assigned = true;
    }
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
