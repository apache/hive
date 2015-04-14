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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.configuration.LlapConfiguration;
import org.apache.hadoop.hive.llap.daemon.registry.ServiceInstance;
import org.apache.hadoop.hive.llap.daemon.registry.ServiceInstanceSet;
import org.apache.hadoop.hive.llap.daemon.registry.impl.LlapRegistryService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.dag.api.TaskAttemptEndReason;
import org.apache.tez.dag.app.AppContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class LlapTaskSchedulerService extends TaskSchedulerService {

  private static final Log LOG = LogFactory.getLog(LlapTaskSchedulerService.class);

  private static final float BACKOFF_FACTOR = 1.2f;

  private final ExecutorService appCallbackExecutor;
  private final TaskSchedulerAppCallback appClientDelegate;

  // interface into the registry service
  private ServiceInstanceSet activeInstances;

  @VisibleForTesting
  final Map<ServiceInstance, NodeInfo> instanceToNodeMap = new HashMap<>();
  
  @VisibleForTesting
  final Set<ServiceInstance> instanceBlackList = new HashSet<ServiceInstance>();

  @VisibleForTesting
  // Tracks currently allocated containers.
  final Map<ContainerId, String> containerToInstanceMap = new HashMap<>();

  // Tracks tasks which could not be allocated immediately.
  @VisibleForTesting
  final TreeMap<Priority, List<TaskInfo>> pendingTasks = new TreeMap<>(new Comparator<Priority>() {
    @Override
    public int compare(Priority o1, Priority o2) {
      return o1.getPriority() - o2.getPriority();
    }
  });

  // Tracks running and queued tasks. Cleared after a task completes.
  private final ConcurrentMap<Object, TaskInfo> knownTasks = new ConcurrentHashMap<>();

  @VisibleForTesting
  final DelayQueue<NodeInfo> disabledNodes = new DelayQueue<>();

  private final ContainerFactory containerFactory;
  private final Random random = new Random();
  private final Clock clock;
  private final ListeningExecutorService executor;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
  private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

  // TODO Track resources used by this application on specific hosts, and make scheduling decisions
  // accordingly.
  // Ideally implement in a way where updates from ZK, if they do come, can just be plugged in.
  // A heap based on available capacity - which is updated each time stats are updated,
  // or anytime assignment numbers are changed. Especially for random allocations (no host request).
  // For non-random allocations - Walk through all pending tasks to get local assignments, then
  // start assigning them to non local hosts.
  // Also setup a max over-subscribe limit as part of this.

  private final AtomicBoolean isStopped = new AtomicBoolean(false);

  private final long nodeReEnableTimeout;

  // Per daemon
  private final int memoryPerInstance;
  private final int coresPerInstance;
  private final int executorsPerInstance;

  // Per Executor Thread
  private final Resource resourcePerExecutor;

  private final LlapRegistryService registry = new LlapRegistryService();
  private final PendingTaskSchedulerCallable pendingTaskSchedulerCallable =
      new PendingTaskSchedulerCallable();
  private ListenableFuture<Void> pendingTaskSchedulerFuture;

  @VisibleForTesting
  private final AtomicInteger dagCounter = new AtomicInteger(1);
  // Statistics to track allocations
  // All of stats variables are visible for testing.
  @VisibleForTesting
  StatsPerDag dagStats = new StatsPerDag();

  public LlapTaskSchedulerService(TaskSchedulerAppCallback appClient, AppContext appContext,
      String clientHostname, int clientPort, String trackingUrl, long customAppIdIdentifier,
      Configuration conf) {
    // Accepting configuration here to allow setting up fields as final

    super(LlapTaskSchedulerService.class.getName());
    this.appCallbackExecutor = createAppCallbackExecutorService();
    this.appClientDelegate = createAppCallbackDelegate(appClient);
    this.clock = appContext.getClock();
    this.containerFactory = new ContainerFactory(appContext, customAppIdIdentifier);
    this.memoryPerInstance =
        conf.getInt(LlapConfiguration.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB,
            LlapConfiguration.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB_DEFAULT);
    this.coresPerInstance =
        conf.getInt(LlapConfiguration.LLAP_DAEMON_VCPUS_PER_INSTANCE,
            LlapConfiguration.LLAP_DAEMON_VCPUS_PER_INSTANCE_DEFAULT);
    this.executorsPerInstance =
        conf.getInt(LlapConfiguration.LLAP_DAEMON_NUM_EXECUTORS,
            LlapConfiguration.LLAP_DAEMON_NUM_EXECUTORS_DEFAULT);
    this.nodeReEnableTimeout =
        conf.getLong(LlapConfiguration.LLAP_DAEMON_TASK_SCHEDULER_NODE_REENABLE_TIMEOUT_MILLIS,
            LlapConfiguration.LLAP_DAEMON_TASK_SCHEDULER_NODE_REENABLE_TIMEOUT_MILLIS_DEFAULT);

    int memoryPerExecutor = (int) (memoryPerInstance / (float) executorsPerInstance);
    int coresPerExecutor = (int) (coresPerInstance / (float) executorsPerInstance);
    this.resourcePerExecutor = Resource.newInstance(memoryPerExecutor, coresPerExecutor);

    String instanceId = conf.getTrimmed(LlapConfiguration.LLAP_DAEMON_SERVICE_HOSTS);

    Preconditions.checkNotNull(instanceId, LlapConfiguration.LLAP_DAEMON_SERVICE_HOSTS
        + " must be defined");

    ExecutorService executorService =
        Executors.newFixedThreadPool(1,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("LlapScheduler").build());
    executor = MoreExecutors.listeningDecorator(executorService);

    LOG.info("Running with configuration: " + "memoryPerInstance=" + memoryPerInstance
        + ", vCoresPerInstance=" + coresPerInstance + ", executorsPerInstance="
        + executorsPerInstance + ", resourcePerInstanceInferred=" + resourcePerExecutor
        + ", nodeReEnableTimeout=" + nodeReEnableTimeout + ", nodeReEnableBackOffFactor="
        + BACKOFF_FACTOR);
  }

  @Override
  public void serviceInit(Configuration conf) {
    registry.init(conf);
  }

  @Override
  public void serviceStart() throws IOException {
    writeLock.lock();
    try {
      pendingTaskSchedulerFuture = executor.submit(pendingTaskSchedulerCallable);
      registry.start();
      activeInstances = registry.getInstances();
      for (ServiceInstance inst : activeInstances.getAll().values()) {
        addNode(inst, new NodeInfo(inst, BACKOFF_FACTOR, clock));
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
        if (registry != null) {
          registry.stop();
        }
        appCallbackExecutor.shutdownNow();
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public Resource getTotalResources() {
    int memory = 0;
    int vcores = 0;
    readLock.lock();
    try {
      for (ServiceInstance inst : activeInstances.getAll().values()) {
        if (inst.isAlive()) {
          Resource r = inst.getResource();
          LOG.info("Found instance " + inst + " with " + r);
          memory += r.getMemory();
          vcores += r.getVirtualCores();
        } else {
          LOG.info("Ignoring dead instance " + inst);
        }
      }
    } finally {
      readLock.unlock();
    }

    return Resource.newInstance(memory, vcores);
  }

  /**
   * The difference between this and getTotalResources() is that this only gives currently free
   * resource instances, while the other lists all the instances that may become available in a
   * while.
   */
  @Override
  public Resource getAvailableResources() {
    // need a state store eventually for current state & measure backoffs
    int memory = 0;
    int vcores = 0;
    readLock.lock();
    try {
      for (ServiceInstance inst : instanceToNodeMap.keySet()) {
        if (inst.isAlive()) {
          Resource r = inst.getResource();
          memory += r.getMemory();
          vcores += r.getVirtualCores();
        }
      }
    } finally {
      readLock.unlock();
    }

    return Resource.newInstance(memory, vcores);
  }

  @Override
  public int getClusterNodeCount() {
    readLock.lock();
    try {
      int n = 0;
      for (ServiceInstance inst : activeInstances.getAll().values()) {
        if (inst.isAlive()) {
          n++;
        }
      }
      return n;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void resetMatchLocalityForAllHeldContainers() {
    // This is effectively DAG completed, and can be used to reset statistics being tracked.
    LOG.info("DAG: " + dagCounter.get() + " completed. Scheduling stats: " + dagStats);
    dagCounter.incrementAndGet();
    dagStats = new StatsPerDag();
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
    TaskInfo taskInfo =
        new TaskInfo(task, clientCookie, priority, capability, hosts, racks, clock.getTime());
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
    TaskInfo taskInfo =
        new TaskInfo(task, clientCookie, priority, capability, null, null, clock.getTime());
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
        LOG.error("Could not determine ContainerId for task: "
            + task
            + " . Could have hit a race condition. Ignoring."
            + " The query may hang since this \"unknown\" container is now taking up a slot permanently");
        return false;
      }
      if (taskInfo.containerId == null) {
        if (taskInfo.assigned) {
          LOG.error("Task: "
              + task
              + " assigned, but could not find the corresponding containerId."
              + " The query may hang since this \"unknown\" container is now taking up a slot permanently");
        } else {
          LOG.info("Ignoring deallocate request for task " + task
              + " which hasn't been assigned to a container");
          removePendingTask(taskInfo);
        }
        return false;
      }
      String hostForContainer = containerToInstanceMap.remove(taskInfo.containerId);
      assert hostForContainer != null;
      ServiceInstance assignedInstance = taskInfo.assignedInstance;
      assert assignedInstance != null;

      if (taskSucceeded) {
        // The node may have been blacklisted at this point - which means it may not be in the
        // activeNodeList.
        NodeInfo nodeInfo = instanceToNodeMap.get(assignedInstance);
        assert nodeInfo != null;
        nodeInfo.registerTaskSuccess();
        // TODO Consider un-blacklisting the node since at least 1 slot should have become available
        // on the node.
      } else if (!taskSucceeded
          && endReason != null
          && EnumSet
              .of(TaskAttemptEndReason.SERVICE_BUSY, TaskAttemptEndReason.COMMUNICATION_ERROR)
              .contains(endReason)) {
        if (endReason == TaskAttemptEndReason.COMMUNICATION_ERROR) {
          dagStats.registerCommFailure(taskInfo.assignedInstance.getHost());
        } else if (endReason == TaskAttemptEndReason.SERVICE_BUSY) {
          dagStats.registerTaskRejected(taskInfo.assignedInstance.getHost());
        }
        disableInstance(assignedInstance, endReason == TaskAttemptEndReason.SERVICE_BUSY);
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
  TaskSchedulerAppCallback createAppCallbackDelegate(TaskSchedulerAppCallback realAppClient) {
    return new TaskSchedulerAppCallbackWrapper(realAppClient, appCallbackExecutor);
  }

  /**
   * @param requestedHosts the list of preferred hosts. null implies any host
   * @return
   */
  private ServiceInstance selectHost(TaskInfo request) {
    String[] requestedHosts = request.requestedHosts;
    readLock.lock(); // Read-lock. Not updating any stats at the moment.
    try {
      // Check if any hosts are active.
      if (getAvailableResources().getMemory() <= 0) {
        refreshInstances();
      }

      // If there's no memory available, fail
      if (getTotalResources().getMemory() <= 0) {
        return null;
      }

      if (requestedHosts != null) {
        for (String host : requestedHosts) {
          // Pick the first host always. Weak attempt at cache affinity.
          Set<ServiceInstance> instances = activeInstances.getByHost(host);
          if (!instances.isEmpty()) {
            for (ServiceInstance inst : instances) {
              if (inst.isAlive() && instanceToNodeMap.containsKey(inst)) {
                // only allocate from the "available" list
                // TODO Change this to work off of what we think is remaining capacity for an
                // instance
                LOG.info("Assigning " + inst + " when looking for " + host);
                return inst;
              }
            }
          }
        }
      }
      /* fall through - miss in locality (random scheduling) */
      ServiceInstance[] all = instanceToNodeMap.keySet().toArray(new ServiceInstance[0]);
      // Check again
      if (all.length > 0) {
        int n = random.nextInt(all.length);
        // start at random offset and iterate whole list
        for (int i = 0; i < all.length; i++) {
          ServiceInstance inst = all[(i + n) % all.length];
          if (inst.isAlive()) {
            LOG.info("Assigning " + inst + " when looking for any host");
            return inst;
          }
        }
      }
    } finally {
      readLock.unlock();
    }

    /* check again whether nodes are disabled or just missing */
    writeLock.lock();
    try {
      for (ServiceInstance inst : activeInstances.getAll().values()) {
        if (inst.isAlive() && instanceBlackList.contains(inst) == false
            && instanceToNodeMap.containsKey(inst) == false) {
          /* that's a good node, not added to the allocations yet */
          addNode(inst, new NodeInfo(inst, BACKOFF_FACTOR, clock));
          // mark it as disabled to let the pending tasks go there
          disableInstance(inst, true);
        }
      }
      /* do not allocate nodes from this process, as then the pending tasks will get starved */
    } finally {
      writeLock.unlock();
    }
    return null;
  }

  private void refreshInstances() {
    try {
      activeInstances.refresh(); // handles its own sync
    } catch (IOException ioe) {
      LOG.warn("Could not refresh list of active instances", ioe);
    }
  }

  private void addNode(ServiceInstance inst, NodeInfo node) {
    instanceToNodeMap.put(inst, node);
  }

  private void reenableDisabledNode(NodeInfo nodeInfo) {
    writeLock.lock();
    try {
      if (!nodeInfo.isBusy()) {
        refreshInstances();
      }
      if (nodeInfo.host.isAlive()) {
        nodeInfo.enableNode();
        instanceBlackList.remove(nodeInfo.host);
        instanceToNodeMap.put(nodeInfo.host, nodeInfo);
      } else {
        if (LOG.isInfoEnabled()) {
          LOG.info("Removing dead node " + nodeInfo);
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void disableInstance(ServiceInstance instance, boolean busy) {
    writeLock.lock();
    try {
      NodeInfo nodeInfo = instanceToNodeMap.remove(instance);
      if (nodeInfo == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Node: " + instance + " already disabled, or invalid. Not doing anything.");
        }
      } else {
        instanceBlackList.add(instance);
        nodeInfo.disableNode(nodeReEnableTimeout);
        nodeInfo.setBusy(busy); // daemon failure vs daemon busy
        // TODO: handle task to container map events in case of hard failures
        disabledNodes.add(nodeInfo);
        if (LOG.isInfoEnabled()) {
          LOG.info("Disabling instance " + instance + " for " + nodeReEnableTimeout + " seconds");
        }
      }
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
        LOG.warn("Could not find task: " + taskInfo.task + " in pending list, at priority: "
            + priority);
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void schedulePendingTasks() {
    writeLock.lock();
    try {
      Iterator<Entry<Priority, List<TaskInfo>>> pendingIterator =
          pendingTasks.entrySet().iterator();
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
    ServiceInstance host = selectHost(taskInfo);
    if (host == null) {
      return false;
    } else {
      Container container =
          containerFactory.createContainer(resourcePerExecutor, taskInfo.priority, host.getHost(),
              host.getRpcPort());
      writeLock.lock(); // While updating local structures
      try {
        dagStats.registerTaskAllocated(taskInfo.requestedHosts, taskInfo.requestedRacks,
            host.getHost());
        taskInfo.setAssignmentInfo(host, container.getId());
        knownTasks.putIfAbsent(taskInfo.task, taskInfo);
        containerToInstanceMap.put(container.getId(), host.getWorkerIdentity());
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
    final ServiceInstance host;
    private final Clock clock;

    long expireTimeMillis = -1;
    private long numSuccessfulTasks = 0;
    private long numSuccessfulTasksAtLastBlacklist = -1;
    float cumulativeBackoffFactor = 1.0f;
    private boolean busy;

    NodeInfo(ServiceInstance host, float backoffFactor, Clock clock) {
      this.host = host;
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
      this.busy = false; // if a task exited, we might have free slots
      numSuccessfulTasks++;
    }

    public void setBusy(boolean busy) {
      this.busy = busy;
    }

    public boolean isBusy() {
      return busy;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return expireTimeMillis - clock.getTime();
    }

    @Override
    public int compareTo(Delayed o) {
      NodeInfo other = (NodeInfo) o;
      if (other.expireTimeMillis > this.expireTimeMillis) {
        return -1;
      } else if (other.expireTimeMillis < this.expireTimeMillis) {
        return 1;
      } else {
        return 0;
      }
    }

    @Override
    public String toString() {
      return "NodeInfo{" + "constBackOffFactor=" + constBackOffFactor + ", host=" + host
          + ", expireTimeMillis=" + expireTimeMillis + ", numSuccessfulTasks=" + numSuccessfulTasks
          + ", numSuccessfulTasksAtLastBlacklist=" + numSuccessfulTasksAtLastBlacklist
          + ", cumulativeBackoffFactor=" + cumulativeBackoffFactor + '}';
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
      sb.append("LocalityBasedAllocationsPerHost=").append(localityBasedNumAllocationsPerHost)
          .append(", ");
      sb.append("NumAllocationsPerHost=").append(numAllocationsPerHost);
      return sb.toString();
    }

    void registerTaskRequest(String[] requestedHosts, String[] requestedRacks) {
      numRequestedAllocations++;
      // TODO Change after HIVE-9987. For now, there's no rack matching.
      if (requestedHosts != null && requestedHosts.length != 0) {
        numRequestsWithLocation++;
      } else {
        numRequestsWithoutLocation++;
      }
    }

    void registerTaskAllocated(String[] requestedHosts, String[] requestedRacks,
        String allocatedHost) {
      // TODO Change after HIVE-9987. For now, there's no rack matching.
      if (requestedHosts != null && requestedHosts.length != 0) {
        Set<String> requestedHostSet = new HashSet<>(Arrays.asList(requestedHosts));
        if (requestedHostSet.contains(allocatedHost)) {
          numLocalAllocations++;
          _registerAllocationInHostMap(allocatedHost, localityBasedNumAllocationsPerHost);
        } else {
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
    ServiceInstance assignedInstance;
    private boolean assigned = false;

    public TaskInfo(Object task, Object clientCookie, Priority priority, Resource capability,
        String[] hosts, String[] racks, long requestTime) {
      this.task = task;
      this.clientCookie = clientCookie;
      this.priority = priority;
      this.capability = capability;
      this.requestedHosts = hosts;
      this.requestedRacks = racks;
      this.requestTime = requestTime;
    }

    void setAssignmentInfo(ServiceInstance instance, ContainerId containerId) {
      this.assignedInstance = instance;
      this.containerId = containerId;
      assigned = true;
    }
  }

  static class ContainerFactory {
    final ApplicationAttemptId customAppAttemptId;
    AtomicLong nextId;

    public ContainerFactory(AppContext appContext, long appIdLong) {
      this.nextId = new AtomicLong(1);
      ApplicationId appId =
          ApplicationId.newInstance(appIdLong, appContext.getApplicationAttemptId()
              .getApplicationId().getId());
      this.customAppAttemptId =
          ApplicationAttemptId.newInstance(appId, appContext.getApplicationAttemptId()
              .getAttemptId());
    }

    public Container createContainer(Resource capability, Priority priority, String hostname,
        int port) {
      ContainerId containerId =
          ContainerId.newContainerId(customAppAttemptId, nextId.getAndIncrement());
      NodeId nodeId = NodeId.newInstance(hostname, port);
      String nodeHttpAddress = "hostname:0"; // TODO: include UI ports

      Container container =
          Container.newInstance(containerId, nodeId, nodeHttpAddress, capability, priority, null);

      return container;
    }
  }
}
