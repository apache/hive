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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.configuration.LlapConfiguration;
import org.apache.hadoop.hive.llap.daemon.registry.ServiceInstance;
import org.apache.hadoop.hive.llap.daemon.registry.ServiceInstanceSet;
import org.apache.hadoop.hive.llap.daemon.registry.impl.LlapRegistryService;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlapTaskSchedulerService extends TaskSchedulerService {

  private static final Logger LOG = LoggerFactory.getLogger(LlapTaskSchedulerService.class);

  private final ExecutorService appCallbackExecutor;
  private final TaskSchedulerAppCallback appClientDelegate;

  // interface into the registry service
  private ServiceInstanceSet activeInstances;

  // Tracks all instances, including ones which have been disabled in the past.
  // LinkedHashMap to provide the same iteration order when selecting a random host.
  @VisibleForTesting
  final Map<ServiceInstance, NodeInfo> instanceToNodeMap = new LinkedHashMap<>();
  // TODO Ideally, remove elements from this once it's known that no tasks are linked to the instance (all deallocated)

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

  // Queue for disabled nodes. Nodes make it out of this queue when their expiration timeout is hit.
  @VisibleForTesting
  final DelayQueue<NodeInfo> disabledNodesQueue = new DelayQueue<>();

  private final ContainerFactory containerFactory;
  private final Random random = new Random();
  private final Clock clock;

  private final ListeningExecutorService nodeEnabledExecutor;
  private final NodeEnablerCallable nodeEnablerCallable =
      new NodeEnablerCallable();

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
  private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

  private final Lock scheduleLock = new ReentrantLock();
  private final Condition scheduleCondition = scheduleLock.newCondition();
  private final ListeningExecutorService schedulerExecutor;
  private final SchedulerCallable schedulerCallable = new SchedulerCallable();

  private final AtomicBoolean isStopped = new AtomicBoolean(false);

  private final NodeBlacklistConf nodeBlacklistConf;

  // Per daemon
  private final int memoryPerInstance;
  private final int coresPerInstance;
  private final int executorsPerInstance;

  private final int numSchedulableTasksPerNode;

  // Per Executor Thread
  private final Resource resourcePerExecutor;

  private final LlapRegistryService registry = new LlapRegistryService();


  private volatile ListenableFuture<Void> nodeEnablerFuture;
  private volatile ListenableFuture<Void> schedulerFuture;

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
    this.nodeBlacklistConf = new NodeBlacklistConf(
        conf.getLong(LlapConfiguration.LLAP_TASK_SCHEDULER_NODE_REENABLE_MIN_TIMEOUT_MILLIS,
            LlapConfiguration.LLAP_TASK_SCHEDULER_NODE_REENABLE_MIN_TIMEOUT_MILLIS_DEFAULT),
        conf.getLong(LlapConfiguration.LLAP_TASK_SCHEDULER_NODE_REENABLE_MAX_TIMEOUT_MILLIS,
            LlapConfiguration.LLAP_TASK_SCHEDULER_NODE_REENABLE_MAX_TIMEOUT_MILLIS_DEFAULT),
        conf.getFloat(LlapConfiguration.LLAP_TASK_SCHEDULER_NODE_DISABLE_BACK_OFF_FACTOR,
            LlapConfiguration.LLAP_TASK_SCHEDULER_NODE_DISABLE_BACK_OFF_FACTOR_DEFAULT));

    this.numSchedulableTasksPerNode = conf.getInt(
        LlapConfiguration.LLAP_TASK_SCHEDULER_NUM_SCHEDULABLE_TASKS_PER_NODE,
        LlapConfiguration.LLAP_TASK_SCHEDULER_NUM_SCHEDULABLE_TASKS_PER_NODE_DEFAULT);

    int memoryPerExecutor = (int) (memoryPerInstance / (float) executorsPerInstance);
    int coresPerExecutor = (int) (coresPerInstance / (float) executorsPerInstance);
    this.resourcePerExecutor = Resource.newInstance(memoryPerExecutor, coresPerExecutor);

    String instanceId = conf.getTrimmed(LlapConfiguration.LLAP_DAEMON_SERVICE_HOSTS);

    Preconditions.checkNotNull(instanceId, LlapConfiguration.LLAP_DAEMON_SERVICE_HOSTS
        + " must be defined");

    ExecutorService executorServiceRaw =
        Executors.newFixedThreadPool(1,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("LlapSchedulerNodeEnabler").build());
    nodeEnabledExecutor = MoreExecutors.listeningDecorator(executorServiceRaw);

    ExecutorService schedulerExecutorServiceRaw = Executors.newFixedThreadPool(1,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("LlapScheduler").build());
    schedulerExecutor = MoreExecutors.listeningDecorator(schedulerExecutorServiceRaw);

    LOG.info("Running with configuration: " + "memoryPerInstance=" + memoryPerInstance
        + ", vCoresPerInstance=" + coresPerInstance + ", executorsPerInstance="
        + executorsPerInstance + ", resourcePerInstanceInferred=" + resourcePerExecutor
        + ", nodeBlacklistConf=" + nodeBlacklistConf);
  }

  @Override
  public void serviceInit(Configuration conf) {
    registry.init(conf);
  }

  @Override
  public void serviceStart() throws IOException {
    writeLock.lock();
    try {
      nodeEnablerFuture = nodeEnabledExecutor.submit(nodeEnablerCallable);
      Futures.addCallback(nodeEnablerFuture, new FutureCallback<Void>() {
        @Override
        public void onSuccess(Void result) {
          LOG.info("NodeEnabledThread exited");
        }

        @Override
        public void onFailure(Throwable t) {
          LOG.warn("NodeEnabledThread exited with error", t);
        }
      });
      schedulerFuture = schedulerExecutor.submit(schedulerCallable);
      Futures.addCallback(schedulerFuture, new FutureCallback<Void>() {
        @Override
        public void onSuccess(Void result) {
          LOG.info("SchedulerThread exited");
        }

        @Override
        public void onFailure(Throwable t) {
          LOG.warn("SchedulerThread exited with error", t);
        }
      });
      registry.start();
      activeInstances = registry.getInstances();
      for (ServiceInstance inst : activeInstances.getAll().values()) {
        addNode(inst, new NodeInfo(inst, nodeBlacklistConf, clock, numSchedulableTasksPerNode));
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
        nodeEnablerCallable.shutdown();
        if (nodeEnablerFuture != null) {
          nodeEnablerFuture.cancel(true);
        }
        nodeEnabledExecutor.shutdownNow();

        schedulerCallable.shutdown();
        if (schedulerFuture != null) {
          schedulerFuture.cancel(true);
        }
        schedulerExecutor.shutdownNow();

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
          LOG.info("Found instance " + inst);
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
      for (Entry<ServiceInstance, NodeInfo> entry : instanceToNodeMap.entrySet()) {
        if (entry.getKey().isAlive() && !entry.getValue().isDisabled()) {
          Resource r = entry.getKey().getResource();
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
  public void dagComplete() {
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
    addPendingTask(taskInfo);
    trySchedulingPendingTasks();
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
    addPendingTask(taskInfo);
    trySchedulingPendingTasks();
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
      ServiceInstance assignedInstance = taskInfo.assignedInstance;
      assert assignedInstance != null;

      NodeInfo nodeInfo = instanceToNodeMap.get(assignedInstance);
      assert nodeInfo != null;
      if (taskSucceeded) {
        // The node may have been blacklisted at this point - which means it may not be in the
        // activeNodeList.

        nodeInfo.registerTaskSuccess();

        if (nodeInfo.isDisabled()) {
          // Re-enable the node. If a task succeeded, a slot may have become available.
          // Also reset commFailures since a task was able to communicate back and indicate success.
          nodeInfo.enableNode();
          // Re-insert into the queue to force the poll thread to remove the element.
          if ( disabledNodesQueue.remove(nodeInfo)) {
            disabledNodesQueue.add(nodeInfo);
          }
        }
        // In case of success, trigger a scheduling run for pending tasks.
        trySchedulingPendingTasks();

      } else if (!taskSucceeded) {
        nodeInfo.registerUnsuccessfulTaskEnd();
        if (endReason != null && EnumSet
            .of(TaskAttemptEndReason.SERVICE_BUSY, TaskAttemptEndReason.COMMUNICATION_ERROR)
            .contains(endReason)) {
          if (endReason == TaskAttemptEndReason.COMMUNICATION_ERROR) {
            dagStats.registerCommFailure(taskInfo.assignedInstance.getHost());
          } else if (endReason == TaskAttemptEndReason.SERVICE_BUSY) {
            dagStats.registerTaskRejected(taskInfo.assignedInstance.getHost());
          }
        }
        boolean commFailure = endReason != null && endReason == TaskAttemptEndReason.COMMUNICATION_ERROR;
        disableInstance(assignedInstance, commFailure);
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
    // Containers are not being tracked for re-use.
    // This is safe to ignore since a deallocate task should have come in earlier.
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
   * @param request the list of preferred hosts. null implies any host
   * @return
   */
  private NodeServiceInstancePair selectHost(TaskInfo request) {
    String[] requestedHosts = request.requestedHosts;
    readLock.lock(); // Read-lock. Not updating any stats at the moment.
    try {
      // Check if any hosts are active.
      if (getAvailableResources().getMemory() <= 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Refreshing instances since total memory is 0");
        }
        refreshInstances();
      }

      // If there's no memory available, fail
      if (getTotalResources().getMemory() <= 0) {
        return null;
      }

      if (requestedHosts != null) {
        int prefHostCount = -1;
        for (String host : requestedHosts) {
          prefHostCount++;
          // Pick the first host always. Weak attempt at cache affinity.
          Set<ServiceInstance> instances = activeInstances.getByHost(host);
          if (!instances.isEmpty()) {
            for (ServiceInstance inst : instances) {
              NodeInfo nodeInfo = instanceToNodeMap.get(inst);
              if (nodeInfo != null && nodeInfo.canAcceptTask()) {
                LOG.info("Assigning " + inst + " when looking for " + host + "." +
                    " FirstRequestedHost=" + (prefHostCount == 0));
                return new NodeServiceInstancePair(inst, nodeInfo);
              }
            }
          }
        }
      }
      /* fall through - miss in locality (random scheduling) */
      Entry<ServiceInstance, NodeInfo>[] all =
          instanceToNodeMap.entrySet().toArray(new Entry[instanceToNodeMap.size()]);
      // Check again
      if (all.length > 0) {
        int n = random.nextInt(all.length);
        // start at random offset and iterate whole list
        for (int i = 0; i < all.length; i++) {
          Entry<ServiceInstance, NodeInfo> inst = all[(i + n) % all.length];
          if (inst.getValue().canAcceptTask()) {
            LOG.info("Assigning " + inst + " when looking for any host, from #hosts=" + all.length);
            return new NodeServiceInstancePair(inst.getKey(), inst.getValue());
          }
        }
      }
      return null;
    } finally {
      readLock.unlock();
    }
  }

  // TODO Each refresh operation should addNodes if they don't already exist.
  // Even better would be to get notifications from the service impl when a node gets added or removed.
  // Instead of having to walk through the entire list. The computation of a node getting added or
  // removed already exists in the DynamicRegistry implementation.
  private void refreshInstances() {
    try {
      activeInstances.refresh(); // handles its own sync
    } catch (IOException ioe) {
      LOG.warn("Could not refresh list of active instances", ioe);
    }
  }

  private void scanForNodeChanges() {
    /* check again whether nodes are disabled or just missing */
    writeLock.lock();
    try {
      for (ServiceInstance inst : activeInstances.getAll().values()) {
        if (inst.isAlive() && instanceToNodeMap.containsKey(inst) == false) {
          /* that's a good node, not added to the allocations yet */
          LOG.info("Found a new node: " + inst + ".");
          addNode(inst, new NodeInfo(inst, nodeBlacklistConf, clock, numSchedulableTasksPerNode));
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void addNode(ServiceInstance inst, NodeInfo node) {
    LOG.info("Adding node: " + inst);
    instanceToNodeMap.put(inst, node);
    // Trigger scheduling since a new node became available.
    trySchedulingPendingTasks();
  }

  private void reenableDisabledNode(NodeInfo nodeInfo) {
    writeLock.lock();
    try {
      if (nodeInfo.hadCommFailure()) {
        // If the node being re-enabled was not marked busy previously, then it was disabled due to
        // some other failure. Refresh the service list to see if it's been removed permanently.
        refreshInstances();
      }
      LOG.info("Attempting to re-enable node: " + nodeInfo.getServiceInstance());
      if (nodeInfo.getServiceInstance().isAlive()) {
        nodeInfo.enableNode();
      } else {
        if (LOG.isInfoEnabled()) {
          LOG.info("Removing dead node " + nodeInfo);
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void disableInstance(ServiceInstance instance, boolean isCommFailure) {
    writeLock.lock();
    try {
      NodeInfo nodeInfo = instanceToNodeMap.get(instance);
      if (nodeInfo == null || nodeInfo.isDisabled()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Node: " + instance + " already disabled, or invalid. Not doing anything.");
        }
      } else {
        nodeInfo.disableNode(isCommFailure);
        // TODO: handle task to container map events in case of hard failures
        disabledNodesQueue.add(nodeInfo);
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void addPendingTask(TaskInfo taskInfo) {
    writeLock.lock();
    try {
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

  @VisibleForTesting
  protected void schedulePendingTasks() {
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

          // TODO Optimization: Add a check to see if there's any capacity available. No point in
          // walking through all active nodes, if they don't have potential capacity.

          TaskInfo taskInfo = taskIter.next();
          if (taskInfo.getNumPreviousAssignAttempts() == 1) {
            dagStats.registerDelayedAllocation();
          }
          taskInfo.triedAssigningTask();
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
    NodeServiceInstancePair nsPair = selectHost(taskInfo);
    if (nsPair == null) {
      return false;
    } else {
      Container container =
          containerFactory.createContainer(resourcePerExecutor, taskInfo.priority,
              nsPair.getServiceInstance().getHost(),
              nsPair.getServiceInstance().getRpcPort());
      writeLock.lock(); // While updating local structures
      try {
        dagStats.registerTaskAllocated(taskInfo.requestedHosts, taskInfo.requestedRacks,
            nsPair.getServiceInstance().getHost());
        taskInfo.setAssignmentInfo(nsPair.getServiceInstance(), container.getId());
        knownTasks.putIfAbsent(taskInfo.task, taskInfo);
        nsPair.getNodeInfo().registerTaskScheduled();
      } finally {
        writeLock.unlock();
      }

      appClientDelegate.taskAllocated(taskInfo.task, taskInfo.clientCookie, container);
      return true;
    }
  }

  private class NodeEnablerCallable implements Callable<Void> {

    private AtomicBoolean isShutdown = new AtomicBoolean(false);
    private static final long REFRESH_INTERVAL = 10000l;
    long nextPollInterval = REFRESH_INTERVAL;
    long lastRefreshTime;

    @Override
    public Void call() {

      lastRefreshTime = System.currentTimeMillis();
      while (!isShutdown.get() && !Thread.currentThread().isInterrupted()) {
        try {
          while (true) {
            NodeInfo nodeInfo = disabledNodesQueue.poll(nextPollInterval, TimeUnit.MILLISECONDS);
            if (nodeInfo != null) {
              long currentTime = System.currentTimeMillis();
              // A node became available. Enable the node and try scheduling.
              reenableDisabledNode(nodeInfo);
              trySchedulingPendingTasks();

              nextPollInterval -= (currentTime - lastRefreshTime);
            }

            if (nextPollInterval < 0 || nodeInfo == null) {
              // timeout expired. Reset the poll interval and refresh nodes.
              nextPollInterval = REFRESH_INTERVAL;
              lastRefreshTime = System.currentTimeMillis();
              // TODO Get rid of this polling once we have notificaitons from the registry sub-system
              if (LOG.isDebugEnabled()) {
                LOG.debug("Refreshing instances based on poll interval");
              }
              refreshInstances();
              scanForNodeChanges();
            }
          }
        } catch (InterruptedException e) {
          if (isShutdown.get()) {
            LOG.info("NodeEnabler thread interrupted after shutdown");
            break;
          } else {
            LOG.warn("NodeEnabler thread interrupted without being shutdown");
            throw new RuntimeException("NodeEnabler thread interrupted without being shutdown", e);
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

  private void trySchedulingPendingTasks() {
    scheduleLock.lock();
    try {
      scheduleCondition.signal();
    } finally {
      scheduleLock.unlock();
    }
  }

  private class SchedulerCallable implements Callable<Void> {
    private AtomicBoolean isShutdown = new AtomicBoolean(false);

    @Override
    public Void call() {
      while (!isShutdown.get() && !Thread.currentThread().isInterrupted()) {
        scheduleLock.lock();
        try {
          scheduleCondition.await();
        } catch (InterruptedException e) {
          if (isShutdown.get()) {
            LOG.info("Scheduler thread interrupted after shutdown");
            break;
          } else {
            LOG.warn("Scheduler thread interrupted without being shutdown");
            throw new RuntimeException("Scheduler thread interrupted without being shutdown", e);
          }
        } finally {
          scheduleLock.unlock();
        }
        // Schedule outside of the scheduleLock - which should only be used to wait on the condition.
        schedulePendingTasks();
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
    private final NodeBlacklistConf blacklistConf;
    final ServiceInstance serviceInstance;
    private final Clock clock;

    long expireTimeMillis = -1;
    private long numSuccessfulTasks = 0;
    private long numSuccessfulTasksAtLastBlacklist = -1;
    float cumulativeBackoffFactor = 1.0f;

    // Indicates whether a node had a recent communication failure.
    private boolean hadCommFailure = false;

    // Indicates whether a node is disabled - for whatever reason - commFailure, busy, etc.
    private boolean disabled = false;

    private int numScheduledTasks = 0;
    private final int numSchedulableTasks;


    /**
     * Create a NodeInfo bound to a service instance
     *
     * @param serviceInstance         the associated serviceInstance
     * @param blacklistConf           blacklist configuration
     * @param clock                   clock to use to obtain timing information
     * @param numSchedulableTasksConf number of schedulable tasks on the node. 0 represents auto
     *                                detect based on the serviceInstance, -1 indicates indicates
     *                                unlimited capacity
     */
    NodeInfo(ServiceInstance serviceInstance, NodeBlacklistConf blacklistConf, Clock clock, int numSchedulableTasksConf) {
      Preconditions.checkArgument(numSchedulableTasksConf >= -1, "NumSchedulableTasks must be >=-1");
      this.serviceInstance = serviceInstance;
      this.blacklistConf = blacklistConf;
      this.clock = clock;

      if (numSchedulableTasksConf == 0) {
        int pendingQueueuCapacity = 0;
        String pendingQueueCapacityString = serviceInstance.getProperties()
            .get(LlapConfiguration.LLAP_DAEMON_TASK_SCHEDULER_WAIT_QUEUE_SIZE);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Setting up node: " + serviceInstance + ", with available capacity=" +
              serviceInstance.getResource().getVirtualCores() + ", pendingQueueCapacity=" +
              pendingQueueCapacityString);
        }

        if (pendingQueueCapacityString != null) {
          pendingQueueuCapacity = Integer.parseInt(pendingQueueCapacityString);
        }
        this.numSchedulableTasks = serviceInstance.getResource().getVirtualCores() + pendingQueueuCapacity;
      } else {
        this.numSchedulableTasks = numSchedulableTasksConf;
      }
      LOG.info("Setting up node: " + serviceInstance + " with schedulableCapacity=" + this.numSchedulableTasks);
    }

    ServiceInstance getServiceInstance() {
      return serviceInstance;
    }

    void enableNode() {
      expireTimeMillis = -1;
      disabled = false;
      hadCommFailure = false;
    }

    void disableNode(boolean commFailure) {
      long duration = blacklistConf.minDelay;
      long currentTime = clock.getTime();
      this.hadCommFailure = commFailure;
      disabled = true;
      if (numSuccessfulTasksAtLastBlacklist == numSuccessfulTasks) {
        // Relying on a task succeeding to reset the exponent.
        // There's no notifications on whether a task gets accepted or not. That would be ideal to
        // reset this.
        cumulativeBackoffFactor = cumulativeBackoffFactor * blacklistConf.backoffFactor;
      } else {
        // Was able to execute something before the last blacklist. Reset the exponent.
        cumulativeBackoffFactor = 1.0f;
      }

      long delayTime = (long) (duration * cumulativeBackoffFactor);
      if (delayTime > blacklistConf.maxDelay) {
        delayTime = blacklistConf.maxDelay;
      }
      if (LOG.isInfoEnabled()) {
        LOG.info("Disabling instance " + serviceInstance + " for " + delayTime + " milli-seconds");
      }
      expireTimeMillis = currentTime + delayTime;
      numSuccessfulTasksAtLastBlacklist = numSuccessfulTasks;
    }

    void registerTaskScheduled() {
      numScheduledTasks++;
    }

    void registerTaskSuccess() {
      numSuccessfulTasks++;
      numScheduledTasks--;
    }

    void registerUnsuccessfulTaskEnd() {
      numScheduledTasks--;
    }

    public boolean isDisabled() {
      return disabled;
    }

    public boolean hadCommFailure() {
      return hadCommFailure;
    }
    /* Returning true does not guarantee that the task will run, considering other queries
    may be running in the system. Also depends upon the capacity usage configuration
     */
    public boolean canAcceptTask() {
      boolean result = !hadCommFailure && !disabled && serviceInstance.isAlive()
          &&(numSchedulableTasks == -1 || ((numSchedulableTasks - numScheduledTasks) > 0));
      LOG.info("canAcceptTask={}, numScheduledTasks={}, numSchedulableTasks={}, hadCommFailure={}, disabled={}, serviceInstance.isAlive={}", result, numScheduledTasks, numSchedulableTasks, hadCommFailure, disabled, serviceInstance.isAlive());
      return result;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(expireTimeMillis - clock.getTime(), TimeUnit.MILLISECONDS);
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
      return "NodeInfo{" + "instance=" + serviceInstance
          + ", expireTimeMillis=" + expireTimeMillis + ", numSuccessfulTasks=" + numSuccessfulTasks
          + ", numSuccessfulTasksAtLastBlacklist=" + numSuccessfulTasksAtLastBlacklist
          + ", cumulativeBackoffFactor=" + cumulativeBackoffFactor
          + ", numSchedulableTasks=" + numSchedulableTasks
          + ", numScheduledTasks=" + numScheduledTasks
          + ", disabled=" + disabled
          + ", commFailures=" + hadCommFailure
          +'}';
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
    private int numAssignAttempts = 0;

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

    void triedAssigningTask() {
      numAssignAttempts++;
    }

    int getNumPreviousAssignAttempts() {
      return numAssignAttempts;
    }
  }

  private static class NodeServiceInstancePair {
    final NodeInfo nodeInfo;
    final ServiceInstance serviceInstance;

    private NodeServiceInstancePair(ServiceInstance serviceInstance, NodeInfo nodeInfo) {
      this.serviceInstance = serviceInstance;
      this.nodeInfo = nodeInfo;
    }

    public ServiceInstance getServiceInstance() {
      return serviceInstance;
    }

    public NodeInfo getNodeInfo() {
      return nodeInfo;
    }
  }

  private static final class NodeBlacklistConf {
    private final long minDelay;
    private final long maxDelay;
    private final float backoffFactor;

    public NodeBlacklistConf(long minDelay, long maxDelay, float backoffFactor) {
      this.minDelay = minDelay;
      this.maxDelay = maxDelay;
      this.backoffFactor = backoffFactor;
    }

    @Override
    public String toString() {
      return "NodeBlacklistConf{" +
          "minDelay=" + minDelay +
          ", maxDelay=" + maxDelay +
          ", backoffFactor=" + backoffFactor +
          '}';
    }
  }
}
