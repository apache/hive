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

package org.apache.hadoop.hive.llap.tezplugins;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.metrics.LlapMetricsSystem;
import org.apache.hadoop.hive.llap.metrics.MetricsUtils;
import org.apache.hadoop.hive.llap.registry.ServiceInstance;
import org.apache.hadoop.hive.llap.registry.ServiceInstanceSet;
import org.apache.hadoop.hive.llap.registry.ServiceInstanceStateChangeListener;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.llap.tezplugins.helpers.MonotonicClock;
import org.apache.hadoop.hive.llap.tezplugins.scheduler.LoggingFutureCallback;
import org.apache.hadoop.hive.llap.tezplugins.metrics.LlapTaskSchedulerMetrics;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Clock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.serviceplugins.api.ServicePluginErrorDefaults;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.serviceplugins.api.TaskScheduler;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlapTaskSchedulerService extends TaskScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(LlapTaskSchedulerService.class);

  private static final TaskStartComparator TASK_INFO_COMPARATOR = new TaskStartComparator();

  private final Configuration conf;

  // interface into the registry service
  private ServiceInstanceSet activeInstances;

  // Tracks all instances, including ones which have been disabled in the past.
  // LinkedHashMap to provide the same iteration order when selecting a random host.
  @VisibleForTesting
  final Map<String, NodeInfo> instanceToNodeMap = new LinkedHashMap<>();
  // TODO Ideally, remove elements from this once it's known that no tasks are linked to the instance (all deallocated)

  // Tracks tasks which could not be allocated immediately.
  @VisibleForTesting
  // Tasks are tracked in the order requests come in, at different priority levels.
  // TODO HIVE-13538 For tasks at the same priority level, it may be worth attempting to schedule tasks with
  // locality information before those without locality information
  final TreeMap<Priority, List<TaskInfo>> pendingTasks = new TreeMap<>(new Comparator<Priority>() {
    @Override
    public int compare(Priority o1, Priority o2) {
      return o1.getPriority() - o2.getPriority();
    }
  });

  // Tracks running and queued tasks. Cleared after a task completes.
  private final ConcurrentMap<Object, TaskInfo> knownTasks = new ConcurrentHashMap<>();
  // Tracks tasks which are running. Useful for selecting a task to preempt based on when it started.
  private final TreeMap<Integer, TreeSet<TaskInfo>> runningTasks = new TreeMap<>();


  // Queue for disabled nodes. Nodes make it out of this queue when their expiration timeout is hit.
  @VisibleForTesting
  final DelayQueue<NodeInfo> disabledNodesQueue = new DelayQueue<>();
  @VisibleForTesting
  final DelayQueue<TaskInfo> delayedTaskQueue = new DelayQueue<>();


  private final ContainerFactory containerFactory;
  private final Random random = new Random();
  @VisibleForTesting
  final Clock clock;

  private final ListeningExecutorService nodeEnabledExecutor;
  private final NodeEnablerCallable nodeEnablerCallable =
      new NodeEnablerCallable();

  private final ListeningExecutorService delayedTaskSchedulerExecutor;
  @VisibleForTesting
  final DelayedTaskSchedulerCallable delayedTaskSchedulerCallable;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
  private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

  private final Lock scheduleLock = new ReentrantLock();
  private final Condition scheduleCondition = scheduleLock.newCondition();
  private final AtomicBoolean pendingScheduleInvocations = new AtomicBoolean(false);
  private final ListeningExecutorService schedulerExecutor;
  private final SchedulerCallable schedulerCallable = new SchedulerCallable();

  private final AtomicBoolean isStopped = new AtomicBoolean(false);
  // Tracks total pending preemptions.
  private final AtomicInteger pendingPreemptions = new AtomicInteger(0);
  // Tracks pending preemptions per host, using the hostname || Always to be accessed inside a lock
  private final Map<String, MutableInt> pendingPreemptionsPerHost = new HashMap<>();

  private final NodeBlacklistConf nodeBlacklistConf;
  private final LocalityDelayConf localityDelayConf;

  // Per daemon
  private final int memoryPerInstance;
  private final int coresPerInstance;
  private final int executorsPerInstance;

  private final int numSchedulableTasksPerNode;

  // Per Executor Thread
  private final Resource resourcePerExecutor;

  // when there are no live nodes in the cluster and this timeout elapses the query is failed
  private final long timeout;
  private final Lock timeoutLock = new ReentrantLock();
  private final ScheduledExecutorService timeoutExecutor;
  private final SchedulerTimeoutMonitor timeoutMonitor;
  private ScheduledFuture<?> timeoutFuture;

  private final LlapRegistryService registry = new LlapRegistryService(false);

  private volatile ListenableFuture<Void> nodeEnablerFuture;
  private volatile ListenableFuture<Void> delayedTaskSchedulerFuture;
  private volatile ListenableFuture<Void> schedulerFuture;

  @VisibleForTesting
  private final AtomicInteger dagCounter = new AtomicInteger(1);
  // Statistics to track allocations
  // All of stats variables are visible for testing.
  @VisibleForTesting
  StatsPerDag dagStats = new StatsPerDag();

  private final LlapTaskSchedulerMetrics metrics;
  private final JvmPauseMonitor pauseMonitor;

  public LlapTaskSchedulerService(TaskSchedulerContext taskSchedulerContext) {
    this(taskSchedulerContext, new MonotonicClock(), true);
  }

  @VisibleForTesting
  public LlapTaskSchedulerService(TaskSchedulerContext taskSchedulerContext, Clock clock,
      boolean initMetrics) {
    super(taskSchedulerContext);
    this.clock = clock;
    this.delayedTaskSchedulerCallable = createDelayedTaskSchedulerCallable();
    try {
      this.conf = TezUtils.createConfFromUserPayload(taskSchedulerContext.getInitialUserPayload());
    } catch (IOException e) {
      throw new TezUncheckedException(
          "Failed to parse user payload for " + LlapTaskSchedulerService.class.getSimpleName(), e);
    }
    this.containerFactory = new ContainerFactory(taskSchedulerContext.getApplicationAttemptId(),
        taskSchedulerContext.getCustomClusterIdentifier());
    // TODO HIVE-13483 Get all of these properties from the registry. This will need to take care of different instances
    // publishing potentially different values when we support changing configurations dynamically.
    // For now, this can simply be fetched from a single registry instance.
    this.memoryPerInstance = HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB);
    this.coresPerInstance = HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_VCPUS_PER_INSTANCE);
    this.executorsPerInstance = HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_NUM_EXECUTORS);
    this.nodeBlacklistConf = new NodeBlacklistConf(
        HiveConf.getTimeVar(conf, ConfVars.LLAP_TASK_SCHEDULER_NODE_REENABLE_MIN_TIMEOUT_MS,
            TimeUnit.MILLISECONDS),
        HiveConf.getTimeVar(conf, ConfVars.LLAP_TASK_SCHEDULER_NODE_REENABLE_MAX_TIMEOUT_MS,
            TimeUnit.MILLISECONDS),
        HiveConf.getFloatVar(conf, ConfVars.LLAP_TASK_SCHEDULER_NODE_DISABLE_BACK_OFF_FACTOR));

    this.numSchedulableTasksPerNode =
        HiveConf.getIntVar(conf, ConfVars.LLAP_TASK_SCHEDULER_NUM_SCHEDULABLE_TASKS_PER_NODE);

    long localityDelayMs = HiveConf
        .getTimeVar(conf, ConfVars.LLAP_TASK_SCHEDULER_LOCALITY_DELAY, TimeUnit.MILLISECONDS);

    this.localityDelayConf = new LocalityDelayConf(localityDelayMs);

    this.timeoutMonitor = new SchedulerTimeoutMonitor();
    this.timeout = HiveConf.getTimeVar(conf,
        ConfVars.LLAP_DAEMON_TASK_SCHEDULER_TIMEOUT_SECONDS, TimeUnit.MILLISECONDS);
    this.timeoutExecutor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("LlapTaskSchedulerTimeoutMonitor")
            .build());
    this.timeoutFuture = null;

    int memoryPerExecutor = (int) (memoryPerInstance / (float) executorsPerInstance);
    int coresPerExecutor = (int) (coresPerInstance / (float) executorsPerInstance);
    this.resourcePerExecutor = Resource.newInstance(memoryPerExecutor, coresPerExecutor);

    String instanceId = HiveConf.getTrimmedVar(conf, ConfVars.LLAP_DAEMON_SERVICE_HOSTS);

    Preconditions.checkNotNull(instanceId, ConfVars.LLAP_DAEMON_SERVICE_HOSTS.varname
        + " must be defined");

    ExecutorService executorServiceRaw =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("LlapSchedulerNodeEnabler").build());
    nodeEnabledExecutor = MoreExecutors.listeningDecorator(executorServiceRaw);

    ExecutorService delayedTaskSchedulerExecutorRaw = Executors.newFixedThreadPool(1,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("LlapSchedulerDelayedTaskHandler")
            .build());
    delayedTaskSchedulerExecutor =
        MoreExecutors.listeningDecorator(delayedTaskSchedulerExecutorRaw);

    ExecutorService schedulerExecutorServiceRaw = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("LlapScheduler").build());
    schedulerExecutor = MoreExecutors.listeningDecorator(schedulerExecutorServiceRaw);

    if (initMetrics) {
      // Initialize the metrics system
      LlapMetricsSystem.initialize("LlapTaskScheduler");
      this.pauseMonitor = new JvmPauseMonitor(conf);
      pauseMonitor.start();
      String displayName = "LlapTaskSchedulerMetrics-" + MetricsUtils.getHostName();
      String sessionId = conf.get("llap.daemon.metrics.sessionid");
      // TODO: Not sure about the use of this. Should we instead use workerIdentity as sessionId?
      this.metrics = LlapTaskSchedulerMetrics.create(displayName, sessionId);
      this.metrics.setNumExecutors(executorsPerInstance);
      this.metrics.setMemoryPerInstance(memoryPerInstance * 1024L * 1024L);
      this.metrics.setCpuCoresPerInstance(coresPerExecutor);
      this.metrics.getJvmMetrics().setPauseMonitor(pauseMonitor);
    } else {
      this.metrics = null;
      this.pauseMonitor = null;
    }

    LOG.info("Running with configuration: " + "memoryPerInstance=" + memoryPerInstance
        + ", vCoresPerInstance=" + coresPerInstance + ", executorsPerInstance="
        + executorsPerInstance + ", resourcePerInstanceInferred=" + resourcePerExecutor
        + ", nodeBlacklistConf=" + nodeBlacklistConf
        + ", localityDelayMs=" + localityDelayMs);
  }

  @Override
  public void initialize() {
    registry.init(conf);
  }

  @Override
  public void start() throws IOException {
    writeLock.lock();
    try {
      nodeEnablerFuture = nodeEnabledExecutor.submit(nodeEnablerCallable);
      Futures.addCallback(nodeEnablerFuture, new LoggingFutureCallback("NodeEnablerThread", LOG));

      delayedTaskSchedulerFuture =
          delayedTaskSchedulerExecutor.submit(delayedTaskSchedulerCallable);
      Futures.addCallback(delayedTaskSchedulerFuture,
          new LoggingFutureCallback("DelayedTaskSchedulerThread", LOG));

      schedulerFuture = schedulerExecutor.submit(schedulerCallable);
      Futures.addCallback(schedulerFuture, new LoggingFutureCallback("SchedulerThread", LOG));

      registry.start();
      registry.registerStateChangeListener(new NodeStateChangeListener());
      activeInstances = registry.getInstances();
      for (ServiceInstance inst : activeInstances.getAll().values()) {
        addNode(inst, new NodeInfo(inst, nodeBlacklistConf, clock, numSchedulableTasksPerNode,
            metrics));
      }
    } finally {
      writeLock.unlock();
    }
  }

  private class NodeStateChangeListener implements ServiceInstanceStateChangeListener {
    private final Logger LOG = LoggerFactory.getLogger(NodeStateChangeListener.class);

    @Override
    public void onCreate(final ServiceInstance serviceInstance) {
      addNode(serviceInstance, new NodeInfo(serviceInstance, nodeBlacklistConf, clock,
          numSchedulableTasksPerNode, metrics));
      LOG.info("Added node with identity: {}", serviceInstance.getWorkerIdentity());
    }

    @Override
    public void onUpdate(final ServiceInstance serviceInstance) {
      instanceToNodeMap.put(serviceInstance.getWorkerIdentity(), new NodeInfo(serviceInstance,
          nodeBlacklistConf, clock, numSchedulableTasksPerNode, metrics));
      LOG.info("Updated node with identity: {}", serviceInstance.getWorkerIdentity());
    }

    @Override
    public void onRemove(final ServiceInstance serviceInstance) {
      // FIXME: disabling this for now
      // instanceToNodeMap.remove(serviceInstance.getWorkerIdentity());
      LOG.info("Removed node with identity: {}", serviceInstance.getWorkerIdentity());
      if (metrics != null) {
        metrics.setClusterNodeCount(activeInstances.size());
      }
      // if there are no more nodes. Signal timeout monitor to start timer
      if (activeInstances.size() == 0) {
        LOG.info("No node found. Signalling scheduler timeout monitor thread to start timer.");
        startTimeoutMonitor();
      }
    }
  }

  private void startTimeoutMonitor() {
    timeoutLock.lock();
    try {
      // If timer is null, start a new one.
      // If timer has completed during previous invocation, start a new one.
      // If timer already started and is not completed, leaving it running without resetting it.
      if ((timeoutFuture == null || (timeoutFuture != null && timeoutFuture.isDone()))
          && activeInstances.size() == 0) {
        timeoutFuture = timeoutExecutor.schedule(timeoutMonitor, timeout, TimeUnit.MILLISECONDS);
        LOG.info("Scheduled timeout monitor task to run after {} ms", timeout);
      } else {
        LOG.info("Timeout monitor task not started. Timeout future state: {}, #instances: {}",
            timeoutFuture == null ? "null" : timeoutFuture.isDone(), activeInstances.size());
      }
    } finally {
      timeoutLock.unlock();
    }
  }

  private void stopTimeoutMonitor() {
    timeoutLock.lock();
    try {
      if (timeoutFuture != null && activeInstances.size() != 0 && timeoutFuture.cancel(false)) {
        LOG.info("Stopped timeout monitor task");
      } else {
        LOG.info("Timeout monitor task not stopped. Timeout future state: {}, #instances: {}",
            timeoutFuture == null ? "null" : timeoutFuture.isDone(), activeInstances.size());
      }
      timeoutFuture = null;
    } finally {
      timeoutLock.unlock();
    }
  }

  @Override
  public void shutdown() {
    writeLock.lock();
    try {
      if (!this.isStopped.getAndSet(true)) {
        nodeEnablerCallable.shutdown();
        if (nodeEnablerFuture != null) {
          nodeEnablerFuture.cancel(true);
        }
        nodeEnabledExecutor.shutdownNow();

        timeoutExecutor.shutdown();
        if (timeoutFuture != null) {
          timeoutFuture.cancel(true);
          timeoutFuture = null;
        }
        timeoutExecutor.shutdownNow();

        delayedTaskSchedulerCallable.shutdown();
        if (delayedTaskSchedulerFuture != null) {
          delayedTaskSchedulerFuture.cancel(true);
        }
        delayedTaskSchedulerExecutor.shutdownNow();

        schedulerCallable.shutdown();
        if (schedulerFuture != null) {
          schedulerFuture.cancel(true);
        }
        schedulerExecutor.shutdownNow();

        if (registry != null) {
          registry.stop();
        }

        if (pauseMonitor != null) {
          pauseMonitor.stop();
        }

        if (metrics != null) {
          LlapMetricsSystem.shutdown();
        }

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
      int numInstancesFound = 0;
      for (ServiceInstance inst : activeInstances.getAll().values()) {
        if (inst.isAlive()) {
          Resource r = inst.getResource();
          memory += r.getMemory();
          vcores += r.getVirtualCores();
          numInstancesFound++;
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("GetTotalResources: numInstancesFound={}, totalMem={}, totalVcores={}",
            numInstancesFound, memory, vcores);
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
      for (Entry<String, NodeInfo> entry : instanceToNodeMap.entrySet()) {
        if (entry.getValue().getServiceInstance().isAlive() && !entry.getValue().isDisabled()) {
          Resource r = entry.getValue().getServiceInstance().getResource();
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
    if (metrics != null) {
      metrics.incrCompletedDagCount();
    }
    dagStats = new StatsPerDag();
  }

  @Override
  public void blacklistNode(NodeId nodeId) {
    LOG.info("BlacklistNode not supported");
    // TODO HIVE-13484 What happens when we try scheduling a task on a node that Tez at this point thinks is blacklisted.
  }

  @Override
  public void unblacklistNode(NodeId nodeId) {
    LOG.info("unBlacklistNode not supported");
  }

  @Override
  public void allocateTask(Object task, Resource capability, String[] hosts, String[] racks,
      Priority priority, Object containerSignature, Object clientCookie) {
    TaskInfo taskInfo =
        new TaskInfo(localityDelayConf, clock, task, clientCookie, priority, capability, hosts, racks, clock.getTime());
    LOG.info("Received allocateRequest. task={}, priority={}, capability={}, hosts={}", task,
        priority, capability, Arrays.toString(hosts));
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
        new TaskInfo(localityDelayConf, clock, task, clientCookie, priority, capability, null, null, clock.getTime());
    LOG.info("Received allocateRequest. task={}, priority={}, capability={}, containerId={}", task,
        priority, capability, containerId);
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
  public boolean deallocateTask(Object task, boolean taskSucceeded, TaskAttemptEndReason endReason, String diagnostics) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing deallocateTask for task={}, taskSucceeded={}, endReason={}", task,
          taskSucceeded, endReason);
    }
    writeLock.lock(); // Updating several local structures
    TaskInfo taskInfo;
    try {
      taskInfo = unregisterTask(task);
      if (taskInfo == null) {
        LOG.error("Could not determine ContainerId for task: "
            + task
            + " . Could have hit a race condition. Ignoring."
            + " The query may hang since this \"unknown\" container is now taking up a slot permanently");
        return false;
      }
      if (taskInfo.containerId == null) {
        if (taskInfo.getState() == TaskInfo.State.ASSIGNED) {
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

      NodeInfo nodeInfo = instanceToNodeMap.get(assignedInstance.getWorkerIdentity());
      assert nodeInfo != null;


      LOG.info("Processing de-allocate request for task={}, state={}, endReason={}", taskInfo.task,
          taskInfo.getState(), endReason);
      // Re-enable the node if preempted
      if (taskInfo.getState() == TaskInfo.State.PREEMPTED) {
        LOG.info("Processing deallocateTask for {} which was preempted, EndReason={}", task, endReason);
        unregisterPendingPreemption(taskInfo.assignedInstance.getHost());
        nodeInfo.registerUnsuccessfulTaskEnd(true);
        if (nodeInfo.isDisabled()) {
          // Re-enable the node. If a task succeeded, a slot may have become available.
          // Also reset commFailures since a task was able to communicate back and indicate success.
          nodeInfo.enableNode();
          // Re-insert into the queue to force the poll thread to remove the element.
          reinsertNodeInfo(nodeInfo);
        }
        // In case of success, trigger a scheduling run for pending tasks.
        trySchedulingPendingTasks();
      } else {
        if (taskSucceeded) {
          // The node may have been blacklisted at this point - which means it may not be in the
          // activeNodeList.

          nodeInfo.registerTaskSuccess();

          if (nodeInfo.isDisabled()) {
            // Re-enable the node. If a task succeeded, a slot may have become available.
            // Also reset commFailures since a task was able to communicate back and indicate success.
            nodeInfo.enableNode();
            // Re-insert into the queue to force the poll thread to remove the element.
            reinsertNodeInfo(nodeInfo);
          }
          // In case of success, trigger a scheduling run for pending tasks.
          trySchedulingPendingTasks();

        } else { // Task Failed
          nodeInfo.registerUnsuccessfulTaskEnd(false);
          if (endReason != null && EnumSet
              .of(TaskAttemptEndReason.EXECUTOR_BUSY, TaskAttemptEndReason.COMMUNICATION_ERROR)
              .contains(endReason)) {
            if (endReason == TaskAttemptEndReason.COMMUNICATION_ERROR) {
              dagStats.registerCommFailure(taskInfo.assignedInstance.getHost());
            } else if (endReason == TaskAttemptEndReason.EXECUTOR_BUSY) {
              dagStats.registerTaskRejected(taskInfo.assignedInstance.getHost());
            }
          }
          if (endReason != null && endReason == TaskAttemptEndReason.NODE_FAILED) {
            LOG.info(
                "Task {} ended on {} nodeInfo.toString() with a NODE_FAILED message." +
                    " An message should come in from the registry to disable this node unless" +
                    " this was a temporary communication failure",
                task, assignedInstance);
          }
          boolean commFailure =
              endReason != null && endReason == TaskAttemptEndReason.COMMUNICATION_ERROR;
          disableInstance(assignedInstance, commFailure);
        }
      }
    } finally {
      writeLock.unlock();
    }
    getContext().containerBeingReleased(taskInfo.containerId);
    return true;
  }

  private void reinsertNodeInfo(final NodeInfo nodeInfo) {
    if ( disabledNodesQueue.remove(nodeInfo)) {
      disabledNodesQueue.add(nodeInfo);
    }
    if (metrics != null) {
      metrics.setDisabledNodeCount(disabledNodesQueue.size());
    }
  }

  @Override
  public Object deallocateContainer(ContainerId containerId) {
    LOG.debug("Ignoring deallocateContainer for containerId: " + containerId);
    // Containers are not being tracked for re-use.
    // This is safe to ignore since a deallocate task will come in.
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

  /**
   * @param request the list of preferred hosts. null implies any host
   * @return
   */
  private SelectHostResult selectHost(TaskInfo request) {
    String[] requestedHosts = request.requestedHosts;
    if (LOG.isDebugEnabled()) {
      LOG.debug("selectingHost for task={} on hosts={}", request.task, Arrays.toString(requestedHosts));
    }
    long schedulerAttemptTime = clock.getTime();
    readLock.lock(); // Read-lock. Not updating any stats at the moment.
    try {
      // If there's no memory available, fail
      if (getTotalResources().getMemory() <= 0) {
        return SELECT_HOST_RESULT_INADEQUATE_TOTAL_CAPACITY;
      }

      boolean shouldDelayForLocality = request.shouldDelayForLocality(schedulerAttemptTime);
      LOG.debug("ShouldDelayForLocality={} for task={} on hosts={}", shouldDelayForLocality,
          request.task, Arrays.toString(requestedHosts));
      if (requestedHosts != null && requestedHosts.length > 0) {
        int prefHostCount = -1;
        boolean requestedHostsWillBecomeAvailable = false;
        for (String host : requestedHosts) {
          prefHostCount++;
          // Pick the first host always. Weak attempt at cache affinity.
          Set<ServiceInstance> instances = activeInstances.getByHost(host);
          if (!instances.isEmpty()) {
            for (ServiceInstance inst : instances) {
              NodeInfo nodeInfo = instanceToNodeMap.get(inst.getWorkerIdentity());
              if (nodeInfo != null) {
                if  (nodeInfo.canAcceptTask()) {
                  // Successfully scheduled.
                  LOG.info(
                      "Assigning " + nodeToString(inst, nodeInfo) + " when looking for " + host +
                          ". local=true" + " FirstRequestedHost=" + (prefHostCount == 0) +
                          (requestedHosts.length > 1 ? ", #prefLocations=" + requestedHosts.length :
                              ""));
                  return new SelectHostResult(inst, nodeInfo);
                } else {
                  // The node cannot accept a task at the moment.
                  if (shouldDelayForLocality) {
                    // Perform some checks on whether the node will become available or not.
                    if (request.shouldForceLocality()) {
                      requestedHostsWillBecomeAvailable = true;
                    } else {
                      if (nodeInfo.getEnableTime() > request.getLocalityDelayTimeout() &&
                          nodeInfo.isDisabled() && nodeInfo.hadCommFailure()) {
                        LOG.debug("Host={} will not become available within requested timeout", nodeInfo);
                        // This node will likely be activated after the task timeout expires.
                      } else {
                        // Worth waiting for the timeout.
                        requestedHostsWillBecomeAvailable = true;
                      }
                    }
                  }
                }
              } else {
                LOG.warn(
                    "Null NodeInfo when attempting to get host with worker identity {}, and host {}",
                    inst.getWorkerIdentity(), host);
                // Leave requestedHostWillBecomeAvailable as is. If some other host is found - delay,
                // else ends up allocating to a random host immediately.
              }
            }
          }
        }
        // Check if forcing the location is required.
        if (shouldDelayForLocality) {
          if (requestedHostsWillBecomeAvailable) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Delaying local allocation for [" + request.task +
                  "] when trying to allocate on [" + Arrays.toString(requestedHosts) + "]" +
                  ". ScheduleAttemptTime=" + schedulerAttemptTime + ", taskDelayTimeout=" +
                  request.getLocalityDelayTimeout());
            }
            return SELECT_HOST_RESULT_DELAYED_LOCALITY;
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Skipping local allocation for [" + request.task +
                  "] when trying to allocate on [" + Arrays.toString(requestedHosts) +
                  "] since none of these hosts are part of the known list");
            }
          }
        }
      }
      /* fall through - miss in locality (random scheduling) or no locality-requested */
      Collection<ServiceInstance> instances = activeInstances.getAll().values();
      ArrayList<NodeInfo> all = new ArrayList<>(instances.size());
      for (ServiceInstance inst : instances) {
        NodeInfo nodeInfo = instanceToNodeMap.get(inst.getWorkerIdentity());
        if (nodeInfo != null && nodeInfo.canAcceptTask()) {
          all.add(nodeInfo);
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Attempting random allocation for task={}", request.task);
      }
      if (all.isEmpty()) {
        return SELECT_HOST_RESULT_DELAYED_RESOURCES;
      }
      NodeInfo randomNode = all.get(random.nextInt(all.size()));
      LOG.info("Assigning " + nodeToString(randomNode.getServiceInstance(), randomNode)
          + " when looking for any host, from #hosts=" + all.size() + ", requestedHosts="
          + ((requestedHosts == null || requestedHosts.length == 0)
              ? "null" : Arrays.toString(requestedHosts)));
      return new SelectHostResult(randomNode.getServiceInstance(), randomNode);
    } finally {
      readLock.unlock();
    }
  }

  private void scanForNodeChanges() {
    /* check again whether nodes are disabled or just missing */
    writeLock.lock();
    try {
      for (ServiceInstance inst : activeInstances.getAll().values()) {
        if (inst.isAlive() && instanceToNodeMap.containsKey(inst.getWorkerIdentity()) == false) {
          /* that's a good node, not added to the allocations yet */
          LOG.info("Found a new node: " + inst + ".");
          addNode(inst, new NodeInfo(inst, nodeBlacklistConf, clock, numSchedulableTasksPerNode,
              metrics));
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void addNode(ServiceInstance inst, NodeInfo node) {
    LOG.info("Adding node: " + inst);
    // we have just added a new node. Signal timeout monitor to reset timer
    if (activeInstances.size() == 1) {
      LOG.info("New node added. Signalling scheduler timeout monitor thread to stop timer.");
      stopTimeoutMonitor();
    }
    instanceToNodeMap.put(inst.getWorkerIdentity(), node);
    if (metrics != null) {
      metrics.setClusterNodeCount(activeInstances.size());
    }
    // Trigger scheduling since a new node became available.
    trySchedulingPendingTasks();
  }

  private void reenableDisabledNode(NodeInfo nodeInfo) {
    writeLock.lock();
    try {
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
      NodeInfo nodeInfo = instanceToNodeMap.get(instance.getWorkerIdentity());
      if (nodeInfo == null || nodeInfo.isDisabled()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Node: " + instance + " already disabled, or invalid. Not doing anything.");
        }
      } else {
        nodeInfo.disableNode(isCommFailure);
        // TODO: handle task to container map events in case of hard failures
        disabledNodesQueue.add(nodeInfo);
        if (metrics != null) {
          metrics.setDisabledNodeCount(disabledNodesQueue.size());
        }
        // Trigger a scheduling run - in case there's some task which was waiting for this node to
        // become available.
        trySchedulingPendingTasks();
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
      // Delayed tasks will not kick in right now. That will happen in the scheduling loop.
      tasksAtPriority.add(taskInfo);
      knownTasks.putIfAbsent(taskInfo.task, taskInfo);
      if (metrics != null) {
        metrics.incrPendingTasksCount();
      }
      if (LOG.isInfoEnabled()) {
        LOG.info("PendingTasksInfo={}", constructPendingTaskCountsLogMessage());
      }
    } finally {
      writeLock.unlock();
    }
  }

  /* Remove a task from the pending list */
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

  /* Register a running task into the runningTasks structure */
  private void registerRunningTask(TaskInfo taskInfo) {
    writeLock.lock();
    try {
      int priority = taskInfo.priority.getPriority();
      TreeSet<TaskInfo> tasksAtpriority = runningTasks.get(priority);
      if (tasksAtpriority == null) {
        tasksAtpriority = new TreeSet<>(TASK_INFO_COMPARATOR);
        runningTasks.put(priority, tasksAtpriority);
      }
      tasksAtpriority.add(taskInfo);
      if (metrics != null) {
        metrics.decrPendingTasksCount();
      }
    } finally {
      writeLock.unlock();
    }
  }

  /* Unregister a task from the known and running structures */
  private TaskInfo unregisterTask(Object task) {
    writeLock.lock();
    try {
      TaskInfo taskInfo = knownTasks.remove(task);
      if (taskInfo != null) {
        if (taskInfo.getState() == TaskInfo.State.ASSIGNED) {
          // Remove from the running list.
          int priority = taskInfo.priority.getPriority();
          Set<TaskInfo> tasksAtPriority = runningTasks.get(priority);
          Preconditions.checkState(tasksAtPriority != null,
              "runningTasks should contain an entry if the task was in running state. Caused by task: {}", task);
          tasksAtPriority.remove(taskInfo);
          if (tasksAtPriority.isEmpty()) {
            runningTasks.remove(priority);
          }
        }
      } else {
        LOG.warn("Could not find TaskInfo for task: {}. Not removing it from the running set", task);
      }
      return taskInfo;
    } finally {
      writeLock.unlock();
    }
  }

  private enum ScheduleResult {
    // Successfully scheduled
    SCHEDULED,

    // Delayed to find a local match
    DELAYED_LOCALITY,

    // Delayed due to temporary resource availability
    DELAYED_RESOURCES,

    // Inadequate total resources - will never succeed / wait for new executors to become available
    INADEQUATE_TOTAL_RESOURCES,
  }


  @VisibleForTesting
  protected void schedulePendingTasks() {
    writeLock.lock();
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("ScheduleRun: {}", constructPendingTaskCountsLogMessage());
      }
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
          ScheduleResult scheduleResult = scheduleTask(taskInfo);
          LOG.info("ScheduleResult for Task: {} = {}", taskInfo, scheduleResult);
          if (scheduleResult == ScheduleResult.SCHEDULED) {
            taskIter.remove();
          } else {
            if (scheduleResult == ScheduleResult.INADEQUATE_TOTAL_RESOURCES) {
              LOG.info("Inadequate total resources before scheduling pending tasks." +
                  " Signalling scheduler timeout monitor thread to start timer.");
              startTimeoutMonitor();
              // TODO Nothing else should be done for this task. Move on.
            }

            // Try pre-empting a task so that a higher priority task can take it's place.
            // Preempt only if there's no pending preemptions to avoid preempting twice for a task.
            String[] potentialHosts;
            if (scheduleResult == ScheduleResult.DELAYED_LOCALITY) {

              // Add the task to the delayed task queue if it does not already exist.
              maybeAddToDelayedTaskQueue(taskInfo);

              // Try preempting a lower priority task in any case.
              // preempt only on specific hosts, if no preemptions already exist on those.
              potentialHosts = taskInfo.requestedHosts;
              //Protect against a bad location being requested.
              if (potentialHosts == null || potentialHosts.length == 0) {
                potentialHosts = null;
              }
            } else {
              // preempt on any host.
              potentialHosts = null;
            }

            // At this point we're dealing with all return types, except ScheduleResult.SCHEDULED.
            if (potentialHosts != null) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Attempting to preempt on requested host for task={}, potentialHosts={}",
                    taskInfo, Arrays.toString(potentialHosts));
              }
              // Preempt on specific host
              boolean shouldPreempt = true;
              for (String host : potentialHosts) {
                // Preempt only if there are not pending preemptions on the same host
                // When the premption registers, the request at the highest priority will be given the slot,
                // even if the initial request was for some other task.
                // TODO Maybe register which task the preemption was for, to avoid a bad non-local allocation.
                MutableInt pendingHostPreemptions = pendingPreemptionsPerHost.get(host);
                if (pendingHostPreemptions != null && pendingHostPreemptions.intValue() > 0) {
                  shouldPreempt = false;
                  LOG.debug(
                      "Not preempting for task={}. Found an existing preemption request on host={}, pendingPreemptionCount={}",
                      taskInfo.task, host, pendingHostPreemptions.intValue());
                  break;
                }
              }
              if (shouldPreempt) {
                LOG.debug("Preempting for {} on potential hosts={}. TotalPendingPreemptions={}",
                    taskInfo.task, Arrays.toString(potentialHosts), pendingPreemptions.get());
                preemptTasks(entry.getKey().getPriority(), 1, potentialHosts);
              } else {
                LOG.debug(
                    "Not preempting for {} on potential hosts={}. An existing preemption request exists",
                    taskInfo.task, Arrays.toString(potentialHosts));
              }
            } else { // Either DELAYED_RESOURCES or DELAYED_LOCALITY with an unknown requested host.
              // Request for a preemption if there's none pending. If a single preemption is pending,
              // and this is the next task to be assigned, it will be assigned once that slot becomes available.
              LOG.debug("Attempting to preempt on any host for task={}, pendingPreemptions={}",
                  taskInfo.task, pendingPreemptions.get());
              if (pendingPreemptions.get() == 0) {
                LOG.info("Preempting for task={} on any available host", taskInfo.task);
                preemptTasks(entry.getKey().getPriority(), 1, null);
              } else {
                if (LOG.isDebugEnabled()) {
                  LOG.debug(
                      "Skipping preemption since there are {} pending preemption request. For task={}",
                      pendingPreemptions.get(), taskInfo);
                }
              }
            }
            // Since there was an allocation failure - don't try assigning tasks at the next priority.

            scheduledAllAtPriority = false;
            // Don't break if this allocation failure was a result of a LOCALITY_DELAY. Others could still be allocated.
            if (scheduleResult != ScheduleResult.DELAYED_LOCALITY) {
              break;
            }
          } // end of else - i.e. could not allocate
        } // end of loop over pending tasks
        if (taskListAtPriority.isEmpty()) {
          // Remove the entry, if there's nothing left at the specific priority level
          pendingIterator.remove();
        }
        if (!scheduledAllAtPriority) {
          LOG.debug(
              "Unable to schedule all requests at priority={}. Skipping subsequent priority levels",
              entry.getKey());
          // Don't attempt scheduling for additional priorities
          break;
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  private String constructPendingTaskCountsLogMessage() {
    StringBuilder sb = new StringBuilder();
    int totalCount = 0;
    sb.append("numPriorityLevels=").append(pendingTasks.size()).append(". ");
    Iterator<Entry<Priority, List<TaskInfo>>> pendingIterator =
        pendingTasks.entrySet().iterator();
    while (pendingIterator.hasNext()) {
      Entry<Priority, List<TaskInfo>> entry = pendingIterator.next();
      int count = entry.getValue() == null ? 0 : entry.getValue().size();
      sb.append("[p=").append(entry.getKey().toString()).append(",c=").append(count).append("]");
      totalCount += count;
    }
    sb.append(". totalPendingTasks=").append(totalCount);
    sb.append(". delayedTaskQueueSize=").append(delayedTaskQueue.size());
    return sb.toString();
  }

  private ScheduleResult scheduleTask(TaskInfo taskInfo) {
    SelectHostResult selectHostResult = selectHost(taskInfo);
    if (selectHostResult.scheduleResult == ScheduleResult.SCHEDULED) {
      NodeServiceInstancePair nsPair = selectHostResult.nodeServiceInstancePair;
      Container container =
          containerFactory.createContainer(resourcePerExecutor, taskInfo.priority,
              nsPair.getServiceInstance().getHost(),
              nsPair.getServiceInstance().getRpcPort(),
              nsPair.getServiceInstance().getServicesAddress());
      writeLock.lock(); // While updating local structures
      try {
        LOG.info("Assigned task {} to container {} on node={}", taskInfo, container.getId(),
            nsPair.getServiceInstance());
        dagStats.registerTaskAllocated(taskInfo.requestedHosts, taskInfo.requestedRacks,
            nsPair.getServiceInstance().getHost());
        taskInfo.setAssignmentInfo(nsPair.getServiceInstance(), container.getId(), clock.getTime());
        registerRunningTask(taskInfo);
        nsPair.getNodeInfo().registerTaskScheduled();
      } finally {
        writeLock.unlock();
      }
      getContext().taskAllocated(taskInfo.task, taskInfo.clientCookie, container);
    }
    return selectHostResult.scheduleResult;
  }

  // Removes tasks from the runningList and sends out a preempt request to the system.
  // Subsequent tasks will be scheduled again once the de-allocate request for the preempted
  // task is processed.
  private void preemptTasks(int forPriority, int numTasksToPreempt, String []potentialHosts) {
    Set<String> preemptHosts;
    if (potentialHosts == null) {
      preemptHosts = null;
    } else {
      preemptHosts = Sets.newHashSet(potentialHosts);
    }
    writeLock.lock();
    List<TaskInfo> preemptedTaskList = null;
    try {
      NavigableMap<Integer, TreeSet<TaskInfo>> orderedMap = runningTasks.descendingMap();
      Iterator<Entry<Integer, TreeSet<TaskInfo>>> iterator = orderedMap.entrySet().iterator();
      int preemptedCount = 0;
      while (iterator.hasNext() && preemptedCount < numTasksToPreempt) {
        Entry<Integer, TreeSet<TaskInfo>> entryAtPriority = iterator.next();
        if (entryAtPriority.getKey() > forPriority) {
          Iterator<TaskInfo> taskInfoIterator = entryAtPriority.getValue().iterator();
          while (taskInfoIterator.hasNext() && preemptedCount < numTasksToPreempt) {
            TaskInfo taskInfo = taskInfoIterator.next();
            if (preemptHosts == null || preemptHosts.contains(taskInfo.assignedInstance.getHost())) {
              // Candidate for preemption.
              preemptedCount++;
              LOG.info("preempting {} for task at priority {} with potentialHosts={}", taskInfo,
                  forPriority, potentialHosts == null ? "" : Arrays.toString(potentialHosts));
              taskInfo.setPreemptedInfo(clock.getTime());
              if (preemptedTaskList == null) {
                preemptedTaskList = new LinkedList<>();
              }
              dagStats.registerTaskPreempted(taskInfo.assignedInstance.getHost());
              preemptedTaskList.add(taskInfo);
              registerPendingPreemption(taskInfo.assignedInstance.getHost());
              // Remove from the runningTaskList
              taskInfoIterator.remove();
            }
          }

          // Remove entire priority level if it's been emptied.
          if (entryAtPriority.getValue().isEmpty()) {
            iterator.remove();
          }
        } else {
          // No tasks qualify as preemptable
          LOG.info("No tasks qualify as killable to schedule tasks at priority {}", forPriority);
          break;
        }
      }
    } finally {
      writeLock.unlock();
    }
    // Send out the preempted request outside of the lock.
    if (preemptedTaskList != null) {
      for (TaskInfo taskInfo : preemptedTaskList) {
        LOG.info("Preempting task {}", taskInfo);
        getContext().preemptContainer(taskInfo.containerId);
        // Preemption will finally be registered as a deallocateTask as a result of preemptContainer
        // That resets preemption info and allows additional tasks to be pre-empted if required.
      }
    }
    // The schedule loop will be triggered again when the deallocateTask request comes in for the
    // preempted task.
  }

  private void registerPendingPreemption(String host) {
    writeLock.lock();
    try {
      pendingPreemptions.incrementAndGet();
      if (metrics != null) {
        metrics.incrPendingPreemptionTasksCount();
      }
      MutableInt val = pendingPreemptionsPerHost.get(host);
      if (val == null) {
        val = new MutableInt(0);
        pendingPreemptionsPerHost.put(host, val);
      }
      val.increment();
    } finally {
      writeLock.unlock();
    }
  }

  private void unregisterPendingPreemption(String host) {
    writeLock.lock();
    try {
      pendingPreemptions.decrementAndGet();
      if (metrics != null) {
        metrics.decrPendingPreemptionTasksCount();
      }
      MutableInt val = pendingPreemptionsPerHost.get(host);
      Preconditions.checkNotNull(val);
      val.decrement();
      // Not bothering with removing the entry. There's a limited number of hosts, and a good
      // chance that the entry will make it back in when the AM is used for a long duration.
    } finally {
      writeLock.unlock();
    }
  }

  private void maybeAddToDelayedTaskQueue(TaskInfo taskInfo) {
    // There's no point adding a task with forceLocality set - since that will never exit the queue.
    // Add other tasks if they are not already in the queue.
    if (!taskInfo.shouldForceLocality() && !taskInfo.isInDelayedQueue()) {
      taskInfo.setInDelayedQueue(true);
      delayedTaskQueue.add(taskInfo);
    }
  }

  private String nodeToString(ServiceInstance serviceInstance, NodeInfo nodeInfo) {
    return serviceInstance.getHost() + ":" + serviceInstance.getRpcPort() + ", workerIdentity=" +
        serviceInstance.getWorkerIdentity() + ", webAddress=" +
        serviceInstance.getServicesAddress() + ", currentlyScheduledTasksOnNode=" + nodeInfo.numScheduledTasks;
  }



  // ------ Inner classes defined after this point ------

  @VisibleForTesting
  class DelayedTaskSchedulerCallable implements Callable<Void> {

    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    @Override
    public Void call() {
      while (!isShutdown.get() && !Thread.currentThread().isInterrupted()) {
        try {
          TaskInfo taskInfo = getNextTask();
          taskInfo.setInDelayedQueue(false);
          // Tasks can exist in the delayed queue even after they have been scheduled.
          // Trigger scheduling only if the task is still in PENDING state.
          processEvictedTask(taskInfo);

        } catch (InterruptedException e) {
          if (isShutdown.get()) {
            LOG.info("DelayedTaskScheduler thread interrupted after shutdown");
            break;
          } else {
            LOG.warn("DelayedTaskScheduler thread interrupted before being shutdown");
            throw new RuntimeException(
                "DelayedTaskScheduler thread interrupted without being shutdown", e);
          }
        }
      }
      return null;
    }

    public void shutdown() {
      isShutdown.set(true);
    }

    public TaskInfo getNextTask() throws InterruptedException {
      return delayedTaskQueue.take();
    }

    public void processEvictedTask(TaskInfo taskInfo) {
      if (shouldScheduleTask(taskInfo)) {
        trySchedulingPendingTasks();
      }
    }

    public boolean shouldScheduleTask(TaskInfo taskInfo) {
      return taskInfo.getState() == TaskInfo.State.PENDING;
    }
  }

  @VisibleForTesting
  DelayedTaskSchedulerCallable createDelayedTaskSchedulerCallable() {
    return new DelayedTaskSchedulerCallable();
  }

  private class NodeEnablerCallable implements Callable<Void> {

    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private static final long REFRESH_INTERVAL = 10000l;
    long nextPollInterval = REFRESH_INTERVAL;
    long lastRefreshTime;

    @Override
    public Void call() {

      lastRefreshTime = LlapTaskSchedulerService.this.clock.getTime();
      while (!isShutdown.get() && !Thread.currentThread().isInterrupted()) {
        try {
          while (true) {
            NodeInfo nodeInfo = disabledNodesQueue.poll(nextPollInterval, TimeUnit.MILLISECONDS);
            if (nodeInfo != null) {
              long currentTime =  LlapTaskSchedulerService.this.clock.getTime();
              // A node became available. Enable the node and try scheduling.
              reenableDisabledNode(nodeInfo);
              trySchedulingPendingTasks();

              nextPollInterval -= (currentTime - lastRefreshTime);
            }

            if (nextPollInterval < 0 || nodeInfo == null) {
              // timeout expired. Reset the poll interval and refresh nodes.
              nextPollInterval = REFRESH_INTERVAL;
              lastRefreshTime = LlapTaskSchedulerService.this.clock.getTime();
              // TODO Get rid of this polling once we have notificaitons from the registry sub-system
              if (LOG.isDebugEnabled()) {
                LOG.debug("Refreshing instances based on poll interval");
              }
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
      pendingScheduleInvocations.set(true);
      scheduleCondition.signal();
    } finally {
      scheduleLock.unlock();
    }
  }

  private class SchedulerTimeoutMonitor implements Runnable {
    private final Logger LOG = LoggerFactory.getLogger(SchedulerTimeoutMonitor.class);

    @Override
    public void run() {
      LOG.info("Reporting SERVICE_UNAVAILABLE error as no instances are running");
      try {
        getContext().reportError(ServicePluginErrorDefaults.SERVICE_UNAVAILABLE,
            "No LLAP Daemons are running", getContext().getCurrentDagInfo());
      } catch (Exception e) {
        LOG.error("Exception when reporting SERVICE_UNAVAILABLE error for dag: {}",
            getContext().getCurrentDagInfo().getName(), e);
      }
    }
  }

  private class SchedulerCallable implements Callable<Void> {
    private AtomicBoolean isShutdown = new AtomicBoolean(false);

    @Override
    public Void call() {
      while (!isShutdown.get() && !Thread.currentThread().isInterrupted()) {
        scheduleLock.lock();
        try {
          while (!pendingScheduleInvocations.get()) {
            scheduleCondition.await();
          }
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

        // Set pending to false since scheduling is about to run. Any triggers up to this point
        // will be handled in the next run.
        // A new request may come in right after this is set to false, but before the actual scheduling.
        // This will be handled in this run, but will cause an immediate run after, which is harmless.
        // This is mainly to handle a trySchedue request while in the middle of a run - since the event
        // which triggered it may not be processed for all tasks in the run.
        pendingScheduleInvocations.set(false);
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

  // ------ Additional static classes defined after this point ------

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
    // This is primarily for tracking and logging purposes for the moment.
    // TODO At some point, treat task rejection and communication failures differently.
    private boolean hadCommFailure = false;

    // Indicates whether a node is disabled - for whatever reason - commFailure, busy, etc.
    private boolean disabled = false;

    private int numPreemptedTasks = 0;
    private int numScheduledTasks = 0;
    private final int numSchedulableTasks;
    private final LlapTaskSchedulerMetrics metrics;

    /**
     * Create a NodeInfo bound to a service instance
     *  @param serviceInstance         the associated serviceInstance
     * @param blacklistConf           blacklist configuration
     * @param clock                   clock to use to obtain timing information
     * @param numSchedulableTasksConf number of schedulable tasks on the node. 0 represents auto
*                                detect based on the serviceInstance, -1 indicates indicates
     * @param metrics
     */
    NodeInfo(ServiceInstance serviceInstance, NodeBlacklistConf blacklistConf, Clock clock,
        int numSchedulableTasksConf, final LlapTaskSchedulerMetrics metrics) {
      Preconditions.checkArgument(numSchedulableTasksConf >= -1, "NumSchedulableTasks must be >=-1");
      this.serviceInstance = serviceInstance;
      this.blacklistConf = blacklistConf;
      this.clock = clock;
      this.metrics = metrics;

      if (numSchedulableTasksConf == 0) {
        int pendingQueueuCapacity = 0;
        String pendingQueueCapacityString = serviceInstance.getProperties()
            .get(ConfVars.LLAP_DAEMON_TASK_SCHEDULER_WAIT_QUEUE_SIZE.varname);
        LOG.info("Setting up node: {} with available capacity={}, pendingQueueSize={}, memory={}",
            serviceInstance, serviceInstance.getResource().getVirtualCores(),
            pendingQueueCapacityString, serviceInstance.getResource().getMemory());
        if (pendingQueueCapacityString != null) {
          pendingQueueuCapacity = Integer.parseInt(pendingQueueCapacityString);
        }
        this.numSchedulableTasks = serviceInstance.getResource().getVirtualCores() + pendingQueueuCapacity;
      } else {
        this.numSchedulableTasks = numSchedulableTasksConf;
        LOG.info("Setting up node: " + serviceInstance + " with schedulableCapacity=" + this.numSchedulableTasks);
      }
      if (metrics != null) {
        metrics.incrSchedulableTasksCount(numSchedulableTasks);
      }

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
        LOG.info("Disabling instance {} for {} milli-seconds. commFailure={}", serviceInstance,
            delayTime, commFailure);
      }
      expireTimeMillis = currentTime + delayTime;
      numSuccessfulTasksAtLastBlacklist = numSuccessfulTasks;
    }

    void registerTaskScheduled() {
      numScheduledTasks++;
      if (metrics != null) {
        metrics.incrRunningTasksCount();
        metrics.decrSchedulableTasksCount();
      }
    }

    void registerTaskSuccess() {
      numSuccessfulTasks++;
      numScheduledTasks--;
      if (metrics != null) {
        metrics.incrSuccessfulTasksCount();
        metrics.decrRunningTasksCount();
        metrics.incrSchedulableTasksCount();
      }
    }

    void registerUnsuccessfulTaskEnd(boolean wasPreempted) {
      numScheduledTasks--;
      if (metrics != null) {
        metrics.decrRunningTasksCount();
        metrics.incrSchedulableTasksCount();
      }
      if (wasPreempted) {
        numPreemptedTasks++;
        if (metrics != null) {
          metrics.incrPreemptedTasksCount();
        }
      }
    }

    /**
     * @return the time at which this node will be re-enabled
     */
    public long getEnableTime() {
      return expireTimeMillis;
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
      if (LOG.isInfoEnabled()) {
        LOG.info("Node[" + serviceInstance.getHost() + ":" + serviceInstance.getRpcPort() + ", " +
                serviceInstance.getWorkerIdentity() + "]: " +
                "canAcceptTask={}, numScheduledTasks={}, numSchedulableTasks={}, hadCommFailure={}, disabled={}, serviceInstance.isAlive={}",
            result, numScheduledTasks, numSchedulableTasks, hadCommFailure, disabled,
            serviceInstance.isAlive());
      }
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
    int numTotalAllocations = 0;
    int numLocalAllocations = 0;
    int numNonLocalAllocations = 0;
    int numAllocationsNoLocalityRequest = 0;
    int numRejectedTasks = 0;
    int numCommFailures = 0;
    int numDelayedAllocations = 0;
    int numPreemptedTasks = 0;
    Map<String, AtomicInteger> localityBasedNumAllocationsPerHost = new HashMap<>();
    Map<String, AtomicInteger> numAllocationsPerHost = new HashMap<>();

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("NumPreemptedTasks=").append(numPreemptedTasks).append(", ");
      sb.append("NumRequestedAllocations=").append(numRequestedAllocations).append(", ");
      sb.append("NumRequestsWithlocation=").append(numRequestsWithLocation).append(", ");
      sb.append("NumLocalAllocations=").append(numLocalAllocations).append(",");
      sb.append("NumNonLocalAllocations=").append(numNonLocalAllocations).append(",");
      sb.append("NumTotalAllocations=").append(numTotalAllocations).append(",");
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
      numTotalAllocations++;
      _registerAllocationInHostMap(allocatedHost, numAllocationsPerHost);
    }

    void registerTaskPreempted(String host) {
      numPreemptedTasks++;
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


  // TODO There needs to be a mechanism to figure out different attempts for the same task. Delays
  // could potentially be changed based on this.

  @VisibleForTesting
  static class TaskInfo implements Delayed {

    enum State {
      PENDING, ASSIGNED, PREEMPTED
    }

    // IDs used to ensure two TaskInfos are different without using the underlying task instance.
    // Required for insertion into a TreeMap
    static final AtomicLong ID_GEN = new AtomicLong(0);
    final long uniqueId;
    final LocalityDelayConf localityDelayConf;
    final Clock clock;
    final Object task;
    final Object clientCookie;
    final Priority priority;
    final Resource capability;
    final String[] requestedHosts;
    final String[] requestedRacks;
    final long requestTime;
    final long localityDelayTimeout;
    long startTime;
    long preemptTime;
    ContainerId containerId;
    ServiceInstance assignedInstance;
    private State state = State.PENDING;
    boolean inDelayedQueue = false;

    private int numAssignAttempts = 0;

    // TaskInfo instances for two different tasks will not be the same. Only a single instance should
    // ever be created for a taskAttempt
    public TaskInfo(LocalityDelayConf localityDelayConf, Clock clock, Object task, Object clientCookie, Priority priority, Resource capability,
        String[] hosts, String[] racks, long requestTime) {
      this.localityDelayConf = localityDelayConf;
      this.clock = clock;
      this.task = task;
      this.clientCookie = clientCookie;
      this.priority = priority;
      this.capability = capability;
      this.requestedHosts = hosts;
      this.requestedRacks = racks;
      this.requestTime = requestTime;
      if (localityDelayConf.getNodeLocalityDelay() == -1) {
        localityDelayTimeout = Long.MAX_VALUE;
      } else if (localityDelayConf.getNodeLocalityDelay() == 0) {
        localityDelayTimeout = 0L;
      } else {
        localityDelayTimeout = requestTime + localityDelayConf.getNodeLocalityDelay();
      }
      this.uniqueId = ID_GEN.getAndIncrement();
    }

    synchronized void setAssignmentInfo(ServiceInstance instance, ContainerId containerId, long startTime) {
      this.assignedInstance = instance;
      this.containerId = containerId;
      this.startTime = startTime;
      this.state = State.ASSIGNED;
    }

    synchronized void setPreemptedInfo(long preemptTime) {
      this.state = State.PREEMPTED;
      this.preemptTime = preemptTime;
    }

    synchronized void setInDelayedQueue(boolean val) {
      this.inDelayedQueue = val;
    }

    synchronized void triedAssigningTask() {
      numAssignAttempts++;
    }

    synchronized int getNumPreviousAssignAttempts() {
      return numAssignAttempts;
    }

    synchronized State getState() {
      return state;
    }

    synchronized boolean isInDelayedQueue() {
      return inDelayedQueue;
    }

    boolean shouldDelayForLocality(long schedulerAttemptTime) {
      // getDelay <=0 means the task will be evicted from the queue.
      return localityDelayTimeout > schedulerAttemptTime;
    }

    boolean shouldForceLocality() {
      return localityDelayTimeout == Long.MAX_VALUE;
    }

    long getLocalityDelayTimeout() {
      return localityDelayTimeout;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TaskInfo taskInfo = (TaskInfo) o;

      if (uniqueId != taskInfo.uniqueId) {
        return false;
      }
      return task.equals(taskInfo.task);

    }

    @Override
    public int hashCode() {
      int result = (int) (uniqueId ^ (uniqueId >>> 32));
      result = 31 * result + task.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "TaskInfo{" +
          "task=" + task +
          ", priority=" + priority +
          ", startTime=" + startTime +
          ", containerId=" + containerId +
          ", assignedInstance=" + assignedInstance +
          ", uniqueId=" + uniqueId +
          ", localityDelayTimeout=" + localityDelayTimeout +
          '}';
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(localityDelayTimeout - clock.getTime(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
      TaskInfo other = (TaskInfo) o;
      if (other.localityDelayTimeout > this.localityDelayTimeout) {
        return -1;
      } else if (other.localityDelayTimeout < this.localityDelayTimeout) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  // Newer tasks first.
  private static class TaskStartComparator implements Comparator<TaskInfo> {

    @Override
    public int compare(TaskInfo o1, TaskInfo o2) {
      if (o1.startTime > o2.startTime) {
        return -1;
      } else if (o1.startTime < o2.startTime) {
        return 1;
      } else {
        // Comparing on time is not sufficient since two may be created at the same time,
        // in which case inserting into a TreeSet/Map would break
        if (o1.uniqueId > o2.uniqueId) {
          return -1;
        } else if (o1.uniqueId < o2.uniqueId) {
          return 1;
        } else {
          return 0;
        }
      }
    }
  }

  private static class SelectHostResult {
    final NodeServiceInstancePair nodeServiceInstancePair;
    final ScheduleResult scheduleResult;

    SelectHostResult(ServiceInstance serviceInstance, NodeInfo nodeInfo) {
      this.nodeServiceInstancePair = new NodeServiceInstancePair(serviceInstance, nodeInfo);
      this.scheduleResult = ScheduleResult.SCHEDULED;
    }

    SelectHostResult(ScheduleResult scheduleResult) {
      this.nodeServiceInstancePair = null;
      this.scheduleResult = scheduleResult;
    }
  }

  private static final SelectHostResult SELECT_HOST_RESULT_INADEQUATE_TOTAL_CAPACITY =
      new SelectHostResult(ScheduleResult.INADEQUATE_TOTAL_RESOURCES);
  private static final SelectHostResult SELECT_HOST_RESULT_DELAYED_LOCALITY =
      new SelectHostResult(ScheduleResult.DELAYED_LOCALITY);
  private static final SelectHostResult SELECT_HOST_RESULT_DELAYED_RESOURCES =
      new SelectHostResult(ScheduleResult.DELAYED_RESOURCES);

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

  @VisibleForTesting
  static final class LocalityDelayConf {
    private final long nodeLocalityDelay;

    public LocalityDelayConf(long nodeLocalityDelay) {
      this.nodeLocalityDelay = nodeLocalityDelay;
    }

    public long getNodeLocalityDelay() {
      return nodeLocalityDelay;
    }

    @Override
    public String toString() {
      return "LocalityDelayConf{" +
          "nodeLocalityDelay=" + nodeLocalityDelay +
          '}';
    }
  }
}
