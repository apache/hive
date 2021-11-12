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

import static org.mockito.Mockito.mock;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.LlapNodeId;
import org.apache.hadoop.hive.llap.daemon.FragmentCompletionHandler;
import org.apache.hadoop.hive.llap.daemon.KilledTaskHandler;
import org.apache.hadoop.hive.llap.daemon.SchedulerFragmentCompletingListener;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryIdentifierProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SignableVertexSpec;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.VertexOrBinary;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorMetrics;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.hadoop.shim.DefaultHadoopShim;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.task.EndReason;
import org.apache.tez.runtime.task.TaskRunner2Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;

public class TaskExecutorTestHelpers {

  private static final Logger LOG = LoggerFactory.getLogger(TestTaskExecutorService.class);

  public static MockRequest createMockRequest(int fragmentNum, int parallelism, long firstAttemptStartTime,
    long currentAttemptStartTime, boolean canFinish, long workTime, boolean isGuaranteed) {
    SubmitWorkRequestProto request = createSubmitWorkRequestProto(
        fragmentNum, parallelism, firstAttemptStartTime, currentAttemptStartTime, isGuaranteed);
    return createMockRequest(canFinish, canFinish, workTime, request, isGuaranteed);
  }

  public static MockRequest createMockRequest(int fragmentNum, int parallelism, long firstAttemptStartTime,
      long currentAttemptStartTime, boolean canFinish, long workTime, boolean isGuaranteed, int dagId) {
    SubmitWorkRequestProto request = createSubmitWorkRequestProto(fragmentNum, parallelism, 0,
        firstAttemptStartTime, currentAttemptStartTime, 1, "MockDag", dagId, isGuaranteed);
    return createMockRequest(canFinish, canFinish, workTime, request, isGuaranteed);
  }

  public static MockRequest createMockRequest(int fragmentNum, int parallelism,
                                              int withinDagPriority,
                                              long firstAttemptStartTime,
                                              long currentAttemptStartTime,
                                              boolean canFinish,
                                              long workTime,
                                              boolean isGuaranteed) {
    SubmitWorkRequestProto
        request = createSubmitWorkRequestProto(fragmentNum, parallelism, 0,
        firstAttemptStartTime, currentAttemptStartTime, withinDagPriority, isGuaranteed);
    return createMockRequest(canFinish, canFinish, workTime, request, isGuaranteed);
  }

  private static MockRequest createMockRequest(boolean canFinish, boolean canFinishQueue,
      long workTime, SubmitWorkRequestProto request, boolean isGuaranteed) {
    QueryFragmentInfo queryFragmentInfo = createQueryFragmentInfo(
        request.getWorkSpec().getVertex(), request.getFragmentNumber());
    return new MockRequest(request, queryFragmentInfo, canFinish, canFinishQueue,
        workTime, null, isGuaranteed);
  }

  public static TaskExecutorService.TaskWrapper createTaskWrapper(SubmitWorkRequestProto request,
      boolean canFinish, boolean canFinishQueue, int workTime) {
    return new TaskExecutorService.TaskWrapper(createMockRequest(
        canFinish, canFinishQueue, workTime, request, request.getIsGuaranteed()), null);
  }

  public static TaskExecutorService.TaskWrapper createTaskWrapper(
      SubmitWorkRequestProto request, boolean canFinish, int workTime) {
    return createTaskWrapper(request, canFinish, canFinish, workTime);
  }

  public static QueryFragmentInfo createQueryFragmentInfo(
      SignableVertexSpec vertex, int fragmentNum) {
    return new QueryFragmentInfo(createQueryInfo(), "fakeVertexName", fragmentNum, 0, vertex, "");
  }

  public static QueryInfo createQueryInfo() {
    QueryIdentifier queryIdentifier = new QueryIdentifier("fake_app_id_string", 1);
    LlapNodeId nodeId = LlapNodeId.getInstance("localhost", 0);
    QueryInfo queryInfo =
        new QueryInfo(queryIdentifier, "fake_app_id_string", "fake_dag_id_string", "fake_dag_name",
            "fakeHiveQueryId", 1, "fakeUser",
            new ConcurrentHashMap<String, LlapDaemonProtocolProtos.SourceStateProto>(),
            new String[0], null, "fakeUser", null, nodeId, null, null, false, null);
    return queryInfo;
  }

  public static SubmitWorkRequestProto createSubmitWorkRequestProto(
      int fragmentNumber, int selfAndUpstreamParallelism, long firstAttemptStartTime,
      long currentAttemptStartTime, boolean isGuaranteed) {
    return createSubmitWorkRequestProto(fragmentNumber, selfAndUpstreamParallelism, 0, firstAttemptStartTime,
      currentAttemptStartTime, 1, isGuaranteed);
  }

  public static SubmitWorkRequestProto createSubmitWorkRequestProto(
      int fragmentNumber, int selfAndUpstreamParallelism, long firstAttemptStartTime,
      long currentAttemptStartTime, String dagName) {
    return createSubmitWorkRequestProto(fragmentNumber, selfAndUpstreamParallelism, 0, firstAttemptStartTime,
        currentAttemptStartTime, 1, dagName, false);
  }

  public static SubmitWorkRequestProto createSubmitWorkRequestProto(
      int fragmentNumber, int selfAndUpstreamParallelism, long firstAttemptStartTime,
      long currentAttemptStartTime, String dagName, boolean isGuaranteed) {
    return createSubmitWorkRequestProto(fragmentNumber, selfAndUpstreamParallelism, 0, firstAttemptStartTime,
        currentAttemptStartTime, 1, dagName, isGuaranteed);
  }

  public static SubmitWorkRequestProto createSubmitWorkRequestProto(
      int fragmentNumber, int selfAndUpstreamParallelism,
      int selfAndUpstreamComplete, long firstAttemptStartTime,
      long currentAttemptStartTime, int withinDagPriority) {
    return createSubmitWorkRequestProto(fragmentNumber, selfAndUpstreamParallelism, selfAndUpstreamComplete, firstAttemptStartTime,
        currentAttemptStartTime, withinDagPriority, "MockDag", false);
  }

  public static SubmitWorkRequestProto createSubmitWorkRequestProto(
      int fragmentNumber, int selfAndUpstreamParallelism,
      int selfAndUpstreamComplete, long firstAttemptStartTime,
      long currentAttemptStartTime, int withinDagPriority, boolean isGuaranteed) {
    return createSubmitWorkRequestProto(fragmentNumber, selfAndUpstreamParallelism, 0, firstAttemptStartTime,
        currentAttemptStartTime, withinDagPriority, "MockDag", isGuaranteed);
  }

  private static SubmitWorkRequestProto createSubmitWorkRequestProto(
      int fragmentNumber, int selfAndUpstreamParallelism, int selfAndUpstreamComplete, long firstAttemptStartTime,
      long currentAttemptStartTime, int withinDagPriority, String mockDag, boolean isGuaranteed) {
    return createSubmitWorkRequestProto(fragmentNumber, selfAndUpstreamParallelism, selfAndUpstreamComplete, firstAttemptStartTime,
        currentAttemptStartTime, withinDagPriority, mockDag, 35, isGuaranteed);
  }

  public static SubmitWorkRequestProto createSubmitWorkRequestProto(
      int fragmentNumber, int selfAndUpstreamParallelism,
      int selfAndUpstreamComplete, long firstAttemptStartTime,
      long currentAttemptStartTime, int withinDagPriority, String dagName, int dagId,
      boolean isGuaranteed) {
    ApplicationId appId = ApplicationId.newInstance(9999, 72);
    TezDAGID dag = TezDAGID.getInstance(appId, 1);
    TezVertexID vId = TezVertexID.getInstance(dag, dagId);
    return SubmitWorkRequestProto
        .newBuilder()
        .setAttemptNumber(0)
        .setFragmentNumber(fragmentNumber)
        .setWorkSpec(
            VertexOrBinary.newBuilder().setVertex(
            SignableVertexSpec.newBuilder()
                .setDagName(dagName)
                .setHiveQueryId(dagName)
                .setUser("MockUser")
                .setTokenIdentifier("MockToken_1")
                .setQueryIdentifier(
                    QueryIdentifierProto.newBuilder()
                        .setApplicationIdString(appId.toString())
                        .setAppAttemptNumber(0)
                        .setDagIndex(dag.getId())
                        .build())
                .setVertexIndex(vId.getId())
                .setVertexName("MockVertex")
                .setProcessorDescriptor(
                    LlapDaemonProtocolProtos.EntityDescriptorProto.newBuilder()
                        .setClassName("MockProcessor").build())
                .build()).build())
        .setAmHost("localhost")
        .setAmPort(12345)
        .setContainerIdString("MockContainer_1")
        .setIsGuaranteed(isGuaranteed)
        .setFragmentRuntimeInfo(LlapDaemonProtocolProtos
            .FragmentRuntimeInfo
            .newBuilder()
            .setFirstAttemptStartTime(firstAttemptStartTime)
            .setCurrentAttemptStartTime(currentAttemptStartTime)
            .setNumSelfAndUpstreamTasks(selfAndUpstreamParallelism)
            .setNumSelfAndUpstreamCompletedTasks(selfAndUpstreamComplete)
            .setWithinDagPriority(withinDagPriority)
            .build())
        .build();
  }

  public static class MockRequest extends TaskRunnerCallable {
    private final long workTime;
    private final boolean canFinish;
    private boolean canUpdateFinishable = false; // Many old tests depend on this.
    private boolean canFinishQueue;

    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private final AtomicBoolean isFinished = new AtomicBoolean(false);
    private final AtomicBoolean wasKilled = new AtomicBoolean(false);
    private final AtomicBoolean wasInterrupted = new AtomicBoolean(false);

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition startedCondition = lock.newCondition();
    private final Condition sleepCondition = lock.newCondition();
    private boolean shouldSleep = true;
    private final Condition finishedCondition = lock.newCondition();
    private final Object killDelay = new Object();
    private boolean isOkToFinish = true;

    public MockRequest(SubmitWorkRequestProto requestProto, QueryFragmentInfo fragmentInfo,
                       boolean canFinish, boolean canFinishQueue, long workTime,
                       TezEvent initialEvent, boolean isGuaranteed) {
      super(requestProto, fragmentInfo, Configuration::new, new ExecutionContextImpl("localhost"),
          null, new Credentials(), 0, mock(AMReporter.class), null, mock(
          LlapDaemonExecutorMetrics.class), mock(KilledTaskHandler.class), mock(
          FragmentCompletionHandler.class), new DefaultHadoopShim(), null,
          requestProto.getWorkSpec().getVertex(), initialEvent, null, mock(
          SchedulerFragmentCompletingListener.class), mock(SocketFactory.class), isGuaranteed, null);
      this.workTime = workTime;
      this.canFinish = canFinish;
      this.canFinishQueue = canFinishQueue;
    }

    public void setCanUpdateFinishable() {
      this.canUpdateFinishable = true;
    }

    @Override
    protected TaskRunner2Result callInternal() {
      try {
        logInfo(super.getRequestId() + " is executing..", null);
        lock.lock();
        try {
          isStarted.set(true);
          startedCondition.signal();
        } finally {
          lock.unlock();
        }

        lock.lock();
        try {
          if (shouldSleep) {
            logInfo(super.getRequestId() + " is sleeping for " + workTime, null);
            sleepCondition.await(workTime, TimeUnit.MILLISECONDS);
          }
        } catch (InterruptedException e) {
          wasInterrupted.set(true);
          return handleKill();
        } finally {
          lock.unlock();
        }
        if (wasKilled.get()) {
          return handleKill();
        } else {
          logInfo(super.getRequestId() + " succeeded", null);
          return new TaskRunner2Result(EndReason.SUCCESS, null, null, false);
        }
      } finally {
        lock.lock();
        try {
          isFinished.set(true);
          finishedCondition.signal();
        } finally {
          lock.unlock();
        }
      }
    }

    private TaskRunner2Result handleKill() {
      boolean hasLogged = false;
      while (true) {
        synchronized (killDelay) {
          if (isOkToFinish) break;
          if (!hasLogged) {
            logInfo("Waiting after the kill: " + getRequestId());
            hasLogged = true;
          }
          try {
            killDelay.wait(100);
          } catch (InterruptedException e) {
          }
        }
      }
      logInfo("Finished with the kill: " + getRequestId());
      return new TaskRunner2Result(EndReason.KILL_REQUESTED, null, null, false);
    }

    public void unblockKill() {
      synchronized (killDelay) {
        logInfo("Unblocking the kill: " + getRequestId());
        isOkToFinish = true;
        killDelay.notifyAll();
      }
    }

    @Override
    public void killTask() {
      lock.lock();
      try {
        wasKilled.set(true);
        shouldSleep = false;
        sleepCondition.signal();
      } finally {
        lock.unlock();
      }
    }

    boolean hasStarted() {
      return isStarted.get();
    }

    boolean hasFinished() {
      return isFinished.get();
    }

    boolean wasPreempted() {
      return wasKilled.get();
    }

    void complete() {
      lock.lock();
      try {
        shouldSleep = false;
        sleepCondition.signal();
      } finally {
        lock.unlock();
      }
    }

    void awaitStart() throws InterruptedException {
      lock.lock();
      try {
        while (!isStarted.get()) {
          startedCondition.await();
        }
      } finally {
        lock.unlock();
      }
    }

    void awaitEnd() throws InterruptedException {
      lock.lock();
      try {
        while (!isFinished.get()) {
          finishedCondition.await();
        }
      } finally {
        lock.unlock();
      }
    }


    @Override
    public boolean canFinish() {
      return canFinish;
    }

    @Override
    public void updateCanFinishForPriority(boolean value) {
      super.updateCanFinishForPriority(value);
      // Note: scheduler will call this based on lack of sources at schedule time and set this
      //       to true... there's no easy way to work around this. Need better classes
      if (this.canUpdateFinishable) {
        this.canFinishQueue = value;
      }
    }

    @Override
    public boolean canFinishForPriority() {
      return canFinishQueue;
    }

    public void setSleepAfterKill() {
      isOkToFinish = false;
    }
  }

  private static void logInfo(String message, Throwable t) {
    LOG.info(message, t);
  }

  private static void logInfo(String message) {
    logInfo(message, null);
  }


}
