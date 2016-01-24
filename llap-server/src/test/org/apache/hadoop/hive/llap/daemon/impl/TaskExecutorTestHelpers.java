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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.daemon.FragmentCompletionHandler;
import org.apache.hadoop.hive.llap.daemon.KilledTaskHandler;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.FragmentSpecProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorMetrics;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.hadoop.shim.DefaultHadoopShim;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.task.EndReason;
import org.apache.tez.runtime.task.TaskRunner2Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskExecutorTestHelpers {

  private static final Logger LOG = LoggerFactory.getLogger(TestTaskExecutorService.class);

  public static MockRequest createMockRequest(int fragmentNum, int parallelism, long startTime,
                                              boolean canFinish, long workTime) {
    SubmitWorkRequestProto
        requestProto = createSubmitWorkRequestProto(fragmentNum, parallelism,
        startTime);
    QueryFragmentInfo queryFragmentInfo = createQueryFragmentInfo(requestProto.getFragmentSpec());
    MockRequest mockRequest = new MockRequest(requestProto, queryFragmentInfo, canFinish, workTime);
    return mockRequest;
  }

  public static TaskExecutorService.TaskWrapper createTaskWrapper(
      SubmitWorkRequestProto request, boolean canFinish, int workTime) {
    QueryFragmentInfo queryFragmentInfo = createQueryFragmentInfo(request.getFragmentSpec());
    MockRequest mockRequest = new MockRequest(request, queryFragmentInfo, canFinish, workTime);
    TaskExecutorService.TaskWrapper
        taskWrapper = new TaskExecutorService.TaskWrapper(mockRequest, null);
    return taskWrapper;
  }

  public static QueryFragmentInfo createQueryFragmentInfo(FragmentSpecProto fragmentSpecProto) {
    QueryInfo queryInfo = createQueryInfo();
    QueryFragmentInfo fragmentInfo =
        new QueryFragmentInfo(queryInfo, "fakeVertexName", fragmentSpecProto.getFragmentNumber(), 0,
            fragmentSpecProto);
    return fragmentInfo;
  }

  public static QueryInfo createQueryInfo() {
    QueryIdentifier queryIdentifier = new QueryIdentifier("fake_app_id_string", 1);
    QueryInfo queryInfo =
        new QueryInfo(queryIdentifier, "fake_app_id_string", "fake_dag_name", 1, "fakeUser",
            new ConcurrentHashMap<String, LlapDaemonProtocolProtos.SourceStateProto>(),
            new String[0], null);
    return queryInfo;
  }

  public static SubmitWorkRequestProto createSubmitWorkRequestProto(
      int fragmentNumber, int selfAndUpstreamParallelism,
      long attemptStartTime) {
    return createSubmitWorkRequestProto(fragmentNumber, selfAndUpstreamParallelism, 0,
        attemptStartTime, 1);
  }

  public static SubmitWorkRequestProto createSubmitWorkRequestProto(
      int fragmentNumber, int selfAndUpstreamParallelism,
      int selfAndUpstreamComplete,
      long attemptStartTime, int withinDagPriority) {
    ApplicationId appId = ApplicationId.newInstance(9999, 72);
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID vId = TezVertexID.getInstance(dagId, 35);
    TezTaskID tId = TezTaskID.getInstance(vId, 389);
    TezTaskAttemptID taId = TezTaskAttemptID.getInstance(tId, fragmentNumber);
    return SubmitWorkRequestProto
        .newBuilder()
        .setFragmentSpec(
            FragmentSpecProto
                .newBuilder()
                .setAttemptNumber(0)
                .setDagName("MockDag")
                .setFragmentNumber(fragmentNumber)
                .setVertexName("MockVertex")
                .setProcessorDescriptor(
                    LlapDaemonProtocolProtos.EntityDescriptorProto.newBuilder()
                        .setClassName("MockProcessor").build())
                .setFragmentIdentifierString(taId.toString()).build()).setAmHost("localhost")
        .setAmPort(12345).setAppAttemptNumber(0).setApplicationIdString("MockApp_1")
        .setContainerIdString("MockContainer_1").setUser("MockUser")
        .setTokenIdentifier("MockToken_1")
        .setFragmentRuntimeInfo(LlapDaemonProtocolProtos
            .FragmentRuntimeInfo
            .newBuilder()
            .setFirstAttemptStartTime(attemptStartTime)
            .setNumSelfAndUpstreamTasks(selfAndUpstreamParallelism)
            .setNumSelfAndUpstreamCompletedTasks(selfAndUpstreamComplete)
            .setWithinDagPriority(withinDagPriority)
            .build())
        .build();
  }

  public static class MockRequest extends TaskRunnerCallable {
    private final long workTime;
    private final boolean canFinish;

    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private final AtomicBoolean isFinished = new AtomicBoolean(false);
    private final AtomicBoolean wasKilled = new AtomicBoolean(false);
    private final AtomicBoolean wasInterrupted = new AtomicBoolean(false);

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition startedCondition = lock.newCondition();
    private final Condition sleepCondition = lock.newCondition();
    private boolean shouldSleep = true;
    private final Condition finishedCondition = lock.newCondition();

    public MockRequest(SubmitWorkRequestProto requestProto, QueryFragmentInfo fragmentInfo,
                       boolean canFinish, long workTime) {
      super(requestProto, fragmentInfo, new Configuration(),
          new ExecutionContextImpl("localhost"), null, new Credentials(), 0, null, null, mock(
              LlapDaemonExecutorMetrics.class),
          mock(KilledTaskHandler.class), mock(
              FragmentCompletionHandler.class), new DefaultHadoopShim());
      this.workTime = workTime;
      this.canFinish = canFinish;
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
            sleepCondition.await(workTime, TimeUnit.MILLISECONDS);
          }
        } catch (InterruptedException e) {
          wasInterrupted.set(true);
          return new TaskRunner2Result(EndReason.KILL_REQUESTED, null, false);
        } finally {
          lock.unlock();
        }
        if (wasKilled.get()) {
          return new TaskRunner2Result(EndReason.KILL_REQUESTED, null, false);
        } else {
          return new TaskRunner2Result(EndReason.SUCCESS, null, false);
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
  }

  private static void logInfo(String message, Throwable t) {
    LOG.info(message, t);
  }

  private static void logInfo(String message) {
    logInfo(message, null);
  }

}
