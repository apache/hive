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

package org.apache.hadoop.hive.llap.daemon.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.hive.llap.counters.FragmentCountersMap;
import org.apache.hadoop.hive.llap.daemon.SchedulerFragmentCompletingListener;
import org.apache.hadoop.hive.llap.protocol.LlapTaskUmbilicalProtocol;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.RuntimeTask;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.api.events.TaskAttemptCompletedEvent;
import org.apache.tez.runtime.api.events.TaskAttemptFailedEvent;
import org.apache.tez.runtime.api.events.TaskAttemptKilledEvent;
import org.apache.tez.runtime.api.events.TaskStatusUpdateEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.runtime.api.impl.TaskStatistics;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.TezHeartbeatRequest;
import org.apache.tez.runtime.api.impl.TezHeartbeatResponse;
import org.apache.tez.runtime.internals.api.TaskReporterInterface;
import org.apache.tez.runtime.task.ErrorReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Responsible for communication between tasks running in a Container and the ApplicationMaster.
 * Takes care of sending heartbeats (regular and OOB) to the AM - to send generated events, and to
 * retrieve events specific to this task.
 *
 */
public class LlapTaskReporter implements TaskReporterInterface {

  private static final Logger LOG = LoggerFactory.getLogger(LlapTaskReporter.class);
  private final LlapTaskUmbilicalProtocol umbilical;
  private final long pollInterval;
  private final long sendCounterInterval;
  private final int maxEventsToGet;
  private final AtomicLong requestCounter;
  private final String containerIdStr;
  private final String fragmentId;
  private final TezEvent initialEvent;
  private final SchedulerFragmentCompletingListener completionListener;
  // The same id as reported by TaskRunnerCallable.getRequestId
  private final String fragmentRequestId;

  private final ListeningExecutorService heartbeatExecutor;

  @VisibleForTesting
  HeartbeatCallable currentCallable;

  public LlapTaskReporter(SchedulerFragmentCompletingListener completionListener, LlapTaskUmbilicalProtocol umbilical, long amPollInterval,
                      long sendCounterInterval, int maxEventsToGet, AtomicLong requestCounter,
      String containerIdStr, final String fragmentId, TezEvent initialEvent,
                          String fragmentRequestId) {
    this.umbilical = umbilical;
    this.pollInterval = amPollInterval;
    this.sendCounterInterval = sendCounterInterval;
    this.maxEventsToGet = maxEventsToGet;
    this.requestCounter = requestCounter;
    this.containerIdStr = containerIdStr;
    this.fragmentId = fragmentId;
    this.initialEvent = initialEvent;
    ExecutorService executor = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("TaskHeartbeatThread").build());
    heartbeatExecutor = MoreExecutors.listeningDecorator(executor);
    this.completionListener = completionListener;
    this.fragmentRequestId = fragmentRequestId;
  }

  /**
   * Register a task to be tracked. Heartbeats will be sent out for this task to fetch events, etc.
   */
  @Override
  public synchronized void registerTask(RuntimeTask task,
                                        ErrorReporter errorReporter) {
    TezCounters tezCounters = task.addAndGetTezCounter(fragmentId);
    FragmentCountersMap.registerCountersForFragment(fragmentId, tezCounters);
    LOG.info("Registered counters for fragment: {} vertexName: {}", fragmentId, task.getVertexName());
    currentCallable = new HeartbeatCallable(completionListener, task, umbilical, pollInterval, sendCounterInterval,
        maxEventsToGet, requestCounter, containerIdStr, initialEvent, fragmentRequestId);
    ListenableFuture<Boolean> future = heartbeatExecutor.submit(currentCallable);
    Futures.addCallback(future, new HeartbeatCallback(errorReporter));
  }

  /**
   * This method should always be invoked before setting up heartbeats for another task running in
   * the same container.
   */
  @Override
  public synchronized void unregisterTask(TezTaskAttemptID taskAttemptID) {
    LOG.info("Unregistered counters for fragment: {}", fragmentId);
    FragmentCountersMap.unregisterCountersForFragment(fragmentId);
    currentCallable.markComplete();
    currentCallable = null;
  }

  @Override
  public void shutdown() {
    heartbeatExecutor.shutdownNow();
  }

  @VisibleForTesting
  static class HeartbeatCallable implements Callable<Boolean> {

    private static final int LOG_COUNTER_START_INTERVAL = 5000; // 5 seconds
    private static final float LOG_COUNTER_BACKOFF = 1.3f;

    private final RuntimeTask task;
    private final EventMetaData updateEventMetadata;
    private final SchedulerFragmentCompletingListener completionListener;
    private final String fragmentRequestId;

    private final LlapTaskUmbilicalProtocol umbilical;

    private final long pollInterval;
    private final long sendCounterInterval;
    private final int maxEventsToGet;
    private final String containerIdStr;

    private final AtomicLong requestCounter;

    private final AtomicBoolean finalEventQueued = new AtomicBoolean(false);
    private final AtomicBoolean askedToDie = new AtomicBoolean(false);

    private LinkedBlockingQueue<TezEvent> eventsToSend = new LinkedBlockingQueue<TezEvent>();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    /*
     * Keeps track of regular timed heartbeats. Is primarily used as a timing mechanism to send /
     * log counters.
     */
    private AtomicInteger nonOobHeartbeatCounter = new AtomicInteger(0);
    private int nextHeartbeatNumToLog = 0;
    /*
     * Tracks the last non-OOB heartbeat number at which counters were sent to the AM. 
     */
    private int prevCounterSendHeartbeatNum = 0;
    private TezEvent initialEvent;

    public HeartbeatCallable(
        SchedulerFragmentCompletingListener completionListener,
        RuntimeTask task, LlapTaskUmbilicalProtocol umbilical,
        long amPollInterval, long sendCounterInterval, int maxEventsToGet,
        AtomicLong requestCounter, String containerIdStr,
        TezEvent initialEvent, String fragmentRequestId) {

      this.pollInterval = amPollInterval;
      this.sendCounterInterval = sendCounterInterval;
      this.maxEventsToGet = maxEventsToGet;
      this.requestCounter = requestCounter;
      this.containerIdStr = containerIdStr;
      this.initialEvent = initialEvent;
      this.completionListener = completionListener;
      this.fragmentRequestId = fragmentRequestId;

      this.task = task;
      this.umbilical = umbilical;
      this.updateEventMetadata = new EventMetaData(EventProducerConsumerType.SYSTEM,
          task.getVertexName(), "", task.getTaskAttemptID());

      nextHeartbeatNumToLog = (Math.max(1,
          (int) (LOG_COUNTER_START_INTERVAL / (amPollInterval == 0 ? 0.000001f
              : (float) amPollInterval))));
    }

    @Override
    public Boolean call() throws Exception {
      // Heartbeat only for active tasks. Errors, etc will be reported directly.
      while (!task.isTaskDone() && !task.wasErrorReported()) {
        ResponseWrapper response = heartbeat(null);

        if (response.shouldDie) {
          // AM sent a shouldDie=true
          LOG.info("Asked to die via task heartbeat: {}", task.getTaskAttemptID());
          return false;
        } else {
          if (response.numEvents < maxEventsToGet) {
            // Wait before sending another heartbeat. Otherwise consider as an OOB heartbeat
            lock.lock();
            try {
              boolean interrupted = condition.await(pollInterval, TimeUnit.MILLISECONDS);
              if (!interrupted) {
                nonOobHeartbeatCounter.incrementAndGet();
              }
            } finally {
              lock.unlock();
            }
          }
        }
      }
      int pendingEventCount = eventsToSend.size();
      if (pendingEventCount > 0) {
        // This is OK because the pending events will be sent via the succeeded/failed messages.
        // TaskDone is set before taskSucceeded/taskTerminated are sent out - which is what causes the
        // thread to exit
        LOG.warn("Exiting TaskReporter thread with pending queue size=" + pendingEventCount);
      }
      return true;
    }

    /**
     * @param eventsArg
     * @return
     * @throws IOException
     *           indicates an RPC communication failure.
     * @throws TezException
     *           indicates an exception somewhere in the AM.
     */
    private synchronized ResponseWrapper heartbeat(Collection<TezEvent> eventsArg) throws IOException,
        TezException {

      if (eventsArg != null) {
        eventsToSend.addAll(eventsArg);
      }

      TezEvent updateEvent = null;
      List<TezEvent> events = new ArrayList<TezEvent>();
      eventsToSend.drainTo(events);

      if (!task.isTaskDone() && !task.wasErrorReported()) {
        boolean sendCounters = false;
        /**
         * Increasing the heartbeat interval can delay the delivery of events. Sending just updated
         * records would save CPU in DAG AM, but certain counters are updated very frequently. Until
         * real time decisions are made based on these counters, it can be sent once per second.
         */
        // Not completely accurate, since OOB heartbeats could go out.
        if ((nonOobHeartbeatCounter.get() - prevCounterSendHeartbeatNum) * pollInterval >= sendCounterInterval) {
          sendCounters = true;
          prevCounterSendHeartbeatNum = nonOobHeartbeatCounter.get();
        }
        updateEvent = new TezEvent(getStatusUpdateEvent(sendCounters), updateEventMetadata);
        events.add(updateEvent);
      }

      long requestId = requestCounter.incrementAndGet();
      int fromEventId = task.getNextFromEventId();
      int fromPreRoutedEventId = task.getNextPreRoutedEventId();
      int maxEvents = Math.min(maxEventsToGet, task.getMaxEventsToHandle());
      TezHeartbeatRequest request = new TezHeartbeatRequest(requestId, events, fromPreRoutedEventId,
          containerIdStr, task.getTaskAttemptID(), fromEventId, maxEvents);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sending heartbeat to AM, request=" + request);
      }

      maybeLogCounters();

      TezHeartbeatResponse response = umbilical.heartbeat(request);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received heartbeat response from AM, response=" + response);
      }

      if (response.shouldDie()) {
        LOG.info("Received should die response from AM: {}", task.getTaskAttemptID());
        askedToDie.set(true);
        return new ResponseWrapper(true, 1);
      }
      if (response.getLastRequestId() != requestId) {
        throw new TezException("AM and Task out of sync" + ", responseReqId="
            + response.getLastRequestId() + ", expectedReqId=" + requestId);
      }

      // The same umbilical is used by multiple tasks. Problematic in the case where multiple tasks
      // are running using the same umbilical.
      int numEventsReceived = 0;
      if (task.isTaskDone() || task.wasErrorReported()) {
        if (response.getEvents() != null && !response.getEvents().isEmpty()) {
          LOG.warn("Current task already complete, Ignoring all event in"
              + " heartbeat response, eventCount=" + response.getEvents().size());
        }
      } else {
        task.setNextFromEventId(response.getNextFromEventId());
        task.setNextPreRoutedEventId(response.getNextPreRoutedEventId());
        List<TezEvent> taskEvents = null;
        if (response.getEvents() != null && !response.getEvents().isEmpty()) {
          if (LOG.isInfoEnabled()) {
            LOG.info("Routing events from heartbeat response to task" + ", currentTaskAttemptId="
                + task.getTaskAttemptID() + ", eventCount=" + response.getEvents().size()
                + " fromEventId=" + fromEventId
                + " nextFromEventId=" + response.getNextFromEventId());
          }
          // This should ideally happen in a separate thread
          numEventsReceived = response.getEvents().size();
          taskEvents = response.getEvents();
        }
        if (initialEvent != null) {
          // We currently only give the initial event to the task on the first heartbeat. Given
          // that the split is ready, it seems pointless to wait, but that's how Tez works.
          List<TezEvent> oldEvents = taskEvents;
          taskEvents = new ArrayList<>(1 + (taskEvents == null ? 0 : taskEvents.size()));
          taskEvents.add(initialEvent);
          initialEvent = null;
          if (oldEvents != null) {
            taskEvents.addAll(oldEvents);
          }
        }
        if (taskEvents != null) {
          task.handleEvents(taskEvents);
        }
      }
      return new ResponseWrapper(false, numEventsReceived);
    }

    public void markComplete() {
      // Notify to clear pending events, if any.
      lock.lock();
      try {
        condition.signal();
      } finally {
        lock.unlock();
      }
    }

    private void maybeLogCounters() {
      if (LOG.isDebugEnabled()) {
        if (nonOobHeartbeatCounter.get() == nextHeartbeatNumToLog) {
          LOG.debug("Counters: " + task.getCounters().toShortString());
          nextHeartbeatNumToLog = (int) (nextHeartbeatNumToLog * (LOG_COUNTER_BACKOFF));
        }
      }
    }

    /**
     * Sends out final events for task success.
     * @param taskAttemptID
     * @return
     * @throws IOException
     *           indicates an RPC communication failure.
     * @throws TezException
     *           indicates an exception somewhere in the AM.
     */
    private boolean taskSucceeded(TezTaskAttemptID taskAttemptID) throws IOException, TezException {
      // Ensure only one final event is ever sent.
      if (!finalEventQueued.getAndSet(true)) {
        TezEvent statusUpdateEvent = new TezEvent(getStatusUpdateEvent(true), updateEventMetadata);
        TezEvent taskCompletedEvent = new TezEvent(new TaskAttemptCompletedEvent(),
            updateEventMetadata);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Invoking OOB heartbeat for successful attempt: {}, isTaskDone={}", taskAttemptID, task.isTaskDone());
        }
        completionListener.fragmentCompleting(fragmentRequestId, SchedulerFragmentCompletingListener.State.SUCCESS);
        return !heartbeat(Lists.newArrayList(statusUpdateEvent, taskCompletedEvent)).shouldDie;
      } else {
        LOG.warn("A final task state event has already been sent. Not sending again");
        return askedToDie.get();
      }
    }

    private TaskStatusUpdateEvent getStatusUpdateEvent(boolean sendCounters) {
      TezCounters counters = null;
      TaskStatistics stats = null;
      float progress = 0;
      if (task.hasInitialized()) {
        progress = task.getProgress();
        // TODO HIVE-12449. Make use of progress notifications once Hive starts sending them out.
        // progressNotified = task.getAndClearProgressNotification();
        if (sendCounters) {
          // send these potentially large objects at longer intervals to avoid overloading the AM
          counters = task.getCounters();
          stats = task.getTaskStatistics();
        }
      }
      return new TaskStatusUpdateEvent(counters, progress, stats, true);
    }

    /**
     * Sends out final events for task failure.
     * @param taskAttemptID
     * @param isKilled
     * @param taskFailureType
     * @param t
     * @param diagnostics
     * @param srcMeta
     * @return
     * @throws IOException
     *           indicates an RPC communication failure.
     * @throws TezException
     *           indicates an exception somewhere in the AM.
     */
    private boolean taskTerminated(TezTaskAttemptID taskAttemptID, boolean isKilled,
                               TaskFailureType taskFailureType, Throwable t, String diagnostics,
                               EventMetaData srcMeta) throws IOException, TezException {
      // Ensure only one final event is ever sent.
      if (!finalEventQueued.getAndSet(true)) {
        List<TezEvent> tezEvents = new ArrayList<>();
        if (diagnostics == null) {
          diagnostics = ExceptionUtils.getStackTrace(t);
        } else {
          diagnostics = diagnostics + ":" + ExceptionUtils.getStackTrace(t);
        }

        if (isKilled) {
          tezEvents.add(new TezEvent(new TaskAttemptKilledEvent(diagnostics),
              srcMeta == null ? updateEventMetadata : srcMeta));
        } else {
          tezEvents.add(new TezEvent(new TaskAttemptFailedEvent(diagnostics,
              taskFailureType),
              srcMeta == null ? updateEventMetadata : srcMeta));
        }
        try {
          tezEvents.add(new TezEvent(getStatusUpdateEvent(true), updateEventMetadata));
        } catch (Exception e) {
          // Counter may exceed limitation
          LOG.warn("Error when get constructing TaskStatusUpdateEvent. Not sending it out");
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Invoking OOB heartbeat for failed/killed attempt: {}, isTaskDone={}, isKilled={}",
              taskAttemptID, task.isTaskDone(), isKilled);
        }
        completionListener.fragmentCompleting(fragmentRequestId,
            isKilled ? SchedulerFragmentCompletingListener.State.KILLED :
                SchedulerFragmentCompletingListener.State.FAILED);
        return !heartbeat(tezEvents).shouldDie;
      } else {
        LOG.warn("A final task state event has already been sent. Not sending again");
        return askedToDie.get();
      }

    }

    private void addEvents(TezTaskAttemptID taskAttemptID, Collection<TezEvent> events) {
      if (events != null && !events.isEmpty()) {
        eventsToSend.addAll(events);
      }
    }
  }

  private static class HeartbeatCallback implements FutureCallback<Boolean> {

    private final ErrorReporter errorReporter;

    HeartbeatCallback(ErrorReporter errorReporter) {
      this.errorReporter = errorReporter;
    }

    @Override
    public void onSuccess(Boolean result) {
      if (result == false) {
        errorReporter.shutdownRequested();
      }
    }

    @Override
    public void onFailure(Throwable t) {
      errorReporter.reportError(t);
    }
  }

  public synchronized boolean taskSucceeded(TezTaskAttemptID taskAttemptID) throws IOException, TezException {
    return currentCallable.taskSucceeded(taskAttemptID);
  }

  @Override
  public boolean taskFailed(TezTaskAttemptID tezTaskAttemptID, TaskFailureType taskFailureType,
                            Throwable throwable, String diagnostics, EventMetaData srcMeta) throws
      IOException, TezException {
    return currentCallable
        .taskTerminated(tezTaskAttemptID, false, taskFailureType, throwable, diagnostics, srcMeta);
  }

  @Override
  public boolean taskKilled(TezTaskAttemptID tezTaskAttemptID, Throwable throwable,
                            String diagnostics,
                            EventMetaData srcMeta) throws IOException, TezException {
    return currentCallable
        .taskTerminated(tezTaskAttemptID, true, null, throwable, diagnostics, srcMeta);
  }

  @Override
  public synchronized void addEvents(TezTaskAttemptID taskAttemptID, Collection<TezEvent> events) {
    currentCallable.addEvents(taskAttemptID, events);
  }

  @Override
  public boolean canCommit(TezTaskAttemptID taskAttemptID) throws IOException {
    return umbilical.canCommit(taskAttemptID);
  }

  private static final class ResponseWrapper {
    boolean shouldDie;
    int numEvents;

    private ResponseWrapper(boolean shouldDie, int numEvents) {
      this.shouldDie = shouldDie;
      this.numEvents = numEvents;
    }
  }
}
