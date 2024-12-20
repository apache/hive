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
package org.apache.hadoop.hive.ql.scheduled;

import java.io.Closeable;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.QueryState;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryKey;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollResponse;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryProgressInfo;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class ScheduledQueryExecutionService implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ScheduledQueryExecutionService.class);

  private static ScheduledQueryExecutionService INSTANCE = null;

  private ScheduledQueryExecutionContext context;
  private AtomicInteger forcedScheduleCheckCounter = new AtomicInteger();
  private AtomicInteger usedExecutors = new AtomicInteger(0);
  private Queue<ScheduledQueryExecutor> runningExecutors = new ConcurrentLinkedQueue<>();

  public static ScheduledQueryExecutionService startScheduledQueryExecutorService(HiveConf inputConf) {
    HiveConf conf = new HiveConf(inputConf);
    MetastoreBasedScheduledQueryService qService = new MetastoreBasedScheduledQueryService(conf);
    ExecutorService executor = buildExecutor(conf);
    ScheduledQueryExecutionContext ctx = new ScheduledQueryExecutionContext(executor, conf, qService);
    return startScheduledQueryExecutorService(ctx);
  }

  private static ExecutorService buildExecutor(HiveConf conf) {
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Scheduled Query Thread %d").build();
    int systemThreads = 2; // poller,reporter
    int minServiceThreads = 1; // always keep 1 thread to be used for executing scheduled queries
    int maxServiceThreads = conf.getIntVar(ConfVars.HIVE_SCHEDULED_QUERIES_MAX_EXECUTORS);
    return new ThreadPoolExecutor(systemThreads + minServiceThreads, systemThreads + maxServiceThreads,
        60L, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        threadFactory);
  }

  public static ScheduledQueryExecutionService startScheduledQueryExecutorService(ScheduledQueryExecutionContext ctx) {
    synchronized (ScheduledQueryExecutionService.class) {
      if (INSTANCE != null) {
        throw new IllegalStateException(
            "There is already a ScheduledQueryExecutionService in service; check it and close it explicitly if necessary");
      }
      INSTANCE = new ScheduledQueryExecutionService(ctx);
      return INSTANCE;
    }
  }

  private ScheduledQueryExecutionService(ScheduledQueryExecutionContext ctx) {
    context = ctx;
    ctx.executor.submit(new ScheduledQueryPoller());
    ctx.executor.submit(new ProgressReporter());
  }

  static boolean isTerminalState(QueryState state) {
    return state == QueryState.FINISHED || state == QueryState.FAILED;
  }

  /**
   * Renames the {@link Thread} to make it more clear what it is working on.
   */
  static class NamedThread implements Closeable {
    private final String oldName;

    public NamedThread(String newName) {
      LOG.info("Starting {} thread - renaming accordingly.", newName);
      oldName = Thread.currentThread().getName();
      Thread.currentThread().setName(newName);
    }

    @Override
    public void close() {
      LOG.info("Thread finished; renaming back to: {}", oldName);
      Thread.currentThread().setName(oldName);
    }
  }

  /**
   * The poller is responsible for checking for available scheduled queries.
   *
   * It also handles forced wakeup calls to reduce the impact that the default check period might be minutes.
   * There might be only 1 running poller service at a time in a hiveserver instance.
   */
  class ScheduledQueryPoller implements Runnable {

    @Override
    public void run() {
      try (NamedThread namedThread = new NamedThread("Scheduled Query Poller")) {
        while (!context.executor.isShutdown()) {
          int origResets = forcedScheduleCheckCounter.get();
          if (usedExecutors.get() < context.getNumberOfExecutors()) {
            try {
              ScheduledQueryPollResponse q = context.schedulerService.scheduledQueryPoll();
              if (q.isSetExecutionId()) {
                context.executor.submit(new ScheduledQueryExecutor(q));
                // skip sleep and poll again if there are available executor
                continue;
              }
            } catch (Throwable t) {
              LOG.error("Unexpected exception during scheduled query submission", t);
            }
          }
          try {
            sleep(context.getIdleSleepTime(), origResets);
          } catch (InterruptedException e) {
            LOG.warn("interrupt discarded");
          }
        }
      }
    }

    private void sleep(long idleSleepTime, int origResets) throws InterruptedException {
      long checkIntrvalMs = 1000;
      for (long i = 0; i < idleSleepTime; i += checkIntrvalMs) {
        Thread.sleep(checkIntrvalMs);
        if (forcedScheduleCheckCounter.get() != origResets) {
          return;
        }
      }
    }

  }

  private void executorStarted(ScheduledQueryExecutor executor) {
    runningExecutors.add(executor);
    usedExecutors.incrementAndGet();
  }

  private void executorStopped(ScheduledQueryExecutor executor) {
    usedExecutors.decrementAndGet();
    runningExecutors.remove(executor);
    forceScheduleCheck();
  }

  /**
   * Responsible for a single execution of a scheduled query.
   *
   * The execution happens in a separate thread.
   */
  class ScheduledQueryExecutor implements Runnable {

    private ScheduledQueryProgressInfo info;
    private final ScheduledQueryPollResponse pollResponse;

    public ScheduledQueryExecutor(ScheduledQueryPollResponse pollResponse) {
      this.pollResponse = pollResponse;
      executorStarted(this);
    }

    public void run() {
      try (NamedThread namedThread = new NamedThread(getThreadName())) {
        processQuery(pollResponse);
      } finally {
        executorStopped(this);
      }
    }

    private String getThreadName() {
      return String.format("Scheduled Query Executor(schedule:%s, execution_id:%d)",
          pollResponse.getScheduleKey().getScheduleName(), pollResponse.getExecutionId());
    }

    public synchronized void reportQueryProgress() {
      if (info != null) {
        LOG.info("Reporting query progress of {} as {} err:{}", info.getScheduledExecutionId(), info.getState(),
            info.getErrorMessage());
        context.schedulerService.scheduledQueryProgress(info);
        if (isTerminalState(info.getState())) {
          info = null;
        }
      }
    }

    private void processQuery(ScheduledQueryPollResponse q) {
      LOG.info("Executing schq:{}, executionId: {}", q.getScheduleKey().getScheduleName(), q.getExecutionId());
      info = new ScheduledQueryProgressInfo();
      info.setScheduledExecutionId(pollResponse.getExecutionId());
      info.setState(QueryState.EXECUTING);
      info.setExecutorQueryId(buildExecutorQueryId(""));
      SessionState state = null;
      try {
        HiveConf conf = new HiveConf(context.conf);
        conf.set(Constants.HIVE_QUERY_EXCLUSIVE_LOCK, lockNameFor(q.getScheduleKey()));
        conf.setVar(HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER, SessionStateUserAuthenticator.class.getName());
        conf.set(Constants.SCHEDULED_QUERY_NAMESPACE, q.getScheduleKey().getClusterNamespace());
        conf.set(Constants.SCHEDULED_QUERY_SCHEDULENAME, q.getScheduleKey().getScheduleName());
        conf.set(Constants.SCHEDULED_QUERY_USER, q.getUser());
        conf.set(Constants.SCHEDULED_QUERY_EXECUTIONID, Long.toString(q.getExecutionId()));
        conf.unset(HiveConf.ConfVars.HIVE_SESSION_ID.varname);
        state = new SessionState(conf, q.getUser());
        state.setIsHiveServerQuery(true);
        SessionState.start(state);
        reportQueryProgress();
        try (
          IDriver driver = DriverFactory.newDriver(DriverFactory.getNewQueryState(conf), null)) {
          info.setExecutorQueryId(buildExecutorQueryId(driver));
          reportQueryProgress();
          driver.run(q.getQuery());
          info.setState(QueryState.FINISHED);
        }
      } catch (Throwable t) {
        info.setErrorMessage(getErrorStringForException(t));
        info.setState(QueryState.FAILED);
      } finally {
        if (state != null) {
          try {
            state.close();
          } catch (Throwable e) {
          }
        }
        reportQueryProgress();
      }
    }

    private String buildExecutorQueryId(IDriver driver) {
      return buildExecutorQueryId(driver.getQueryState().getQueryId());
    }

    private String buildExecutorQueryId(String queryId) {
      return String.format("%s/%s", context.executorHostName, queryId);
    }

    private String lockNameFor(ScheduledQueryKey scheduleKey) {
      return String.format("scheduled_query_%s_%s", scheduleKey.getClusterNamespace(), scheduleKey.getScheduleName());
    }

    private String getErrorStringForException(Throwable t) {
      if (t instanceof CommandProcessorException) {
        CommandProcessorException cpr = (CommandProcessorException) t;
        return String.format("%s", cpr.getMessage());
      } else {
        return String.format("%s: %s", t.getClass().getName(), t.getMessage());
      }
    }
  }

  /**
   * Reports progress periodically.
   *
   * To retain the running state of all the in-flight scheduled query executions;
   * this class initiates a reporting round periodically.
   */
  class ProgressReporter implements Runnable {

    @Override
    public void run() {
      try (NamedThread namedThread = new NamedThread("Scheduled Query Progress Reporter")) {
        while (!context.executor.isShutdown()) {
          try {
            Thread.sleep(context.getProgressReporterSleepTime());
          } catch (InterruptedException e) {
            LOG.warn("interrupt discarded");
          }
          try {
            for (ScheduledQueryExecutor worker : runningExecutors) {
              worker.reportQueryProgress();
            }
          } catch (Exception e) {
            LOG.error("ProgressReporter encountered exception ", e);
          }
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    synchronized (ScheduledQueryExecutionService.class) {
      if (INSTANCE == null || INSTANCE != this) {
        throw new IllegalStateException("The current ScheduledQueryExecutionService INSTANCE is invalid");
      }
      context.executor.shutdown();
      forceScheduleCheck();
      try {
        context.executor.awaitTermination(1, TimeUnit.SECONDS);
        context.executor.shutdownNow();
        INSTANCE = null;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Forces the poller thread to re-check schedules before the normal timeout happens.
   */
  public static void forceScheduleCheck() {
    INSTANCE.forcedScheduleCheckCounter.incrementAndGet();
  }

  @VisibleForTesting
  public static int getForcedScheduleCheckCount() {
    return INSTANCE.forcedScheduleCheckCounter.get();
  }
}
