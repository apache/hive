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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
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
  private ScheduledQueryExecutor worker;
  private AtomicInteger forcedScheduleCheckCounter = new AtomicInteger();

  public static ScheduledQueryExecutionService startScheduledQueryExecutorService(HiveConf inputConf) {
    HiveConf conf = new HiveConf(inputConf);
    MetastoreBasedScheduledQueryService qService = new MetastoreBasedScheduledQueryService(conf);
    ExecutorService executor = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Scheduled Query Thread %d").build());
    ScheduledQueryExecutionContext ctx = new ScheduledQueryExecutionContext(executor, conf, qService);
    return startScheduledQueryExecutorService(ctx);
  }

  public static ScheduledQueryExecutionService startScheduledQueryExecutorService(ScheduledQueryExecutionContext ctx) {
    synchronized (ScheduledQueryExecutionService.class) {
      if (INSTANCE != null) {
        throw new IllegalStateException(
            "There is already a ScheduledQueryExecutionService in service; check it and close it explicitly if neccessary");
      }
      INSTANCE = new ScheduledQueryExecutionService(ctx);
      return INSTANCE;
    }
  }

  private ScheduledQueryExecutionService(ScheduledQueryExecutionContext ctx) {
    context = ctx;
    ctx.executor.submit(worker = new ScheduledQueryExecutor());
    ctx.executor.submit(new ProgressReporter());
  }

  static boolean isTerminalState(QueryState state) {
    return state == QueryState.FINISHED || state == QueryState.FAILED;
  }

  class ScheduledQueryExecutor implements Runnable {

    private ScheduledQueryProgressInfo info;

    @Override
    public void run() {
      while (true) {
        ScheduledQueryPollResponse q = context.schedulerService.scheduledQueryPoll();
        if (q.isSetExecutionId()) {
          try{
            processQuery(q);
          } catch (Throwable t) {
            LOG.error("Unexpected exception during scheduled query processing", t);
          }
        } else {
          try {
            sleep(context.getIdleSleepTime());
          } catch (InterruptedException e) {
            LOG.warn("interrupt discarded");
          }
        }
      }
    }

    private void sleep(long idleSleepTime) throws InterruptedException {
      long checkIntrvalMs = 1000;
      int origResets = forcedScheduleCheckCounter.get();
      for (long i = 0; i < idleSleepTime; i += checkIntrvalMs) {
        Thread.sleep(checkIntrvalMs);
        if (forcedScheduleCheckCounter.get() != origResets) {
          return;
        }
      }
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
      SessionState state = null;
      info = new ScheduledQueryProgressInfo();
      info.setScheduledExecutionId(q.getExecutionId());
      info.setState(QueryState.EXECUTING);
      try {
        HiveConf conf = new HiveConf(context.conf);
        conf.set(Constants.HIVE_QUERY_EXCLUSIVE_LOCK, lockNameFor(q.getScheduleKey()));
        conf.setVar(HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER, SessionStateUserAuthenticator.class.getName());
        conf.unset(HiveConf.ConfVars.HIVESESSIONID.varname);
        state = new SessionState(conf, q.getUser());
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
      return String.format("%s/%s", context.executorHostName, driver.getQueryState().getQueryId());
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

  class ProgressReporter implements Runnable {

    @Override
    public void run() {
      while (true) {
        try {
          Thread.sleep(context.getProgressReporterSleepTime());
        } catch (InterruptedException e) {
          LOG.warn("interrupt discarded");
        }
        try {
          worker.reportQueryProgress();
        } catch (Exception e) {
          LOG.error("ProgressReporter encountered exception ", e);
        }
      }
    }
  }

  @VisibleForTesting
  @Override
  public void close() throws IOException {
    synchronized (ScheduledQueryExecutionService.class) {
      if (INSTANCE == null || INSTANCE != this) {
        throw new IllegalStateException("The current ScheduledQueryExecutionService INSTANCE is invalid");
      }
      INSTANCE = null;
      context.executor.shutdown();
      try {
        context.executor.awaitTermination(1, TimeUnit.SECONDS);
        context.executor.shutdownNow();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public static void forceScheduleCheck() {
    INSTANCE.forcedScheduleCheckCounter.incrementAndGet();
  }

  @VisibleForTesting
  public static int getForcedScheduleCheckCount() {
    return INSTANCE.forcedScheduleCheckCounter.get();
  }
}
