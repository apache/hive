package org.apache.hadoop.hive.ql.schq;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.QueryState;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollResponse;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryProgressInfo;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class ScheduledQueryExecutionService implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ScheduledQueryExecutionService.class);

  private ScheduledQueryExecutionContext context;
  private ScheduledQueryExecutor worker;

  public static ScheduledQueryExecutionService startScheduledQueryExecutorService(HiveConf conf) {
    MetastoreBasedScheduledQueryService qService = new MetastoreBasedScheduledQueryService(conf);
    ExecutorService executor =
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Scheduled Query Thread %d").build());
    ScheduledQueryExecutionContext ctx = new ScheduledQueryExecutionContext(executor, conf, qService);
    return new ScheduledQueryExecutionService(ctx);
  }

  public ScheduledQueryExecutionService(ScheduledQueryExecutionContext ctx) {
    context = ctx;
    ctx.executor.submit(worker = new ScheduledQueryExecutor());
    ctx.executor.submit(new ProgressReporter());
  }

  class ScheduledQueryExecutor implements Runnable {

    private ScheduledQueryProgressInfo info;

    @Override
    public void run() {
      while (true) {
        ScheduledQueryPollResponse q = context.schedulerService.scheduledQueryPoll();
        if (q.isSetExecutionId()) {
          processQuery(q);
        } else {
          try {
            Thread.sleep(context.getIdleSleepTime());
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }

    public synchronized void reportQueryProgress() {
      if (info != null) {
        LOG.info("Reporting query progress of {} as {} err:{}", info.getScheduledExecutionId(), info.getState(),
            info.getErrorMessage());
        context.schedulerService.scheduledQueryProgress(info);
      }
    }

    private void processQuery(ScheduledQueryPollResponse q) {
      SessionState state = null;
      try {
        state = SessionState.start(context.conf);
        info = new ScheduledQueryProgressInfo();
        info.setScheduledExecutionId(q.getExecutionId());
        info.setState(QueryState.EXECUTING);
        try (
          IDriver driver = DriverFactory.newDriver(DriverFactory.getNewQueryState(context.conf), q.getUser(), null)) {
          info.setExecutorQueryId(driver.getQueryState().getQueryId());
          reportQueryProgress();
          CommandProcessorResponse resp;
          resp = driver.run(q.getQuery());
          if (resp.getResponseCode() != 0) {
            throw resp;
          }
          // FIXME: use transitionstate instead
          info.setState(QueryState.FINISHED);
        }
      } catch (Throwable t) {
        info.setErrorMessage(getErrorStringForException(t));
        info.setState(QueryState.ERRORED);
      } finally {
        if (state != null) {
          try {
            state.close();
          } catch (Throwable e) {
            //FIXME does it really have to throw exception?
          }
        }

        synchronized (this) {
          reportQueryProgress();
          info = null;
        }
      }
    }

    private String getErrorStringForException(Throwable t) {
      if (t instanceof CommandProcessorResponse) {
        CommandProcessorResponse cpr = (CommandProcessorResponse) t;
        return String.format("%s", cpr.getErrorMessage());
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
          Thread.currentThread().interrupt();
        }
        worker.reportQueryProgress();
      }
    }
  }

  @VisibleForTesting
  @Override
  public void close() throws IOException {
    context.executor.shutdown();
    try {
      context.executor.awaitTermination(1, TimeUnit.SECONDS);
      context.executor.shutdownNow();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    

  }

}
