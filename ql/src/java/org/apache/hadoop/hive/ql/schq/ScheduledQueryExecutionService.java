package org.apache.hadoop.hive.ql.schq;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.QueryState;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollResponse;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryProgressInfo;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class ScheduledQueryExecutionService {

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

    private ScheduledQueryPollResponse executing;
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
        context.schedulerService.scheduledQueryProgress(info);
      }
    }

    private void processQuery(ScheduledQueryPollResponse q) {
      try {
        SessionState.start(context.conf);
        info = new ScheduledQueryProgressInfo();
        info.setScheduledExecutionId(q.getExecutionId());
        info.setState(QueryState.EXECUTING);
        executing = q;
        // FIXME: missing impersonation?
        IDriver driver = DriverFactory.newDriver(context.conf);
        info.setExecutorQueryId(driver.getQueryState().getQueryId());
        CommandProcessorResponse resp;
        resp = driver.run(q.getQuery());
        if (resp.getResponseCode() != 0) {
          throw resp;
        }
        // FIXME: use transitionstate instead
        info.setState(QueryState.FINISHED);
      } catch (Throwable t) {
        info.setErrorMessage(getErrorStringForException(t));
        info.setState(QueryState.ERRORED);
      } finally {
        synchronized (this) {
          reportQueryProgress();
          executing = null;
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
          // FIXME configurable
          Thread.sleep(context.getProgressReporterSleepTime());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        worker.reportQueryProgress();
      }
    }
  }

}
