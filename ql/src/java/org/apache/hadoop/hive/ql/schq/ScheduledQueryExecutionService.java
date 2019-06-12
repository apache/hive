package org.apache.hadoop.hive.ql.schq;

import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.schq.IScheduledQueryService.ScheduledQueryPollResp;
import org.apache.hadoop.hive.ql.session.SessionState;

public class ScheduledQueryExecutionService {

  private ScheduledQueryExecutionContext context;
  private ScheduledQueryExecutor worker;

  public ScheduledQueryExecutionService(ScheduledQueryExecutionContext ctx) {
    context = ctx;
    ctx.executor.submit(worker = new ScheduledQueryExecutor());
    ctx.executor.submit(new ProgressReporter());
  }

  class ScheduledQueryExecutor implements Runnable {

    private ScheduledQueryPollResp executing;
    private String hiveQueryId;

    @Override
    public void run() {
      while (true) {
        ScheduledQueryPollResp q = context.schedulerService.scheduledQueryPoll("x");
        if (q != null) {
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
      if (executing != null) {
        worker.reportQueryState("RUNNING", null);
      }
    }

    private void processQuery(ScheduledQueryPollResp q) {
      try {
        SessionState.start(context.conf);
        executing = q;
        // FIXME: missing impersonation
        IDriver driver = DriverFactory.newDriver(context.conf);
        hiveQueryId = driver.getQueryState().getQueryId();
        CommandProcessorResponse resp;
        resp = driver.run(q.queryString);
        if (resp.getResponseCode() != 0) {
          throw resp;
        }
        reportQueryState("FINISHED", null);
      } catch (Throwable t) {
        reportQueryState("ERROR", getErrorStringForException(t));
      } finally {
        executing = null;
      }
    }

    private synchronized void reportQueryState(String state, String errorMessage) {
      //FIXME no progress message after FINISGH/ERRORED
      //FIXME hivequeryid
      System.out.println(hiveQueryId);
      context.schedulerService.scheduledQueryProgress(executing.executionId, state, errorMessage);
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
