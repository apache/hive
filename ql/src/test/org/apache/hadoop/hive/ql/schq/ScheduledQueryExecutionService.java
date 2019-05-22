package org.apache.hadoop.hive.ql.schq;

import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.schq.IScheduledQueryX.ScheduledQueryPollResp;
import org.apache.hadoop.hive.ql.session.SessionState;

public class ScheduledQueryExecutionService {

  private ScheduledQueryExecutionContext context;
  private ScheduledQueryExecutor worker;

  class ScheduledQueryExecutor implements Runnable {

    private ScheduledQueryPollResp executing;

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

    private void processQuery(ScheduledQueryPollResp q) {
      try {
        SessionState.start(context.conf);
        executing = q;
        // FIXME: missing impersonation
        IDriver driver = DriverFactory.newDriver(context.conf);
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

    private void reportQueryState(String string, String errorStringForException) {
      System.out.println("QueryState: " + string + " -- " + errorStringForException);
    }

    private String getErrorStringForException(Throwable t) {
      if (t instanceof CommandProcessorResponse) {
        CommandProcessorResponse cpr = (CommandProcessorResponse) t;
        return String.format("%s", cpr.getErrorMessage());
      } else {
        return String.format("%s: %s", t.getClass().getName(), t.getMessage());
      }
    }

    public String getStatus() {
      return "ok";
    }
  }

  class ProgressReporter implements Runnable {

    @Override
    public void run() {
      while (true) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        System.out.println(worker.getStatus());
      }
    }
  }

  public ScheduledQueryExecutionService(ScheduledQueryExecutionContext c1) {
    context = c1;
    c1.executor.submit(worker = new ScheduledQueryExecutor());
    c1.executor.submit(new ProgressReporter());
  }

}
