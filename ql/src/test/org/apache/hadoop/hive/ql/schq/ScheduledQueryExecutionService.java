package org.apache.hadoop.hive.ql.schq;

import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.schq.IScheduledQueryX.ScheduledQueryPollResp;

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
      executing = q;
      IDriver driver = DriverFactory.newDriver(context.conf);
      CommandProcessorResponse resp = driver.compileAndRespond(q.queryString);
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
