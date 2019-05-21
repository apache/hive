package org.apache.hadoop.hive.ql.schq;

import java.util.concurrent.ExecutorService;

public class ScheduledQueryExecutionService {

  private ScheduledQueryExecutionContext context;
  private ScheduledQueryExecutor worker;

  class ProgressReporter implements Runnable {

    @Override
    public void run() {
      Thread.sleep(1000);
      System.out.println(worker.getStatus());
    }
  }

  class ScheduledQueryExecutor implements Runnable {

    @Override
    public void run() {
      try {
        Thread.sleep(context.getIdleSleepTime());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    public String getStatus() {
      return "ok";
    }
  }



  public ScheduledQueryExecutionService(ExecutorService executor) {
    context = new ScheduledQueryExecutionContext();
    executor.submit(worker = new ScheduledQueryExecutor());
    executor.submit(new ProgressReporter());
  }

}
