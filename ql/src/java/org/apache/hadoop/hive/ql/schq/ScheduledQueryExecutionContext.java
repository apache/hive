package org.apache.hadoop.hive.ql.schq;

import java.util.concurrent.ExecutorService;

import org.apache.hadoop.hive.conf.HiveConf;

public class ScheduledQueryExecutionContext {

  public final ExecutorService executor;
  public final IScheduledQueryService schedulerService;
  public final HiveConf conf;

  public ScheduledQueryExecutionContext(
      ExecutorService executor,
      HiveConf conf,
      IScheduledQueryService service) {
    this.executor = executor;
    this.conf = conf;
    this.schedulerService = service;
  }

  public long getIdleSleepTime() {
    // FIXME make this configurable?
    return 1000;
  }

  public long getProgressReporterSleepTime() {
    // FIXME make this configurable?
    return 1000;
  }

}
