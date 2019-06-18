package org.apache.hadoop.hive.ql.schq;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

public class ScheduledQueryExecutionContext {

  public final ExecutorService executor;
  public final IScheduledQueryMaintenanceService schedulerService;
  public final HiveConf conf;

  public ScheduledQueryExecutionContext(
      ExecutorService executor,
      HiveConf conf,
      IScheduledQueryMaintenanceService service) {
    this.executor = executor;
    this.conf = conf;
    this.schedulerService = service;
  }

  /**
   * @return time in milliseconds
   */
  public long getIdleSleepTime() {
    return conf.getTimeVar(ConfVars.HIVE_SCHEDULED_QUERIES_EXECUTOR_IDLE_SLEEP_TIME, TimeUnit.MILLISECONDS);
  }

  // Interval
  public long getProgressReporterSleepTime() {
    return conf.getTimeVar(ConfVars.HIVE_SCHEDULED_QUERIES_EXECUTOR_PROGRESS_REPORT_INTERVAL, TimeUnit.MILLISECONDS);
  }

}
