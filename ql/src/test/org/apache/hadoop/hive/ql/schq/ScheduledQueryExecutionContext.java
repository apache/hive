package org.apache.hadoop.hive.ql.schq;

public class ScheduledQueryExecutionContext {

  public long getIdleSleepTime() {
    // FIXME make this configurable?
    return 1000;
  }

}
