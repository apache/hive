package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.ql.qoption.QTestOptionHandler;
import org.apache.hadoop.hive.ql.schq.ScheduledQueryExecutionService;

public class QTestScheduledQueryServiceProvider implements QTestOptionHandler {

  private boolean enabled;
  private ScheduledQueryExecutionService service;

  @Override
  public void processArguments(String arguments) {
    enabled = true;
  }

  @Override
  public void beforeTest(QTestUtil qt) throws Exception {
    if (enabled) {
      service = ScheduledQueryExecutionService.startScheduledQueryExecutorService(qt.conf);
    }
  }

  public void afterTest(QTestUtil qt) throws Exception {
    if(service != null) {
      service.close();
    }
    service = null;
    enabled = false;
  }

}
