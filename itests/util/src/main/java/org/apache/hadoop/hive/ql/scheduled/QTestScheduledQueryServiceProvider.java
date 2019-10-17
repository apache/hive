package org.apache.hadoop.hive.ql.scheduled;

import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.hive.ql.qoption.QTestOptionHandler;

/**
 * Adding this qtestoption enables the scheduled query service.
 */
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
      service = ScheduledQueryExecutionService.startScheduledQueryExecutorService(qt.getConf());
    }
  }

  @Override
  public void afterTest(QTestUtil qt) throws Exception {
    if(service != null) {
      service.close();
    }
    service = null;
    enabled = false;
  }

}
