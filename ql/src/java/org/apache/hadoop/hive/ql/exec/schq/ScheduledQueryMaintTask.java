package org.apache.hadoop.hive.ql.exec.schq;

import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.schq.ScheduledQueryMaintWork;

public class ScheduledQueryMaintTask extends Task<ScheduledQueryMaintWork> {

  private static final long serialVersionUID = 1L;

  @Override
  public String getName() {
    return "SCHEDULED QUERY MAINT TASK";
  }

  @Override
  protected int execute(DriverContext driverContext) {
    return 0;
  }

  @Override
  public StageType getType() {
    return StageType.SCHEDULED_QUERY_MAINT;
  }

}
