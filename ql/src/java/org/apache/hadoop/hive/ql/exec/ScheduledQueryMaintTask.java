package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;

import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.repl.ExternalTableCopyTaskBuilder.DirCopyWork;
import org.apache.hadoop.hive.ql.parse.ScheduledQueryMaintWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;

public class ScheduledQueryMaintTask extends Task<ScheduledQueryMaintWork> {

  private static final long serialVersionUID = 1L;

  @Override
  public String getName() {
    return "SCHEDULED QUERY MAINT TASK";
  }

  @Override
  protected int execute(DriverContext driverContext) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public StageType getType() {
    return StageType.SCHEDULED_QUERY_MAINT;
  }

}
