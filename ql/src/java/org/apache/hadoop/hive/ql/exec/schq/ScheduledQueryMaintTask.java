package org.apache.hadoop.hive.ql.exec.schq;

import org.apache.hadoop.hive.metastore.api.ScheduledQueryMaintenanceRequest;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.schq.ScheduledQueryMaintWork;
import org.apache.thrift.TException;

public class ScheduledQueryMaintTask extends Task<ScheduledQueryMaintWork> {

  private static final long serialVersionUID = 1L;

  @Override
  public String getName() {
    return "SCHEDULED QUERY MAINT TASK";
  }

  @Override
  public int execute(DriverContext driverContext) {
    ScheduledQueryMaintenanceRequest request = buildScheduledQueryRequest();
    try {
      Hive.get().getMSC().scheduledQueryMaintenance(request);
    } catch (TException | HiveException e) {
      setException(e);
      LOG.error("Failed", e);
      return 1;
    }
    return 0;
  }

  private ScheduledQueryMaintenanceRequest buildScheduledQueryRequest() {
    ScheduledQueryMaintenanceRequest req = new ScheduledQueryMaintenanceRequest();
    req.setType(work.getType());
    req.setScheduledQuery(work.getScheduledQuery());
    return req;
  }

  @Override
  public StageType getType() {
    return StageType.SCHEDULED_QUERY_MAINT;
  }

}
