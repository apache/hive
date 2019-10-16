package org.apache.hadoop.hive.ql.scheduled;

import java.io.Serializable;

import org.apache.hadoop.hive.metastore.api.ScheduledQuery;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryMaintenanceRequestType;

public class ScheduledQueryMaintWork implements Serializable {

  private static final long serialVersionUID = 1L;
  private ScheduledQueryMaintenanceRequestType type;
  private ScheduledQuery schq;

  public ScheduledQueryMaintWork(ScheduledQueryMaintenanceRequestType type, ScheduledQuery schq) {
    this.type = type;
    this.schq = schq;
  }

  public ScheduledQueryMaintenanceRequestType getType() {
    return type;
  }

  public ScheduledQuery getScheduledQuery() {
    return schq;
  }

}
