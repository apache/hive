package org.apache.hadoop.hive.ql.schq;

import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollResponse;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryProgressInfo;

/**
 * Interface to cover Scheduled Query source service
 *
 * Note that the main reason to have this layer is to make testing easier.
 */
public interface IScheduledQueryMaintenanceService {

  ScheduledQueryPollResponse scheduledQueryPoll();

  void scheduledQueryProgress(ScheduledQueryProgressInfo info);

  String getClusterNamespace();

}
