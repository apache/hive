package org.apache.hadoop.hive.ql.schq;

import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollResponse;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryProgressInfo;

/**
 * Interface to cover Scheduled Query source service
 *
 * Main reason to have this layer in place is to make testing easier.
 * 
 * @deprecated rething methods
 */
//FIXME rethink
@Deprecated
public interface IScheduledQueryService {

  ScheduledQueryPollResponse scheduledQueryPoll(String catalog);

  void scheduledQueryProgress(ScheduledQueryProgressInfo info);
}
