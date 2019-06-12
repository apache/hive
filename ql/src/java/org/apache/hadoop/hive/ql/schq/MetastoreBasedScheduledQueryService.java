package org.apache.hadoop.hive.ql.schq;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollRequest;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollResponse;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryProgressInfo;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetastoreBasedScheduledQueryService implements IScheduledQueryService {

  private static final Logger LOG = LoggerFactory.getLogger(MetastoreBasedScheduledQueryService.class);

  public MetastoreBasedScheduledQueryService(HiveConf conf) {

  }

  @Override
  public ScheduledQueryPollResponse scheduledQueryPoll(String catalog) {
    try {
      ScheduledQueryPollRequest request = new ScheduledQueryPollRequest();
      request.setClusterNamespace(catalog);
      ScheduledQueryPollResponse resp = Hive.get().getMSC().scheduledQueryPoll(request);
      return resp;
    } catch (Exception e) {
      logException("Exception while polling scheduled queries", e);
      return null;
    }
  }

  @Override
  public void scheduledQueryProgress(ScheduledQueryProgressInfo info) {
    try {
      Hive.get().getMSC().scheduledQueryProgress(info);
    } catch (Exception e) {
      logException("Exception while updating scheduled execution status of: " + info.getScheduledExecutionId(), e);
    }
  }

  static void logException(String msg, Exception e) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(msg, e);
    } else {
      LOG.info(msg + ": " + e.getMessage());
    }
  }

}
