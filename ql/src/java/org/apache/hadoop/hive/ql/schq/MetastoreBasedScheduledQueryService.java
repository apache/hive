package org.apache.hadoop.hive.ql.schq;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollRequest;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollResponse;
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
      logException("Exception while reading metastore runtime stats", e);
      return null;
    }
  }

  @Override
  public void scheduledQueryProgress(int executionId, String state, String errorMsg) {
    // TODO Auto-generated method stub

  }

  static void logException(String msg, Exception e) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(msg, e);
    } else {
      LOG.info(msg + ": " + e.getMessage());
    }
  }

}
