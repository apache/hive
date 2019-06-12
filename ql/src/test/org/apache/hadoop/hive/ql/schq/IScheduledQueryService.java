package org.apache.hadoop.hive.ql.schq;

/**
 * Interface to cover Scheduled Query source service
 * 
 * Main reason to have this layer in place is to make testing easier.
 */
public interface IScheduledQueryService {

  static class ScheduledQueryPollResp {
    //FIXME ??
    //String queryId;
    String queryString;
    int executionId;
  }

  ScheduledQueryPollResp scheduledQueryPoll(String catalog);

  void scheduledQueryProgress(int executionId, String state, String errorMsg);
}
