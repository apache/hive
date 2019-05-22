package org.apache.hadoop.hive.ql.schq;

public interface IScheduledQueryX {

  static class ScheduledQueryPollResp {
    //String queryId;
    String queryString;
    int executionId;
  }

  ScheduledQueryPollResp scheduledQueryPoll(String catalog);

  void scheduledQueryProgress(int executionId, String state, String errorMsg);
}
