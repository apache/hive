package org.apache.hadoop.hive.ql.schq;

//FIXME rename
public interface IScheduledQueryX {

  static class ScheduledQueryPollResp {
    //FIXME ??
    //String queryId;
    String queryString;
    int executionId;
  }

  ScheduledQueryPollResp scheduledQueryPoll(String catalog);

  void scheduledQueryProgress(int executionId, String state, String errorMsg);
}
