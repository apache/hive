package org.apache.hadoop.hive.ql.schq;

public class ScheduledQueryX implements IScheduledQueryX {
  int id = 0;
  private String stmt;

  public ScheduledQueryX(String string) {
    stmt = string;
  }

  @Override
  public ScheduledQueryPollResp scheduledQueryPoll(String catalog) {

    ScheduledQueryPollResp r = new ScheduledQueryPollResp();
    r.executionId = id++;
    r.queryString = stmt;
    if (id >= 1) {
      return r;
    } else {
      return null;
    }
  }

  @Override
  public void scheduledQueryProgress(int executionId, String state, String errorMsg) {
    System.out.printf("%d, state: %s, error: %s", executionId, state, errorMsg);
  }

}
