package org.apache.hadoop.hive.ql.hooks;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

public class MapJoinCounterHook implements ExecuteWithHookContext {

  public void run(HookContext hookContext) {
    HiveConf conf = hookContext.getConf();
    boolean enableConvert = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVECONVERTJOIN);
    if (!enableConvert) {
      return;
    }

    QueryPlan plan = hookContext.getQueryPlan();
    String queryID = plan.getQueryId();
    // String query = SessionState.get().getCmd();

    int convertedMapJoin = 0;
    int commonJoin = 0;
    int backupCommonJoin = 0;
    int convertedLocalMapJoin = 0;
    int localMapJoin = 0;

    List<TaskRunner> list = hookContext.getCompleteTaskList();
    for (TaskRunner tskRunner : list) {
      Task tsk = tskRunner.getTask();
      int tag = tsk.getTaskTag();
      switch (tag) {
      case Task.COMMON_JOIN:
        commonJoin++;
        break;
      case Task.CONVERTED_LOCAL_MAPJOIN:
        convertedLocalMapJoin++;
        break;
      case Task.CONVERTED_MAPJOIN:
        convertedMapJoin++;
        break;
      case Task.BACKUP_COMMON_JOIN:
        backupCommonJoin++;
        break;
      case Task.LOCAL_MAPJOIN:
         localMapJoin++;
         break;
      }
    }
    LogHelper console = SessionState.getConsole();
    console.printError("[MapJoinCounter PostHook] CONVERTED_LOCAL_MAPJOIN: " + convertedLocalMapJoin
        + " CONVERTED_MAPJOIN: " + convertedMapJoin + " LOCAL_MAPJOIN: "+localMapJoin+ " COMMON_JOIN: "+commonJoin
        + " BACKUP_COMMON_JOIN: " + backupCommonJoin);
  }
}
