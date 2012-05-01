/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.hooks;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.io.CachingPrintStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.hooks.conf.FBHiveConf;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.json.JSONObject;


/**
 * Implementation of a post execute hook that checks whether
 * a partition is archived or not and also sets that query
 * time for the partition.
 */
public class ReplicationHook extends BaseReplicationHook implements ExecuteWithHookContext {

  static final private Log LOG = LogFactory.getLog(ReplicationHook.class.getName());

  private HiveConf conf;

  public ReplicationHook() throws Exception {
    super();
    conf = new HiveConf(this.getClass());
  }

  /**
   * Set this replication hook's hive configuration.
   * Expose this as a public function in case run() cannot get the HiveConf
   * from the session, e.g., if ReplicationHook is not called after a CLI query.
   * @param conf the configuration to use
   */
  public void setHiveConf(HiveConf conf) {
    this.conf = conf;
  }

  public void run(SessionState sess, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs, LineageInfo lInfo, UserGroupInformation ugi)
    throws Exception {
    run(sess, inputs, outputs, lInfo, ugi, null, HookContext.HookType.POST_EXEC_HOOK);
  }

  public void run(SessionState sess, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs, LineageInfo lInfo, UserGroupInformation ugi,
      List<TaskRunner> completedTasks, HookContext.HookType hookType)
    throws Exception {

    assert(hookType == HookContext.HookType.POST_EXEC_HOOK ||
           hookType == HookContext.HookType.ON_FAILURE_HOOK);

    String command = "";
    String commandType = "";
    String user_info = "";
    String inputStr = "";
    String outputStr = "";
    String queryId = "";
    String querySrc = "";
    String startTimeStr = "";
    String packageName = "";

    if (sess != null) {
      command     = StringEscapeUtils.escapeJava(sess.getCmd());
      commandType = StringEscapeUtils.escapeJava(sess.getCommandType());
      setHiveConf(sess.getConf());
      queryId = conf.getVar(HiveConf.ConfVars.HIVEQUERYID);

      querySrc = conf.get(JobStatsHook.HIVE_QUERY_SOURCE, "");
      packageName = conf.get(FBHiveConf.FB_CURRENT_CLUSTER);
    }

    if (ugi != null) {
      user_info = StringEscapeUtils.escapeJava(ugi.getUserName());
    }

    if (inputs != null) {
      inputStr = entitiesToString(inputs);
    }

    if (outputs != null) {
      outputStr = entitiesToString(outputs);
    }

    // Retrieve the time the Driver.run method started from the PerfLogger, as this corresponds
    // to approximately the time when the query started to be processed, and format it.
    // If, some how, this time was not set, it will default to 0000-00-00 00:00:00 in the db.
    Long startTimeMillis = PerfLogger.getPerfLogger().getStartTime(PerfLogger.DRIVER_RUN);
    if (startTimeMillis != null) {
      Date startTime = new Date(startTimeMillis.longValue());
      startTimeStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(startTime);
    } else {
      LOG.error("Start time was null in ReplicationHook");
    }

    ArrayList<Object> sqlParams = new ArrayList<Object>();
    sqlParams.add(StringEscapeUtils.escapeJava(command));
    sqlParams.add(StringEscapeUtils.escapeJava(commandType));
    sqlParams.add(StringEscapeUtils.escapeJava(inputStr));
    sqlParams.add(outputStr);
    sqlParams.add(StringEscapeUtils.escapeJava(queryId));
    sqlParams.add(StringEscapeUtils.escapeJava(user_info));
    sqlParams.add(StringEscapeUtils.escapeJava(querySrc));
    sqlParams.add(startTimeStr);
    sqlParams.add(packageName);

    // Assertion at beginning of method guarantees this string will remain empty
    String sql = "";
    if (hookType == HookContext.HookType.POST_EXEC_HOOK) {
      sql = "insert into snc1_command_log set command = ?, command_type = ?, inputs = ?, " +
            "outputs = ?, queryId = ?, user_info = ?, query_src = ?, start_time = ?, " +
            "package_name = ?";
    } else if (hookType == HookContext.HookType.ON_FAILURE_HOOK) {

      List<String> errors = ((CachingPrintStream)sess.err).getOutput();
      String localErrorString = "";
      if (!errors.isEmpty()) {
        JSONObject localErrorObj = new JSONObject();
        localErrorObj.put("localErrors", errors);
        localErrorString = localErrorObj.toString();
      }

      sqlParams.add(localErrorString);

      sql = "insert into snc1_failed_command_log set command = ?, command_type = ?, inputs = ?, " +
            "outputs = ?, queryId = ?, user_info = ?, query_src = ?, start_time = ?, " +
            "package_name = ?, local_errors = ?";
    }
    HookUtils.runInsert(conf, urlFactory, sql, sqlParams, HookUtils
        .getSqlNumRetry(conf));
  }

@Override
  public void run(HookContext hookContext) throws Exception {
    SessionState ss = SessionState.get();
    Set<ReadEntity> inputs = hookContext.getInputs();
    Set<WriteEntity> outputs = hookContext.getOutputs();
    LineageInfo linfo = hookContext.getLinfo();
    UserGroupInformation ugi = hookContext.getUgi();
    this.run(ss, inputs, outputs, linfo, ugi,
             hookContext.getCompleteTaskList(), hookContext.getHookType());
  }

  public static String entitiesToString(Set<? extends Serializable> entities) {
    StringBuilder stringBuilder = new StringBuilder();

    boolean first = true;

    for (Serializable  o : entities) {
      if (!first) {
        stringBuilder.append(",");
      }
      first = false;
      stringBuilder.append(o.toString());
    }
    return stringBuilder.toString();
  }
}
