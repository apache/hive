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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
/**
 * Hook Context keeps all the necessary information for all the hooks.
 * New implemented hook can get the query plan, job conf and the list of all completed tasks from this hook context
 */
public class HookContext {

  static public enum HookType {
    PRE_EXEC_HOOK, POST_EXEC_HOOK, ON_FAILURE_HOOK
  }

  private QueryPlan queryPlan;
  private HiveConf conf;
  private List<TaskRunner> completeTaskList;
  private Set<ReadEntity> inputs;
  private Set<WriteEntity> outputs;
  private LineageInfo linfo;
  private UserGroupInformation ugi;
  private HookType hookType;
  final private Map<String, ContentSummary> inputPathToContentSummary;
  private final String ipAddress;
  private final String userName;
  // unique id set for operation when run from HS2, base64 encoded value of
  // TExecuteStatementResp.TOperationHandle.THandleIdentifier.guid
  private final String operationId;

  public HookContext(QueryPlan queryPlan, HiveConf conf,
      Map<String, ContentSummary> inputPathToContentSummary, String userName, String ipAddress,
      String operationId) throws Exception {
    this.queryPlan = queryPlan;
    this.conf = conf;
    this.inputPathToContentSummary = inputPathToContentSummary;
    completeTaskList = new ArrayList<TaskRunner>();
    inputs = queryPlan.getInputs();
    outputs = queryPlan.getOutputs();
    ugi = Utils.getUGI();
    linfo= null;
    if(SessionState.get() != null){
      linfo = SessionState.get().getLineageState().getLineageInfo();
    }
    this.userName = userName;
    this.ipAddress = ipAddress;
    this.operationId = operationId;
  }

  public QueryPlan getQueryPlan() {
    return queryPlan;
  }

  public void setQueryPlan(QueryPlan queryPlan) {
    this.queryPlan = queryPlan;
  }

  public HiveConf getConf() {
    return conf;
  }

  public void setConf(HiveConf conf) {
    this.conf = conf;
  }

  public List<TaskRunner> getCompleteTaskList() {
    return completeTaskList;
  }

  public void setCompleteTaskList(List<TaskRunner> completeTaskList) {
    this.completeTaskList = completeTaskList;
  }

  public void addCompleteTask(TaskRunner completeTaskRunner) {
    completeTaskList.add(completeTaskRunner);
  }

  public Set<ReadEntity> getInputs() {
    return inputs;
  }

  public void setInputs(Set<ReadEntity> inputs) {
    this.inputs = inputs;
  }

  public Set<WriteEntity> getOutputs() {
    return outputs;
  }

  public void setOutputs(Set<WriteEntity> outputs) {
    this.outputs = outputs;
  }

  public LineageInfo getLinfo() {
    return linfo;
  }

  public void setLinfo(LineageInfo linfo) {
    this.linfo = linfo;
  }

  public UserGroupInformation getUgi() {
    return ugi;
  }

  public void setUgi(UserGroupInformation ugi) {
    this.ugi = ugi;
  }

  public Map<String, ContentSummary> getInputPathToContentSummary() {
    return inputPathToContentSummary;
  }

  public HookType getHookType() {
    return hookType;
  }

  public void setHookType(HookType hookType) {
    this.hookType = hookType;
  }

  public String getIpAddress() {
    return this.ipAddress;
 }

  public String getOperationName() {
    return queryPlan.getOperationName();
  }

  public String getUserName() {
    return this.userName;
  }

  public String getOperationId() {
    return operationId;
  }
}
