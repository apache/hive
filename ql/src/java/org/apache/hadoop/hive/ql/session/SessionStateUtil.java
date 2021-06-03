/*
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

package org.apache.hadoop.hive.ql.session;

import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionStateUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SessionStateUtil.class);
  private static final String COMMIT_INFO_PREFIX = "COMMIT_INFO.";

  private SessionStateUtil() {

  }
  
  public static Optional<QueryState> getQueryState(Configuration conf) {
    return Optional.ofNullable(SessionState.get())
        .map(session -> session.getQueryState(conf.get(HiveConf.ConfVars.HIVEQUERYID.varname)));
  }

  public static Object getResource(Configuration conf, String key) {
    return getQueryState(conf).map(state -> state.getResource(key)).orElse(null);
  }

  public static String getProperty(Configuration conf, String key) {
    return (String) getResource(conf, key);
  }

  public static void addResource(Configuration conf, String key, Object resource) {
    getQueryState(conf).ifPresent(state -> state.addResource(key, resource));
  }

  public static Optional<CommitInfo> getCommitInfo(Configuration conf, String tableName) {
    return Optional.ofNullable(getResource(conf, COMMIT_INFO_PREFIX + tableName))
        .filter(o -> o instanceof CommitInfo)
        .map(o -> (CommitInfo) o);
  }

  public static CommitInfo newCommitInfo() {
    return new CommitInfo();
  }

  public static class CommitInfo {

    String tableName;
    String jobIdStr;
    int taskNum;
    Map<String, String> props;

    public CommitInfo withTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public CommitInfo withJobID(String jobIdStr) {
      this.jobIdStr = jobIdStr;
      return this;
    }

    public CommitInfo withTaskNum(int taskNum) {
      this.taskNum = taskNum;
      return this;
    }

    public CommitInfo withProps(Map<String, String> props) {
      this.props = props;
      return this;
    }

    public void save(QueryState queryState) {
      queryState.addResource(COMMIT_INFO_PREFIX + tableName, this);
    }

    public String getTableName() {
      return tableName;
    }

    public String getJobIdStr() {
      return jobIdStr;
    }

    public int getTaskNum() {
      return taskNum;
    }

    public Map<String, String> getProps() {
      return props;
    }
  }
}