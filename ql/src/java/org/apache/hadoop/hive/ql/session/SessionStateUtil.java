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

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionStateUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SessionStateUtil.class);

  private static final String COMMIT_INFO_PREFIX = "COMMIT_INFO.";
  private static final String CONFLICT_DETECTION_FILTER = "conflictDetectionFilter.";
  public static final String DEFAULT_TABLE_LOCATION = "defaultLocation";

  private SessionStateUtil() {
  }

  /**
   * @param conf Configuration object used for getting the query state, should contain the query id
   * @param key The resource identifier
   * @return The requested resource, or an empty Optional if either the SessionState, QueryState or the resource itself
   * could not be found
   */
  public static Optional<Object> getResource(Configuration conf, String key) {
    return getQueryState(conf)
        .map(queryState -> queryState.getResource(key));
  }

  /**
   * @param conf Configuration object used for getting the query state, should contain the query id
   * @param key The resource identifier
   * @return The requested string property, or an empty Optional if either the SessionState, QueryState or the
   * resource itself could not be found, or the resource is not of type String
   */
  public static Optional<String> getProperty(Configuration conf, String key) {
    return getResource(conf, key).filter(String.class::isInstance)
        .map(String.class::cast);
  }

  /**
   * @param conf Configuration object used for getting the query state, should contain the query id
   * @param key The resource identifier
   * @param resource The resource to save into the QueryState
   */
  public static void addResource(Configuration conf, String key, Object resource) {
    getQueryState(conf)
        .ifPresent(queryState -> queryState.addResource(key, resource));
  }

  public static void addResourceOrThrow(Configuration conf, String key, Object resource) {
    getQueryState(conf)
        .orElseThrow(() -> new IllegalStateException("Query state is missing; failed to add resource for " + key))
        .addResource(key, resource);
  }

  /**
   * @param conf Configuration object used for getting the query state, should contain the query id
   * @param tableName Name of the table for which the commit info should be retrieved
   * @return the CommitInfo map. Key: jobId, Value: {@link CommitInfo}, or empty Optional if not present
   */
  public static Optional<Map<String, CommitInfo>> getCommitInfo(Configuration conf, String tableName) {
    return getResource(conf, COMMIT_INFO_PREFIX + tableName)
        .map(obj -> (Map<String, CommitInfo>) obj);
  }

  /**
   * @param conf Configuration object used for getting the query state, should contain the query id
   * @param tableName Name of the table for which the commit info should be stored
   * @param jobId The job ID
   * @param taskNum The number of successful tasks for the job
   * @param additionalProps Any additional properties related to the job commit
   */
  public static void addCommitInfo(
      Configuration conf, String tableName, String jobId, int taskNum, Map<String, String> additionalProps) {

    CommitInfo commitInfo = new CommitInfo()
        .withJobID(jobId)
        .withTaskNum(taskNum)
        .withProps(additionalProps);

    getCommitInfo(conf, tableName)
        .ifPresentOrElse(commitInfoMap -> commitInfoMap.put(jobId, commitInfo),
            () -> {
              Map<String, CommitInfo> newCommitInfoMap = Maps.newHashMap();
              newCommitInfoMap.put(jobId, commitInfo);

              addResource(conf, COMMIT_INFO_PREFIX + tableName, newCommitInfoMap);
            });
  }

  public static Optional<ExprNodeGenericFuncDesc> getConflictDetectionFilter(Configuration conf, Object tableName) {
    return getResource(conf, CONFLICT_DETECTION_FILTER + tableName)
        .map(ExprNodeGenericFuncDesc.class::cast);
  }

  public static void setConflictDetectionFilter(Configuration conf, String tableName, ExprNodeDesc filterExpr) {
    String key = CONFLICT_DETECTION_FILTER + tableName;

    getConflictDetectionFilter(conf, tableName)
        .ifPresentOrElse(prevFilterExpr -> {
            if (!prevFilterExpr.isSame(filterExpr)) {
              ExprNodeDesc disjunction = null;
              try {
                disjunction = ExprNodeDescUtils.disjunction(prevFilterExpr, filterExpr);
              } catch (UDFArgumentException e) {
                LOG.warn("Unable to create conflict detection filter, proceeding without it: ", e);
              }
              addResource(conf, key, disjunction);
            }
        }, () -> addResource(conf, key, filterExpr));
  }

  public static Optional<QueryState> getQueryState(Configuration conf) {
    return Optional.ofNullable(SessionState.get())
        .map(ss -> ss.getQueryState(HiveConf.getQueryId(conf)));
  }

  /**
   * Container class for job commit information.
   */
  public static class CommitInfo {
    String jobIdStr;
    int taskNum;
    Map<String, String> props;

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