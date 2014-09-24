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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.JobProfile;
import org.apache.hive.hcatalog.templeton.tool.JobState;

/**
 * QueueStatusBean - The results of an exec call.
 */
public class QueueStatusBean {
  public JobStatus status;
  public JobProfile profile;

  public final String id;
  public String parentId;
  public String percentComplete;
  public Long exitValue;
  public String user;
  public String callback;
  public String completed;
  public Map<String, Object> userargs;
  public String msg;

  public QueueStatusBean(String jobId, String errMsg) {
    this.id = jobId;
    this.msg = errMsg;
  }

  /**
   * Create a new QueueStatusBean
   *
   * @param state      store job state
   * @param status     job status
   * @param profile    job profile
   */
  public QueueStatusBean(JobState state, JobStatus status, JobProfile profile)
    throws IOException {
    this.status = status;
    this.profile = profile;

    id = profile.getJobID().toString();
    parentId = state.getParent();
    percentComplete = state.getPercentComplete();
    exitValue = state.getExitValue();
    user = profile.getUser();
    callback = state.getCallback();
    completed = state.getCompleteStatus();
    userargs = state.getUserArgs();
  }
}
