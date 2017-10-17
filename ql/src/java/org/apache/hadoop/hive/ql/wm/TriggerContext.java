/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.wm;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.QueryInfo;

/**
 * Some context information that are required for rule evaluation.
 */
public class TriggerContext {
  private Set<String> desiredCounters = new HashSet<>();
  private Map<String, Long> currentCounters = new HashMap<>();
  private String queryId;
  private long queryStartTime;

  public TriggerContext(final long queryStartTime, final String queryId) {
    this.queryStartTime = queryStartTime;
    this.queryId = queryId;
  }

  public String getQueryId() {
    return queryId;
  }

  public void setQueryId(final String queryId) {
    this.queryId = queryId;
  }

  public long getQueryStartTime() {
    return queryStartTime;
  }

  public void setQueryStartTime(final long queryStartTime) {
    this.queryStartTime = queryStartTime;
  }

  public Set<String> getDesiredCounters() {
    return desiredCounters;
  }

  public void setDesiredCounters(final Set<String> desiredCounters) {
    this.desiredCounters = desiredCounters;
  }

  public Map<String, Long> getCurrentCounters() {
    return currentCounters;
  }

  public void setCurrentCounters(final Map<String, Long> currentCounters) {
    this.currentCounters = currentCounters;
  }

  public long getElapsedTime() {
    return System.currentTimeMillis() - queryStartTime;
  }
}
