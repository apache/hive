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

package org.apache.hadoop.hive.metastore.model;

import org.apache.hadoop.hive.metastore.api.Schedule;
import org.apache.hadoop.hive.metastore.api.ScheduledQuery;

/**
 * Represents a runtime stat query entry.
 *
 * As a query may contain a large number of operatorstat entries; they are stored together in a single row in the metastore.
 * The number of operator stat entries this entity has; is shown in the weight column.
 */
public class MScheduledQuery {

  private String scheduleName;
  private boolean enabled;
  private String clusterNamespace;
  private String schedule;
  private String user;
  private String query;
  private int nextExecution;

  public MScheduledQuery(ScheduledQuery s) {
    scheduleName = s.getScheduleName();
    enabled = s.isEnabled();
    clusterNamespace = s.getClusterNamespace();
    schedule = s.getSchedule().getCron();
    user = s.getUser();
    query = s.getQuery();
    nextExecution = s.getNextExecution();
  }

  public static MScheduledQuery fromThrift(ScheduledQuery s) {
    return new MScheduledQuery(s);
  }

  public static ScheduledQuery toThrift(MScheduledQuery s) {
    ScheduledQuery ret = new ScheduledQuery();
    ret.setScheduleName(s.scheduleName);
    ret.setEnabled(s.enabled);
    ret.setClusterNamespace(s.clusterNamespace);
    Schedule sschedule = new Schedule();
    sschedule.setCron(s.schedule);
    ret.setSchedule(sschedule);
    ret.setUser(s.user);
    ret.setQuery(s.query);
    ret.setNextExecution(s.nextExecution);
    return ret;
  }

  public ScheduledQuery toThrift() {
    return toThrift(this);
  }

}
