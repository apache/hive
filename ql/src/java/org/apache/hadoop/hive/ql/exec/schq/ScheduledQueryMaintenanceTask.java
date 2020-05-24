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

package org.apache.hadoop.hive.ql.exec.schq;

import org.apache.hadoop.hive.metastore.api.ScheduledQueryMaintenanceRequest;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryMaintenanceRequestType;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.scheduled.ScheduledQueryExecutionService;
import org.apache.hadoop.hive.ql.scheduled.ScheduledQueryMaintenanceWork;
import org.apache.thrift.TException;

/**
 * Scheduled query maintenance task.
 *
 * CREATES/ALTERS or DROPs a scheduled query.
 */
public class ScheduledQueryMaintenanceTask extends Task<ScheduledQueryMaintenanceWork> {

  private static final long serialVersionUID = 1L;

  @Override
  public String getName() {
    return "SCHEDULED QUERY MAINTENANCE TASK";
  }

  @Override
  public int execute() {
    ScheduledQueryMaintenanceRequest request = buildScheduledQueryRequest();
    try {
      Hive.get().getMSC().scheduledQueryMaintenance(request);
      if (work.getScheduledQuery().isSetNextExecution()
          || request.getType() == ScheduledQueryMaintenanceRequestType.CREATE) {
        // we might have a scheduled query available for execution; immediately:
        // * in case a schedule is altered to be executed at a specific time
        // * in case we created a new scheduled query - for say run every second
        ScheduledQueryExecutionService.forceScheduleCheck();
      }
    } catch (TException | HiveException e) {
      setException(e);
      LOG.error("Failed", e);
      return 1;
    }
    return 0;
  }

  private ScheduledQueryMaintenanceRequest buildScheduledQueryRequest() {
    ScheduledQueryMaintenanceRequest req = new ScheduledQueryMaintenanceRequest();
    req.setType(work.getType());
    req.setScheduledQuery(work.getScheduledQuery());
    return req;
  }

  @Override
  public StageType getType() {
    return StageType.SCHEDULED_QUERY_MAINT;
  }
}
