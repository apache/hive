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
package org.apache.hadoop.hive.ql.queryhistory;

import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.queryhistory.schema.QueryHistoryRecord;

public class FromPerfLoggerUpdater {

  private static final FromPerfLoggerUpdater INSTANCE = new FromPerfLoggerUpdater();

  public static FromPerfLoggerUpdater getInstance() {
    return INSTANCE;
  }

  public void consume(PerfLogger perfLogger, QueryHistoryRecord record) {
    record.setPlanningDuration(perfLogger.getEndTime(PerfLogger.COMPILE) - perfLogger.getStartTime(PerfLogger.COMPILE));
    record.setPlanningStartTime(perfLogger.getStartTime(PerfLogger.COMPILE));

    record.setPreparePlanDuration(perfLogger.getPreparePlanDuration());
    record.setPreparePlanStartTime(perfLogger.getEndTime(PerfLogger.COMPILE));

    record.setGetSessionDuration(perfLogger.getDuration(PerfLogger.TEZ_GET_SESSION));
    record.setGetSessionStartTime(perfLogger.getStartTime(PerfLogger.TEZ_GET_SESSION));

    record.setExecutionDuration(perfLogger.getRunDagDuration());
    record.setExecutionStartTime(perfLogger.getStartTime(PerfLogger.TEZ_RUN_DAG));
  }
}
