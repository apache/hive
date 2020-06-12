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
package org.apache.hadoop.hive.ql.scheduled;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.hive.ql.qoption.QTestOptionHandler;

/**
 * Adding this qtestoption enables the scheduled query service.
 */
public class QTestScheduledQueryServiceProvider implements QTestOptionHandler {

  private boolean enabled;
  private ScheduledQueryExecutionService service;

  public QTestScheduledQueryServiceProvider(HiveConf conf) {
    conf.setVar(HiveConf.ConfVars.HIVE_SCHEDULED_QUERIES_EXECUTOR_IDLE_SLEEP_TIME, "1s");
    conf.setVar(HiveConf.ConfVars.HIVE_SCHEDULED_QUERIES_EXECUTOR_PROGRESS_REPORT_INTERVAL, "1s");
  }

  @Override
  public void processArguments(String arguments) {
    enabled = true;
  }

  @Override
  public void beforeTest(QTestUtil qt) throws Exception {
    if (enabled) {
      service = ScheduledQueryExecutionService.startScheduledQueryExecutorService(qt.getConf());
    }
  }

  @Override
  public void afterTest(QTestUtil qt) throws Exception {
    if(service != null) {
      service.close();
    }
    service = null;
    enabled = false;
  }

}
