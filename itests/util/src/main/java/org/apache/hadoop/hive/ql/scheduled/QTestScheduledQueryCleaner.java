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
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.ScheduledQuery;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryKey;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryMaintenanceRequest;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryMaintenanceRequestType;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.hive.ql.hooks.ScheduledQueryCreationRegistryHook;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.qoption.QTestOptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * This handler does an aftertest cleanup; and deleters scheduled queries.
 */
public class QTestScheduledQueryCleaner implements QTestOptionHandler {

  private static final Logger LOG = LoggerFactory.getLogger(QTestScheduledQueryCleaner.class);

  @Override
  public void processArguments(String arguments) {
  }

  @Override
  public void beforeTest(QTestUtil qt) throws Exception {

  }

  @Override
  public void afterTest(QTestUtil qt) throws Exception {
    clearScheduledQueries(qt.getConf());

  }

  private void clearScheduledQueries(HiveConf conf) {
    if (System.getenv(QTestUtil.QTEST_LEAVE_FILES) != null) {
      return;
    }
    Set<String> scheduledQueries = ScheduledQueryCreationRegistryHook.getSchedules();
    for (String name : scheduledQueries) {
      ScheduledQueryMaintenanceRequest request = new ScheduledQueryMaintenanceRequest();
      request.setType(ScheduledQueryMaintenanceRequestType.DROP);
      ScheduledQuery schq = new ScheduledQuery();
      schq.setScheduleKey(new ScheduledQueryKey(name, conf.getVar(ConfVars.HIVE_SCHEDULED_QUERIES_NAMESPACE)));
      request.setScheduledQuery(schq);
      try {
        Hive db = Hive.get(conf); // propagate new conf to meta store

        db.getMSC().scheduledQueryMaintenance(request);
        db.close(false);
      } catch (Exception e) {
        LOG.error("Can't remove scheduled query: " + name + " " + e.getMessage());
      }
    }
    scheduledQueries.clear();
  }
}
