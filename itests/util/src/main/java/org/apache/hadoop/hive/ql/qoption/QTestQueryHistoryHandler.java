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
package org.apache.hadoop.hive.ql.qoption;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ServiceContext;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.hive.ql.qoption.QTestOptionHandler;
import org.apache.hadoop.hive.ql.queryhistory.QueryHistoryService;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * QTest Query History Service option
 * <p>
 * Enables the query history service for a qtest.
 * <p>
 * Example:
 * --! qt:queryhistory
 */
public class QTestQueryHistoryHandler implements QTestOptionHandler {
  private boolean enabled;

  @Override
  public void processArguments(String arguments) {
    enabled = true;
  }

  @Override
  public void beforeTest(QTestUtil qt) throws Exception {
    HiveConf hiveConf = qt.getConf();
    if (enabled) {
      HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_QUERY_HISTORY_ENABLED, true);
      // no matter what is default size, in qtest let's persist as soon as possible
      HiveConf.setIntVar(hiveConf, HiveConf.ConfVars.HIVE_QUERY_HISTORY_BATCH_SIZE, 0);

      QueryHistoryService queryHistoryService = QueryHistoryService.newInstance(hiveConf, new ServiceContext(() ->
          "localhost", () -> 0));
      queryHistoryService.start();
    }
  }

  @Override
  public void afterTest(QTestUtil qt) throws Exception {
    enabled = false;
    HiveConf.setBoolVar(qt.getConf(), HiveConf.ConfVars.HIVE_QUERY_HISTORY_ENABLED, false);
  }
}
