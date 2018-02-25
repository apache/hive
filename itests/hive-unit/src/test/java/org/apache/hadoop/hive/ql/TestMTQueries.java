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

package org.apache.hadoop.hive.ql;

import java.io.File;

/**
 * Suite for testing running of queries in multi-threaded mode.
 */
public class TestMTQueries extends BaseTestQueries {

  public TestMTQueries() {
    File logDirFile = new File(logDir);
    if (!(logDirFile.exists() || logDirFile.mkdirs())) {
      fail("Could not create " + logDir);
    }
  }

  public void testMTQueries1() throws Exception {
    String[] testNames = new String[] {"join2.q", "groupby1.q", "input1.q", "input19.q"};

    File[] qfiles = setupQFiles(testNames);
    QTestUtil[] qts = QTestUtil.queryListRunnerSetup(qfiles, resDir, logDir, "q_test_init_src_with_stats.sql",
      "q_test_cleanup_src_with_stats.sql");
    for (QTestUtil util : qts) {
      // derby fails creating multiple stats aggregator concurrently
      util.getConf().setBoolean("hive.exec.submitviachild", true);
      util.getConf().setBoolean("hive.exec.submit.local.task.via.child", true);
      util.getConf().set("hive.stats.dbclass", "fs");
      util.getConf().set("hive.mapred.mode", "nonstrict");
      util.getConf().set("hive.stats.column.autogather", "false");
    }
    boolean success = QTestUtil.queryListRunnerMultiThreaded(qfiles, qts);
    if (!success) {
      fail("One or more queries failed");
    }
  }
}
