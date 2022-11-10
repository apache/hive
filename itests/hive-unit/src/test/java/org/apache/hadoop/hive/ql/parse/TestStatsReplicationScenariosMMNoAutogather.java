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
package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests statistics replication for ACID tables.
 */
public class TestStatsReplicationScenariosMMNoAutogather extends TestStatsReplicationScenarios {
  @Rule
  public final TestName testName = new TestName();

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());
    overrides.put(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "true");
    overrides.put(HiveConf.ConfVars.HIVE_TXN_MANAGER.varname,
              "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
    overrides.put(MetastoreConf.ConfVars.CAPABILITY_CHECK.getHiveName(), "false");
    overrides.put(HiveConf.ConfVars.REPL_BOOTSTRAP_DUMP_OPEN_TXN_TIMEOUT.varname, "1s");
    overrides.put("mapred.input.dir.recursive", "true");


    internalBeforeClassSetup(overrides, overrides,
            TestStatsReplicationScenariosMMNoAutogather.class, false, AcidTableKind.INSERT_ONLY);
  }
}
