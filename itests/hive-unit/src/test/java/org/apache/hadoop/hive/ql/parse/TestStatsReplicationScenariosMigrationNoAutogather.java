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
public class TestStatsReplicationScenariosMigrationNoAutogather extends TestStatsReplicationScenarios {
  @Rule
  public final TestName testName = new TestName();

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
            GzipJSONMessageEncoder.class.getCanonicalName());

    Map<String, String> replicaConfigs = new HashMap<String, String>() {{
      put("hive.support.concurrency", "true");
      put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
      put("hive.metastore.client.capability.check", "false");
      put("hive.repl.bootstrap.dump.open.txn.timeout", "1s");
      put("hive.exec.dynamic.partition.mode", "nonstrict");
      put("hive.strict.checks.bucketing", "false");
      put("hive.mapred.mode", "nonstrict");
      put("mapred.input.dir.recursive", "true");
      put("hive.metastore.disallow.incompatible.col.type.changes", "false");
      put("hive.strict.managed.tables", "true");
    }};
    replicaConfigs.putAll(overrides);

    Map<String, String> primaryConfigs = new HashMap<String, String>() {{
      put("hive.metastore.client.capability.check", "false");
      put("hive.repl.bootstrap.dump.open.txn.timeout", "1s");
      put("hive.exec.dynamic.partition.mode", "nonstrict");
      put("hive.strict.checks.bucketing", "false");
      put("hive.mapred.mode", "nonstrict");
      put("mapred.input.dir.recursive", "true");
      put("hive.metastore.disallow.incompatible.col.type.changes", "false");
      put("hive.support.concurrency", "false");
      put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager");
      put("hive.strict.managed.tables", "false");
    }};
    primaryConfigs.putAll(overrides);

    internalBeforeClassSetup(primaryConfigs, replicaConfigs,
            TestStatsReplicationScenariosMigrationNoAutogather.class, false, null);
  }
}
