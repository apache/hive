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
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageEncoder;
import org.apache.hive.hcatalog.api.repl.ReplicationV1CompatRule;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestRule;

import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TestReplWithJsonMessageFormat extends TestReplicationScenarios {
  @Rule
  public TestRule replV1BackwardCompatibleRule =
      new ReplicationV1CompatRule(metaStoreClient, hconf, new ArrayList<String>() {{
          add("testEventFilters");
          add("testReplConfiguredCleanupOfNotificationEvents");
          add("testCleanerThreadStartupWait");
      }});

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
            JSONMessageEncoder.class.getCanonicalName());
    internalBeforeClassSetup(overrides);
  }

}
