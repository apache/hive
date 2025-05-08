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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.ql.txn.compactor.CompactorUtil.COMPACTOR_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class TestStatsUpdater {
  @Test
  public void testCreateConfigurationAddsCompactorPropertiesFromTableProperties() {
    StatsUpdater statsUpdater = new StatsUpdater();

    HiveConf conf = new HiveConf();
    Map<String, String> tableProperties = new HashMap<String, String>() {{
      put(COMPACTOR_PREFIX + "test.property", "test-value");
    }};

    Map<String, String> ciProperties = new HashMap<>();
    HiveConf statsUpdaterConf = statsUpdater.createConfiguration(conf, "testQueue", tableProperties, ciProperties);

    assertThat(statsUpdaterConf.get("test.property"), is("test-value"));
  }

  @Test
  public void testCreateConfigurationAddsCompactorPropertiesFromCompactionInfo() {
    StatsUpdater statsUpdater = new StatsUpdater();

    HiveConf conf = new HiveConf();
    Map<String, String> tableProperties = new HashMap<>();
    Map<String, String> ciProperties = new HashMap<String, String>() {{
      put(COMPACTOR_PREFIX + "test.property", "test-value");
    }};

    HiveConf statsUpdaterConf = statsUpdater.createConfiguration(conf, "testQueue", tableProperties, ciProperties);

    assertThat(statsUpdaterConf.get("test.property"), is("test-value"));
  }

  @Test
  public void testCreateConfigurationOverridesExistingCompactorProperties() {
    StatsUpdater statsUpdater = new StatsUpdater();

    HiveConf conf = new HiveConf();
    conf.set(COMPACTOR_PREFIX + "test.property", "test-value");

    Map<String, String> tableProperties = new HashMap<String, String>() {{
      put(COMPACTOR_PREFIX + "test.property", "changed-in-table-properties");
    }};

    Map<String, String> ciProperties = new HashMap<String, String>() {{
      put(COMPACTOR_PREFIX + "test.property", "changed-in-compaction-info");
    }};

    HiveConf statsUpdaterConf = statsUpdater.createConfiguration(conf, "testQueue", tableProperties, ciProperties);

    assertThat(statsUpdaterConf.get("test.property"), is("changed-in-compaction-info"));
  }
}
