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
package org.apache.hadoop.hive.ql.exec.tez;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.Assert;
import org.junit.Test;
import java.util.Map;

import java.lang.reflect.Field;
import java.util.HashMap;

public class TestTezConfigurationFactory {
  private static final Field updatingResource;

  static {
    try {
      //Cache the field handle so that we can avoid expensive conf.getPropertySources(key) later
      updatingResource = Configuration.class.getDeclaredField("updatingResource");
    } catch (NoSuchFieldException | SecurityException e) {
      throw new RuntimeException(e);
    }
    updatingResource.setAccessible(true);
  }

  @Test
  public void testAddProgrammaticallyAddedTezOptsToDagConf() {
    Map<String, String> dagConf = new HashMap<>();
    JobConf conf = new JobConf();

    conf.set(TezConfiguration.TEZ_GENERATE_DEBUG_ARTIFACTS, "true");
    TezConfigurationFactory.addProgrammaticallyAddedTezOptsToDagConf(dagConf, conf);
    Assert.assertTrue("dagConfig should contain value for " + TezConfiguration.TEZ_GENERATE_DEBUG_ARTIFACTS,
        dagConf.containsKey(TezConfiguration.TEZ_GENERATE_DEBUG_ARTIFACTS));

    conf.clear();
    dagConf.clear();

    conf.set(TezConfiguration.TEZ_GENERATE_DEBUG_ARTIFACTS, "true", "tez-site.xml");
    TezConfigurationFactory.addProgrammaticallyAddedTezOptsToDagConf(dagConf, conf);
    Assert.assertFalse(
        "dagConfig should not contain value for xml sourced config " + TezConfiguration.TEZ_GENERATE_DEBUG_ARTIFACTS,
        dagConf.containsKey(TezConfiguration.TEZ_GENERATE_DEBUG_ARTIFACTS));

    conf.set(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS, "asdf");
    TezConfigurationFactory.addProgrammaticallyAddedTezOptsToDagConf(dagConf, conf);
    // filtered out because it's AM scoped
    Assert.assertFalse("dagConfig should not contain value for " + TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS,
        dagConf.containsKey(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS));

    conf.set("hive.something", "asdf");
    TezConfigurationFactory.addProgrammaticallyAddedTezOptsToDagConf(dagConf, conf);
    // only tez options are added
    Assert.assertFalse("dagConfig should not contain value for a hive config", dagConf.containsKey("hive.something"));
  }
}
