/**
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

package org.apache.hadoop.hive.common;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Lists;
public class TestStatsSetupConst {

  @Test
  public void testSetBasicStatsState_missesUpgrade() {
    Map<String, String> params=new HashMap<>();
    params.put(StatsSetupConst.COLUMN_STATS_ACCURATE, "FALSE");
    StatsSetupConst.setBasicStatsState(params, String.valueOf(true));
    assertEquals("{\"BASIC_STATS\":\"true\"}",params.get(StatsSetupConst.COLUMN_STATS_ACCURATE));
  }

  @Test
  public void setColumnStatsState_camelcase() {
    Map<String, String> params=new HashMap<>();
    StatsSetupConst.setColumnStatsState(params, Lists.newArrayList("Foo"));
    String val1 = params.get(StatsSetupConst.COLUMN_STATS_ACCURATE);
    StatsSetupConst.setColumnStatsState(params, Lists.newArrayList("Foo"));
    String val2 = params.get(StatsSetupConst.COLUMN_STATS_ACCURATE);
    assertEquals(val1, val2);
  }

  @Test
  public void testSetBasicStatsState_none() {
    Map<String, String> params=new HashMap<>();
    StatsSetupConst.setBasicStatsState(params, String.valueOf(true));
    assertEquals("{\"BASIC_STATS\":\"true\"}",params.get(StatsSetupConst.COLUMN_STATS_ACCURATE));
  }

}
