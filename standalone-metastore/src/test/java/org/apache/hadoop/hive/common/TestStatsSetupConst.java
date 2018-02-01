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

package org.apache.hadoop.hive.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.Test;

import com.google.common.collect.Lists;
import org.junit.experimental.categories.Category;

@Category(MetastoreUnitTest.class)
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

  @Test
  public void testSetBasicStatsState_falseIsAbsent() {
    Map<String, String> params=new HashMap<>();
    StatsSetupConst.setBasicStatsState(params, String.valueOf(true));
    StatsSetupConst.setBasicStatsState(params, String.valueOf(false));
    assertNull(params.get(StatsSetupConst.COLUMN_STATS_ACCURATE));
  }

  // earlier implementation have quoted boolean values...so the new implementation should preserve this
  @Test
  public void testStatColumnEntriesCompat() {
    Map<String, String> params0=new HashMap<>();
    StatsSetupConst.setBasicStatsState(params0, String.valueOf(true));
    StatsSetupConst.setColumnStatsState(params0, Lists.newArrayList("Foo"));

    assertEquals("{\"BASIC_STATS\":\"true\",\"COLUMN_STATS\":{\"Foo\":\"true\"}}",params0.get(StatsSetupConst.COLUMN_STATS_ACCURATE));
  }

  @Test
  public void testColumnEntries_orderIndependence() {
    Map<String, String> params0=new HashMap<>();
    StatsSetupConst.setBasicStatsState(params0, String.valueOf(true));
    StatsSetupConst.setColumnStatsState(params0, Lists.newArrayList("Foo","Bar"));
    Map<String, String> params1=new HashMap<>();
    StatsSetupConst.setColumnStatsState(params1, Lists.newArrayList("Bar","Foo"));
    StatsSetupConst.setBasicStatsState(params1, String.valueOf(true));

    assertEquals(params0.get(StatsSetupConst.COLUMN_STATS_ACCURATE),params1.get(StatsSetupConst.COLUMN_STATS_ACCURATE));
  }

  @Test
  public void testColumnEntries_orderIndependence2() {
    Map<String, String> params0=new HashMap<>();
    // in case jackson is able to deserialize...it may use a different implementation for the map - which may not preserve order
    StatsSetupConst.setBasicStatsState(params0, String.valueOf(true));
    StatsSetupConst.setColumnStatsState(params0, Lists.newArrayList("year"));
    StatsSetupConst.setColumnStatsState(params0, Lists.newArrayList("year","month"));
    Map<String, String> params1=new HashMap<>();
    StatsSetupConst.setColumnStatsState(params1, Lists.newArrayList("month","year"));
    StatsSetupConst.setBasicStatsState(params1, String.valueOf(true));

    System.out.println(params0.get(StatsSetupConst.COLUMN_STATS_ACCURATE));
    assertEquals(params0.get(StatsSetupConst.COLUMN_STATS_ACCURATE),params1.get(StatsSetupConst.COLUMN_STATS_ACCURATE));
  }

  // FIXME: current objective is to keep the previous outputs...but this is possibly bad..
  @Test
  public void testColumnEntries_areKept_whenBasicIsAbsent() {
    Map<String, String> params=new HashMap<>();
    StatsSetupConst.setBasicStatsState(params, String.valueOf(false));
    StatsSetupConst.setColumnStatsState(params, Lists.newArrayList("Foo"));
    assertEquals("{\"COLUMN_STATS\":{\"Foo\":\"true\"}}",params.get(StatsSetupConst.COLUMN_STATS_ACCURATE));
  }
}
