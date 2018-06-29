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

package org.apache.hadoop.hive.ql.stats;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.plan.Statistics.State;
import org.apache.hadoop.hive.ql.stats.BasicStats;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

public class TestBasicStats {

  public static class LocalPartishBuilder {
    Map<String, String> params = new HashMap<>();

    public LocalPartishBuilder numRows(int i) {
      params.put(StatsSetupConst.ROW_COUNT, String.valueOf(i));
      return this;
    }

    public LocalPartishBuilder rawDataSize(int i) {
      params.put(StatsSetupConst.RAW_DATA_SIZE, String.valueOf(i));
      return this;
    }

    public LocalPartishBuilder totalSize(int i) {
      params.put(StatsSetupConst.TOTAL_SIZE, String.valueOf(i));
      return this;
    }

    public Partish buildPartition() {
      Partition partition = Mockito.mock(Partition.class);
      org.apache.hadoop.hive.metastore.api.Partition tpartition = Mockito.mock(org.apache.hadoop.hive.metastore.api.Partition.class);
      doReturn(tpartition).when(partition).getTPartition();
      doReturn(params).when(tpartition).getParameters();
      return Partish.buildFor(null, partition);
    }
  }

  @Test
  public void testDataSizeEstimator() {
    Partish p1 = new LocalPartishBuilder().totalSize(10).buildPartition();

    HiveConf conf = new HiveConf();
    conf.setFloatVar(ConfVars.HIVE_STATS_DESERIALIZATION_FACTOR, 13.0f);
    BasicStats.Factory factory = new BasicStats.Factory(new BasicStats.DataSizeEstimator(conf));

    BasicStats res = factory.build(p1);

    assertEquals(130, res.getDataSize());
  }

  @Test
  public void mergeWithEmpty() {

    HiveConf conf = new HiveConf();
    int avgRowSize = 100;
    int r0 = 13;
    int r1 = 15;
    int deserFactor = (int) conf.getFloatVar(ConfVars.HIVE_STATS_DESERIALIZATION_FACTOR);
    Partish p0 = new LocalPartishBuilder().numRows(r0).rawDataSize(avgRowSize * r0).buildPartition();
    Partish p1 = new LocalPartishBuilder().totalSize(r1 * avgRowSize / deserFactor).buildPartition();

    BasicStats.Factory factory =
        new BasicStats.Factory(new BasicStats.DataSizeEstimator(conf), new BasicStats.RowNumEstimator(avgRowSize));

    BasicStats bs0 = factory.build(p0);
    BasicStats bs1 = factory.build(p1);

    BasicStats res = BasicStats.buildFrom(Lists.newArrayList(bs0, bs1));

    assertEquals(r0 + r1, res.getNumRows());
    assertEquals(avgRowSize * (r0 + r1), res.getDataSize());
  }

  @Test
  @Ignore("HIVE-18062 will fix this")
  public void mergedKeepsPartialStateEvenIfValuesAreSuccessfullyEstimated() {
    Partish p0 = new LocalPartishBuilder().numRows(10).rawDataSize(100).buildPartition();
    Partish p1 = new LocalPartishBuilder().totalSize(10).buildPartition();

    HiveConf conf = new HiveConf();
    BasicStats.Factory factory =
        new BasicStats.Factory(new BasicStats.DataSizeEstimator(conf), new BasicStats.RowNumEstimator(10));

    BasicStats bs0 = factory.build(p0);
    BasicStats bs1 = factory.build(p1);

    BasicStats res = BasicStats.buildFrom(Lists.newArrayList(bs0, bs1));

    assertEquals(State.PARTIAL, res.getState());
  }


}
