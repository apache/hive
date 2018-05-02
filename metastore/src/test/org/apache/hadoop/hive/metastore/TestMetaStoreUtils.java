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

package org.apache.hadoop.hive.metastore;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.Assert;
import org.junit.Test;

public class TestMetaStoreUtils {

  @Test
  public void testColumnsIncluded() {
    FieldSchema col1 = new FieldSchema("col1", "string", "col1 comment");
    FieldSchema col2 = new FieldSchema("col2", "string", "col2 comment");
    FieldSchema col3 = new FieldSchema("col3", "string", "col3 comment");
    Assert.assertTrue(MetaStoreUtils.columnsIncluded(Arrays.asList(col1), Arrays.asList(col1)));
    Assert.assertTrue(MetaStoreUtils.columnsIncluded(Arrays.asList(col1, col2), Arrays.asList(col1, col2)));
    Assert.assertTrue(MetaStoreUtils.columnsIncluded(Arrays.asList(col1, col2), Arrays.asList(col2, col1)));
    Assert.assertTrue(MetaStoreUtils.columnsIncluded(Arrays.asList(col1, col2), Arrays.asList(col1, col2, col3)));
    Assert.assertTrue(MetaStoreUtils.columnsIncluded(Arrays.asList(col1, col2), Arrays.asList(col3, col2, col1)));
    Assert.assertFalse(MetaStoreUtils.columnsIncluded(Arrays.asList(col1, col2), Arrays.asList(col1)));
  }

  @Test
  public void isFastStatsSameWithNullPartitions() {
    Partition partition = new Partition();
    Assert.assertFalse(MetaStoreUtils.isFastStatsSame(null, null));
    Assert.assertFalse(MetaStoreUtils.isFastStatsSame(null, partition));
    Assert.assertFalse(MetaStoreUtils.isFastStatsSame(partition, null));
  }

  @Test
  public void isFastStatsSameWithNoMatchingStats() {
    Partition oldPartition = new Partition();
    Map<String, String> stats = new HashMap<>();
    oldPartition.setParameters(stats);
    Assert.assertFalse(MetaStoreUtils.isFastStatsSame(oldPartition, null));
    stats.put("someKeyThatIsNotInFastStats","value");
    oldPartition.setParameters(stats);
    Assert.assertFalse(MetaStoreUtils.isFastStatsSame(oldPartition, null));
  }

  @Test
  public void isFastStatsSameMatchingButOnlyOneStat() {
    Partition oldPartition = new Partition();
    Partition newPartition = new Partition();
    Map<String, String> stats = new HashMap<>();
    stats.put(StatsSetupConst.fastStats[0], "1");
    oldPartition.setParameters(stats);
    newPartition.setParameters(stats);
    Assert.assertFalse(MetaStoreUtils.isFastStatsSame(oldPartition, newPartition));
  }

  @Test
  public void isFastStatsSameMatching() {
    Partition oldPartition = new Partition();
    Partition newPartition = new Partition();
    Map<String, String> stats = new HashMap<>();
    for (int i=0; i<StatsSetupConst.fastStats.length; i++) {
      stats.put(StatsSetupConst.fastStats[i], String.valueOf(i));
    }
    oldPartition.setParameters(stats);
    newPartition.setParameters(stats);
    Assert.assertTrue(MetaStoreUtils.isFastStatsSame(oldPartition, newPartition));
  }

  @Test
  public void isFastStatsSameDifferent() {
    Partition oldPartition = new Partition();
    Partition newPartition = new Partition();
    Map<String, String> oldStats = new HashMap<>();
    for (int i=0; i<StatsSetupConst.fastStats.length; i++) {
      oldStats.put(StatsSetupConst.fastStats[i], String.valueOf(i));
    }
    oldPartition.setParameters(oldStats);
    Map<String, String> newStats = new HashMap<>();
    for (int i=0; i<StatsSetupConst.fastStats.length; i++) {
      //set the values to i+1 so they are different in the new stats
      newStats.put(StatsSetupConst.fastStats[i], String.valueOf(i+1));
    }
    newPartition.setParameters(newStats);
    Assert.assertFalse(MetaStoreUtils.isFastStatsSame(oldPartition, newPartition));
  }

  @Test
  public void isFastStatsSameNullStatsInNew() {
    Partition oldPartition = new Partition();
    Partition newPartition = new Partition();
    Map<String, String> oldStats = new HashMap<>();
    for (int i=0; i<StatsSetupConst.fastStats.length; i++) {
      oldStats.put(StatsSetupConst.fastStats[i], String.valueOf(i));
    }
    oldPartition.setParameters(oldStats);
    Map<String, String> newStats = new HashMap<>();
    for (int i=0; i<StatsSetupConst.fastStats.length; i++) {
      newStats.put(StatsSetupConst.fastStats[i], null);
    }
    newPartition.setParameters(newStats);
    Assert.assertFalse(MetaStoreUtils.isFastStatsSame(oldPartition, newPartition));
  }

}
