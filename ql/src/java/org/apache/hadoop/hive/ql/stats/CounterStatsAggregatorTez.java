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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.tez.common.counters.TezCounters;

/**
 * This class aggregates stats via counters and does so for Tez Tasks.
 * With dbclass=counters this class will compute table/partition statistics
 * using hadoop counters. They will be published using special keys and
 * then retrieved on the client after the insert/ctas statement ran.
 */
public class CounterStatsAggregatorTez implements StatsAggregator, StatsCollectionTaskIndependent {

  private static final Log LOG = LogFactory.getLog(CounterStatsAggregatorTez.class.getName());

  private TezCounters counters;
  private final CounterStatsAggregator mrAggregator;
  private boolean delegate;

  public CounterStatsAggregatorTez() {
    mrAggregator = new CounterStatsAggregator();
  }

  @Override
  public boolean connect(Configuration hconf, Task sourceTask) {
    if (!(sourceTask instanceof TezTask)) {
      delegate = true;
      return mrAggregator.connect(hconf, sourceTask);
    }
    counters = ((TezTask) sourceTask).getTezCounters();
    return counters != null;
  }

  @Override
  public String aggregateStats(String keyPrefix, String statType) {
    String result;

    if (delegate) {
      result = mrAggregator.aggregateStats(keyPrefix, statType);
    } else {
      long value = 0;
      for (String groupName : counters.getGroupNames()) {
        if (groupName.startsWith(keyPrefix)) {
          value += counters.getGroup(groupName).findCounter(statType).getValue();
        }
      }
      result = String.valueOf(value);
    }
    LOG.info("Counter based stats for ("+keyPrefix+") are: "+result);
    return result;
  }

  @Override
  public boolean closeConnection() {
    return true;
  }

  @Override
  public boolean cleanUp(String keyPrefix) {
    return true;
  }
}
