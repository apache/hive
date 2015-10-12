/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.stats;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hive.spark.counter.SparkCounters;

public class CounterStatsAggregatorSpark
  implements StatsAggregator, StatsCollectionTaskIndependent {

  private static final Log LOG = LogFactory.getLog(CounterStatsAggregatorSpark.class);

  private SparkCounters sparkCounters;

  @SuppressWarnings("rawtypes")
  @Override
  public boolean connect(StatsCollectionContext scc) {
    SparkTask task = (SparkTask) scc.getTask();
    sparkCounters = task.getSparkCounters();
    if (sparkCounters == null) {
      return false;
    }
    return true;
  }

  @Override
  public String aggregateStats(String keyPrefix, String statType) {
    long value = sparkCounters.getValue(keyPrefix, statType);
    String result = String.valueOf(value);
    LOG.info(
      String.format("Counter based stats for (%s, %s) are: %s", keyPrefix, statType, result));
    return result;
  }

  @Override
  public boolean closeConnection(StatsCollectionContext scc) {
    return true;
  }

  @Override
  public boolean cleanUp(String keyPrefix) {
    return true;
  }
}
