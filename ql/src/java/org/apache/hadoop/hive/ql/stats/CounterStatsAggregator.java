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
import org.apache.hadoop.hive.ql.exec.mr.ExecDriver;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

public class CounterStatsAggregator implements StatsAggregator, StatsCollectionTaskIndependent {

  private static final Log LOG = LogFactory.getLog(CounterStatsAggregator.class.getName());

  private Counters counters;
  private JobClient jc;

  @Override
  public boolean connect(StatsCollectionContext scc) {
    Task<?> sourceTask = scc.getTask();
    if (sourceTask instanceof MapRedTask) {
      try {
        jc = new JobClient(toJobConf(scc.getHiveConf()));
        RunningJob job = jc.getJob(((MapRedTask)sourceTask).getJobID());
        if (job != null) {
          counters = job.getCounters();
        }
      } catch (Exception e) {
        LOG.error("Failed to get Job instance for " + sourceTask.getJobID(),e);
      }
    }
    return counters != null;
  }

  private JobConf toJobConf(Configuration hconf) {
    return hconf instanceof JobConf ? (JobConf)hconf : new JobConf(hconf, ExecDriver.class);
  }

  @Override
  public String aggregateStats(String counterGrpName, String statType) {
    long value = 0;
    if (counters != null) {
      // In case of counters, aggregation is done by JobTracker / MR AM itself
      // so no need to aggregate, simply return the counter value for requested stat.
      value = counters.getGroup(counterGrpName).getCounter(statType);
    }
    return String.valueOf(value);
  }

  @Override
  public boolean closeConnection(StatsCollectionContext scc) {
    try {
      jc.close();
    } catch (IOException e) {
      LOG.error("Error closing job client for stats aggregator.", e);
    }
    return true;
  }

  @Override
  public boolean cleanUp(String keyPrefix) {
    return true;
  }
}
