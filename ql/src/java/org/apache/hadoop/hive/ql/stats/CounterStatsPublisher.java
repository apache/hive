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

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.mapred.Reporter;

public class CounterStatsPublisher implements StatsPublisher {

  private static final Log LOG = LogFactory.getLog(CounterStatsPublisher.class.getName());

  private Reporter reporter;

  @Override
  public boolean init(StatsCollectionContext context) {
    return true;
  }

  @Override
  public boolean connect(StatsCollectionContext statsContext) {
    MapredContext context = MapredContext.get();
    if (context == null || context.getReporter() == null) {
      return false;
    }
    reporter = context.getReporter();
    return true;
  }

  @Override
  public boolean publishStat(String fileID, Map<String, String> stats) {
    for (Map.Entry<String, String> entry : stats.entrySet()) {
      try {
        reporter.incrCounter(fileID, entry.getKey(), Long.valueOf(entry.getValue()));
      } catch (Exception e) {
        LOG.error("Failed to increment counter value " + entry.getValue() + " for " + entry.getKey()
          + ": " + e, e);
        return false;
      }
    }
    return true;
  }
  @Override
  public boolean closeConnection(StatsCollectionContext context) {
    return true;
  }
}
