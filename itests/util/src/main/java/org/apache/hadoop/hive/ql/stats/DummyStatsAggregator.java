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

import org.apache.hadoop.hive.conf.HiveConf;

/**
 * An test implementation for StatsAggregator.
 * The method corresponding to the configuration parameter
 * hive.test.dummystats.aggregator fail, whereas all
 * other methods succeed.
 */

public class DummyStatsAggregator implements StatsAggregator {
  String errorMethod = null;

  // This is a test. The parameter hive.test.dummystats.aggregator's value
  // denotes the method which needs to throw an error.
  @Override
  public boolean connect(StatsCollectionContext scc) {
    errorMethod = HiveConf.getVar(scc.getHiveConf(), HiveConf.ConfVars.HIVETESTMODEDUMMYSTATAGGR);
    if (errorMethod.equalsIgnoreCase("connect")) {
      return false;
    }

    return true;
  }

  @Override
  public String aggregateStats(String keyPrefix, String statType) {
    return null;
  }

  @Override
  public boolean closeConnection(StatsCollectionContext scc) {
    if (errorMethod.equalsIgnoreCase("closeConnection")) {
      return false;
    }
    return true;
  }
}
