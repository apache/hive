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

import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * An test implementation for StatsAggregator.
 * aggregateStats prints the length of the keyPrefix to SessionState's out stream
 * All other methods are no-ops.
 */

public class KeyVerifyingStatsAggregator implements StatsAggregator {

  @Override
  public boolean connect(StatsCollectionContext scc) {
    return true;
  }

  @Override
  public String aggregateStats(String keyPrefix, String statType) {
    SessionState ss = SessionState.get();
    // Have to use the length instead of the actual prefix because the prefix is location dependent
    // 17 is 16 (16 byte MD5 hash) + 1 for the path separator
    // Can be less than 17 due to unicode characters
    ss.out.println("Stats prefix is hashed: " + new Boolean(keyPrefix.length() <= 17));
    return null;
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
