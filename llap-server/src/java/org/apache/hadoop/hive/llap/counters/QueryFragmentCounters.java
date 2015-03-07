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
package org.apache.hadoop.hive.llap.counters;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per query counters.
 */
public class QueryFragmentCounters {

  public static enum Counter {
    NUM_VECTOR_BATCHES,
    NUM_DECODED_BATCHES,
    SELECTED_ROWGROUPS,
    NUM_ERRORS,
    ROWS_EMITTED
  }

  private String appId;
  private Map<String, Long> counterMap;

  public QueryFragmentCounters() {
    this("Not Specified");
  }

  public QueryFragmentCounters(String applicationId) {
    this.appId = applicationId;
    this.counterMap = new ConcurrentHashMap<>();
  }

  public void incrCounter(Counter counter) {
    incrCounter(counter, 1);
  }

  public void incrCounter(Counter counter, long delta) {
    if (counterMap.containsKey(counter.name())) {
      long val = counterMap.get(counter.name());
      counterMap.put(counter.name(), val + delta);
    } else {
      setCounter(counter, delta);
    }
  }

  public void setCounter(Counter counter, long value) {
    counterMap.put(counter.name(), value);
  }

  @Override
  public String toString() {
    return "ApplicationId: " + appId + " Counters: " + counterMap;
  }
}
