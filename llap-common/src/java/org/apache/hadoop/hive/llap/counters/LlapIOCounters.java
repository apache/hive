/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.counters;

import java.util.ArrayList;
import java.util.List;

/**
 * LLAP IO related counters.
 */
public enum LlapIOCounters {
  NUM_VECTOR_BATCHES(true),
  NUM_DECODED_BATCHES(true),
  SELECTED_ROWGROUPS(true),
  NUM_ERRORS(true),
  ROWS_EMITTED(true),
  METADATA_CACHE_HIT(true),
  METADATA_CACHE_MISS(true),
  CACHE_HIT_BYTES(true),
  CACHE_MISS_BYTES(true),
  ALLOCATED_BYTES(true),
  ALLOCATED_USED_BYTES(true),
  TOTAL_IO_TIME_NS(false),
  DECODE_TIME_NS(false),
  HDFS_TIME_NS(false),
  CONSUMER_TIME_NS(false);

  // flag to indicate if these counters are subject to change across different test runs
  private boolean testSafe;

  LlapIOCounters(final boolean testSafe) {
    this.testSafe = testSafe;
  }

  public static List<String> testSafeCounterNames() {
    List<String> testSafeCounters = new ArrayList<>();
    for (LlapIOCounters counter : values()) {
      if (counter.testSafe) {
        testSafeCounters.add(counter.name());
      }
    }
    return testSafeCounters;
  }
}
