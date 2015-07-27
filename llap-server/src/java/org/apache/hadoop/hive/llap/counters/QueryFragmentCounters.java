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

import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.cache.LowLevelCacheCounters;

/**
 * Per query counters.
 */
public class QueryFragmentCounters implements LowLevelCacheCounters {
  private final boolean doUseTimeCounters;

  public static enum Counter {
    NUM_VECTOR_BATCHES,
    NUM_DECODED_BATCHES,
    SELECTED_ROWGROUPS,
    NUM_ERRORS,
    ROWS_EMITTED,
    METADATA_CACHE_HIT,
    METADATA_CACHE_MISS,
    CACHE_HIT_BYTES,
    CACHE_MISS_BYTES,
    ALLOCATED_BYTES,
    ALLOCATED_USED_BYTES,
    TOTAL_IO_TIME_US,
    DECODE_TIME_US,
    HDFS_TIME_US,
    CONSUMER_TIME_US
  }

  public static enum Desc {
    MACHINE,
    TABLE,
    FILE,
    STRIPES
  }

  private final AtomicLongArray fixedCounters;
  private final Object[] descs;

  public QueryFragmentCounters(Configuration conf) {
    fixedCounters = new AtomicLongArray(Counter.values().length);
    descs = new Object[Desc.values().length];
    doUseTimeCounters = HiveConf.getBoolVar(conf, ConfVars.LLAP_ORC_ENABLE_TIME_COUNTERS);
    if (!doUseTimeCounters) {
      setCounter(Counter.TOTAL_IO_TIME_US, -1);
      setCounter(Counter.DECODE_TIME_US, -1);
      setCounter(Counter.HDFS_TIME_US, -1);
      setCounter(Counter.CONSUMER_TIME_US, -1);
    }
  }

  public void incrCounter(Counter counter) {
    incrCounter(counter, 1);
  }

  public void incrCounter(Counter counter, long delta) {
    fixedCounters.addAndGet(counter.ordinal(), delta);
  }

  @Override
  public final long startTimeCounter() {
    return (doUseTimeCounters ? System.nanoTime() : 0);
  }

  public void incrTimeCounter(Counter counter, long startTime) {
    if (!doUseTimeCounters) return;
    fixedCounters.addAndGet(counter.ordinal(), System.nanoTime() - startTime);
  }

  public void setCounter(Counter counter, long value) {
    fixedCounters.set(counter.ordinal(), value);
  }

  public void setDesc(Desc key, Object desc) {
    descs[key.ordinal()] = desc;
  }

  @Override
  public void recordCacheHit(long bytesHit) {
    incrCounter(Counter.CACHE_HIT_BYTES, bytesHit);
  }

  @Override
  public void recordCacheMiss(long bytesMissed) {
    incrCounter(Counter.CACHE_MISS_BYTES, bytesMissed);
  }

  @Override
  public void recordAllocBytes(long bytesUsed, long bytesAllocated) {
    incrCounter(Counter.ALLOCATED_USED_BYTES, bytesUsed);
    incrCounter(Counter.ALLOCATED_BYTES, bytesAllocated);
  }

  @Override
  public void recordHdfsTime(long startTime) {
    incrTimeCounter(Counter.HDFS_TIME_US, startTime);
  }

  @Override
  public String toString() {
    // We rely on NDC information in the logs to map counters to attempt.
    // If that is not available, appId should either be passed in, or extracted from NDC.
    StringBuilder sb = new StringBuilder("Fragment counters for [");
    for (int i = 0; i < descs.length; ++i) {
      if (i != 0) {
        sb.append(", ");
      }
      if (descs[i] != null) {
        sb.append(descs[i]);
      }
    }
    sb.append("]: [ ");
    for (int i = 0; i < fixedCounters.length(); ++i) {
      if (i != 0) {
        sb.append(", ");
      }
      sb.append(Counter.values()[i].name()).append("=").append(fixedCounters.get(i));
    }
    sb.append(" ]");
    return sb.toString();
  }
}
