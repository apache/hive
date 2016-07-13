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
import org.apache.tez.common.counters.TezCounters;

/**
 * Per query counters.
 */
public class QueryFragmentCounters implements LowLevelCacheCounters {
  private final boolean doUseTimeCounters;

  public static enum Desc {
    MACHINE,
    TABLE,
    FILE,
    STRIPES
  }

  private final AtomicLongArray fixedCounters;
  private final Object[] descs;
  private final TezCounters tezCounters;

  public QueryFragmentCounters(Configuration conf, final TezCounters tezCounters) {
    fixedCounters = new AtomicLongArray(LlapIOCounters.values().length);
    descs = new Object[Desc.values().length];
    doUseTimeCounters = HiveConf.getBoolVar(conf, ConfVars.LLAP_ORC_ENABLE_TIME_COUNTERS);
    this.tezCounters = tezCounters;
    if (!doUseTimeCounters) {
      setCounter(LlapIOCounters.TOTAL_IO_TIME_NS, -1);
      setCounter(LlapIOCounters.DECODE_TIME_NS, -1);
      setCounter(LlapIOCounters.HDFS_TIME_NS, -1);
      setCounter(LlapIOCounters.CONSUMER_TIME_NS, -1);
    }
  }

  public void incrCounter(LlapIOCounters counter) {
    incrCounter(counter, 1);
  }

  public void incrCounter(LlapIOCounters counter, long delta) {
    fixedCounters.addAndGet(counter.ordinal(), delta);
    if (tezCounters != null) {
      tezCounters.findCounter(LlapIOCounters.values()[counter.ordinal()]).increment(delta);
    }
  }

  @Override
  public final long startTimeCounter() {
    return (doUseTimeCounters ? System.nanoTime() : 0);
  }

  public void incrTimeCounter(LlapIOCounters counter, long startTime) {
    if (!doUseTimeCounters) return;
    long delta = System.nanoTime() - startTime;
    fixedCounters.addAndGet(counter.ordinal(), delta);
    if (tezCounters != null) {
      tezCounters.findCounter(LlapIOCounters.values()[counter.ordinal()]).increment(delta);
    }
  }

  public void setCounter(LlapIOCounters counter, long value) {
    fixedCounters.set(counter.ordinal(), value);
    if (tezCounters != null) {
      tezCounters.findCounter(LlapIOCounters.values()[counter.ordinal()]).setValue(value);
    }
  }

  public void setDesc(Desc key, Object desc) {
    descs[key.ordinal()] = desc;
  }

  @Override
  public void recordCacheHit(long bytesHit) {
    incrCounter(LlapIOCounters.CACHE_HIT_BYTES, bytesHit);
  }

  @Override
  public void recordCacheMiss(long bytesMissed) {
    incrCounter(LlapIOCounters.CACHE_MISS_BYTES, bytesMissed);
  }

  @Override
  public void recordAllocBytes(long bytesUsed, long bytesAllocated) {
    incrCounter(LlapIOCounters.ALLOCATED_USED_BYTES, bytesUsed);
    incrCounter(LlapIOCounters.ALLOCATED_BYTES, bytesAllocated);
  }

  @Override
  public void recordHdfsTime(long startTime) {
    incrTimeCounter(LlapIOCounters.HDFS_TIME_NS, startTime);
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
      sb.append(LlapIOCounters.values()[i].name()).append("=").append(fixedCounters.get(i));
    }
    sb.append(" ]");
    return sb.toString();
  }

  public TezCounters getTezCounters() {
    return tezCounters;
  }
}
