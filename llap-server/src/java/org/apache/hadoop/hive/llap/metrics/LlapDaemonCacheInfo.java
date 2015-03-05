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
package org.apache.hadoop.hive.llap.metrics;

import org.apache.hadoop.metrics2.MetricsInfo;

import com.google.common.base.Objects;

/**
 * Metrics information for llap cache.
 */
public enum LlapDaemonCacheInfo implements MetricsInfo {
  LLAP_DAEMON_CACHE_METRICS("Llap daemon cache related metrics"),
  CACHE_CAPACITY_REMAINING("Amount of memory available in cache in bytes"),
  CACHE_CAPACITY_TOTAL("Total amount of memory allocated for cache in bytes"),
  CACHE_CAPACITY_USED("Amount of memory used in cache in bytes"),
  CACHE_REQUESTED_BYTES("Disk ranges that are requested in bytes"),
  CACHE_HIT_BYTES("Disk ranges that are cached in bytes"),
  CACHE_HIT_RATIO("Ratio of disk ranges cached vs requested"),
  CACHE_READ_REQUESTS("Number of disk range requests to cache"),
  CACHE_ALLOCATED_ARENA("Number of arenas allocated"),
  CACHE_NUM_LOCKED_BUFFERS("Number of locked buffers in cache");

  private final String desc;

  LlapDaemonCacheInfo(String desc) {
    this.desc = desc;
  }

  @Override
  public String description() {
    return desc;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("name", name()).add("description", desc)
        .toString();
  }
}