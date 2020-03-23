/*
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

package org.apache.hadoop.hive.llap.cache;

public final class LrfuCacheAttribute implements CacheAttribute {
  /** Priority for cache policy (should be pretty universal). */
  public double priority;
  /** Last priority update time for cache policy (should be pretty universal). */
  public long lastUpdate = -1;
  /** Index in heap for LRFU/LFU cache policies. */
  public int indexInHeap = LowLevelLrfuCachePolicy.NOT_IN_CACHE;

  public LrfuCacheAttribute(double priority, long lastUpdate) {
    this.priority = priority;
    this.lastUpdate = lastUpdate;
  }

  @Override public String toString() {
    return "LrfuCacheAttribute{"
        + "priority="
        + priority
        + ", lastUpdate="
        + lastUpdate
        + ", indexInHeap="
        + indexInHeap
        + '}';
  }

  @Override public double getPriority() {
    return priority;
  }

  @Override public void setPriority(double priority) {
    this.priority = priority;
  }

  @Override public long getLastUpdate() {
    return lastUpdate;
  }

  @Override public void setTouchTime(long time) {
    lastUpdate = time;
  }

  @Override public int getIndex() {
    return indexInHeap;
  }

  @Override public void setIndex(int index) {
    indexInHeap = index;
  }
}
