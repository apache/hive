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
package org.apache.hadoop.hive.llap.cache;

import javax.management.MXBean;

/**
 * MXbean to expose cache allocator related information through JMX.
 */
@MXBean
public interface BuddyAllocatorMXBean {

  /**
   * Gets if bytebuffers are allocated directly offheap.
   *
   * @return gets if direct bytebuffer allocation
   */
  public boolean getIsDirect();

  /**
   * Gets minimum allocation size of allocator.
   *
   * @return minimum allocation size
   */
  public int getMinAllocation();

  /**
   * Gets maximum allocation size of allocator.
   *
   * @return maximum allocation size
   */
  public int getMaxAllocation();

  /**
   * Gets the arena size.
   *
   * @return arena size
   */
  public int getArenaSize();

  /**
   * Gets the maximum cache size.
   *
   * @return max cache size
   */
  public long getMaxCacheSize();
}