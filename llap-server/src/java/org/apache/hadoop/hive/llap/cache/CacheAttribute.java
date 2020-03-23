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

/**
 * Cache attributes Interface to allow sophisticated cache policy add their api without adding memory overhead.
 */
public interface CacheAttribute {
  /**
   * @return buffer priority (actual semantic is up to {@link LowLevelCachePolicy}).
   */
  double getPriority();
  void setPriority(double priority);

  /**
   * @return actual time where the buffer was touched last.
   */
  long getLastUpdate();
  void setTouchTime(long time);

  /**
   * This is used LRFU replacement strategy
   */
  int getIndex();
  void setIndex(int index);
}
